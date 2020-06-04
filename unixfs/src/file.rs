use crate::pb::{UnixFs, UnixFsReadFailed, UnixFsType};
use std::borrow::Cow;
use std::fmt;

pub mod reader;
pub mod visit;

/// Container for the unixfs metadata, which can be present at the root of the file trees.
#[derive(Debug, Default)]
pub struct FileMetadata {
    mode: Option<u32>,
    mtime: Option<(i64, u32)>,
}

// TODO: add way to get std::fs::Permissions out of this, or maybe some libc like type?
impl FileMetadata {
    /// Returns the full file mode, if one has been specified.
    ///
    /// The full file mode is originally read through `st_mode` field of `stat` struct defined in
    /// `sys/stat.h` and it's defining OpenGroup standard. Lowest 3 bytes will correspond to read,
    /// write, and execute rights per user, group, and other and 4th byte determines sticky bits,
    /// set user id or set group id. Following two bytes correspond to the different file types, as
    /// defined by the same OpenGroup standard:
    /// https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/sys_stat.h.html
    pub fn mode(&self) -> Option<u32> {
        self.mode
    }

    /// Returns the raw timestamp of last modification time, if specified.
    ///
    /// The timestamp is `(seconds, nanos)` similar to `std::time::Duration` with the exception of
    /// allowing seconds to be negative. The seconds are calculated from `1970-01-01 00:00:00` or
    /// the common "unix epoch".
    pub fn mtime(&self) -> Option<(i64, u32)> {
        self.mtime
    }
}

impl<'a> From<&'a UnixFs<'_>> for FileMetadata {
    fn from(data: &'a UnixFs<'_>) -> Self {
        let mode = data.mode;
        let mtime = data
            .mtime
            .clone()
            .map(|ut| (ut.Seconds, ut.FractionalNanoseconds.unwrap_or(0)));

        FileMetadata { mode, mtime }
    }
}

/// Describes the errors which can happen during a visit or lower level block-by-block walking of
/// the DAG.
#[derive(Debug)]
pub enum FileReadFailed {
    /// Unsupported UnixFs file; these might exist, but currently there are no workarounds for
    /// handling them.
    File(FileError),
    /// FileReader can only process raw or file type of unixfs content.
    // This is the raw value instead of the enum by design not to expose the quick-protobuf types
    UnexpectedType(i32),
    /// Parsing failed
    Read(UnixFsReadFailed),
    LinkInvalidCid {
        nth: usize,
        hash: Vec<u8>,
        name: Cow<'static, str>,
        cause: cid::Error,
    },
}

impl fmt::Display for FileReadFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FileReadFailed::*;

        match self {
            File(e) => write!(fmt, "{}", e),
            UnexpectedType(t) => write!(
                fmt,
                "unexpected type for UnixFs: {} or {:?}",
                t,
                UnixFsType::from(*t)
            ),
            Read(e) => write!(fmt, "reading failed: {}", e),
            LinkInvalidCid {
                nth, name, cause, ..
            } => write!(
                fmt,
                "failed to convert link #{} ({:?}) to Cid: {}",
                nth, name, cause
            ),
        }
    }
}

impl std::error::Error for FileReadFailed {}

impl From<UnixFsReadFailed> for FileReadFailed {
    fn from(e: UnixFsReadFailed) -> Self {
        FileReadFailed::Read(e)
    }
}

/// Errors which can happen while processing UnixFS type File or Raw blocks.
#[derive(Debug)]
pub enum FileError {
    /// There are nonequal number of links and blocksizes and thus the file ranges for linked trees
    /// or blocks cannot be determined.
    LinksAndBlocksizesMismatch,
    /// Errored when the filesize is non-zero.
    NoLinksNoContent,
    /// Unsupported: non-root block defines metadata.
    NonRootDefinesMetadata(FileMetadata),
    /// A non-leaf node in the tree has no filesize value which is used to determine the file range
    /// for this tree.
    IntermediateNodeWithoutFileSize,
    /// The tree or merkle dag should only collapse or stay the same length.
    TreeExpandsOnLinks,
    /// The tree links contain overlapping file segments. This is at least unsupported right now,
    /// but the larger segment could be collapsed down to the reused part.
    TreeOverlapsBetweenLinks,
    /// Reader has been fed a link to earlier range.
    EarlierLink,
    /// The tree links contain a hole from a file segment to the next tree. This is at least
    /// unsupported right now. Zeroes could be generated for the hole.
    TreeJumpsBetweenLinks,
    /// These values should not be present for unixfs files with File or Raw. If they have a valid
    /// meaning, support for such has not been implemented.
    UnexpectedRawOrFileProperties {
        hash_type: Option<u64>,
        fanout: Option<u64>,
    },
}

impl fmt::Display for FileError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FileError::*;
        match self {
            LinksAndBlocksizesMismatch => write!(
                fmt,
                "different number of links and blocksizes: cannot determine subtree ranges"
            ),
            NoLinksNoContent => write!(
                fmt,
                "filesize is non-zero while there are no links or content"
            ),
            NonRootDefinesMetadata(metadata) => {
                write!(fmt, "unsupported: non-root defines {:?}", metadata)
            }
            IntermediateNodeWithoutFileSize => {
                write!(fmt, "intermediatery node with links but no filesize")
            }
            TreeExpandsOnLinks => write!(
                fmt,
                "total size of tree expands through links, it should only get smaller or keep size"
            ),
            TreeOverlapsBetweenLinks => write!(fmt, "unsupported: tree contains overlap"),
            EarlierLink => write!(fmt, "error: earlier link given"),
            TreeJumpsBetweenLinks => write!(fmt, "unsupported: tree contains holes"),
            UnexpectedRawOrFileProperties { hash_type, fanout } => write!(
                fmt,
                "unsupported: File or Raw with hash_type {:?} or fanount {:?}",
                hash_type, fanout
            ),
        }
    }
}

impl std::error::Error for FileError {}

impl From<FileError> for FileReadFailed {
    fn from(e: FileError) -> Self {
        Self::File(e)
    }
}

/// This exists to help matching the borrowed slice in `Option<Cow<'_, [u8]>>` in this file
/// or defaulting to empty array. In the processing inside this file, the Cow never represents
/// owned value.
///
/// This at least sounded useful early on as the quick-protobuf produces many Option<Cow> values
/// which are a bit tricky to handle. We never turn them into Option<Cow::Owned> so we can safely
/// use these.
pub(crate) trait UnwrapBorrowedExt<'a> {
    /// Does not default but requires there to be an borrowed inner value.
    fn unwrap_borrowed(self) -> &'a [u8];

    /// Unwraps the Cow of [u8] into empty or wrapped slice.
    fn unwrap_borrowed_or_empty(self) -> &'a [u8]
    where
        Self: 'a;
}

impl<'a> UnwrapBorrowedExt<'a> for Option<Cow<'a, [u8]>> {
    fn unwrap_borrowed(self) -> &'a [u8] {
        match self {
            Some(Cow::Borrowed(x)) => x,
            Some(Cow::Owned(_)) => panic!("unexpected Cow::Owned"),
            None => panic!("Unexpected None"),
        }
    }
    fn unwrap_borrowed_or_empty(self) -> &'a [u8] {
        match self {
            Some(Cow::Borrowed(x)) => x,
            None => &[][..],
            _ => panic!("should not be Cow::Owned"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{reader::*, visit::*, UnwrapBorrowedExt};
    use hex_literal::hex;
    use sha2::Digest;
    use std::convert::TryFrom;
    use std::fmt;
    use std::io::Read;
    use std::time::Instant;

    const CONTENT_FILE: &'static [u8] = &[
        0x0a, 0x0d, 0x08, 0x02, 0x12, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x18, 0x07,
    ];

    #[test]
    fn just_content() {
        let fr = FileReader::from_block(CONTENT_FILE).unwrap();

        match fr.content() {
            (FileContent::Just(b"content"), _) => {}
            x => panic!("unexpected: {:?}", x),
        }
    }

    #[test]
    fn empty_file() {
        let block = &[0x0a, 0x04, 0x08, 0x02, 0x18, 0x00];
        let fr = FileReader::from_block(block).unwrap();
        assert_eq!(fr.content().0.unwrap_content(), &[][..]);
    }

    #[test]
    fn traversal() {
        let mut blocks = vec![
            // root for "foobar\n" from go-ipfs 0.5 add -s size-2
            //     root
            //      |
            //  ----+-----
            //  |  |  |  |
            // fo ob ar  \n
            vec![
                0x12, 0x28, 0x0a, 0x22, 0x12, 0x20, 0xfe, 0xf9, 0xfe, 0x18, 0x04, 0x94, 0x2b, 0x35,
                0xe1, 0x9e, 0x14, 0x5a, 0x03, 0xf9, 0xc9, 0xd5, 0xca, 0x9c, 0x99, 0x7d, 0xda, 0x0a,
                0x94, 0x16, 0xf3, 0xf5, 0x15, 0xa5, 0x2f, 0x1b, 0x3c, 0xe1, 0x12, 0x00, 0x18, 0x0a,
                0x12, 0x28, 0x0a, 0x22, 0x12, 0x20, 0xdf, 0xb9, 0x4b, 0x75, 0xac, 0xb2, 0x08, 0xfd,
                0x48, 0x73, 0xd8, 0x48, 0x72, 0xaf, 0x58, 0xbd, 0x65, 0xc7, 0x31, 0x77, 0x0a, 0x7d,
                0x4c, 0x0d, 0xee, 0xb4, 0x08, 0x8e, 0x87, 0x39, 0x0b, 0xfe, 0x12, 0x00, 0x18, 0x0a,
                0x12, 0x28, 0x0a, 0x22, 0x12, 0x20, 0x05, 0x44, 0x97, 0xae, 0x4e, 0x89, 0x81, 0x2c,
                0x83, 0x27, 0x6a, 0x48, 0xe3, 0xe6, 0x79, 0x01, 0x3a, 0x78, 0x8b, 0x7c, 0x0e, 0xb0,
                0x27, 0x12, 0xdf, 0x15, 0x09, 0x5c, 0x02, 0xd6, 0xcd, 0x2c, 0x12, 0x00, 0x18, 0x0a,
                0x12, 0x28, 0x0a, 0x22, 0x12, 0x20, 0xcc, 0x33, 0x2c, 0xeb, 0x37, 0xde, 0xa7, 0xd3,
                0xd7, 0xc0, 0x0d, 0x13, 0x93, 0x11, 0x76, 0x38, 0xd3, 0xed, 0x96, 0x35, 0x75, 0x83,
                0x6c, 0x6d, 0x44, 0xa2, 0x49, 0x51, 0xe4, 0x44, 0xcf, 0x5d, 0x12, 0x00, 0x18, 0x09,
                0x0a, 0x0c, 0x08, 0x02, 0x18, 0x07, 0x20, 0x02, 0x20, 0x02, 0x20, 0x02, 0x20, 0x01,
            ],
            // first actual bytes: fo
            vec![0x0a, 0x08, 0x08, 0x02, 0x12, 0x02, 0x66, 0x6f, 0x18, 0x02],
            // ob
            vec![0x0a, 0x08, 0x08, 0x02, 0x12, 0x02, 0x6f, 0x62, 0x18, 0x02],
            // ar
            vec![0x0a, 0x08, 0x08, 0x02, 0x12, 0x02, 0x61, 0x72, 0x18, 0x02],
            // \n
            vec![0x0a, 0x07, 0x08, 0x02, 0x12, 0x01, 0x0a, 0x18, 0x01],
        ];

        // now we can just pop the blocks off, they will be in order of
        blocks.reverse();

        // something 'static to hold on between two blocks
        let mut state: Option<Traversal>;

        // filled on root
        let mut offsets = {
            let root = blocks.pop().unwrap();
            let root = FileReader::from_block(&root).unwrap();

            let (mut offsets, traversal) = match root.content() {
                (FileContent::Spread(iter), traversal) => {
                    (iter.map(|(_, range)| range).collect::<Vec<_>>(), traversal)
                }
                x => unreachable!("unexpected {:?}", x),
            };

            // reverse again to pop again
            offsets.reverse();
            state = Some(traversal);
            offsets
        };

        let mut combined: Vec<u8> = Vec::new();

        while let Some(next) = blocks.pop() {
            let fr = state
                .take()
                .map(|traversal| traversal.continue_walk(&next, &offsets.pop().unwrap()))
                .unwrap() //.unwrap_or_else(|| FileReader::from_block(&next))
                .unwrap();

            let (content, traversal) = fr.content();
            combined.extend(content.unwrap_content());
            state = Some(traversal);
        }

        assert_eq!(combined, b"foobar\n");
    }

    #[test]
    fn traversal_from_blockstore() {
        let started_at = Instant::now();

        // this depends on go-ipfs 0.5 flatfs blockstore and linux-5.6.14.tar.xz imported
        let blocks = FakeShardedBlockstore {
            root: PathBuf::from("/home/joonas/Programs/go-ipfs/ipfs_home/blocks"),
        };

        //let start = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4"; // balanced
        let start = "Qmf1crhdrQEsVUjvmnSF3Q5PHc825MaHZ5cPhtVS2eJ1p4"; // trickle
        let start = cid::Cid::try_from(start).unwrap().to_bytes();

        let mut hasher = sha2::Sha256::new();

        let mut work_hwm = 0;
        let mut buf_hwm = 0;
        let mut bytes = (0, 0);

        let mut work = Vec::new();
        work.push((start, None, None, None));

        let mut state: Option<Traversal> = None;
        let mut block_buffer = Vec::new();

        // This is used to do "size validation" for the tree.
        let mut tree_sizes: Vec<u64> = Vec::new();

        while let Some((key, size, mut range, size_index)) = work.pop() {
            // println!("{:?}", RLE(tree_sizes.as_slice()));

            block_buffer.clear();

            blocks
                .as_file(&key)
                .unwrap()
                .read_to_end(&mut block_buffer)
                .unwrap();

            buf_hwm = buf_hwm.max(block_buffer.len());

            if let Some(size) = size {
                // the size on PBLink is the size of the subtree, so it's a bit tricky to validate
                // I guess it could be given as nested items to further "scope it down". this is
                // per tree.
                //
                // with generational indices the zeroes could be removed here and be done with it

                let size = size as u64;
                let index = size_index.unwrap();
                assert!(
                    tree_sizes[index] >= size,
                    "tree size exhausted, failed: {} >= {}",
                    tree_sizes[index],
                    size
                );
                tree_sizes[index] -= size;

                let mut index = index;

                // this sort of works by isn't actually generational indices which would be safer.
                // the indexing still does work ok as this "cleaning" routine is done only after
                // completly processing the subtree.

                while tree_sizes[index] == 0 && index + 1 == tree_sizes.len() {
                    tree_sizes.pop();
                    if index > 0 {
                        index -= 1;
                    } else {
                        break;
                    }
                }
            }

            let slice = &block_buffer[..];

            bytes.0 += slice.len();

            let reader = match state.take() {
                Some(t) => t.continue_walk(slice, &range.take().unwrap()),
                None => FileReader::from_block(slice),
            };

            let reader = reader.unwrap_or_else(|e| {
                panic!("failed to start or continue from {:02x?}: {:?}", key, e)
            });

            //key.clear();
            //recycler.push(key);

            let (content, traversal) = reader.content();

            state = Some(traversal);

            match content {
                FileContent::Just(content) => {
                    bytes.1 += content.len();
                    hasher.input(content);
                }
                FileContent::Spread(iter) => {
                    let mapped = iter.map(|(link, range)| {
                        assert_eq!(link.Name.as_deref(), Some(""));

                        let hash = link.Hash.unwrap_borrowed().to_vec();
                        let size = link.Tsize.unwrap_or(0);
                        let index = tree_sizes.len();
                        tree_sizes.push(size);

                        (hash, Some(size), Some(range), Some(index))
                    });

                    let before = work.len();
                    work.extend(mapped);

                    // not using a vecdeque does make this a bit more difficult to read but the DFS
                    // order is going to the first child first, which needs to be the last entry in
                    // vec when using pop
                    (&mut work[before..]).reverse();
                }
            }

            work_hwm = work_hwm.max(work.len());
        }

        let elapsed = started_at.elapsed();
        println!("{:?}", elapsed);
        println!("{:?}", bytes);

        assert!(
            tree_sizes.iter().all(|&x| x == 0),
            "some trees were not exhausted: {:?}",
            tree_sizes
        );

        let result = hasher.result();

        assert_eq!(
            &result[..],
            hex!("33763f3541711e39fa743da45ff9512d54ade61406173f3d267ba4484cec7ea3")
        );
        assert_eq!(work_hwm, 176);
        assert_eq!(buf_hwm, 262158);
    }

    #[derive(Default)]
    struct DigestVisit(sha2::Sha256, usize);

    impl Visitor for DigestVisit {
        fn visit_content(&mut self, content: &[u8]) {
            self.0.input(content);
            self.1 += content.len();
        }
    }

    // This has been used as a poor mans benchmark, taking ~430ms to process the ~110MB in release
    // build.
    #[test]
    fn visitor_traversal_from_blockstore() {
        let started_at = Instant::now();

        // this depends on go-ipfs 0.5 flatfs blockstore and linux-5.6.14.tar.xz imported
        let blocks = FakeShardedBlockstore {
            root: PathBuf::from("/home/joonas/Programs/go-ipfs/ipfs_home/blocks"),
        };

        let start = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        let start = cid::Cid::try_from(start).unwrap().to_bytes();

        let visit = IdleFileVisit::new(DigestVisit::default());

        let mut block_buffer = Vec::new();
        blocks
            .as_file(&start)
            .unwrap()
            .read_to_end(&mut block_buffer)
            .unwrap();

        let mut visit = visit.start(&block_buffer[..]).unwrap().1.unwrap_continued();

        loop {
            let key = visit.pending_links().next().unwrap();

            block_buffer.clear();
            blocks
                .as_file(&key.to_bytes())
                .unwrap()
                .read_to_end(&mut block_buffer)
                .unwrap();

            match visit.continue_walk(&block_buffer[..]).unwrap().1 {
                Visitation::Completed(DigestVisit(sha, bytes)) => {
                    let result = sha.result();
                    let elapsed = started_at.elapsed();
                    println!("{:?}", elapsed);

                    assert_eq!(
                        &result[..],
                        hex!("33763f3541711e39fa743da45ff9512d54ade61406173f3d267ba4484cec7ea3")
                    );
                    assert_eq!(bytes, 111_812_744);
                    break;
                }
                Visitation::Continues(next) => visit = next,
            }
        }
    }

    #[test]
    fn scoped_visitor_traversal_from_blockstore() {
        let started_at = Instant::now();

        // this depends on go-ipfs 0.5 flatfs blockstore and linux-5.6.14.tar.xz imported
        let blocks = FakeShardedBlockstore {
            root: PathBuf::from("/home/joonas/Programs/go-ipfs/ipfs_home/blocks"),
        };

        let start = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        let start = cid::Cid::try_from(start).unwrap().to_bytes();

        let visit = IdleFileVisit::new(DigestVisit::default())
            .with_target_range(500_000..(500_000 + 300_000));

        let mut block_buffer = Vec::new();
        blocks
            .as_file(&start)
            .unwrap()
            .read_to_end(&mut block_buffer)
            .unwrap();

        let mut visit = visit.start(&block_buffer[..]).unwrap().1.unwrap_continued();

        loop {
            let key = visit.pending_links().next().unwrap();

            block_buffer.clear();
            blocks
                .as_file(&key.to_bytes())
                .unwrap()
                .read_to_end(&mut block_buffer)
                .unwrap();

            match visit.continue_walk(&block_buffer[..]).unwrap().1 {
                Visitation::Completed(DigestVisit(sha, bytes)) => {
                    let result = sha.result();
                    let elapsed = started_at.elapsed();
                    println!("{:?}", elapsed);

                    assert_eq!(
                        &result[..],
                        hex!("6369d42caf9966c5c7c1796b9c99248c0a8fbf506a690fcaefc478d4a97b3683")
                    );
                    assert_eq!(bytes, 300_000);
                    break;
                }
                Visitation::Continues(next) => visit = next,
            }
        }
    }

    #[test]
    fn less_than_block_scoped_traversal_from_blockstore() {
        let started_at = Instant::now();

        // this depends on go-ipfs 0.5 flatfs blockstore and linux-5.6.14.tar.xz imported
        let blocks = FakeShardedBlockstore {
            root: PathBuf::from("/home/joonas/Programs/go-ipfs/ipfs_home/blocks"),
        };

        let start = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        let start = cid::Cid::try_from(start).unwrap().to_bytes();

        let mut block_buffer = Vec::new();
        blocks
            .as_file(&start)
            .unwrap()
            .read_to_end(&mut block_buffer)
            .unwrap();

        let visit =
            IdleFileVisit::new(DigestVisit::default()).with_target_range(500_000..(500_000 + 32));

        let mut visit = match visit.start(&block_buffer[..]).unwrap().1 {
            Visitation::Continues(x) => x,
            _ => unreachable!(),
        };

        loop {
            let key = visit.pending_links().next().unwrap();

            block_buffer.clear();
            blocks
                .as_file(&key.to_bytes())
                .unwrap()
                .read_to_end(&mut block_buffer)
                .unwrap();

            match visit.continue_walk(&block_buffer[..]).unwrap().1 {
                Visitation::Completed(DigestVisit(sha, bytes)) => {
                    let result = sha.result();
                    let elapsed = started_at.elapsed();
                    println!("{:?}", elapsed);

                    assert_eq!(
                        &result[..],
                        hex!("62ab21fbe03d0ab6be3938740272bf0b70b516f15901e8ee51ecffb71dfe9e2b")
                    );
                    assert_eq!(bytes, 32);
                    break;
                }
                Visitation::Continues(next) => visit = next,
            }
        }
    }

    #[test]
    fn scoped_traversal_out_of_bounds_from_blockstore() {
        let started_at = Instant::now();

        // this depends on go-ipfs 0.5 flatfs blockstore and linux-5.6.14.tar.xz imported
        let blocks = FakeShardedBlockstore {
            root: PathBuf::from("/home/joonas/Programs/go-ipfs/ipfs_home/blocks"),
        };

        let start = "QmTEn8ypAkbJXZUXCRHBorwF2jM8uTUW9yRLzrcQouSoD4";
        let start = cid::Cid::try_from(start).unwrap().to_bytes();

        let mut block_buffer = Vec::new();
        blocks
            .as_file(&start)
            .unwrap()
            .read_to_end(&mut block_buffer)
            .unwrap();

        let visit = IdleFileVisit::new(DigestVisit::default())
            .with_target_range(500_000_000..(500_000_000 + 32));

        let DigestVisit(sha, bytes) = visit
            .start(&block_buffer[..])
            .unwrap()
            .1
            .unwrap_completion();

        let result = sha.result();

        let elapsed = started_at.elapsed();
        println!("{:?}", elapsed);

        // this is the empty sha256 hash
        assert_eq!(
            &result[..],
            hex!("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        assert_eq!(bytes, 0);
    }

    use std::path::PathBuf;

    struct FakeShardedBlockstore {
        root: PathBuf,
    }

    impl FakeShardedBlockstore {
        fn as_path(&self, key: &[u8]) -> PathBuf {
            let encoded = multibase::Base::Base32Upper.encode(key);
            let len = encoded.len();

            // this is safe because base32 is ascii
            let dir = &encoded[(len - 3)..(len - 1)];
            assert_eq!(dir.len(), 2);

            let mut path = self.root.clone();
            path.push(dir);
            path.push(encoded);
            path.set_extension("data");
            path
        }

        fn as_file(&self, key: &[u8]) -> std::io::Result<std::fs::File> {
            // assume that we have a block store with second-to-last/2 sharding
            // files in Base32Upper

            let path = self.as_path(key);
            //println!("{} -> {:?}", cid::Cid::try_from(key).unwrap(), path);

            std::fs::OpenOptions::new().read(true).open(path)
        }
    }

    /// Debug wrapper for a slice which is expected to have a lot of the same numbers, like an
    /// dense storage for merkledag size validation, in which case T = u64.
    struct RLE<'a, T: fmt::Display + PartialEq>(&'a [T]);

    impl<'a, T: fmt::Display + PartialEq> fmt::Debug for RLE<'_, T> {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            let total = self.0.len();

            write!(fmt, "{{ total: {}, rle: [", total)?;

            let mut last = None;
            let mut count = 0;

            for c in self.0 {
                match last {
                    Some(x) if x == c => count += 1,
                    Some(x) => {
                        if count > 1 {
                            write!(fmt, "{} x {}, ", count, x)?;
                        } else {
                            write!(fmt, "{}, ", x)?;
                        }
                        last = Some(c);
                        count = 1;
                    }
                    None => {
                        last = Some(c);
                        count = 1;
                    }
                }
            }

            if let Some(x) = last {
                if count > 1 {
                    write!(fmt, "{} x {}, ", count, x)?;
                } else {
                    write!(fmt, "{}, ", x)?;
                }
            }

            write!(fmt, "] }}")
        }
    }
}
