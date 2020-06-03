use crate::pb::{FlatUnixFs, PBLink, RangeLinks, UnixFsType};
use std::convert::TryFrom;
use std::fmt;
use std::ops::Range;

use crate::file::{FileError, FileMetadata, FileReadFailed, UnwrapBorrowedExt};

/// Navigates the UnixFs files, which are either:
///  - single block files which have everything needed to all of the contents
///  - multi block files which have trees of trees until Raw leaf blocks
///
/// The trees can have different shapes but it doesn't really matter for our depth-first approach.
/// For seeking, the each sub-tree linking node will have blocksizes for the trees representing
/// which the original file offsets covered by the tree.
///
/// A file doesn't know it's name. It only has a name when part of a directory, and then the name
/// is on a PbLink::Name. With UnixFs the names are always UTF-8. The root CID is not interesting
/// either: we just need the root block.
pub struct FileReader<'a> {
    offset: u64,
    end: Ending,
    had_links: bool,
    links: Vec<PBLink<'a>>,
    data: &'a [u8],
    blocksizes: Vec<u64>,
    metadata: FileMetadata,
}

impl AsRef<FileMetadata> for FileReader<'_> {
    fn as_ref(&self) -> &FileMetadata {
        &self.metadata
    }
}

#[derive(Debug)]
enum Ending {
    /// The block represented a subtree without actual content
    TreeCoverage(u64),
    /// The block repressented a leaf with actual content
    Chunk(u64),
}

impl Ending {
    fn check_is_suitable_next(&self, next: &Range<u64>) -> Result<(), FileReadFailed> {
        match self {
            Ending::TreeCoverage(cover_end) if &next.start < cover_end => {
                if &next.end > cover_end {
                    // tree must be collapsing; we cant have root be some smaller *file* range than
                    // the child
                    Err(FileError::TreeExpandsOnLinks.into())
                } else {
                    Ok(())
                }
            }
            Ending::TreeCoverage(cover_end) => {
                if &next.start <= cover_end {
                    // when moving to sibling at the same high or above, it's coverage must start
                    // from where we stopped
                    Err(FileError::TreeOverlapsBetweenLinks.into())
                } else {
                    Ok(())
                }
            }
            Ending::Chunk(chunk_end) => {
                if &next.start != chunk_end {
                    // when continuing on from leaf node to either tree at above or a chunk at
                    // next, the next must continue where we stopped
                    Err(FileError::TreeJumpsBetweenLinks.into())
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl<'a> FileReader<'a> {
    /// Method for starting the file traversal. `data` is the raw data from unixfs block.
    pub fn from_block(data: &'a [u8]) -> Result<Self, FileReadFailed> {
        let inner = FlatUnixFs::try_from(data)?;
        let metadata = FileMetadata::from(&inner.data);
        Self::from_parts(inner, 0, metadata)
    }

    /// Called by Traversal to continue traversing a file tree traversal.
    fn from_continued(
        traversal: Traversal,
        offset: u64,
        data: &'a [u8],
    ) -> Result<Self, FileReadFailed> {
        let inner = FlatUnixFs::try_from(data)?;

        if inner.data.mode.is_some() || inner.data.mtime.is_some() {
            let metadata = FileMetadata::from(&inner.data);
            return Err(FileError::NonRootDefinesMetadata(metadata))?;
        }

        Self::from_parts(inner, offset, traversal.metadata)
    }

    fn from_parts(
        inner: FlatUnixFs<'a>,
        offset: u64,
        metadata: FileMetadata,
    ) -> Result<Self, FileReadFailed> {
        let empty_or_no_content = inner
            .data
            .Data
            .as_ref()
            .map(|cow| cow.as_ref().is_empty())
            .unwrap_or(true);
        let is_zero_bytes = inner.data.filesize.unwrap_or(0) == 0;

        if inner.data.Type != UnixFsType::File && inner.data.Type != UnixFsType::Raw {
            Err(FileReadFailed::UnexpectedType(inner.data.Type.into()))
        } else if inner.links.len() != inner.data.blocksizes.len() {
            Err(FileReadFailed::File(FileError::LinksAndBlocksizesMismatch))
        } else if empty_or_no_content && !is_zero_bytes && inner.links.is_empty() {
            Err(FileReadFailed::File(FileError::NoLinksNoContent))
        } else {
            // raw and file seem to be same except the raw is preferred in trickle dag
            let data = inner.data.Data.unwrap_borrowed_or_empty();

            if inner.data.hashType.is_some() || inner.data.fanout.is_some() {
                return Err(FileError::UnexpectedRawOrFileProperties {
                    hash_type: inner.data.hashType,
                    fanout: inner.data.fanout,
                }
                .into());
            }

            let end = if inner.links.is_empty() {
                // can unwrap because `data` is all of the data
                let filesize = inner.data.filesize.unwrap_or(data.len() as u64);
                Ending::Chunk(offset + filesize)
            } else {
                match inner.data.filesize {
                    Some(filesize) => Ending::TreeCoverage(offset + filesize),
                    None => return Err(FileError::IntermediateNodeWithoutFileSize.into()),
                }
            };

            Ok(Self {
                offset,
                end,
                had_links: !inner.links.is_empty(),
                links: inner.links,
                data,
                blocksizes: inner.data.blocksizes,
                metadata,
            })
        }
    }

    pub fn content(
        self,
    ) -> (
        FileContent<'a, impl Iterator<Item = (PBLink<'a>, Range<u64>)>>,
        Traversal,
    ) {
        let traversal = Traversal {
            last_had_links: !self.had_links,
            last_ending: self.end,
            last_offset: self.offset,

            metadata: self.metadata,
        };

        if self.links.is_empty() {
            (FileContent::Just(self.data), traversal)
        } else {
            let zipped = self.links.into_iter().zip(self.blocksizes.into_iter());
            (
                FileContent::Spread(RangeLinks::from_links_and_blocksizes(
                    zipped,
                    Some(self.offset),
                )),
                traversal,
            )
        }
    }
}

#[derive(Debug)]
pub struct Traversal {
    last_had_links: bool,
    last_ending: Ending,
    last_offset: u64,

    metadata: FileMetadata,
}

impl Traversal {
    pub fn continue_walk<'a>(
        self,
        next_block: &'a [u8],
        tree_range: &Range<u64>,
    ) -> Result<FileReader<'a>, FileReadFailed> {
        self.last_ending.check_is_suitable_next(&tree_range)?;

        // Hitting this assert would be a logic error on part of the traversal link processor. This
        // does not guard against processing the same block multiple times, which can be tough one
        // to debug.
        assert!(
            self.last_offset <= tree_range.start,
            "We can go down or forward but not backwards, failed: {} <= {} with {:?}",
            self.last_offset,
            tree_range.start,
            tree_range
        );

        FileReader::from_continued(self, tree_range.start, next_block)
    }
}

impl AsRef<FileMetadata> for Traversal {
    fn as_ref(&self) -> &FileMetadata {
        &self.metadata
    }
}

pub enum FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)> + 'a,
{
    /// When reaching the leaf level of a DAG we finally find the actual content. For empty files
    /// without content this will be an empty slice.
    Just(&'a [u8]),
    /// The content of the file is spread over a number of blocks; iteration must follow from index
    /// depth-first from the first link to reach the given the bytes in the given byte offset
    /// range.
    Spread(I),
}

impl<'a, I> FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)>,
{
    pub fn unwrap_content(self) -> &'a [u8] {
        match self {
            FileContent::Just(x) => x,
            y => panic!("Expected FileContent::Just, found: {:?}", y),
        }
    }
}

impl<'a, I> fmt::Debug for FileContent<'a, I>
where
    I: Iterator<Item = (PBLink<'a>, Range<u64>)>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileContent::Just(bytes) => write!(fmt, "Just({} bytes)", bytes.len()),
            FileContent::Spread(_) => write!(fmt, "Spread(...)"),
        }
    }
}
