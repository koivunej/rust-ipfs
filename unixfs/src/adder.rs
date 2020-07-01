use cid::Cid;
use std::path::Path;

use crate::pb::{FlatUnixFs, PBLink, UnixFs, UnixFsType};
use quick_protobuf::{MessageWrite, Writer};
use std::borrow::Cow;
use std::io::Write;
use std::num::NonZeroUsize;

use sha2::{Digest, Sha256};

#[derive(Default)]
struct Adder;

impl Adder {
    fn push(&mut self, path: impl AsRef<Path>, cid: Cid) -> Result<(), ()> {
        todo!("should this be able to return block?")
    }

    fn finish(self) -> Vec<Vec<u8>> {
        todo!("return all directories blocks")
    }
}

#[derive(Default)]
struct FileAdder {
    chunker: Chunker,
    block_buffer: Vec<u8>,
    unflushed_links: Vec<(Cid, u64, u64)>,
    total_blocks: usize,
    filesize: u64,
}

impl FileAdder {
    fn with_chunker(chunker: Chunker) -> Self {
        FileAdder {
            chunker,
            ..Default::default()
        }
    }

    fn push(&mut self, input: &[u8]) -> Result<(impl Iterator<Item = (Cid, Vec<u8>)>, usize), ()> {
        // case 0: full chunk is not ready => empty iterator, full read
        // case 1: full chunk becomes ready, maybe short read => at least one block
        //     1a: not enough links => iterator of one
        //     1b: link block is ready => iterator of two blocks

        let (accepted, ready) = self.chunker.accept(input, &self.block_buffer);
        self.block_buffer.extend_from_slice(accepted);
        let written = accepted.len();

        let (leaf, links) = if !ready {
            (None, None)
        } else {
            let leaf = Some(self.flush_buffered_leaf().unwrap());

            let links = self.flush_buffered_links(NonZeroUsize::new(174).unwrap());

            (leaf, links)
        };

        Ok((leaf.into_iter().chain(links.into_iter()), written))
    }

    fn finish(mut self) -> impl Iterator<Item = (Cid, Vec<u8>)> {
        let last_leaf = self.flush_buffered_leaf();
        let root_links = self.flush_buffered_links(NonZeroUsize::new(1).unwrap());
        // should probably error if there is neither?
        last_leaf.into_iter().chain(root_links.into_iter())
    }

    fn flush_buffered_leaf(&mut self) -> Option<(Cid, Vec<u8>)> {
        if !self.block_buffer.is_empty() {
            let bytes = self.block_buffer.len();

            let inner = FlatUnixFs {
                links: Vec::new(),
                data: UnixFs {
                    Type: UnixFsType::File,
                    Data: Some(Cow::Borrowed(self.block_buffer.as_slice())),
                    filesize: Some(self.block_buffer.len() as u64),
                    // no blocksizes as there are no links
                    blocksizes: Vec::new(),
                    hashType: None,
                    fanout: None,
                    mode: None,
                    mtime: None,
                },
            };

            let (cid, vec) = render_and_hash(inner);

            let total_size = vec.len();

            self.block_buffer.clear();
            self.unflushed_links
                .push((cid.clone(), total_size as u64, bytes as u64));

            Some((cid, vec))
        } else {
            None
        }
    }

    fn flush_buffered_links(&mut self, min_links: NonZeroUsize) -> Option<(Cid, Vec<u8>)> {
        if self.unflushed_links.len() >= min_links.get() {
            let mut links = Vec::with_capacity(self.unflushed_links.len());
            let mut blocksizes = Vec::with_capacity(self.unflushed_links.len());

            let mut nested_size = 0;

            for (cid, total_size, block_size) in self.unflushed_links.drain(..) {
                links.push(PBLink {
                    Hash: Some(cid.to_bytes().into()),
                    Name: Some("".into()),
                    Tsize: Some(total_size),
                });
                blocksizes.push(block_size);
                nested_size += block_size;
            }

            let inner = FlatUnixFs {
                links,
                data: UnixFs {
                    Type: UnixFsType::File,
                    blocksizes,
                    filesize: Some(nested_size),
                    ..Default::default()
                },
            };

            println!("flushing {:#?}", inner.data);

            let (cid, vec) = render_and_hash(inner);
            Some((cid, vec))
        } else {
            None
        }
    }
}

fn render_and_hash(flat: FlatUnixFs<'_>) -> (Cid, Vec<u8>) {
    let mut out = Vec::with_capacity(flat.get_size());
    let mut writer = Writer::new(&mut out);
    flat.write_message(&mut writer)
        .expect("unsure how this could fail");
    let cid = Cid::new_v0(multihash::wrap(
        multihash::Code::Sha2_256,
        &Sha256::digest(&out),
    ))
    .unwrap();
    (cid, out)
}

enum Chunker {
    Size(usize),
}

impl std::default::Default for Chunker {
    fn default() -> Self {
        Chunker::Size(256 * 1024)
    }
}

impl Chunker {
    fn accept<'a>(&mut self, input: &'a [u8], buffered: &[u8]) -> (&'a [u8], bool) {
        use Chunker::*;

        match self {
            Size(max) => {
                let l = input.len().min(*max);
                let accepted = &input[..l];
                let ready = l + input.len() >= *max;
                (accepted, ready)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{Chunker, /*Adder,*/ FileAdder};
    use crate::test_support::FakeBlockstore;
    // use cid::Cid;
    // use std::str::FromStr;

    #[test]
    fn favourite_single_block_file() {
        let blocks = FakeBlockstore::with_fixtures();
        // everyones favourite content
        let content = b"foobar\n";

        let mut adder = FileAdder::default();

        {
            let (mut ready_blocks, bytes) = adder.push(content).unwrap();
            assert!(ready_blocks.next().is_none());
            assert_eq!(bytes, content.len());
        }

        // real impl would probably hash this ... except maybe hashing is faster when done inline?
        // or maybe not
        let (_, file_block) = adder
            .finish()
            .next()
            .expect("there must have been the root block");

        assert_eq!(
            blocks.get_by_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"),
            file_block.as_slice()
        );
    }

    #[test]
    fn favourite_multi_block_file() {
        // root should be QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6

        let blocks = FakeBlockstore::with_fixtures();
        let content = b"foobar\n";
        let mut adder = FileAdder::with_chunker(Chunker::Size(2));

        let mut written = 0;
        let mut blocks_received = Vec::new();

        while written < content.len() {
            let (blocks, pushed) = adder.push(&content[written..]).unwrap();
            assert!(pushed > 0 && pushed <= 2, "pushed: {}", pushed);
            blocks_received.extend(blocks.map(|(_, slice)| slice.to_vec()));
            written += pushed;
        }

        let last_blocks = adder.finish();
        blocks_received.extend(last_blocks.map(|(_, slice)| slice.to_vec()));

        // the order here is "fo", "ob", "ar", "\n", root block
        let expected = [
            "QmfVyMoStzTvdnUR7Uotzh82gmL427q9z3xW5Y8fUoszi4",
            "QmdPyW4CWE3QBkgjWfjM5f7Tjb3HukxVuBXZtkqAGwsMnm",
            "QmNhDQpphvMWhdCzP74taRzXDaEfPGq8vWfFRzD7mEgePM",
            "Qmc5m94Gu7z62RC8waSKkZUrCCBJPyHbkpmGzEePxy2oXJ",
            "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6",
        ]
        .iter()
        .map(|key| blocks.get_by_str(key).to_vec())
        .collect::<Vec<_>>();

        assert_eq!(blocks_received, expected);
    }
}
