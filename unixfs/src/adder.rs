use cid::Cid;
use std::path::Path;

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
    unflushed_links: Vec<(Cid, usize)>,
    total_blocks: usize,
}

impl FileAdder {
    fn push<'a>(
        &'a mut self,
        input: &[u8],
    ) -> Result<(impl Iterator<Item = (&'a Cid, &'a [u8])>, usize), ()> {
        // case 0: full chunk is not ready => empty iterator, full read
        // case 1: full chunk becomes ready, maybe short read => at least one block
        //     1a: not enough links => iterator of one
        //     1b: link block is ready => iterator of two blocks

        let (accepted, ready) = self.chunker.accept(input, &self.block_buffer);
        self.block_buffer.extend_from_slice(accepted);
        let written = accepted.len();

        if !ready {
            Ok((std::iter::empty(), written))
        } else {
            // if self.unflushed_links.
            todo!()
        }
    }

    fn finish(self) -> Result<Vec<u8>, quick_protobuf::Error> {
        use crate::pb::{FlatUnixFs, UnixFs, UnixFsType};
        use quick_protobuf::{MessageWrite, Writer};
        use std::borrow::Cow;
        use std::io::Write;

        let message = if !self.block_buffer.is_empty() {
            assert!(self.unflushed_links.is_empty());

            FlatUnixFs {
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
            }
        } else {
            todo!("finish with link block")
        };

        let mut out = Vec::with_capacity(message.get_size());

        let mut writer = Writer::new(&mut out);

        message.write_message(&mut writer)?;

        Ok(out)
    }
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
                let l = input.len().min(buffered.len() + input.len().max(*max));
                let accepted = &input[..l];
                let ready = l + input.len() >= *max;
                (accepted, ready)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{Adder, FileAdder};
    use crate::test_support::FakeBlockstore;
    use cid::Cid;
    use std::str::FromStr;

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
        let file_block = adder.finish().unwrap();

        assert_eq!(
            blocks.get_by_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"),
            file_block.as_slice()
        );
    }
}
