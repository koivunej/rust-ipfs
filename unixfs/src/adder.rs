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
}

impl FileAdder {
    fn push<'a>(&'a mut self, bytes: &[u8]) -> Result<Option<&'a [u8]>, ()> {
        todo!("return if a new non-root block needs to be stored")
    }

    fn finish(self) -> Vec<u8> {
        todo!("return the root block")
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

        let mut file_adder = FileAdder::default();
        assert!(file_adder.push(content).unwrap().is_none());
        let file_block = file_adder.finish();

        // real impl would probably hash this ... except maybe hashing is faster when done inline?
        // or maybe not

        assert_eq!(blocks.get_by_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"), file_block.as_slice());
    }

    #[test]
    fn single_link_directory() {
        let blocks = FakeBlockstore::with_fixtures();
        // everyones favourite content
        let content = b"foobar\n";

        let mut file_adder = FileAdder::default();
        assert!(file_adder.push(content).unwrap().is_none());
        let file_block = file_adder.finish();

        // here we would need to turn the file_block into Cid, for example by storing it.
        drop(file_block);

        let mut adder = Adder::default();
        adder.push("foobar", Cid::from_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap())
            .expect("this error would probably only be for unsupported sharded dirs?");

        let dirs = adder.finish();

        assert_eq!(blocks.get_by_str("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL"), dirs[0].as_slice());
    }
}
