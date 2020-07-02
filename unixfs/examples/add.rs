use cid::Cid;
use ipfs_unixfs::adder::FileAdder;
use std::fmt;
use std::io::BufRead;

fn main() {
    // read stdin, maybe produce stdout car?

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    let mut adder = FileAdder::default();
    let mut stats = Stats::default();

    loop {
        match stdin.fill_buf().unwrap() {
            x if x.is_empty() => {
                let blocks = adder.finish();
                stats.process(blocks);
                break;
            }
            x => {
                let (blocks, consumed) = adder.push(x).expect("no idea what could fail here?");
                stdin.consume(consumed);
                stats.process(blocks);
            }
        }
    }

    eprintln!("{}", stats);
}

#[derive(Default)]
struct Stats {
    blocks: usize,
    block_bytes: u64,
    last: Option<Cid>,
}

impl fmt::Display for Stats {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.last.as_ref() {
            Some(cid) => write!(
                fmt,
                "{} blocks, {} block bytes, last_cid: {}",
                self.blocks, self.block_bytes, cid
            ),
            None => write!(
                fmt,
                "{} blocks, {} block bytes",
                self.blocks, self.block_bytes
            ),
        }
    }
}

impl Stats {
    fn process<I: Iterator<Item = (Cid, Vec<u8>)>>(&mut self, new_blocks: I) {
        for (cid, block) in new_blocks {
            self.last = Some(cid);
            self.blocks += 1;
            self.block_bytes += block.len() as u64;
        }
    }
}
