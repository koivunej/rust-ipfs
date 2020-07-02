use cid::Cid;
use ipfs_unixfs::file::adder::FileAdder;
use std::fmt;
use std::io::{BufRead, BufReader};
use std::time::Duration;

fn main() {
    // read stdin, maybe produce stdout car?

    let stdin = std::io::stdin();
    let stdin = stdin.lock();

    let mut adder = FileAdder::default();

    let mut stdin = BufReader::with_capacity(adder.size_hint(), stdin);

    let mut stats = Stats::default();

    let mut input = 0;

    loop {
        match stdin.fill_buf().unwrap() {
            x if x.is_empty() => {
                eprintln!("finishing");
                eprintln!("{:?}", adder);
                let blocks = adder.finish();
                stats.process(blocks);
                break;
            }
            x => {
                let mut total = 0;

                while total < x.len() {
                    let (blocks, consumed) = adder
                        .push(&x[total..])
                        .expect("no idea what could fail here?");
                    stats.process(blocks);

                    input += consumed;
                    total += consumed;
                }

                assert_eq!(total, x.len());
                stdin.consume(total);
            }
        }
    }

    let (maxrss, user_time, system_time) = unsafe {
        let mut rusage: libc::rusage = std::mem::zeroed();

        let retval = libc::getrusage(libc::RUSAGE_SELF, &mut rusage as *mut _);

        assert_eq!(retval, 0);

        (rusage.ru_maxrss, rusage.ru_utime, rusage.ru_stime)
    };

    let user_time = to_duration(user_time);
    let system_time = to_duration(system_time);

    eprintln!("{}", stats);

    let total = user_time + system_time;

    eprintln!(
        "Max RSS: {} KB, utime: {:?}, stime: {:?}, total: {:?}",
        maxrss, user_time, system_time, total
    );

    let megabytes = 1024.0 * 1024.0;

    eprintln!(
        "Input: {:.2} MB/s (read {} bytes)",
        (input as f64 / megabytes) / total.as_secs_f64(),
        input
    );

    eprintln!(
        "Output: {:.2} MB/s",
        (stats.block_bytes as f64 / megabytes) / total.as_secs_f64()
    );
}

fn to_duration(tv: libc::timeval) -> Duration {
    assert!(tv.tv_sec >= 0);

    Duration::new(tv.tv_sec as u64, tv.tv_usec as u32)
}

#[derive(Default)]
struct Stats {
    blocks: usize,
    block_bytes: u64,
    last: Option<Cid>,
}

impl fmt::Display for Stats {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hash = self.last.as_ref().unwrap().hash();
        let cidv1 = Cid::new_v1(cid::Codec::DagProtobuf, hash.to_owned());
        write!(
            fmt,
            "{} blocks, {} block bytes, {} or {}",
            self.blocks,
            self.block_bytes,
            self.last.as_ref().unwrap(),
            cidv1,
        )
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
