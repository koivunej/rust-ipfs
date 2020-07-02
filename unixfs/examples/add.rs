use cid::Cid;
use ipfs_unixfs::file::adder::FileAdder;
use std::fmt;
use std::io::{Read, BufRead, BufReader};
use std::time::Duration;
use std::num::NonZeroUsize;

enum Mode {
    BufReader,
    SizeHint,
    ThreadedSizeHint(NonZeroUsize),
}

fn main() {
    // read stdin, maybe produce stdout car?

    let stdin = std::io::stdin();

    let mut adder = FileAdder::default();

    eprintln!("using size_hint: {}", adder.size_hint());
    let mut stats = Stats::default();
    let mut input = 0;

    let mode = Mode::BufReader;
    //let mode = Mode::SizeHint;
    //let mode = Mode::ThreadedSizeHint(NonZeroUsize::new(4).unwrap());

    match mode {
        Mode::BufReader => {
            let mut stdin = BufReader::with_capacity(adder.size_hint(), stdin.lock());

            loop {
                let buf = match stdin.fill_buf().unwrap() {
                    last if last.is_empty() => {
                        eprintln!("finishing\n{:?}", adder);
                        let blocks = adder.finish();
                        stats.process(blocks);
                        break;
                    }
                    buf => buf
                };

                let mut total = 0;

                while total < buf.len() {
                    let (blocks, consumed) = adder
                        .push(&buf[total..])
                        .expect("no idea what could fail here?");
                    stats.process(blocks);

                    input += consumed;
                    total += consumed;
                }

                assert_eq!(total, buf.len());
                stdin.consume(total);
            }
        },
        Mode::SizeHint => {
            let mut stdin = stdin.lock();
            let mut buf = vec![0u8; adder.size_hint()];

            loop {
                let mut new_bytes = 0;
                let mut last = false;

                loop {
                    let read = stdin.read(&mut buf[new_bytes..]).unwrap();

                    new_bytes += read;

                    if read == 0 {
                        last = true;
                        break
                    } else if new_bytes == buf.capacity() {
                        break
                    }
                }

                let mut total = 0;

                while total < new_bytes {
                    let (blocks, consumed) = adder.push(&buf[total..new_bytes]).unwrap();

                    stats.process(blocks);

                    input += consumed;
                    total += consumed;
                }

                assert_eq!(total, new_bytes);

                if last {
                    eprintln!("finishing\n{:?}", adder);
                    let blocks = adder.finish();
                    stats.process(blocks);
                    break
                }
            }
        },
        Mode::ThreadedSizeHint(buffers) => {
            let (filled_buffer_tx, filled_buffer_rx) = std::sync::mpsc::channel::<Vec<u8>>();
            let (consumed_buffer_tx, consumed_buffer_rx) = std::sync::mpsc::channel::<Vec<u8>>();

            let jh = std::thread::spawn(move || {
                let mut stdin = stdin.lock();

                for mut buf in consumed_buffer_rx.iter() {
                    assert_eq!(buf.len(), buf.capacity());
                    let mut new_bytes = 0;
                    let mut last = false;

                    loop {
                        let read = stdin.read(&mut buf[new_bytes..]).unwrap();

                        new_bytes += read;

                        if read == 0 {
                            last = true;
                            break
                        } else if new_bytes == buf.capacity() {
                            break
                        }
                    }

                    if new_bytes > 0 {
                        if new_bytes < buf.capacity() {
                            // this should only happen on the last iteration
                            buf.truncate(new_bytes);
                        }
                        filled_buffer_tx.send(buf).unwrap();
                    }

                    if last {
                        break;
                    }
                }
            });

            for _ in 0..buffers.get() {
                consumed_buffer_tx.send(vec![0u8; adder.size_hint()]).unwrap();
            }

            for buf in filled_buffer_rx.iter() {
                let mut total = 0;
                let len = buf.len();

                while total < len {
                    let (blocks, consumed) = adder.push(&buf[total..len]).unwrap();

                    stats.process(blocks);

                    input += consumed;
                    total += consumed;
                }

                assert_eq!(total, len);

                // ignore this to avoid error in the end
                let _ = consumed_buffer_tx.send(buf);
            }

            eprintln!("finishing\n{:?}", adder);
            let blocks = adder.finish();
            stats.process(blocks);

            jh.join().unwrap();
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
