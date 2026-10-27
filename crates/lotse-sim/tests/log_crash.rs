//! Crash-recovery property for the log, checked over many seeds.
//!
//! Each run appends random batches, syncs and truncates at random points, and
//! pulls the plug several times. After every recovery:
//! 1. every batch that was synced is still there, byte for byte;
//! 2. the log is a prefix of what was written, with nothing invented;
//! 3. the log ends on a batch boundary and accepts the next append.

use std::path::Path;

use bytes::{Bytes, BytesMut};
use lotse_log::{Log, LogConfig};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Rng;
use lotse_sim::{Sim, SimRuntime, check_seeds};

const DIR: &str = "partitions/p0";

fn config(rt: &SimRuntime) -> LogConfig {
    // Small and varied segments so rolls happen constantly.
    LogConfig { segment_bytes: 256 + rt.below(4096), index_interval_bytes: 64 + rt.below(512) }
}

fn random_batch(rt: &SimRuntime, base_offset: u64, term: u64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for i in 0..1 + rt.below(8) {
        let value: Vec<u8> = (0..rt.below(200)).map(|_| rt.next_u64() as u8).collect();
        b.push(i as i64, None, Some(&value), &[]);
    }
    let mut owned = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut owned, base_offset, term).unwrap();
    owned.freeze()
}

/// The batches written so far, in offset order.
struct History {
    batches: Vec<Bytes>,
    /// How many of `batches` are known to be durable.
    durable: usize,
}

impl History {
    fn end_of(&self, n: usize) -> u64 {
        match n {
            0 => 0,
            n => Batch::parse(&self.batches[n - 1]).unwrap().0.next_offset(),
        }
    }
}

async fn verify(log: &Log<SimRuntime>, history: &mut History) {
    let end = log.next_offset();
    let survived = history.batches.iter().take_while(|b| Batch::parse(b).unwrap().0.next_offset() <= end).count();
    assert!(
        survived >= history.durable,
        "lost synced data: {survived} of {} durable batches survived",
        history.durable
    );
    assert_eq!(end, history.end_of(survived), "log does not end on a batch boundary that was written");
    for (i, expected) in history.batches[..survived].iter().enumerate() {
        let (b, _) = Batch::parse(expected).unwrap();
        let got = log.read(b.base_offset(), 1).await.unwrap();
        assert_eq!(got.slice(..expected.len()), expected, "batch {i} at offset {} differs", b.base_offset());
    }
    history.batches.truncate(survived);
    // Log::open syncs what it recovered.
    history.durable = survived;
}

#[test]
fn log_survives_crashes() {
    check_seeds(300, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut history = History { batches: Vec::new(), durable: 0 };
            let incarnations = 2 + rt.below(5);
            for term in 1..=incarnations {
                let mut log = Log::open(rt.clone(), DIR, config(&rt)).await.unwrap();
                verify(&log, &mut history).await;

                for _ in 0..rt.below(60) {
                    match rt.below(20) {
                        0..=14 => {
                            let b = random_batch(&rt, log.next_offset(), term);
                            log.append(b.clone()).await.unwrap();
                            history.batches.push(b);
                        }
                        15..=17 => {
                            log.sync().await.unwrap();
                            history.durable = history.batches.len();
                        }
                        _ if !history.batches.is_empty() => {
                            // Raft cuts a conflicting suffix at a batch boundary.
                            let keep = rt.below(history.batches.len() as u64 + 1) as usize;
                            log.truncate_from(history.end_of(keep)).await.unwrap();
                            history.batches.truncate(keep);
                            history.durable = history.durable.min(keep);
                        }
                        _ => {}
                    }
                }
                drop(log);
                rt.crash();
            }
            let log = Log::open(rt.clone(), DIR, config(&rt)).await.unwrap();
            verify(&log, &mut history).await;
            assert!(rt.disk().file_len(&lotse_log::segment_path(Path::new(DIR), log.start_offset())).is_some());
        });
    });
}
