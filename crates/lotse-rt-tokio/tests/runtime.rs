//! The production runtime against the real file system and localhost TCP.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use lotse_log::{Log, LogConfig};
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Conn, Disk, Host, Listener, MAX_FRAME, Net, OpenMode, Rng, SegmentFile, Spawn};
use lotse_rt_tokio::TokioRuntime;

/// A fresh directory under the system temp dir, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lotse-rt-tokio-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn batch(base_offset: u64, values: &[&[u8]]) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for v in values {
        b.push(0, None, Some(v), &[]);
    }
    let mut owned = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut owned, base_offset, 1).unwrap();
    owned.freeze()
}

#[tokio::test]
async fn the_log_runs_on_the_real_disk() {
    let tmp = TempDir::new();
    let rt = TokioRuntime::new(&tmp.0);
    let config = LogConfig { segment_bytes: 300, index_interval_bytes: 64 };
    let mut log = Log::open(rt.clone(), "p0", config).await.unwrap();
    for i in 0..20u64 {
        log.append(batch(i * 2, &[b"aaaaaaaaaaaaaaaa", b"bbbbbbbbbbbbbbbb"])).await.unwrap();
    }
    log.sync().await.unwrap();
    assert!(log.segment_count() > 1, "segments rolled");
    drop(log);
    let mut log = Log::open(rt.clone(), "p0", config).await.unwrap();
    assert_eq!(log.next_offset(), 40, "everything synced is there after reopening");
    let read = log.read(13, 1 << 20).await.unwrap();
    assert!(!read.is_empty());
    log.truncate_from(10).await.unwrap();
    assert_eq!(log.next_offset(), 10);
    log.reset(100).await.unwrap();
    drop(log);
    let log = Log::open(rt, "p0", config).await.unwrap();
    assert_eq!((log.start_offset(), log.next_offset()), (100, 100));
}

#[tokio::test]
async fn disk_operations_keep_the_contract() {
    let tmp = TempDir::new();
    let rt = TokioRuntime::new(&tmp.0);
    rt.create_dir_all(Path::new("a/b")).await.unwrap();
    let f = rt.open(Path::new("a/b/f"), OpenMode::CreateNew).await.unwrap();
    assert!(rt.open(Path::new("a/b/f"), OpenMode::CreateNew).await.is_err(), "CreateNew refuses an existing file");
    assert!(rt.open(Path::new("a/b/missing"), OpenMode::Existing).await.is_err());
    assert_eq!(f.append(Bytes::from_static(b"hello ")).await.unwrap(), 0);
    assert_eq!(f.append(Bytes::from_static(b"world")).await.unwrap(), 6);
    f.sync().await.unwrap();
    assert_eq!(f.size().await.unwrap(), 11);
    assert_eq!(&f.read_at(6, 100).await.unwrap()[..], b"world", "short read at the end");
    f.truncate(5).await.unwrap();
    assert_eq!(&f.read_at(0, 100).await.unwrap()[..], b"hello");
    rt.rename(Path::new("a/b/f"), Path::new("a/b/g")).await.unwrap();
    assert_eq!(rt.list(Path::new("a/b")).await.unwrap(), vec![PathBuf::from("a/b/g")]);
    let g = rt.open(Path::new("a/b/g"), OpenMode::Existing).await.unwrap();
    assert_eq!(g.size().await.unwrap(), 5, "the length is read from the file when it is opened");
    rt.remove(Path::new("a/b/g")).await.unwrap();
    assert!(rt.list(Path::new("a/b")).await.unwrap().is_empty());
}

#[tokio::test]
async fn tcp_carries_whole_frames_in_order() {
    let rt = TokioRuntime::new(std::env::temp_dir());
    let listener = rt.listen("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr();
    let server = rt.spawn(async move {
        let (conn, _) = listener.accept().await.unwrap();
        let mut got = Vec::new();
        while let Some(frame) = conn.recv().await.unwrap() {
            got.push(frame);
        }
        got
    });
    let conn = rt.connect(&addr).await.unwrap();
    let big = Bytes::from(vec![7u8; 1 << 20]);
    for i in 0..50u32 {
        conn.send(Bytes::from(i.to_be_bytes().to_vec())).await.unwrap();
    }
    conn.send(big.clone()).await.unwrap();
    conn.send(Bytes::new()).await.unwrap();
    assert!(conn.send(Bytes::from(vec![0u8; MAX_FRAME + 1])).await.is_err(), "oversized frames are refused");
    drop(conn);
    let got = server.await;
    assert_eq!(got.len(), 52);
    for (i, f) in got[..50].iter().enumerate() {
        assert_eq!(&f[..], &(i as u32).to_be_bytes()[..]);
    }
    assert_eq!(got[50], big);
    assert!(got[51].is_empty(), "an empty frame is a frame, not the end");
}

#[tokio::test]
async fn clock_rng_and_tasks_work() {
    let rt = TokioRuntime::new(std::env::temp_dir());
    let start = rt.now();
    rt.sleep(Duration::from_millis(20)).await;
    assert!(rt.now() - start >= Duration::from_millis(20));
    assert_ne!(rt.next_u64(), rt.next_u64());
    assert_eq!(rt.spawn(async { 41 + 1 }).await, 42);
    let panicked = tokio::spawn(async move {
        let rt = TokioRuntime::new(std::env::temp_dir());
        rt.spawn(async { panic!("inside the task") }).await
    })
    .await;
    assert!(panicked.is_err(), "a task's panic reaches whoever awaits it");
}

/// The host is read for real: memory and the data volume are known, and
/// CPU use from the second call on (it is measured between two calls).
#[tokio::test]
async fn host_stats_are_read_from_the_machine() {
    let rt = TokioRuntime::new(std::env::temp_dir());
    let first = rt.host_stats().await;
    assert_eq!(first.cpu, None, "no CPU use before a second measurement");
    let second = rt.host_stats().await;
    if cfg!(target_os = "linux") {
        assert!(second.cpu.is_some_and(|c| (0.0..=1.0).contains(&c)), "{second:?}");
        assert!(second.memory_bytes.is_some_and(|m| m > 0), "{second:?}");
        assert!(second.memory_limit_bytes >= second.memory_bytes, "{second:?}");
        let (free, total) = (second.disk_free_bytes.unwrap(), second.disk_total_bytes.unwrap());
        assert!(free <= total && total > 0, "{second:?}");
    }
}
