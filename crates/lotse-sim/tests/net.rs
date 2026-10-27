//! The simulated network keeps TCP's promises and fails the way TCP fails.

use std::time::Duration;

use bytes::Bytes;
use lotse_rt::{Clock, Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, check_seeds};

#[test]
fn frames_arrive_whole_in_order_and_after_some_latency() {
    check_seeds(50, |seed| {
        Sim::new(seed).run(|rt| async move {
            let a = rt.node("a");
            let b = rt.node("b");
            let listener = b.listen("b:0").await.unwrap();
            let addr = listener.local_addr();
            assert!(addr.starts_with("b:"));
            let server = b.spawn(async move {
                let (conn, peer) = listener.accept().await.unwrap();
                assert!(peer.starts_with("a:"));
                let mut got = Vec::new();
                while let Some(f) = conn.recv().await.unwrap() {
                    got.push(f);
                }
                got
            });
            let conn = a.connect(&addr).await.unwrap();
            let start = a.now();
            for i in 0..100u32 {
                conn.send(Bytes::from(i.to_le_bytes().to_vec())).await.unwrap();
            }
            drop(conn); // a clean close: the server drains, then sees the end
            let got = server.await;
            assert_eq!(got.len(), 100);
            for (i, f) in got.iter().enumerate() {
                assert_eq!(&f[..], &(i as u32).to_le_bytes()[..], "in order and whole");
            }
            assert!(a.now() >= start, "virtual time only moves forward");
        });
    });
}

#[test]
fn a_partition_breaks_connections_and_refuses_new_ones_until_healed() {
    Sim::new(3).run(|rt| async move {
        let (a, b) = (rt.node("a"), rt.node("b"));
        let listener = b.listen("b:7000").await.unwrap();
        let conn = a.connect("b:7000").await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        conn.send(Bytes::from_static(b"before")).await.unwrap();
        rt.net().block("a", "b");
        assert!(accepted.recv().await.is_err(), "in-flight frames are lost with the connection");
        assert!(conn.send(Bytes::from_static(b"during")).await.is_err());
        assert!(a.connect("b:7000").await.is_err(), "unreachable while blocked");
        rt.net().heal();
        let again = a.connect("b:7000").await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        again.send(Bytes::from_static(b"after")).await.unwrap();
        assert_eq!(&accepted.recv().await.unwrap().unwrap()[..], b"after");
    });
}

#[test]
fn an_isolated_host_loses_every_connection() {
    Sim::new(4).run(|rt| async move {
        let (a, b, c) = (rt.node("a"), rt.node("b"), rt.node("c"));
        let lb = b.listen("b:1").await.unwrap();
        let lc = c.listen("c:1").await.unwrap();
        let ab = a.connect("b:1").await.unwrap();
        let ac = a.connect("c:1").await.unwrap();
        let (_bb, _) = lb.accept().await.unwrap();
        let (_cc, _) = lc.accept().await.unwrap();
        rt.net().isolate("a");
        assert!(ab.send(Bytes::from_static(b"x")).await.is_err());
        assert!(ac.send(Bytes::from_static(b"x")).await.is_err());
        rt.net().reconnect("a");
        assert!(a.connect("b:1").await.is_ok());
    });
}

#[test]
fn a_dropped_peer_makes_sends_fail_and_nothing_listens_on_a_closed_port() {
    Sim::new(5).run(|rt| async move {
        let (a, b) = (rt.node("a"), rt.node("b"));
        let listener = b.listen("b:9").await.unwrap();
        let conn = a.connect("b:9").await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        drop(accepted);
        assert!(conn.send(Bytes::from_static(b"x")).await.is_err());
        drop(listener);
        assert!(a.connect("b:9").await.is_err());
        assert!(a.listen("b:9").await.is_err(), "a host listens only on its own name");
    });
}

#[test]
fn a_receiver_waits_for_frames_without_busy_looping() {
    Sim::new(6).run(|rt| async move {
        let (a, b) = (rt.node("a"), rt.node("b"));
        let listener = b.listen("b:2").await.unwrap();
        let conn = a.connect("b:2").await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let sender = a.clone();
        a.spawn(async move {
            sender.sleep(Duration::from_secs(3)).await;
            conn.send(Bytes::from_static(b"late")).await.unwrap();
            sender.sleep(Duration::from_secs(1)).await;
        });
        let start = b.now();
        assert_eq!(&accepted.recv().await.unwrap().unwrap()[..], b"late");
        assert!(b.now() - start >= Duration::from_secs(3));
    });
}
