use super::tcp_connect;

/// Every connection bounds how long written data may stay unacknowledged,
/// so writes towards a host that vanished fail within seconds (ADR 0130).
#[tokio::test]
async fn connections_bound_unacknowledged_data() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let tcp = tcp_connect(&addr).await.unwrap();
    assert!(tcp.nodelay().unwrap());
    #[cfg(any(target_os = "linux", target_os = "android"))]
    assert_eq!(socket2::SockRef::from(&tcp).tcp_user_timeout().unwrap(), Some(std::time::Duration::from_secs(10)));
}

/// A `recv` dropped while a frame is half read loses nothing: the next
/// `recv` returns the whole frame. Callers race `recv` against timers and
/// signals, so a cancelled read must not leave the stream mid-frame.
#[tokio::test]
async fn a_cancelled_recv_loses_no_bytes() {
    use lotse_rt::Conn;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    let (mut peer, ours) = tokio::io::duplex(1 << 16);
    let conn = super::TokioConn::new(ours, None, None, None);
    let payload: Vec<u8> = (0..200u8).collect();
    let mut wire = (payload.len() as u32).to_be_bytes().to_vec();
    wire.extend_from_slice(&payload);
    let wait = Duration::from_millis(50);

    // Half the length prefix, then a cancelled recv.
    peer.write_all(&wire[..2]).await.unwrap();
    assert!(tokio::time::timeout(wait, conn.recv()).await.is_err(), "no whole frame yet");
    // The rest of the prefix and part of the payload, cancelled again.
    peer.write_all(&wire[2..100]).await.unwrap();
    assert!(tokio::time::timeout(wait, conn.recv()).await.is_err(), "no whole frame yet");
    // The rest arrives later.
    let rest = wire[100..].to_vec();
    let sender = tokio::spawn(async move {
        tokio::time::sleep(wait).await;
        peer.write_all(&rest).await.unwrap();
        peer
    });
    let frame = conn.recv().await.expect("the stream is intact").expect("a frame");
    assert_eq!(&frame[..], &payload[..]);
    // And the stream stays in step: a second frame follows whole.
    let mut peer = sender.await.unwrap();
    peer.write_all(&[0, 0, 0, 3, 7, 8, 9]).await.unwrap();
    assert_eq!(&conn.recv().await.unwrap().unwrap()[..], &[7, 8, 9]);
    // A clean close between frames is the end.
    drop(peer);
    assert!(conn.recv().await.unwrap().is_none());
}
