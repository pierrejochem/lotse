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
