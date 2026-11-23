//! The console executable serves HTTPS with the certificate its `[https]`
//! names, and plain HTTP only on loopback (ADR 0137).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A port nothing listens on, below the ephemeral range.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1_200) as u16 * 10 + 7;
    loop {
        let port = 20_000 + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("lotse-console-https-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A config for the demo cluster with `extra` after `listen`.
fn config(dir: &Path, listen: &str, extra: &str) -> PathBuf {
    let file = dir.join("lotse-console.toml");
    let text = format!(
        "listen = \"{listen}\"\n{extra}\n[auth]\nmode = \"dev\"\nuser = \"pierre\"\n\n[[clusters]]\nid = \"demo\"\nkind = \"demo\"\n"
    );
    std::fs::write(&file, text).unwrap();
    file
}

fn console(config: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lotse-console"));
    cmd.arg("--config").arg(config).stdout(Stdio::null()).stderr(Stdio::piped());
    cmd
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The raw answer to `GET path`, over `tls` or plain; empty when the
/// connection closes without one.
async fn get(addr: &str, tls: Option<Arc<rustls::ClientConfig>>, path: &str) -> String {
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut out = Vec::new();
    match tls {
        None => {
            let mut s = tcp;
            s.write_all(request.as_bytes()).await.unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out)).await;
        }
        Some(cfg) => {
            let mut s = tokio_rustls::TlsConnector::from(cfg)
                .connect(rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap(), tcp)
                .await
                .expect("a TLS handshake with the console");
            s.write_all(request.as_bytes()).await.unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out)).await;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Waits until something listens on `addr`.
async fn listening(addr: &str, child: &mut Child) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(child.try_wait().unwrap().is_none(), "the console exited");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing listens on {addr}");
}

#[tokio::test]
async fn https_with_the_configured_certificate() {
    let d = dir("serve");
    let ca = lotse_rt_tokio::dev_certs::DevCa::new();
    let issued = ca.custom("console.example", &[], &["127.0.0.1", "localhost"]);
    std::fs::write(d.join("console.pem"), &issued.cert_pem).unwrap();
    std::fs::write(d.join("console.key"), &issued.key_pem).unwrap();
    let addr = format!("127.0.0.1:{}", free_port());
    // Relative to the config file.
    let file = config(&d, &addr, "[https]\ncert = \"console.pem\"\nkey = \"console.key\"\n");
    let mut child = Running(console(&file).spawn().unwrap());
    listening(&addr, &mut child.0).await;

    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(ca.ca_pem().as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let tls = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let answer = get(&addr, Some(tls), "/api/v1/clusters").await;
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.contains("\"demo\""), "{answer}");

    // Plain HTTP to the HTTPS listener gets no HTTP answer.
    let plain = get(&addr, None, "/api/v1/clusters").await;
    assert!(!plain.contains("HTTP/1.1 "), "plain HTTP answered: {plain}");
    drop(child);
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test]
async fn plain_http_elsewhere_than_loopback_and_a_bad_certificate_stop_the_start() {
    let d = dir("refuse");
    let public = format!("0.0.0.0:{}", free_port());
    let mut child = console(&config(&d, &public, "")).spawn().unwrap();
    let mut waited = 0;
    while child.try_wait().unwrap().is_none() {
        if waited == 100 {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the console serves plain HTTP on {public}");
        }
        waited += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("loopback") && stderr.contains("[https]"), "{stderr}");

    std::fs::write(d.join("empty.pem"), "").unwrap();
    std::fs::write(d.join("empty.key"), "").unwrap();
    let addr = format!("127.0.0.1:{}", free_port());
    let out = console(&config(&d, &addr, "[https]\ncert = \"empty.pem\"\nkey = \"empty.key\"\n")).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("[https]") && stderr.contains("empty.pem"), "{stderr}");
    let _ = std::fs::remove_dir_all(&d);
}
