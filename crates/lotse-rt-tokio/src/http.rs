//! A small HTTP/1.1 client for `GET`: enough to read an identity provider's
//! discovery document and key set. HTTPS checks the server against the
//! public web roots (and any CA added with
//! [`crate::TokioRuntime::with_http_ca`]); plain HTTP only reaches loopback.

use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_rt::MAX_HTTP_BODY;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// How long one request may take, connecting included.
const TIMEOUT: Duration = Duration::from_secs(10);

/// Room for the status line and headers on top of the body.
const MAX_HEAD: usize = 64 * 1024;

/// TLS for HTTPS: the web's roots and any extra CAs.
pub(crate) fn client_config(extra: &[rustls::pki_types::CertificateDer<'static>]) -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    for c in extra {
        roots.add(c.clone()).map_err(io::Error::other)?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

struct Url {
    tls: bool,
    host: String,
    port: u16,
    path: String,
}

fn parse(url: &str) -> io::Result<Url> {
    let bad = || io::Error::new(ErrorKind::InvalidInput, format!("{url} is not an http(s) URL"));
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(bad());
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(bad());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (h.to_owned(), p.parse().map_err(|_| bad())?),
        _ => (authority.to_owned(), if tls { 443 } else { 80 }),
    };
    Ok(Url { tls, host, port, path: path.to_owned() })
}

/// `GET url`.
pub(crate) async fn get(url: &str, tls: Arc<ClientConfig>) -> io::Result<Bytes> {
    tokio::time::timeout(TIMEOUT, fetch(url, tls, None))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, format!("{url} did not answer in time")))?
}

/// `POST url` with `fields` as `application/x-www-form-urlencoded`.
pub(crate) async fn post_form(url: &str, fields: &[(&str, &str)], tls: Arc<ClientConfig>) -> io::Result<Bytes> {
    let body = fields.iter().map(|(k, v)| format!("{}={}", encode(k), encode(v))).collect::<Vec<_>>().join("&");
    tokio::time::timeout(TIMEOUT, fetch(url, tls, Some(body)))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, format!("{url} did not answer in time")))?
}

/// Percent-encoding for form fields: everything but unreserved characters.
fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

async fn fetch(url: &str, tls: Arc<ClientConfig>, form: Option<String>) -> io::Result<Bytes> {
    let u = parse(url)?;
    let host = u.host.trim_start_matches('[').trim_end_matches(']').to_owned();
    let addr = format!("{}:{}", u.host, u.port);
    if !u.tls {
        let resolved: Vec<std::net::SocketAddr> = tokio::net::lookup_host(&addr).await?.collect();
        if resolved.is_empty() || resolved.iter().any(|a| !a.ip().is_loopback()) {
            return Err(io::Error::new(ErrorKind::PermissionDenied, format!("{url}: plain HTTP only on loopback")));
        }
    }
    let tcp = tokio::net::TcpStream::connect(&addr).await?;
    let request = match &form {
        None => format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nUser-Agent: lotse\r\nConnection: close\r\n\r\n",
            u.path, u.host
        ),
        Some(body) => format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nUser-Agent: lotse\r\nConnection: close\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            u.path,
            u.host,
            body.len()
        ),
    };
    let raw = if u.tls {
        let name = ServerName::try_from(host).map_err(|e| io::Error::new(ErrorKind::InvalidInput, e.to_string()))?;
        let stream = tokio_rustls::TlsConnector::from(tls).connect(name, tcp).await?;
        exchange(stream, request.as_bytes()).await?
    } else {
        exchange(tcp, request.as_bytes()).await?
    };
    body(&raw).map_err(|e| io::Error::new(ErrorKind::InvalidData, format!("{url}: {e}")))
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &[u8]) -> io::Result<Vec<u8>> {
    s.write_all(request).await?;
    s.flush().await?;
    let mut out = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        match s.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            // A server that closes without TLS close_notify: the response
            // is checked for completeness below.
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        if out.len() > MAX_HTTP_BODY + MAX_HEAD {
            return Err(io::Error::new(ErrorKind::InvalidData, "the response is too large"));
        }
    }
    Ok(out)
}

/// The body of a complete `200` response.
fn body(raw: &[u8]) -> Result<Bytes, String> {
    let end = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("no end of headers")?;
    let head = std::str::from_utf8(&raw[..end]).map_err(|_| "headers are not text")?;
    let rest = &raw[end + 4..];
    let mut lines = head.split("\r\n");
    let status = lines.next().ok_or("no status line")?;
    let code = status.split(' ').nth(1).ok_or("no status code")?;
    if code != "200" {
        return Err(format!("status {code}"));
    }
    let mut chunked = false;
    let mut length = None;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else { continue };
        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
        if k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked") {
            chunked = true;
        } else if k == "content-length" {
            length = Some(v.parse::<usize>().map_err(|_| "bad content-length")?);
        }
    }
    let body = if chunked {
        let mut out = Vec::new();
        let mut rest = rest;
        loop {
            let eol = rest.windows(2).position(|w| w == b"\r\n").ok_or("truncated chunk")?;
            let size = std::str::from_utf8(&rest[..eol]).map_err(|_| "bad chunk size")?;
            let size = size.split(';').next().unwrap_or("");
            let size = usize::from_str_radix(size.trim(), 16).map_err(|_| "bad chunk size")?;
            rest = &rest[eol + 2..];
            if size == 0 {
                break;
            }
            if rest.len() < size + 2 {
                return Err("truncated chunk".into());
            }
            out.extend_from_slice(&rest[..size]);
            rest = &rest[size + 2..];
        }
        out
    } else if let Some(n) = length {
        if rest.len() < n {
            return Err("truncated body".into());
        }
        rest[..n].to_vec()
    } else {
        rest.to_vec()
    };
    if body.len() > MAX_HTTP_BODY {
        return Err("the body is too large".into());
    }
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_are_read_whole() {
        let r = body(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").unwrap();
        assert_eq!(&r[..], b"hello");
        let r =
            body(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nhel\r\n2;x=y\r\nlo\r\n0\r\n\r\n").unwrap();
        assert_eq!(&r[..], b"hello");
        assert!(body(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").is_err());
        assert!(body(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nhello").is_err(), "cut short");
        assert!(body(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n9\r\nhel").is_err());
    }

    #[test]
    fn urls() {
        let u = parse("https://idp.example:8443/realms/x/.well-known/openid-configuration").unwrap();
        assert_eq!(
            (u.tls, u.host.as_str(), u.port, u.path.as_str()),
            (true, "idp.example", 8443, "/realms/x/.well-known/openid-configuration")
        );
        let u = parse("http://127.0.0.1/keys").unwrap();
        assert_eq!((u.tls, u.port), (false, 80));
        assert_eq!(parse("https://idp.example").unwrap().path, "/");
        assert!(parse("ftp://x/").is_err() && parse("https://user@x/").is_err());
    }

    #[test]
    fn form_fields_are_percent_encoded() {
        assert_eq!(encode("a b&c=d/é"), "a%20b%26c%3Dd%2F%C3%A9");
        assert_eq!(encode("AZaz09-._~"), "AZaz09-._~");
    }
}
