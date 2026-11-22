//! A client's certificate and key in files, for a client that renews its
//! certificate by itself
//! ([ADR 0123](../../docs/adr/0123-client-certificates-are-renewed-over-a-connection-made-with-them.md)).

use std::future::Future;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use lotse_rt::CertificateStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::{TlsConfig, TokioRuntime, blocking, sync_dir};

/// The certificate and key files a client connects with, and the runtime
/// whose TLS reads them. [`CertificateStore::replace`] writes the key, then
/// the chain, each through a file beside it moved into place whole, and
/// then gives the runtime the TLS material read from the files again, so
/// connections made from then on show the new certificate; open ones keep
/// the old one, which stays valid until it expires.
#[derive(Debug, Clone)]
pub struct CertificateFiles {
    runtime: TokioRuntime,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    crl: Option<PathBuf>,
}

impl CertificateFiles {
    /// The files `runtime` was given with [`TlsConfig::from_files`]: the
    /// CA bundle, the certificate (PEM chain, the certificate first) and
    /// its key.
    pub fn new(
        runtime: TokioRuntime,
        ca: impl Into<PathBuf>,
        cert: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
    ) -> Self {
        CertificateFiles { runtime, ca: ca.into(), cert: cert.into(), key: key.into(), crl: None }
    }

    /// The revocation list as well, for a runtime made with
    /// [`TlsConfig::from_files_with_crl`].
    pub fn with_crl(self, crl: impl Into<PathBuf>) -> Self {
        CertificateFiles { crl: Some(crl.into()), ..self }
    }
}

impl CertificateStore for CertificateFiles {
    fn leaf(&self) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        let cert = self.cert.clone();
        blocking(move || {
            let pem = std::fs::read(&cert)?;
            CertificateDer::pem_slice_iter(&pem)
                .next()
                .map(|c| c.map(|c| c.to_vec()))
                .unwrap_or_else(|| Err(rustls::pki_types::pem::Error::NoItemsFound))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", cert.display())))
        })
    }

    fn replace(&self, key_pem: &str, chain_pem: &str) -> impl Future<Output = io::Result<()>> + Send {
        let files = self.clone();
        let (key_pem, chain_pem) = (key_pem.to_string(), chain_pem.to_string());
        async move {
            let f = files.clone();
            blocking(move || {
                // The key first: a certificate without its key would not
                // load, a key without its certificate is noticed at once.
                write_whole(&f.key, &key_pem, true)?;
                write_whole(&f.cert, &chain_pem, false)
            })
            .await?;
            let tls = TlsConfig::from_files_with_crl(&files.ca, Some((&files.cert, &files.key)), files.crl.as_ref())
                .map_err(io::Error::other)?;
            files.runtime.reload_tls(tls);
            Ok(())
        }
    }
}

/// Writes `text` to a file beside `path` and moves it into place, so a
/// reader sees the old file or the new one, never a part; a `secret` is
/// readable by its owner only.
fn write_whole(path: &Path, text: &str, secret: bool) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".new");
    let tmp = PathBuf::from(tmp);
    let _ = std::fs::remove_file(&tmp);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = secret;
    let mut file = options.open(&tmp)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or(Path::new("")))
}

#[cfg(test)]
mod tests;
