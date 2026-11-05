//! `lotse certs`: a CA for the cluster and a certificate for each node,
//! written where the cluster file's `[tls]` section says, and client
//! certificates for principals (ADR 0047). For small setups; larger ones
//! bring certificates from their own PKI to the same paths.
//!
//! The CA key lives next to the CA certificate (`ca.pem` → `ca.key`); keep
//! it off the nodes once they have their certificates. An existing CA and
//! existing node certificates are kept, so the command can run again after
//! a node is added.
//!
//! Certificates are valid for [`LEAF_DAYS`] unless asked otherwise, the CA
//! for [`CA_DAYS`] (ADR 0060). To renew a node's certificate, remove it and
//! its key and run the command again; the node picks up the new files while
//! running. `--revoke` adds a certificate to the revocation list `[tls] crl`
//! names, which goes to every node like the CA.

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyIdMethod, KeyPair, KeyUsagePurpose, RevokedCertParams, SanType, SerialNumber,
};
use time::{Duration, OffsetDateTime};

use crate::config::{ClusterFile, host_of};

/// What went wrong making certificates.
#[derive(Debug, thiserror::Error)]
pub enum CertError {
    /// The cluster file uses no TLS.
    #[error("[tls] names no certificate paths; there is nothing to make")]
    NoTls,
    /// A file could not be read or written.
    #[error("{path}: {source}")]
    Io {
        /// The file.
        path: String,
        /// Why.
        source: std::io::Error,
    },
    /// A certificate or key could not be read or made.
    #[error("{what}: {detail}")]
    Cert {
        /// Which one.
        what: String,
        /// Why.
        detail: String,
    },
    /// Making a client certificate needs the CA, made first.
    #[error("{0} is missing: run `lotse certs --config <file>` first")]
    NoCa(String),
    /// Revoking needs `[tls] crl`.
    #[error("[tls] names no crl file; add `crl = \"<path>\"` to say where the revocation list goes")]
    NoCrl,
    /// A certificate to revoke that this cluster's CA did not issue.
    #[error("{0} was not issued by the cluster's CA")]
    NotOurs(String),
    /// A principal name that would not make a file name.
    #[error("{0:?} is not a principal name this command takes: letters, digits, `.`, `_`, `-` and `@` only")]
    BadName(String),
}

/// What the command did with one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Written now.
    Wrote(PathBuf),
    /// There already; left as it was.
    Kept(PathBuf),
}

/// How long a node or client certificate is valid unless asked otherwise.
pub const LEAF_DAYS: u32 = 365;

/// How long a CA made by this command is valid.
pub const CA_DAYS: u32 = 3650;

/// How long a revocation list says it is good for. Nodes do not refuse an
/// older list (rustls does not by default), so this is for other readers.
const CRL_DAYS: i64 = 3650;

/// What `revoke` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revoked {
    /// The certificate's serial number, hex.
    pub serial: String,
    /// Whether the list named it already.
    pub already: bool,
    /// The list written.
    pub crl: PathBuf,
}

struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
}

/// The CA key's path: the CA certificate's, with `.key`.
pub fn ca_key_path(ca: &Path) -> PathBuf {
    ca.with_extension("key")
}

/// Makes the CA (unless there is one) and every node's certificate and key
/// (unless there are), at the paths of the file's `[tls]` section.
/// Certificates made now are valid for `days`. If `[tls]` names a revocation
/// list that does not exist yet, an empty one is written, so nodes start.
pub fn node_certs(file: &ClusterFile, days: u32) -> Result<Vec<Outcome>, CertError> {
    let ca_path = PathBuf::from(file.tls.ca.as_deref().ok_or(CertError::NoTls)?);
    let mut out = Vec::new();
    let ca = load_or_make_ca(&ca_path, &mut out)?;
    if let Some(crl) = file.crl_path() {
        if crl.exists() {
            out.push(Outcome::Kept(crl));
        } else {
            write(&crl, &revocation_list(&ca, Vec::new(), 1)?, false)?;
            out.push(Outcome::Wrote(crl));
        }
    }
    for node in &file.nodes {
        let (_, cert, key) = file.tls_paths(node.id).ok_or(CertError::NoTls)?;
        if cert.exists() && key.exists() {
            out.push(Outcome::Kept(cert));
            out.push(Outcome::Kept(key));
            continue;
        }
        let uri = format!("{}{}", lotse_rt::PeerIdentity::NODE_URI, node.id);
        let mut sans = vec![SanType::URI(uri.try_into().map_err(|e| cert_err("the node URI", e))?)];
        let mut hosts: Vec<&str> = [&node.peer, &node.client].iter().filter_map(|a| host_of(a)).collect();
        hosts.dedup();
        for h in hosts {
            sans.push(san(h)?);
        }
        let (pem, key_pem) = issue(&ca, &format!("node-{}", node.id), sans, true, days)?;
        write(&cert, &pem, false)?;
        write(&key, &key_pem, true)?;
        out.push(Outcome::Wrote(cert));
        out.push(Outcome::Wrote(key));
    }
    Ok(out)
}

/// A client certificate for `principal` (its common name, which a node
/// takes as the principal), signed by the cluster's CA, as
/// `<out>/<principal>.pem` and `.key`, valid for `days`. Never over
/// existing files.
pub fn client_cert(file: &ClusterFile, principal: &str, out_dir: &Path, days: u32) -> Result<Vec<Outcome>, CertError> {
    let ok = |c: char| c.is_ascii_alphanumeric() || "._-@".contains(c);
    if principal.is_empty() || !principal.chars().all(ok) || principal.starts_with('.') {
        return Err(CertError::BadName(principal.into()));
    }
    let ca_path = PathBuf::from(file.tls.ca.as_deref().ok_or(CertError::NoTls)?);
    if !ca_path.exists() || !ca_key_path(&ca_path).exists() {
        return Err(CertError::NoCa(ca_key_path(&ca_path).display().to_string()));
    }
    let ca = load_or_make_ca(&ca_path, &mut Vec::new())?;
    let cert = out_dir.join(format!("{principal}.pem"));
    let key = out_dir.join(format!("{principal}.key"));
    for p in [&cert, &key] {
        if p.exists() {
            return Err(CertError::Io {
                path: p.display().to_string(),
                source: std::io::Error::new(std::io::ErrorKind::AlreadyExists, "exists already; not overwritten"),
            });
        }
    }
    let (pem, key_pem) = issue(&ca, principal, Vec::new(), false, days)?;
    write(&cert, &pem, false)?;
    write(&key, &key_pem, true)?;
    Ok(vec![Outcome::Wrote(cert), Outcome::Wrote(key)])
}

/// Adds the certificate in the PEM file `cert` to the revocation list
/// `[tls] crl` names, signed by the cluster's CA. Running nodes refuse it
/// and end its connections once the new list reaches them.
pub fn revoke(file: &ClusterFile, cert: &Path) -> Result<Revoked, CertError> {
    let crl_path = file.crl_path().ok_or(CertError::NoCrl)?;
    let ca_path = PathBuf::from(file.tls.ca.as_deref().ok_or(CertError::NoTls)?);
    if !ca_path.exists() || !ca_key_path(&ca_path).exists() {
        return Err(CertError::NoCa(ca_key_path(&ca_path).display().to_string()));
    }
    let ca = load_or_make_ca(&ca_path, &mut Vec::new())?;
    let ca_pem = read(&ca_path)?;
    let (_, ca_block) = x509_parser::pem::parse_x509_pem(ca_pem.as_bytes()).map_err(|e| cert_err("the CA", e))?;
    let ca_parsed = ca_block.parse_x509().map_err(|e| cert_err("the CA", e))?;

    let what = cert.display().to_string();
    let pem = read(cert)?;
    let (_, block) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).map_err(|e| cert_err(&what, e))?;
    let parsed = block.parse_x509().map_err(|e| cert_err(&what, e))?;
    if parsed.issuer().as_raw() != ca_parsed.subject().as_raw() {
        return Err(CertError::NotOurs(what));
    }
    let serial = parsed.raw_serial().to_vec();

    // What the list says so far: its entries are kept, its number goes up.
    let mut revoked = Vec::new();
    let mut number = 1u64;
    if crl_path.exists() {
        let text = read(&crl_path)?;
        let crl_what = crl_path.display().to_string();
        let (_, block) = x509_parser::pem::parse_x509_pem(text.as_bytes()).map_err(|e| cert_err(&crl_what, e))?;
        let (_, list) =
            <x509_parser::revocation_list::CertificateRevocationList as x509_parser::prelude::FromDer<_>>::from_der(
                &block.contents,
            )
            .map_err(|e| cert_err(&crl_what, e))?;
        if list.issuer().as_raw() != ca_parsed.subject().as_raw() {
            return Err(CertError::Cert { what: crl_what, detail: "was not signed by the cluster's CA".into() });
        }
        if let Some(n) = list.crl_number() {
            number = n.to_bytes_be().iter().fold(0u64, |acc, b| acc.saturating_mul(256).saturating_add(u64::from(*b)));
        }
        number = number.saturating_add(1);
        for r in list.iter_revoked_certificates() {
            revoked.push(RevokedCertParams {
                serial_number: SerialNumber::from_slice(r.raw_serial()),
                revocation_time: r.revocation_date.to_datetime(),
                reason_code: None,
                invalidity_date: None,
            });
        }
    }
    let already = revoked.iter().any(|r| r.serial_number.as_ref() == serial.as_slice());
    if !already {
        revoked.push(RevokedCertParams {
            serial_number: SerialNumber::from_slice(&serial),
            revocation_time: now(),
            reason_code: None,
            invalidity_date: None,
        });
    }
    replace(&crl_path, &revocation_list(&ca, revoked, number)?)?;
    let serial = serial.iter().map(|b| format!("{b:02x}")).collect();
    Ok(Revoked { serial, already, crl: crl_path })
}

/// A certificate's expiry, read from a PEM file: the seconds until it ends
/// (negative once it has).
pub fn expires_in(path: &Path) -> Result<i64, CertError> {
    let what = path.display().to_string();
    let pem = read(path)?;
    let (_, block) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).map_err(|e| cert_err(&what, e))?;
    let parsed = block.parse_x509().map_err(|e| cert_err(&what, e))?;
    Ok((parsed.validity().not_after.to_datetime() - now()).whole_seconds())
}

/// The command line runs outside any node, on the real clock.
fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn revocation_list(ca: &Ca, revoked: Vec<RevokedCertParams>, number: u64) -> Result<String, CertError> {
    let now = now();
    let params = CertificateRevocationListParams {
        this_update: now,
        next_update: now + Duration::days(CRL_DAYS),
        crl_number: SerialNumber::from(number),
        issuing_distribution_point: None,
        revoked_certs: revoked,
        key_identifier_method: KeyIdMethod::Sha256,
    };
    let list = params.signed_by(&ca.cert, &ca.key).map_err(|e| cert_err("the revocation list", e))?;
    list.pem().map_err(|e| cert_err("the revocation list", e))
}

fn load_or_make_ca(path: &Path, out: &mut Vec<Outcome>) -> Result<Ca, CertError> {
    let key_path = ca_key_path(path);
    if path.exists() || key_path.exists() {
        let cert_pem = read(path)?;
        let key_pem = read(&key_path)?;
        let key = KeyPair::from_pem(&key_pem).map_err(|e| cert_err("the CA key", e))?;
        let (_, pem) =
            x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).map_err(|e| cert_err("the CA certificate", e))?;
        let parsed = pem.parse_x509().map_err(|e| cert_err("the CA certificate", e))?;
        if parsed.public_key().subject_public_key.data.as_ref() != key.public_key_raw() {
            return Err(CertError::Cert {
                what: key_path.display().to_string(),
                detail: format!("is not the key of {}", path.display()),
            });
        }
        // Signing needs the CA as rcgen sees it: its name and key, which
        // is all that ends up in what it issues.
        let params = CertificateParams::from_ca_cert_pem(&cert_pem).map_err(|e| cert_err("the CA certificate", e))?;
        let cert = params.self_signed(&key).map_err(|e| cert_err("the CA certificate", e))?;
        out.push(Outcome::Kept(path.to_path_buf()));
        out.push(Outcome::Kept(key_path));
        return Ok(Ca { cert, key });
    }
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.distinguished_name.push(DnType::CommonName, "Lotse cluster CA");
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    (params.not_before, params.not_after) = validity(CA_DAYS);
    let key = KeyPair::generate().map_err(|e| cert_err("the CA key", e))?;
    let cert = params.self_signed(&key).map_err(|e| cert_err("the CA certificate", e))?;
    write(path, &cert.pem(), false)?;
    write(&key_path, &key.serialize_pem(), true)?;
    out.push(Outcome::Wrote(path.to_path_buf()));
    out.push(Outcome::Wrote(key_path));
    Ok(Ca { cert, key })
}

/// From a little before now, for clocks slightly behind, to `days` ahead.
fn validity(days: u32) -> (OffsetDateTime, OffsetDateTime) {
    let now = now();
    (now - Duration::minutes(5), now + Duration::days(i64::from(days)))
}

fn issue(
    ca: &Ca,
    common_name: &str,
    sans: Vec<SanType>,
    server: bool,
    days: u32,
) -> Result<(String, String), CertError> {
    let mut params = CertificateParams::default();
    (params.not_before, params.not_after) = validity(days);
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.subject_alt_names = sans;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = if server {
        vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth]
    } else {
        vec![ExtendedKeyUsagePurpose::ClientAuth]
    };
    params.use_authority_key_identifier_extension = true;
    let key = KeyPair::generate().map_err(|e| cert_err(common_name, e))?;
    let cert = params.signed_by(&key, &ca.cert, &ca.key).map_err(|e| cert_err(common_name, e))?;
    Ok((cert.pem(), key.serialize_pem()))
}

fn san(host: &str) -> Result<SanType, CertError> {
    Ok(match host.parse::<std::net::IpAddr>() {
        Ok(ip) => SanType::IpAddress(ip),
        Err(_) => SanType::DnsName(host.try_into().map_err(|e| cert_err(host, e))?),
    })
}

fn cert_err(what: &str, e: impl std::fmt::Display) -> CertError {
    CertError::Cert { what: what.into(), detail: e.to_string() }
}

fn read(path: &Path) -> Result<String, CertError> {
    std::fs::read_to_string(path).map_err(|source| CertError::Io { path: path.display().to_string(), source })
}

/// Replaces a file whole: written beside it, then renamed over it, so a node
/// reading it sees the old list or the new one, never half.
fn replace(path: &Path, text: &str) -> Result<(), CertError> {
    let io = |source| CertError::Io { path: path.display().to_string(), source };
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    write(&tmp, text, false)?;
    std::fs::rename(&tmp, path).map_err(io)
}

/// Writes a new file; a key is readable by its owner only.
fn write(path: &Path, text: &str, secret: bool) -> Result<(), CertError> {
    let io = |source| CertError::Io { path: path.display().to_string(), source };
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(io)?;
    }
    let mut open = std::fs::OpenOptions::new();
    open.write(true).create_new(true);
    #[cfg(unix)]
    if secret {
        std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
    }
    #[cfg(not(unix))]
    let _ = secret;
    use std::io::Write as _;
    let mut f = open.open(path).map_err(io)?;
    f.write_all(text.as_bytes()).and_then(|()| f.sync_all()).map_err(io)
}
