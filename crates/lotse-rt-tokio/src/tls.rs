//! TLS 1.3 for both kinds of link, with rustls.
//!
//! One certificate authority, the cluster's, vouches for everything:
//!
//! - **Node links** need a node certificate on both ends (the URI
//!   `lotse://node/<id>` among its subject alternative names). The chain is
//!   checked against the CA but not the host name: nodes dial each other by
//!   address, and the node itself checks that the certificate names the node
//!   it meant to reach.
//! - **Client links** check the server's certificate against the CA and the
//!   host name dialled, as any TLS client does, and take a client
//!   certificate if one is offered; its subject's common name is the
//!   principal.
//!
//! Only TLS 1.3 is spoken.
//!
//! A certificate revocation list may come with the CA (ADR 0060): every
//! handshake checks the other end's certificate against it, and a runtime
//! given new material ([`crate::TokioRuntime::reload_tls`]) closes the
//! connections of certificates it now revokes. Only end-entity certificates
//! are checked; a certificate whose issuer has no list here is not revoked.
//! Each CA certificate of the bundle verifies on its own, with only the
//! lists its key signed: webpki takes the first list whose issuer has the
//! certificate's issuer name, so with two CAs of one name (a rotation that
//! keeps it) another CA's list would refuse every certificate of the
//! other. A handshake passes if one CA's verification passes. Closing
//! connections on a reload matches the issuer's name and, where the list
//! and the certificate both carry one, the authority key identifier
//! (ADR 0096).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use lotse_rt::{Link, PeerIdentity};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};

/// What is wrong with TLS material.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file holds nothing usable.
    #[error("{what}: {detail}")]
    Pem {
        /// Which input.
        what: &'static str,
        /// What went wrong.
        detail: String,
    },
    /// rustls refused the configuration.
    #[error("the TLS configuration is not usable: {0}")]
    Config(#[from] rustls::Error),
    /// A file could not be read.
    #[error("cannot read {path}: {source}")]
    Io {
        /// The file.
        path: String,
        /// Why.
        source: std::io::Error,
    },
}

/// The cluster's CA and, for a node or a client with a certificate, this
/// end's certificate chain and key. Build it once; it is cheap to clone.
#[derive(Clone)]
pub struct TlsConfig {
    pub(crate) inner: Arc<Tls>,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig").field("has_certificate", &self.inner.node_server.is_some()).finish()
    }
}

/// Revoked certificates by issuer name and serial (both DER), with the
/// authority key identifier of each list that names them (`None`: a list
/// without one, which matches by name).
type Revoked = BTreeMap<(Vec<u8>, Vec<u8>), Vec<Option<Vec<u8>>>>;

pub(crate) struct Tls {
    /// Every certificate the revocation lists name, by issuer name and
    /// serial (both DER), with the authority key identifier of each list
    /// that names it (`None`: a list without one, which matches by name).
    revoked: Revoked,
    node_server: Option<Arc<ServerConfig>>,
    client_server: Option<Arc<ServerConfig>>,
    node_client: Arc<ClientConfig>,
    client_client: Arc<ClientConfig>,
}

impl TlsConfig {
    /// From PEM: the CA certificate(s) to trust, and optionally this end's
    /// certificate chain and private key. A node needs both; a client needs
    /// them only to authenticate with a certificate.
    pub fn from_pem(ca: &[u8], identity: Option<(&[u8], &[u8])>) -> Result<TlsConfig, TlsError> {
        TlsConfig::from_pem_with_crl(ca, identity, None)
    }

    /// [`TlsConfig::from_pem`] with a certificate revocation list (PEM, one
    /// or more `X509 CRL` blocks) that every handshake checks.
    pub fn from_pem_with_crl(
        ca: &[u8],
        identity: Option<(&[u8], &[u8])>,
        crl: Option<&[u8]>,
    ) -> Result<TlsConfig, TlsError> {
        let mut crls = Vec::new();
        let mut revoked = Revoked::new();
        if let Some(pem) = crl {
            for der in CertificateRevocationListDer::pem_slice_iter(pem) {
                let der = der.map_err(|e| TlsError::Pem { what: "the revocation list", detail: e.to_string() })?;
                let (_, list) = <x509_parser::revocation_list::CertificateRevocationList as x509_parser::prelude::FromDer<_>>::from_der(der.as_ref())
                    .map_err(|e| TlsError::Pem { what: "the revocation list", detail: e.to_string() })?;
                let issuer = list.issuer().as_raw().to_vec();
                let key_id = list.extensions().iter().find_map(|e| match e.parsed_extension() {
                    x509_parser::extensions::ParsedExtension::AuthorityKeyIdentifier(a) => {
                        a.key_identifier.as_ref().map(|k| k.0.to_vec())
                    }
                    _ => None,
                });
                for r in list.iter_revoked_certificates() {
                    revoked.entry((issuer.clone(), r.raw_serial().to_vec())).or_default().push(key_id.clone());
                }
                crls.push(der);
            }
            if crls.is_empty() {
                return Err(TlsError::Pem { what: "the revocation list", detail: "no CRL found".into() });
            }
        }
        let cas: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ca)
            .collect::<Result<_, _>>()
            .map_err(|e| TlsError::Pem { what: "the CA certificate", detail: e.to_string() })?;
        if cas.is_empty() {
            return Err(TlsError::Pem { what: "the CA certificate", detail: "no certificate found".into() });
        }
        // One anchor per CA certificate, with the lists it signed. A list
        // no CA here signed names no certificate a handshake lets in.
        let mut anchors = Vec::new();
        for ca in &cas {
            let mut roots = RootCertStore::empty();
            roots.add(ca.clone())?;
            let (_, parsed) = x509_parser::parse_x509_certificate(ca.as_ref())
                .map_err(|e| TlsError::Pem { what: "the CA certificate", detail: e.to_string() })?;
            let signed: Vec<CertificateRevocationListDer<'static>> = crls
                .iter()
                .filter(|der| {
                    <x509_parser::revocation_list::CertificateRevocationList as x509_parser::prelude::FromDer<_>>::from_der(
                        der.as_ref(),
                    )
                    .is_ok_and(|(_, l)| {
                        l.issuer().as_raw() == parsed.subject().as_raw()
                            && l.verify_signature(parsed.public_key()).is_ok()
                    })
                })
                .cloned()
                .collect();
            anchors.push((Arc::new(roots), signed));
        }
        let identity = match identity {
            Some((chain, key)) => {
                let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain)
                    .collect::<Result<_, _>>()
                    .map_err(|e| TlsError::Pem { what: "the certificate", detail: e.to_string() })?;
                if chain.is_empty() {
                    return Err(TlsError::Pem { what: "the certificate", detail: "no certificate found".into() });
                }
                let key = PrivateKeyDer::from_pem_slice(key)
                    .map_err(|e| TlsError::Pem { what: "the private key", detail: e.to_string() })?;
                Some((chain, key))
            }
            None => None,
        };
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let versions = &[&rustls::version::TLS13];

        let server = |required: bool| -> Result<Option<Arc<ServerConfig>>, TlsError> {
            let Some((chain, key)) = &identity else { return Ok(None) };
            let mut each = Vec::new();
            for (roots, crls) in &anchors {
                let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
                    .with_crls(crls.clone())
                    .only_check_end_entity_revocation()
                    .allow_unknown_revocation_status();
                let verifier = if required { verifier } else { verifier.allow_unauthenticated() };
                each.push(verifier.build().map_err(|e| rustls::Error::General(e.to_string()))?);
            }
            let verifier = Arc::new(AnyClientVerifier::new(each));
            let mut cfg = ServerConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(versions)?
                .with_client_cert_verifier(verifier)
                .with_single_cert(chain.clone(), key.clone_key())?;
            // No session resumption: connections are long-lived. Tickets would
            // also sit unread at a client that only sends, and closing a socket
            // with unread data resets it, losing frames still in flight.
            cfg.send_tls13_tickets = 0;
            Ok(Some(Arc::new(cfg)))
        };
        let mut each: Vec<Arc<dyn ServerCertVerifier>> = Vec::new();
        for (roots, crls) in &anchors {
            each.push(
                WebPkiServerVerifier::builder_with_provider(roots.clone(), provider.clone())
                    .with_crls(crls.clone())
                    .only_check_end_entity_revocation()
                    .allow_unknown_revocation_status()
                    .build()
                    .map_err(|e| rustls::Error::General(e.to_string()))?,
            );
        }
        let webpki: Arc<dyn ServerCertVerifier> = Arc::new(AnyServerVerifier(each));
        let client = |verifier: Arc<dyn ServerCertVerifier>| -> Result<Arc<ClientConfig>, TlsError> {
            let builder = ClientConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(versions)?
                .dangerous()
                .with_custom_certificate_verifier(verifier);
            let cfg = match &identity {
                Some((chain, key)) => builder.with_client_auth_cert(chain.clone(), key.clone_key())?,
                None => builder.with_no_client_auth(),
            };
            Ok(Arc::new(cfg))
        };
        Ok(TlsConfig {
            inner: Arc::new(Tls {
                revoked,
                node_server: server(true)?,
                client_server: server(false)?,
                node_client: client(Arc::new(ChainOnly(webpki.clone())))?,
                client_client: client(webpki)?,
            }),
        })
    }

    /// [`TlsConfig::from_pem`] from files.
    pub fn from_files(
        ca: impl AsRef<std::path::Path>,
        identity: Option<(impl AsRef<std::path::Path>, impl AsRef<std::path::Path>)>,
    ) -> Result<TlsConfig, TlsError> {
        TlsConfig::from_files_with_crl(ca, identity, None::<&std::path::Path>)
    }

    /// [`TlsConfig::from_pem_with_crl`] from files.
    pub fn from_files_with_crl(
        ca: impl AsRef<std::path::Path>,
        identity: Option<(impl AsRef<std::path::Path>, impl AsRef<std::path::Path>)>,
        crl: Option<impl AsRef<std::path::Path>>,
    ) -> Result<TlsConfig, TlsError> {
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|source| TlsError::Io { path: p.display().to_string(), source })
        };
        let ca = read(ca.as_ref())?;
        let identity = match identity {
            Some((c, k)) => Some((read(c.as_ref())?, read(k.as_ref())?)),
            None => None,
        };
        let crl = match crl {
            Some(p) => Some(read(p.as_ref())?),
            None => None,
        };
        TlsConfig::from_pem_with_crl(&ca, identity.as_ref().map(|(c, k)| (c.as_slice(), k.as_slice())), crl.as_deref())
    }

    /// Whether the revocation list names `cert` (an end-entity certificate,
    /// DER), by its issuer and serial number: a list of its issuer's name
    /// whose authority key identifier is the certificate's, or one of
    /// them lacks it.
    pub fn revokes(&self, cert: &[u8]) -> bool {
        if self.inner.revoked.is_empty() {
            return false;
        }
        match x509_parser::parse_x509_certificate(cert) {
            Ok((_, c)) => {
                let Some(lists) = self.inner.revoked.get(&(c.issuer().as_raw().to_vec(), c.raw_serial().to_vec()))
                else {
                    return false;
                };
                let key_id = lotse_auth::x509::authority_key_id(&c);
                lists.iter().any(|l| match (l, key_id) {
                    (Some(l), Some(k)) => l == k,
                    _ => true,
                })
            }
            // It passed a handshake, so it parses; if not, it is no
            // certificate this list could name.
            Err(_) => false,
        }
    }

    pub(crate) fn server(&self, link: Link) -> Option<Arc<ServerConfig>> {
        match link {
            Link::Node => self.inner.node_server.clone(),
            Link::Client => self.inner.client_server.clone(),
        }
    }

    pub(crate) fn client(&self, link: Link) -> Arc<ClientConfig> {
        match link {
            Link::Node => self.inner.node_client.clone(),
            Link::Client => self.inner.client_client.clone(),
        }
    }
}

/// Checks the chain against the CA and ignores the host name; the node
/// checks the node id the certificate names instead.
#[derive(Debug)]
struct ChainOnly(Arc<dyn ServerCertVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // webpki checks the chain (signatures, validity, usage) before the
        // name, so a name mismatch means everything else held.
        match self.0.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// Of several verifications' errors, the most telling: one from a CA that
/// knew the certificate's issuer rather than an unknown issuer.
fn telling(errors: Vec<rustls::Error>) -> rustls::Error {
    let unknown = |e: &rustls::Error| matches!(e, rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer));
    let mut errors = errors.into_iter();
    let first = errors.next().unwrap_or(rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer));
    if !unknown(&first) {
        return first;
    }
    errors.find(|e| !unknown(e)).unwrap_or(first)
}

/// A client certificate passes if one CA's verification, with that CA's
/// own revocation lists, passes.
#[derive(Debug)]
struct AnyClientVerifier {
    each: Vec<Arc<dyn ClientCertVerifier>>,
    hints: Vec<rustls::DistinguishedName>,
}

impl AnyClientVerifier {
    fn new(each: Vec<Arc<dyn ClientCertVerifier>>) -> AnyClientVerifier {
        let mut hints = Vec::new();
        for v in &each {
            for h in v.root_hint_subjects() {
                if !hints.iter().any(|k: &rustls::DistinguishedName| k.as_ref() == h.as_ref()) {
                    hints.push(h.clone());
                }
            }
        }
        AnyClientVerifier { each, hints }
    }
}

impl ClientCertVerifier for AnyClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.each[0].offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.each[0].client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &self.hints
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let mut errors = Vec::new();
        for v in &self.each {
            match v.verify_client_cert(end_entity, intermediates, now) {
                Ok(ok) => return Ok(ok),
                Err(e) => errors.push(e),
            }
        }
        Err(telling(errors))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.each[0].verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.each[0].verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.each[0].supported_verify_schemes()
    }
}

/// A server certificate passes if one CA's verification, with that CA's
/// own revocation lists, passes.
#[derive(Debug)]
struct AnyServerVerifier(Vec<Arc<dyn ServerCertVerifier>>);

impl ServerCertVerifier for AnyServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let mut errors = Vec::new();
        for v in &self.0 {
            match v.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
                Ok(ok) => return Ok(ok),
                Err(e) => errors.push(e),
            }
        }
        Err(telling(errors))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0[0].verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0[0].verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0[0].supported_verify_schemes()
    }
}

/// Who a verified end-entity certificate names: a node if it carries exactly
/// one node URI, otherwise the principal in its subject's common name.
/// `None` for a certificate that names nobody, or claims two nodes.
pub(crate) fn identity_of(cert: &CertificateDer<'_>) -> Option<PeerIdentity> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    let mut nodes = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::URI(uri) = name
                && uri.starts_with(PeerIdentity::NODE_URI)
            {
                // A malformed node URI makes the certificate name nobody,
                // rather than falling through to its common name.
                nodes.push(PeerIdentity::node_from_uri(uri)?);
            }
        }
    }
    match nodes.as_slice() {
        [id] => return Some(PeerIdentity::Node(*id)),
        [] => {}
        _ => return None,
    }
    let cn = cert.subject().iter_common_name().next()?.as_str().ok()?;
    (!cn.is_empty()).then(|| PeerIdentity::Principal(cn.to_owned()))
}
