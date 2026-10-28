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

use std::fmt;
use std::sync::Arc;

use lotse_rt::{Link, PeerIdentity};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
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

pub(crate) struct Tls {
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
        let mut roots = RootCertStore::empty();
        let mut any = false;
        for cert in CertificateDer::pem_slice_iter(ca) {
            let cert = cert.map_err(|e| TlsError::Pem { what: "the CA certificate", detail: e.to_string() })?;
            roots.add(cert)?;
            any = true;
        }
        if !any {
            return Err(TlsError::Pem { what: "the CA certificate", detail: "no certificate found".into() });
        }
        let roots = Arc::new(roots);
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
            let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone());
            let verifier = if required { verifier } else { verifier.allow_unauthenticated() };
            let verifier = verifier.build().map_err(|e| rustls::Error::General(e.to_string()))?;
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
        let webpki = WebPkiServerVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|e| rustls::Error::General(e.to_string()))?;
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
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|source| TlsError::Io { path: p.display().to_string(), source })
        };
        let ca = read(ca.as_ref())?;
        let identity = match identity {
            Some((c, k)) => Some((read(c.as_ref())?, read(k.as_ref())?)),
            None => None,
        };
        TlsConfig::from_pem(&ca, identity.as_ref().map(|(c, k)| (c.as_slice(), k.as_slice())))
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
struct ChainOnly(Arc<WebPkiServerVerifier>);

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

/// Who a verified end-entity certificate names: a node if it carries exactly
/// one node URI, otherwise the principal in its subject's common name.
/// `None` for a certificate that names nobody, or claims two nodes.
pub(crate) fn identity_of(cert: &CertificateDer<'_>) -> Option<PeerIdentity> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    let mut nodes = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::URI(uri) = name {
                if uri.starts_with(PeerIdentity::NODE_URI) {
                    // A malformed node URI makes the certificate name nobody,
                    // rather than falling through to its common name.
                    nodes.push(PeerIdentity::node_from_uri(uri)?);
                }
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
