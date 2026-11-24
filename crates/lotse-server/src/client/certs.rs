//! The client certificate inventory (ADR 0091): nodes report the
//! certificates they accept (`RecordCertificates`, from nodes only) and a
//! cluster admin lists them (`DescribeCertificates`), together with what was
//! revoked (`RevokeCertificate`, ADR 0090).
//!
//! Both are served by the metadata group's leader from its applied state.
//! A report that adds nothing the state does not hold already is answered
//! without a write, so reconnecting clients cost the metadata group nothing.

use std::sync::Arc;

use lotse_proto::{
    CertificateEntry, CertificateRequestEntry, DescribeCertificateRequestsResponse, DescribeCertificatesResponse,
    ErrorCode, RecordCertificateRequest, RecordCertificatesRequest, RequestCertificateRequest, Response,
    certificate_request_state, certificate_state,
};
use lotse_rt::Runtime;
use sha2::{Digest, Sha256};

use super::admin::{AdminThen, admin_response, renewed_response};
use super::{Clients, ClusterView, Outbox};
use crate::auth::Principal;
use crate::metadata::{CertKey, CertRequest, Change, REQUEST_TTL_MS, SeenCert};
use crate::replica::StateMachine;

type Groups<R, S> = std::collections::BTreeMap<crate::wire::GroupId, crate::replica::Replica<R, S>>;

/// Most certificates in one report.
pub(crate) const MAX_REPORTED: usize = 256;

/// A report within this of what the state holds changes nothing.
pub(crate) const LAST_SEEN_EVERY_MS: u64 = 3_600_000;

/// Longest signing request, bytes: an ECDSA one is about 200.
const MAX_CSR: usize = 4096;

/// Longest certificate a CA holder records, bytes.
const MAX_CERT: usize = 8192;

/// Most requests the state holds at once: a request is cheap to make and
/// stays for 7 days, or 30 once signed, so the number is bounded.
const MAX_REQUESTS: usize = 256;

/// Most of those a user asked for in their own name: the rest stays for
/// cluster admins (ADR 0119).
const MAX_OWN_REQUESTS: usize = 192;

/// Longest principal name in a request, bytes; as `lotse certs --client`
/// takes it, so the CA holder can sign it.
const MAX_PRINCIPAL: usize = 128;

/// A principal name a request may carry: what `lotse certs --client` signs.
fn principal_ok(name: &str) -> bool {
    let ok = |c: char| c.is_ascii_alphanumeric() || "._-@".contains(c);
    !name.is_empty() && name.len() <= MAX_PRINCIPAL && name.chars().all(ok) && !name.starts_with('.')
}

/// "Expiring" means ending within this: 30 days.
const EXPIRING_MS: u64 = 30 * 24 * 3_600_000;

impl Clients {
    /// `RecordCertificates`: a node reports client certificates it accepted.
    pub(super) fn record_certificates<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: RecordCertificatesRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let invalid = |why: &str| outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(why), None));
        if req.certificates.len() > MAX_REPORTED {
            return invalid("a report holds at most 256 certificates");
        }
        let mut seen = Vec::with_capacity(req.certificates.len());
        for c in &req.certificates {
            let Ok(issuer) = <[u8; 32]>::try_from(&c.issuer[..]) else {
                return invalid("the issuer is the 32 bytes of a SHA-256");
            };
            if c.serial.is_empty() || c.serial.len() > 64 || c.principal.len() > 256 {
                return invalid("a serial has 1 to 64 bytes and a principal at most 256");
            }
            let key = CertKey { issuer, serial: c.serial.to_vec() };
            let cert = SeenCert {
                principal: c.principal.clone(),
                not_before_ms: c.not_before_ms,
                not_after_ms: c.not_after_ms,
                first_seen_ms: c.seen_ms,
                last_seen_ms: c.seen_ms,
            };
            seen.push((key, cert));
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        // Nothing new: every certificate known, and seen lately enough.
        let known = &replica.metadata().certs;
        seen.retain(|(k, c)| {
            !known.get(k).is_some_and(|k| k.last_seen_ms.saturating_add(LAST_SEEN_EVERY_MS) > c.last_seen_ms)
        });
        if seen.is_empty() {
            return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
        }
        let change = Change::RecordCertificates { seen, at_ms: view.unix_ms };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `DescribeCertificates`: the inventory and the revocations, by expiry.
    pub(super) fn describe_certificates<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        at_least: Option<u64>,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_read(view, groups, at_least) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeCertificatesResponse { error, leader_hint, now_ms: 0, certificates: Vec::new() };
                return outbox.push(id, &Response::DescribeCertificates(resp));
            }
        };
        let state = replica.metadata();
        let now = view.unix_ms;
        let mut keys: Vec<&CertKey> = state.certs.keys().chain(state.revoked_certs.keys()).collect();
        keys.sort();
        keys.dedup();
        let mut certificates: Vec<CertificateEntry> = keys
            .into_iter()
            .map(|key| {
                let seen = state.certs.get(key);
                let revoked = state.revoked_certs.get(key);
                let not_after_ms = seen.map(|c| c.not_after_ms).or(revoked.map(|r| r.not_after_ms)).unwrap_or(0);
                let state_of = if revoked.is_some() {
                    certificate_state::REVOKED
                } else if not_after_ms < now {
                    certificate_state::EXPIRED
                } else if not_after_ms - now < EXPIRING_MS {
                    certificate_state::EXPIRING
                } else {
                    certificate_state::VALID
                };
                CertificateEntry {
                    issuer: key.issuer.to_vec().into(),
                    serial: key.serial.clone().into(),
                    principal: seen.map(|c| c.principal.clone()).unwrap_or_default(),
                    not_before_ms: seen.map_or(0, |c| c.not_before_ms),
                    not_after_ms,
                    first_seen_ms: seen.map_or(0, |c| c.first_seen_ms),
                    last_seen_ms: seen.map_or(0, |c| c.last_seen_ms),
                    state: state_of,
                    reason: revoked.map(|r| r.reason.clone()).unwrap_or_default(),
                    revoked_at_ms: revoked.map_or(0, |r| r.at_ms),
                }
            })
            .collect();
        certificates.sort_by(|a, b| (a.not_after_ms, &a.serial).cmp(&(b.not_after_ms, &b.serial)));
        let resp =
            DescribeCertificatesResponse { error: ErrorCode::NONE, leader_hint: None, now_ms: now, certificates };
        outbox.push(id, &Response::DescribeCertificates(resp));
    }
}

impl Clients {
    /// `RequestCertificate`: a cluster admin asks the CA holder for a client
    /// certificate, or a user asks for one in their own name (ADR 0119).
    /// The request is the SHA-256 of its signing request, so the same one
    /// sent twice is one.
    pub(super) fn request_certificate<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: RequestCertificateRequest,
        principal: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let invalid = |why: &str| outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(why), None));
        // Access let a user who is not a cluster admin through only for
        // their own name.
        let own = !view.access.cluster_admin(principal);
        if !principal_ok(&req.principal) {
            if own {
                return invalid(&format!(
                    "the name {:?} cannot be a certificate's principal: a principal is letters, digits, `.`, `_`, \
                     `-` and `@`, at most 128 bytes, not starting with `.`; ask a cluster admin",
                    req.principal
                ));
            }
            return invalid(
                "a principal is letters, digits, `.`, `_`, `-` and `@`, at most 128 bytes, not starting with `.`",
            );
        }
        if req.csr.is_empty() || req.csr.len() > MAX_CSR {
            return invalid("a signing request has 1 to 4096 bytes");
        }
        let key: [u8; 32] = Sha256::digest(&req.csr).into();
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let known = &replica.metadata().cert_requests;
        if known.contains_key(&key) {
            return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
        }
        let change = if own {
            // The user's pending request goes when this one applies, so it
            // does not count against the caps.
            let replaced = |r: &CertRequest| r.own() && r.principal == req.principal && r.pending(view.unix_ms);
            let held = known.values().filter(|r| !replaced(r)).count();
            if held >= MAX_REQUESTS {
                return invalid("256 requests are open or recently signed; wait for the CA holder or until they lapse");
            }
            let own_held = known.values().filter(|r| r.own() && !replaced(r)).count();
            if own_held >= MAX_OWN_REQUESTS {
                return invalid(
                    "192 requests in their askers' own names are open or recently signed; wait for the CA holder \
                     or until they lapse, or ask a cluster admin",
                );
            }
            Change::RequestOwnCertificate {
                id: key,
                principal: req.principal,
                csr: req.csr.to_vec(),
                at_ms: view.unix_ms,
            }
        } else {
            if known.len() >= MAX_REQUESTS {
                return invalid("256 requests are open or recently signed; wait for the CA holder or until they lapse");
            }
            Change::RequestCertificate {
                id: key,
                principal: req.principal,
                csr: req.csr.to_vec(),
                requested_by: principal.to_string(),
                at_ms: view.unix_ms,
            }
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `RenewCertificate` (ADR 0123): the metadata leader signs a new
    /// certificate for the principal the connection's certificate names,
    /// with the request's key and the renewal CA, records it and answers
    /// with its chain once that record applied. Only a connection made with
    /// a client certificate in its renewal window may ask.
    pub(super) fn renew_certificate<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: lotse_proto::RenewCertificateRequest,
        principal: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let refuse = |code: ErrorCode, why: &str| outbox.push(id, &renewed_response(code, Some(why), None, &[], 0));
        let Some((cert, name)) = outbox.presented.clone() else {
            return refuse(
                ErrorCode::INVALID_REQUEST,
                "only a connection made with a client certificate renews it; sign in with the certificate",
            );
        };
        // A session that signed in again with SASL acts as someone else.
        if !matches!(principal, Principal::User { name: n, .. } if *n == name) {
            return refuse(ErrorCode::UNAUTHORIZED, "the session no longer acts as the certificate's principal");
        }
        if !lotse_auth::renew::may_renew(cert.not_before_ms, cert.not_after_ms, view.unix_ms) {
            let opens = lotse_auth::renew::window_opens_ms(cert.not_before_ms, cert.not_after_ms);
            return refuse(
                ErrorCode::INVALID_REQUEST,
                &format!(
                    "the certificate may be renewed from {opens} ms since the epoch, in its last 30 days or once two \
                     thirds of its validity have passed"
                ),
            );
        }
        if req.csr.is_empty() || req.csr.len() > MAX_CSR {
            return refuse(ErrorCode::INVALID_REQUEST, "a signing request has 1 to 4096 bytes");
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &renewed_response(code, None, h, &[], 0)),
        };
        let Some(ca) = view.admin.renewal.get() else {
            return refuse(
                ErrorCode::INVALID_REQUEST,
                "this cluster has no renewal CA ([tls] renewal_ca); ask the CA holder for a new certificate",
            );
        };
        let renews = CertKey { issuer: cert.issuer, serial: cert.serial.clone() };
        if !replica.metadata().room_for_renewal(&renews, view.unix_ms) {
            return refuse(ErrorCode::INVALID_REQUEST, "4096 renewed certificates are held; try again later");
        }
        let Some(random) = self.random.as_ref() else {
            return refuse(ErrorCode::INVALID_REQUEST, "this node draws no serial numbers");
        };
        let mut serial = [0u8; 16];
        serial[..8].copy_from_slice(&random().to_be_bytes());
        serial[8..].copy_from_slice(&random().to_be_bytes());
        let signed = match ca.sign(&req.csr, &name, serial, view.unix_ms) {
            Ok(s) => s,
            Err(e) => return refuse(ErrorCode::INVALID_REQUEST, &e.to_string()),
        };
        let key = CertKey { issuer: signed.facts.issuer, serial: signed.facts.serial.clone() };
        let change = Change::RenewCertificate {
            cert: key.clone(),
            principal: name,
            not_before_ms: signed.facts.not_before_ms,
            not_after_ms: signed.facts.not_after_ms,
            renews,
            at_ms: view.unix_ms,
        };
        let chain = vec![bytes::Bytes::from(signed.der), bytes::Bytes::copy_from_slice(ca.certificate())];
        let then = AdminThen::Renewed { cert: key, chain, not_after_ms: signed.facts.not_after_ms };
        self.propose_admin(replica, view.now, &change, (outbox, id), then);
    }

    /// `RecordCertificate`: the CA holder records what it signed.
    pub(super) fn record_certificate<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: RecordCertificateRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let invalid = |why: &str| outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(why), None));
        let (Ok(request), Ok(issuer)) = (<[u8; 32]>::try_from(&req.id[..]), <[u8; 32]>::try_from(&req.issuer[..]))
        else {
            return invalid("the request and the issuer are 32 bytes each");
        };
        if req.serial.is_empty()
            || req.serial.len() > 64
            || req.certificate.is_empty()
            || req.certificate.len() > MAX_CERT
        {
            return invalid("a serial has 1 to 64 bytes and a certificate 1 to 8192");
        }
        if req.not_after_ms <= req.not_before_ms {
            return invalid("a certificate ends after it starts");
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let open = replica
            .metadata()
            .cert_requests
            .get(&request)
            .filter(|r| r.signed.is_none() && r.at_ms.saturating_add(REQUEST_TTL_MS) >= view.unix_ms);
        let Some(open) = open else {
            return invalid("no open request has that id: it is unknown, signed already or lapsed");
        };
        // The certificate must answer the request and come from the
        // cluster's CA, and say of itself what the CA holder says of it
        // (ADR 0096).
        let cas = view.admin.client_cas.get();
        let facts = match lotse_auth::x509::check_signed(&req.certificate, &open.csr, &open.principal, &cas) {
            Ok(f) => f,
            Err(e) => return invalid(&e.to_string()),
        };
        if (facts.issuer, &facts.serial[..], facts.not_before_ms, facts.not_after_ms)
            != (issuer, &req.serial[..], req.not_before_ms, req.not_after_ms)
        {
            return invalid("the certificate's issuer, serial or validity is not what the request says");
        }
        let change = Change::RecordCertificate {
            id: request,
            certificate: req.certificate.to_vec(),
            cert: CertKey { issuer, serial: req.serial.to_vec() },
            not_before_ms: req.not_before_ms,
            not_after_ms: req.not_after_ms,
            at_ms: view.unix_ms,
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `DescribeCertificateRequests`: the requests, newest first; to a user
    /// who is not a cluster admin, only those they asked for (ADR 0119).
    pub(super) fn describe_certificate_requests<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        principal: &Principal,
        at_least: Option<u64>,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_read(view, groups, at_least) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeCertificateRequestsResponse { error, leader_hint, now_ms: 0, requests: Vec::new() };
                return outbox.push(id, &Response::DescribeCertificateRequests(resp));
            }
        };
        let now = view.unix_ms;
        let all = view.access.cluster_admin(principal);
        let asker = principal.to_string();
        let mine = |r: &CertRequest| all || r.requested_by == asker;
        let mut requests: Vec<CertificateRequestEntry> = replica
            .metadata()
            .cert_requests
            .iter()
            .filter(|(_, r)| mine(r))
            .map(|(key, r)| {
                let expires_at_ms = r.at_ms.saturating_add(REQUEST_TTL_MS);
                let state = match &r.signed {
                    Some(_) => certificate_request_state::SIGNED,
                    None if expires_at_ms < now => certificate_request_state::EXPIRED,
                    None => certificate_request_state::PENDING,
                };
                CertificateRequestEntry {
                    id: key.to_vec().into(),
                    principal: r.principal.clone(),
                    csr: r.csr.clone().into(),
                    requested_by: r.requested_by.clone(),
                    requested_at_ms: r.at_ms,
                    expires_at_ms,
                    state,
                    certificate: r.signed.as_ref().map(|s| s.certificate.clone()).unwrap_or_default().into(),
                    signed_at_ms: r.signed.as_ref().map_or(0, |s| s.at_ms),
                }
            })
            .collect();
        requests.sort_by(|a, b| (b.requested_at_ms, &b.id).cmp(&(a.requested_at_ms, &a.id)));
        let resp =
            DescribeCertificateRequestsResponse { error: ErrorCode::NONE, leader_hint: None, now_ms: now, requests };
        outbox.push(id, &Response::DescribeCertificateRequests(resp));
    }
}
