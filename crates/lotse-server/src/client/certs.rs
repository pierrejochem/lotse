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
    CertificateEntry, DescribeCertificatesResponse, ErrorCode, RecordCertificatesRequest, Response, certificate_state,
};
use lotse_rt::Runtime;

use super::admin::{AdminThen, admin_response};
use super::{Clients, ClusterView, Outbox};
use crate::metadata::{CertKey, Change, SeenCert};
use crate::replica::StateMachine;

type Groups<R, S> = std::collections::BTreeMap<crate::wire::GroupId, crate::replica::Replica<R, S>>;

/// Most certificates in one report.
pub(crate) const MAX_REPORTED: usize = 256;

/// A report within this of what the state holds changes nothing.
pub(crate) const LAST_SEEN_EVERY_MS: u64 = 3_600_000;

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
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
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
