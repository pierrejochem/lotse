//! `DescribeSessions` (ADR 0121): a node lists its open client sessions.
//! Who sees the whole cluster gets all of them; anyone else only those made
//! with their own name.

use lotse_proto::{DescribeSessionsResponse, ErrorCode, Response, SessionEntry, session_principal};

use super::{Clients, ClusterView, Outbox};
use crate::access::Access;
use crate::auth::{OpenSession, Principal};

/// The sessions `who` may see of `open` at `now_ms`, oldest first: those
/// signed in and not yet ended.
pub(super) fn visible(access: &Access, who: &Principal, open: &[OpenSession], now_ms: u64) -> Vec<SessionEntry> {
    let all = access.sees_cluster(who);
    open.iter()
        .filter(|s| s.expires_ms == 0 || s.expires_ms > now_ms)
        .filter_map(|s| {
            let p = s.principal.as_ref()?;
            let mine = match (p, who) {
                (Principal::User { name, .. }, Principal::User { name: me, .. }) => name == me,
                _ => false,
            };
            (all || mine).then(|| entry(p, s))
        })
        .collect()
}

fn entry(p: &Principal, s: &OpenSession) -> SessionEntry {
    let (principal_kind, name, groups) = match p {
        Principal::Anonymous => (session_principal::ANONYMOUS, String::new(), Vec::new()),
        Principal::Node(id) => (session_principal::NODE, id.to_string(), Vec::new()),
        Principal::User { name, groups } => (session_principal::USER, name.clone(), groups.clone()),
    };
    SessionEntry {
        principal_kind,
        name,
        groups,
        mechanism: s.mechanism,
        peer: s.peer.clone(),
        connected_ms: s.connected_ms,
        credential_ms: s.credential_ms,
        expires_ms: s.expires_ms,
        // Task 5 fills it from the session's token.
        token_id: None,
    }
}

impl Clients {
    /// Answers `DescribeSessions` for this node.
    pub(super) fn describe_sessions(&self, outbox: &Outbox, id: u32, view: &ClusterView<'_>, who: &Principal) {
        let sessions = visible(view.access, who, &view.node.sessions.list(), view.unix_ms);
        let resp = DescribeSessionsResponse {
            error: ErrorCode::NONE,
            node_id: view.node.id as u32,
            now_ms: view.unix_ms,
            sessions,
        };
        outbox.push(id, &Response::DescribeSessions(resp));
    }
}

#[cfg(test)]
mod tests;
