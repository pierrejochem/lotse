//! Access tokens (ADR 0133): `CreateToken`, `DescribeTokens` and
//! `RevokeToken`, served by the metadata group's leader from its applied
//! state, so a user reads their own changes.
//!
//! A token acts as its owner. Any user creates, lists and revokes their own
//! tokens; a cluster admin creates and revokes anyone's, and a cluster admin
//! or viewer lists all of them. Someone else's token is unknown to a user,
//! as everything a principal may not see is (ADR 0039). A session signed in
//! with a token cannot create tokens, so a leaked token cannot mint
//! successors that outlive it.
//!
//! The leader takes the creation time from its own clock and adds the
//! lifetime, both carried in the record, so every replica keeps the same
//! expiry. The record's index becomes the token's id. The limits are checked
//! against what the leader has applied: records still in flight are not
//! counted, so concurrent creates can pass a limit by as many as are in
//! flight, as with renewed certificates.

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_auth::token::Lifetime;
use lotse_proto::{
    CreateTokenRequest, CreateTokenResponse, DescribeTokensRequest, DescribeTokensResponse, ErrorCode, LeaderHint,
    Response, RevokeTokenRequest, TokenEntry, session_mechanism,
};
use lotse_rt::Runtime;

use super::admin::{AdminThen, admin_response};
use super::{Clients, ClusterView, Outbox};
use crate::auth::Principal;
use crate::metadata::{Change, MetadataState};
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Longest token name, bytes.
const MAX_NAME: usize = 64;

/// Longest owner name, bytes: a user's name.
const MAX_OWNER: usize = 255;

/// Why a token request waits for the cluster version.
const TOO_EARLY: &str = "tokens need cluster version 0.1; raise it with `lotse upgrade finalize`";

/// A `CreateToken` answer; the id and expiry go only with success.
pub(super) fn created(
    error: ErrorCode,
    why: Option<&str>,
    leader_hint: Option<LeaderHint>,
    id: u64,
    expires_at_ms: u64,
) -> Response {
    let ok = error == ErrorCode::NONE;
    Response::CreateToken(CreateTokenResponse {
        error,
        error_msg: why.map(str::to_string),
        leader_hint,
        id: if ok { id } else { 0 },
        expires_at_ms: if ok { expires_at_ms } else { 0 },
    })
}

/// The owner a token `who` asks for gets: the one named, or `who` itself
/// when none is. Only a cluster admin names someone else.
fn owner_for(view: &ClusterView<'_>, who: &Principal, named: &str) -> Result<String, (ErrorCode, String)> {
    let me = match who {
        Principal::User { name, .. } => Some(name.as_str()),
        _ => None,
    };
    let owner = match (named, me) {
        ("", Some(me)) => me,
        ("", None) => return Err((ErrorCode::INVALID_REQUEST, "name the user the token acts as".into())),
        (named, _) => named,
    };
    if owner.len() > MAX_OWNER {
        return Err((ErrorCode::INVALID_REQUEST, format!("a user's name has at most {MAX_OWNER} bytes")));
    }
    if me != Some(owner) && !view.access.cluster_admin(who) {
        let why = format!("{who} may create tokens only for themselves; a cluster admin creates them for others");
        return Err((ErrorCode::UNAUTHORIZED, why));
    }
    Ok(owner.to_string())
}

/// Whether `who` sees every token, not only their own.
fn sees_all(view: &ClusterView<'_>, who: &Principal) -> bool {
    view.access.cluster_admin(who) || view.access.sees_cluster(who)
}

/// Whether `who` owns a token of `owner`.
fn owns(who: &Principal, owner: &str) -> bool {
    matches!(who, Principal::User { name, .. } if name == owner)
}

/// Whether the cluster version allows the token records (ADR 0127).
fn allowed(state: &MetadataState) -> bool {
    state.writes(crate::version::TOKENS_SINCE)
}

impl Clients {
    /// `CreateToken`: checks the request and the owner, then proposes the
    /// token; answered with its id once applied. `session` is who asks and
    /// how they signed in.
    pub(super) fn create_token<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: CreateTokenRequest,
        session: (&Principal, u8),
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let (who, mechanism) = session;
        let refuse = |code: ErrorCode, why: &str| outbox.push(id, &created(code, Some(why), None, 0, 0));
        if mechanism == session_mechanism::TOKEN {
            return refuse(ErrorCode::UNAUTHORIZED, "a session signed in with a token cannot create tokens");
        }
        let owner = match owner_for(view, who, &req.owner) {
            Ok(owner) => owner,
            Err((code, why)) => return refuse(code, &why),
        };
        let Some(lifetime) = Lifetime::from_code(req.lifetime) else {
            let why = format!(
                "lifetime {} is not one of the five: 1 (7 days), 2 (30 days), 3 (90 days), 4 (180 days) or 5 (365 days)",
                req.lifetime
            );
            return refuse(ErrorCode::INVALID_REQUEST, &why);
        };
        if req.name.is_empty() || req.name.len() > MAX_NAME {
            return refuse(ErrorCode::INVALID_REQUEST, &format!("a token's name has 1 to {MAX_NAME} bytes"));
        }
        let Ok(hash) = <[u8; 32]>::try_from(&req.secret_hash[..]) else {
            return refuse(ErrorCode::INVALID_REQUEST, "the secret's hash is the 32 bytes of a SHA-256");
        };
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &created(code, None, h, 0, 0)),
        };
        let state = replica.metadata();
        if !allowed(state) {
            return refuse(ErrorCode::UNSUPPORTED_VERSION, TOO_EARLY);
        }
        if let Err(why) = state.token_room(&owner, view.unix_ms) {
            return refuse(ErrorCode::INVALID_REQUEST, &why);
        }
        let created_at_ms = view.unix_ms;
        let expires_at_ms = created_at_ms.saturating_add(lifetime.millis());
        let change = Change::CreateToken {
            id: 0,
            name: req.name,
            owner,
            hash,
            created_at_ms,
            expires_at_ms,
            created_by: who.to_string(),
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Token { id: 0, expires_at_ms });
    }

    /// `DescribeTokens`: the live tokens `who` may see, never a hash.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn describe_tokens<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: &Outbox,
        id: u32,
        req: DescribeTokensRequest,
        who: &Principal,
        at_least: Option<u64>,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_read(view, groups, at_least) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeTokensResponse { error, leader_hint, tokens: Vec::new() };
                return outbox.push(id, &Response::DescribeTokens(resp));
            }
        };
        let all = sees_all(view, who);
        let tokens = replica
            .metadata()
            .tokens
            .iter()
            .filter(|(_, t)| t.expires_at_ms > view.unix_ms)
            .filter(|(_, t)| all || owns(who, &t.owner))
            .filter(|(_, t)| req.owner.as_ref().is_none_or(|o| *o == t.owner))
            .map(|(&id, t)| TokenEntry {
                id,
                name: t.name.clone(),
                owner: t.owner.clone(),
                created_at_ms: t.created_at_ms,
                expires_at_ms: t.expires_at_ms,
                created_by: t.created_by.clone(),
            })
            .collect();
        let resp = DescribeTokensResponse { error: ErrorCode::NONE, leader_hint: None, tokens };
        outbox.push(id, &Response::DescribeTokens(resp));
    }

    /// `RevokeToken`: one's own token, or anyone's for a cluster admin; a
    /// token `who` may not revoke is as unknown as one that does not exist.
    pub(super) fn revoke_token<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: RevokeTokenRequest,
        who: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        if !allowed(state) {
            return outbox.push(id, &admin_response(ErrorCode::UNSUPPORTED_VERSION, Some(TOO_EARLY), None));
        }
        let mine = |owner: &str| owns(who, owner) || view.access.cluster_admin(who);
        if !state.live_token(req.id, view.unix_ms).is_some_and(|t| mine(&t.owner)) {
            let why = format!("there is no token {:016x}", req.id);
            return outbox.push(id, &admin_response(ErrorCode::UNKNOWN_RESOURCE, Some(&why), None));
        }
        let change = Change::RevokeToken { id: req.id };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }
}
