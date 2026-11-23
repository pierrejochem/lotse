//! The token tool: listing access tokens (ADR 0133). Creating and revoking
//! them stays in the console and the clients, where a person sees the
//! secret once.

use lotse_client::Client;
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::{Value, json};

use crate::{Args, Outcome, ToolSpec, done, spec};

pub(crate) fn specs() -> Vec<ToolSpec> {
    vec![spec(
        "list_tokens",
        "Access tokens: your own, or with owner a user's, which only a cluster admin or viewer may list. Names, \
         owners and times in ms since the Unix epoch; no secret is kept anywhere to list.",
        json!({
            "owner": {"type": "string", "description": "List this user's tokens instead of your own."},
        }),
        &[],
        true,
        false,
    )]
}

/// A token's id as the token text and `list_tokens` write it: 16 lowercase
/// hex digits. `list_sessions` uses the same, so the two tools' ids match.
pub(crate) fn token_id(id: u64) -> Value {
    json!(format!("{id:016x}"))
}

pub(crate) async fn list_tokens<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let owner = a.opt_str("owner")?;
    let tokens: Vec<Value> = c
        .describe_tokens(owner)
        .await?
        .into_iter()
        .map(|t| {
            json!({
                "id": token_id(t.id),
                "name": t.name,
                "owner": t.owner,
                "created_at_ms": t.created_at_ms,
                "expires_at_ms": t.expires_at_ms,
                "created_by": t.created_by,
            })
        })
        .collect();
    let text = format!("{} tokens.", tokens.len());
    done(json!({ "tokens": tokens }), text)
}
