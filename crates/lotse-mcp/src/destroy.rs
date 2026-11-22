//! Tools that destroy: deleting a resource, purging a queue, requeueing a
//! dead-letter queue. Each runs only when its `confirm` argument is the
//! resource's name exactly, so a vague instruction destroys nothing
//! (ADR 0134); the cluster then checks the principal's role as for any
//! request.

use lotse_client::Client;
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::json;

use crate::{Args, Outcome, ToolError, ToolSpec, done, spec};

pub(crate) fn specs() -> Vec<ToolSpec> {
    vec![
        spec(
            "delete_resource",
            "Deletes a stream, queue or exchange created at runtime, with its data and bindings. Cannot be undone. \
             confirm must be the resource's name.",
            json!({
                "name": {"type": "string", "description": "The stream's, queue's or exchange's name."},
                "confirm": {"type": "string", "description": "The same name again, exactly: nothing is deleted otherwise."},
                "if_empty": {"type": "boolean", "description": "Delete only if it holds no record or unsettled message; false by default."},
            }),
            &["name", "confirm"],
            false,
            true,
        ),
        spec(
            "purge_queue",
            "Settles every message of a queue that is ready now; messages held by consumers or open transactions \
             stay. Cannot be undone. confirm must be the queue's name.",
            json!({
                "queue": {"type": "string", "description": "The queue's name."},
                "confirm": {"type": "string", "description": "The same name again, exactly: nothing is purged otherwise."},
            }),
            &["queue", "confirm"],
            false,
            true,
        ),
        spec(
            "requeue_dead_letters",
            "Moves every ready message of a dead-letter queue back to the queue its dead-letter headers name. \
             confirm must be the dead-letter queue's name.",
            json!({
                "queue": {"type": "string", "description": "The dead-letter queue's name."},
                "confirm": {"type": "string", "description": "The same name again, exactly: nothing is moved otherwise."},
            }),
            &["queue", "confirm"],
            false,
            true,
        ),
    ]
}

/// Whether `confirm` names `name` exactly.
pub(crate) fn confirmed(name: &str, confirm: Option<&str>) -> Result<(), ToolError> {
    if confirm == Some(name) {
        return Ok(());
    }
    Err(ToolError::new("CONFIRMATION_MISMATCH", format!("confirm must be {name:?} exactly; nothing was changed")))
}

pub(crate) async fn delete_resource<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    confirmed(name, a.opt_str("confirm")?)?;
    let if_empty = a.opt_bool("if_empty")?.unwrap_or(false);
    let id = if if_empty { c.delete_if_empty(name).await? } else { c.delete(name).await? };
    done(json!({"name": name, "resource_id": id, "deleted": true}), format!("Deleted {name}."))
}

pub(crate) async fn purge_queue<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let queue = a.str("queue")?;
    confirmed(queue, a.opt_str("confirm")?)?;
    let n = c.purge(queue).await?;
    done(json!({"queue": queue, "purged": n}), format!("Purged {n} messages from {queue}."))
}

pub(crate) async fn requeue_dead_letters<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let queue = a.str("queue")?;
    confirmed(queue, a.opt_str("confirm")?)?;
    let n = c.requeue(queue).await?;
    done(json!({"queue": queue, "requeued": n}), format!("Moved {n} messages from {queue} back to their queues."))
}
