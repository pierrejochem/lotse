//! Tools that create and change: streams, queues, exchanges, bindings,
//! settings and partition counts. New resources go to the client's tenant.

use lotse_client::Client;
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::json;

use crate::{Args, Outcome, ToolError, ToolSpec, done, spec};

/// Replicas of a new resource when the call names none: three, or every
/// node of a smaller cluster.
const DEFAULT_REPLICATION: u64 = 3;

pub(crate) fn specs() -> Vec<ToolSpec> {
    let configs = json!({
        "type": "object",
        "additionalProperties": {"type": "string"},
        "description": "Settings, name to value as a string, such as retention.ms or max.deliveries.",
    });
    vec![
        spec(
            "create_stream",
            "Creates a stream with partitions, each replicated by replication nodes. Settings it does not name take \
             the cluster's defaults (retention.ms, retention.bytes, cleanup.policy, delete.retention.ms).",
            json!({
                "name": {"type": "string", "description": "The new stream's name."},
                "partitions": {"type": "integer", "minimum": 1, "description": "The number of partitions."},
                "replication": {"type": "integer", "minimum": 1, "maximum": 255, "description": "Nodes that hold each partition; 3 by default, or every node of a smaller cluster."},
                "configs": configs,
            }),
            &["name", "partitions"],
            false,
            false,
        ),
        spec(
            "create_queue",
            "Creates a queue replicated by replication nodes, with optional settings (ack.timeout.ms, \
             max.deliveries, message.ttl.ms, max.length, overflow) and a dead-letter queue of the same tenant.",
            json!({
                "name": {"type": "string", "description": "The new queue's name."},
                "replication": {"type": "integer", "minimum": 1, "maximum": 255, "description": "Nodes that hold the queue; 3 by default, or every node of a smaller cluster."},
                "configs": configs,
                "dead_letter_queue": {"type": "string", "description": "The queue that receives the messages its limits retire; without one they are dropped."},
            }),
            &["name"],
            false,
            false,
        ),
        spec(
            "declare_exchange",
            "Declares an exchange of a type: direct (routing key equals binding key), topic (dot-separated words, * \
             one word, # zero or more), fanout (every bound queue) or headers (by message headers).",
            json!({
                "name": {"type": "string", "description": "The new exchange's name."},
                "type": {"type": "string", "enum": ["direct", "topic", "fanout", "headers"], "description": "How it routes."},
            }),
            &["name", "type"],
            false,
            false,
        ),
        spec(
            "bind",
            "Binds a queue to an exchange with a binding key.",
            json!({
                "exchange": {"type": "string", "description": "The exchange's name."},
                "queue": {"type": "string", "description": "The queue's name."},
                "routing_key": {"type": "string", "description": "The binding key; empty for a fanout exchange."},
            }),
            &["exchange", "queue", "routing_key"],
            false,
            false,
        ),
        spec(
            "unbind",
            "Removes a queue's binding to an exchange with that binding key.",
            json!({
                "exchange": {"type": "string", "description": "The exchange's name."},
                "queue": {"type": "string", "description": "The queue's name."},
                "routing_key": {"type": "string", "description": "The binding key the binding was made with."},
            }),
            &["exchange", "queue", "routing_key"],
            false,
            false,
        ),
        spec(
            "alter_configs",
            "Changes settings of a stream or queue created at runtime: set gives settings new values, reset returns \
             them to their defaults. replication.factor is set on its own and moves every partition to that many \
             nodes. Destructive: retention, cleanup policy, length limits, message lifetime and the delivery \
             limit can delete data, so a change to retention.ms, retention.bytes, delete.retention.ms, \
             cleanup.policy, max.length, max.length.bytes, overflow, message.ttl.ms or max.deliveries runs only when \
             confirm is the resource's name.",
            json!({
                "name": {"type": "string", "description": "The stream's or queue's name."},
                "set": configs,
                "reset": {"type": "array", "items": {"type": "string"}, "description": "Settings to return to their defaults."},
                "confirm": {"type": "string", "description": "The same name again, exactly; needed when a setting that can delete data changes, and nothing is changed without it."},
            }),
            &["name"],
            false,
            true,
        ),
        spec(
            "create_partitions",
            "Raises a stream's partition count to count. Existing partitions stay as they are, so a key may map to \
             another partition afterwards.",
            json!({
                "name": {"type": "string", "description": "The stream's name."},
                "count": {"type": "integer", "minimum": 1, "description": "The new partition count, above the current one."},
            }),
            &["name", "count"],
            false,
            false,
        ),
        spec(
            "set_quota",
            "Sets or removes a storage limit, in bytes with every replica counted: a tenant's own limit (no \
             prefix), which needs the cluster-admin role, or a limit on the names that start with prefix, which a \
             tenant admin may set under the prefix of their binding unless a cluster admin set it. Once a limit is \
             full, new data under it is refused; nothing is deleted.",
            json!({
                "tenant": {"type": "string", "description": "The tenant."},
                "prefix": {"type": "string", "description": "A name prefix; leave out for the tenant's own limit."},
                "bytes": {"type": "integer", "minimum": 1, "description": "The limit in bytes; leave out to remove the limit."},
            }),
            &["tenant"],
            false,
            false,
        ),
    ]
}

/// The replication asked for, or the default for the cluster's size.
async fn replication<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Result<u8, ToolError> {
    match a.opt_ranged("replication", 1, 255)? {
        Some(n) => Ok(n as u8),
        None => {
            let nodes = c.metadata(&[]).await?.brokers.len() as u64;
            Ok(DEFAULT_REPLICATION.min(nodes.max(1)) as u8)
        }
    }
}

fn pairs(v: &[(String, String)]) -> Vec<(&str, &str)> {
    v.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

pub(crate) async fn create_stream<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let partitions = a.opt_ranged("partitions", 1, u64::from(u32::MAX))?.unwrap_or(1) as u32;
    let configs = a.opt_strings("configs")?;
    let replication = replication(c, a).await?;
    let id = c.create_stream_with(name, partitions, replication, &pairs(&configs)).await?;
    let text = format!("Created stream {name} with {partitions} partitions on {replication} nodes each.");
    done(json!({"name": name, "resource_id": id, "partitions": partitions, "replication": replication}), text)
}

pub(crate) async fn create_queue<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let configs = a.opt_strings("configs")?;
    let dlq = a.opt_str("dead_letter_queue")?;
    let replication = replication(c, a).await?;
    let id = c.create_queue_with(name, replication, &pairs(&configs), dlq).await?;
    let text = format!("Created queue {name} on {replication} nodes.");
    done(json!({"name": name, "resource_id": id, "replication": replication, "dead_letter_queue": dlq}), text)
}

pub(crate) async fn declare_exchange<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let kind = match a.str("type")? {
        "direct" => ExchangeKind::Direct,
        "topic" => ExchangeKind::Topic,
        "fanout" => ExchangeKind::Fanout,
        "headers" => ExchangeKind::Headers,
        other => {
            return Err(ToolError::invalid(format!("\"type\" is direct, topic, fanout or headers, not {other:?}")));
        }
    };
    let id = c.declare_exchange(name, kind).await?;
    done(json!({"name": name, "resource_id": id}), format!("Declared exchange {name}."))
}

pub(crate) async fn bind<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>, unbind: bool) -> Outcome {
    let exchange = a.str("exchange")?;
    let queue = a.str("queue")?;
    let key = a.str("routing_key")?;
    if unbind {
        c.unbind(exchange, queue, key).await?;
    } else {
        c.bind(exchange, queue, key).await?;
    }
    let text = if unbind {
        format!("Unbound {queue} from {exchange} for key {key:?}.")
    } else {
        format!("Bound {queue} to {exchange} with key {key:?}.")
    };
    done(json!({"exchange": exchange, "queue": queue, "routing_key": key}), text)
}

/// Settings whose change can delete data: retention, the cleanup policy, a
/// queue's length limits, and its message lifetime and delivery limit, which
/// drop messages when the queue has no dead-letter queue. `alter_configs`
/// changes them only with `confirm`.
pub(crate) const DATA_LOSING: [&str; 9] = [
    "retention.ms",
    "retention.bytes",
    "delete.retention.ms",
    "cleanup.policy",
    "max.length",
    "max.length.bytes",
    "overflow",
    "message.ttl.ms",
    "max.deliveries",
];

/// The first of `keys` whose change can delete data.
pub(crate) fn data_losing<'k>(mut keys: impl Iterator<Item = &'k str>) -> Option<&'k str> {
    keys.find(|k| DATA_LOSING.contains(k))
}

pub(crate) async fn alter_configs<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let set = a.opt_strings("set")?;
    let reset = a.opt_str_list("reset")?;
    if set.is_empty() && reset.is_empty() {
        return Err(ToolError::invalid("give settings to change in \"set\" or \"reset\""));
    }
    if let Some(key) = data_losing(set.iter().map(|(k, _)| k.as_str()).chain(reset.iter().map(String::as_str))) {
        crate::destroy::confirmed(name, a.opt_str("confirm")?)
            .map_err(|e| ToolError::new(&e.code, format!("{key} can delete data: {}", e.message)))?;
    }
    let reset_refs: Vec<&str> = reset.iter().map(String::as_str).collect();
    c.alter_configs(name, &pairs(&set), &reset_refs).await?;
    let text = format!("Changed {} settings of {name}.", set.len() + reset.len());
    done(
        json!({"name": name, "set": set.into_iter().map(|(k, v)| (k, json!(v))).collect::<serde_json::Map<_, _>>(), "reset": reset}),
        text,
    )
}

pub(crate) async fn set_quota<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let tenant = a.str("tenant")?;
    let prefix = a.opt_str("prefix")?.unwrap_or("");
    let bytes = a.opt_ranged("bytes", 1, u64::MAX)?;
    c.alter_quota(tenant, prefix, bytes).await?;
    let what =
        if prefix.is_empty() { format!("tenant {tenant}") } else { format!("prefix {prefix} in tenant {tenant}") };
    let text = match bytes {
        Some(n) => format!("The storage limit of {what} is {n} bytes."),
        None => format!("The storage limit of {what} is removed."),
    };
    done(json!({"tenant": tenant, "prefix": prefix, "bytes": bytes}), text)
}

pub(crate) async fn create_partitions<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let count = a.opt_ranged("count", 1, u64::from(u32::MAX))?.unwrap_or(1) as u32;
    c.create_partitions(name, count).await?;
    done(json!({"name": name, "partitions": count}), format!("{name} now has {count} partitions."))
}
