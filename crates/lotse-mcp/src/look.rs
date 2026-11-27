//! Tools that look: the cluster, its resources, consumer groups, nodes,
//! sessions and the upgrade. Each sees what the principal may see: the
//! cluster filters `Metadata` and `DescribeMetrics` by role.

use std::collections::BTreeMap;

use lotse_client::{Client, ClientError};
use lotse_proto::{
    ConfigEntry, DescribeMetricsResponse, DescribeUpgradeResponse, ErrorCode, GroupMetrics, MetadataResponse,
    PartitionMetrics, QueueMetrics, ResourceMetadata, kind,
};
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::{Value, json};

use crate::{Args, Outcome, ToolError, ToolSpec, done, kind_name, spec};

pub(crate) fn specs() -> Vec<ToolSpec> {
    vec![
        spec(
            "cluster_overview",
            "The cluster at a glance: each node with its version, uptime, connections and the Raft groups it hosts \
             and leads, partitions without a leader, nodes low on disk, and the cluster version.",
            json!({}),
            &[],
            true,
            false,
        ),
        spec(
            "list_resources",
            "Streams, queues and exchanges you may see, with their tenant, partitions, leaders and voters, and \
             each exchange's type and bindings.",
            json!({
                "kind": {"type": "string", "enum": ["stream", "queue", "exchange"], "description": "Only resources of this kind."},
            }),
            &[],
            true,
            false,
        ),
        spec(
            "describe_stream",
            "One stream: per partition the leader, offsets (log start, high watermark, last stable offset), size in \
             bytes, replicas behind the leader in offsets and open transactions; its settings; and the consumer \
             groups reading it with their lag in offsets to the last stable offset.",
            json!({
                "name": {"type": "string", "description": "The stream's name."},
            }),
            &["name"],
            true,
            false,
        ),
        spec(
            "describe_queue",
            "One queue: messages ready, in flight and held by open transactions, its consumers with their credit, \
             its settings, and its dead-letter queue with the messages ready there.",
            json!({
                "name": {"type": "string", "description": "The queue's name."},
            }),
            &["name"],
            true,
            false,
        ),
        spec(
            "describe_consumer_group",
            "One consumer group: its members and the partitions each owns, and per partition the committed offset, \
             the last stable offset, the high watermark and the lag in offsets: to the last stable offset (what a \
             read_committed group can read, `lag`) and to the high watermark.",
            json!({
                "group_id": {"type": "string", "description": "The consumer group's id."},
                "tenant": {"type": "string", "description": "The tenant the group id lives in. Without it, the group in every tenant you may see."},
            }),
            &["group_id"],
            true,
            false,
        ),
        spec(
            "node_metrics",
            "Each node's metrics: traffic in bytes, connections, CPU in per mille, memory and disk in bytes, the \
             free space limit, the commit latency histogram in ms, and the groups, queues and consumer groups it \
             hosts or leads. Counters run since the node started.",
            json!({
                "node_id": {"type": "integer", "minimum": 0, "description": "Only this node."},
            }),
            &[],
            true,
            false,
        ),
        spec(
            "list_sessions",
            "Open client sessions on every node: principal, sign-in mechanism, peer address, and when each was \
             made and ends, in ms since the Unix epoch. A cluster admin or viewer sees all, anyone else their own.",
            json!({}),
            &[],
            true,
            false,
        ),
        spec(
            "describe_upgrade",
            "Where a rolling upgrade stands: the cluster version, which node holds the restart turn, whether turns \
             are paused, and each node's release and the cluster versions it runs with. Needs the cluster-admin or \
             cluster-viewer role.",
            json!({}),
            &[],
            true,
            false,
        ),
        spec(
            "describe_quotas",
            "Storage limits and usage of the tenants you may see: each tenant's own limit and its prefix limits, \
             in bytes with every replica counted, whether each is full, and the usage at the last count, which runs \
             every few seconds. A tenant without a limit is listed with its usage.",
            json!({
                "tenant": {"type": "string", "description": "Only this tenant."},
            }),
            &[],
            true,
            false,
        ),
    ]
}

/// The answers of every node that answered, and which did not, with why.
fn split(
    all: Vec<(u32, Result<DescribeMetricsResponse, ClientError>)>,
) -> (Vec<DescribeMetricsResponse>, Vec<(u32, String)>) {
    let mut answered = Vec::new();
    let mut silent = Vec::new();
    for (node, r) in all {
        match r {
            Ok(m) => answered.push(m),
            Err(e) => silent.push((node, e.to_string())),
        }
    }
    (answered, silent)
}

/// The leader's view of one partition: from the node that says it leads,
/// the latest term if two do for a moment.
fn led(metrics: &[DescribeMetricsResponse], resource_id: u64, partition: u32) -> Option<&PartitionMetrics> {
    metrics
        .iter()
        .flat_map(|m| &m.partitions)
        .filter(|p| p.leader && p.resource_id == resource_id && p.partition == partition)
        .max_by_key(|p| p.term)
}

/// The queue's counts from the node that leads it now: the reporter with the
/// highest term, as [`led`] picks a partition's leader. A node reports a
/// queue only while it leads it, and for a moment two may.
pub(crate) fn led_queue(metrics: &[DescribeMetricsResponse], resource_id: u64) -> Option<&QueueMetrics> {
    let term = |m: &DescribeMetricsResponse| {
        m.partitions.iter().filter(|p| p.leader && p.resource_id == resource_id).map(|p| p.term).max().unwrap_or(0)
    };
    metrics
        .iter()
        .filter_map(|m| m.queues.iter().find(|q| q.resource_id == resource_id).map(|q| (term(m), q)))
        .reduce(|best, next| if next.0 > best.0 { next } else { best })
        .map(|(_, q)| q)
}

/// Every consumer group once. Only the coordinator group's leader reports
/// groups, but while its leadership moves two nodes may; the report of the
/// node with the highest coordinator term wins.
pub(crate) fn group_reports(metrics: &[DescribeMetricsResponse]) -> Vec<&GroupMetrics> {
    let mut best: BTreeMap<(&str, &str), (u64, &GroupMetrics)> = BTreeMap::new();
    for m in metrics {
        let term = m
            .partitions
            .iter()
            .filter(|p| p.leader && p.group == m.coordinator_group)
            .map(|p| p.term)
            .max()
            .unwrap_or(0);
        for g in &m.groups {
            let slot = best.entry((g.tenant.as_str(), g.group_id.as_str())).or_insert((term, g));
            if term > slot.0 {
                *slot = (term, g);
            }
        }
    }
    best.into_values().map(|(_, g)| g).collect()
}

/// How far a committed offset is behind a partition's end, to each of the
/// two ends `DescribeMetrics` gives.
pub(crate) struct Lag {
    /// To the last stable offset: all a read_committed group can read.
    pub(crate) stable: u64,
    /// To the high watermark: all a read_uncommitted group can read.
    pub(crate) high_watermark: u64,
}

pub(crate) fn lag(p: &PartitionMetrics, committed: u64) -> Lag {
    Lag { stable: p.last_stable.saturating_sub(committed), high_watermark: p.high_watermark.saturating_sub(committed) }
}

/// Whether the cluster did not show a thing to this principal, as opposed to
/// failing to answer: a refusal and an unknown name look alike on purpose
/// (what a principal may not see is unknown to it). Anything else, such as
/// no node answering, is a failure the agent should hear about.
pub(crate) fn not_shown(e: &ClientError) -> bool {
    match e {
        ClientError::Broker { code, .. } => *code == ErrorCode::UNAUTHORIZED || *code == ErrorCode::UNKNOWN_RESOURCE,
        ClientError::UnknownStream(..) => true,
        _ => false,
    }
}

/// A resource's settings as JSON, or null with the reason: null alone when
/// the principal may not read them, null and the error when the read failed.
pub(crate) fn settings(r: Result<Vec<ConfigEntry>, ClientError>) -> (Value, Option<String>) {
    match r {
        Ok(entries) => {
            let map: serde_json::Map<String, Value> = entries.into_iter().map(|e| (e.key, json!(e.value))).collect();
            (Value::Object(map), None)
        }
        Err(e) if not_shown(&e) => (Value::Null, None),
        Err(e) => (Value::Null, Some(e.to_string())),
    }
}

/// The cluster version of an upgrade answer, or why there is none: nothing
/// for a principal that may not read it, the error for a failed read.
pub(crate) fn upgrade_version(r: Result<DescribeUpgradeResponse, ClientError>) -> (Option<String>, Option<String>) {
    match r {
        Ok(u) => (u.cluster_version.map(version), None),
        Err(e) if not_shown(&e) => (None, None),
        Err(e) => (None, Some(e.to_string())),
    }
}

/// The resource called `name` of `want` kind, as the principal sees it.
fn find(m: &MetadataResponse, name: &str, want: u8) -> Result<ResourceMetadata, ToolError> {
    m.resources
        .iter()
        .find(|r| r.name == name && r.error == ErrorCode::NONE && r.kind == want)
        .cloned()
        .ok_or_else(|| ToolError::unknown(format!("there is no {} named {name:?} that you may see", kind_name(want))))
}

pub(crate) async fn cluster_overview<R: Net + Clock + Spawn + Rng>(c: &Client<R>) -> Outcome {
    let m = c.metadata(&[]).await?;
    let (metrics, silent) = split(c.node_metrics().await?);
    // Only cluster admins and viewers may read it; everyone else gets the
    // rest, and a failed read is named, not mistaken for a refusal.
    let (cluster_version, cluster_version_error) = upgrade_version(c.describe_upgrade().await);
    let nodes: Vec<Value> = m
        .brokers
        .iter()
        .map(|b| {
            let mut node = json!({
                "node_id": b.node_id,
                "address": format!("{}:{}", b.host, b.port),
                "zone": if b.rack.is_empty() { Value::Null } else { json!(b.rack) },
            });
            match metrics.iter().find(|n| n.node_id == b.node_id) {
                Some(n) => {
                    node["answered"] = json!(true);
                    node["version"] = json!(n.version);
                    node["uptime_ms"] = json!(n.uptime_ms);
                    node["connections"] = json!(n.connections);
                    node["groups_hosted"] = json!(n.partitions.len());
                    node["groups_led"] = json!(n.partitions.iter().filter(|p| p.leader).count());
                    node["disk_low"] = json!(n.disk.map(|d| d.low));
                }
                None => {
                    node["answered"] = json!(false);
                    let why = silent.iter().find(|(id, _)| *id == b.node_id).map(|(_, w)| w.clone());
                    node["error"] = json!(why);
                }
            }
            node
        })
        .collect();
    let leaderless: Vec<Value> = m
        .resources
        .iter()
        .filter(|r| r.error == ErrorCode::NONE)
        .flat_map(|r| {
            r.partitions
                .iter()
                .filter(|p| p.leader == 0)
                .map(move |p| json!({"name": r.name, "partition": p.partition}))
        })
        .collect();
    let low: Vec<u32> = metrics.iter().filter(|n| n.disk.is_some_and(|d| d.low)).map(|n| n.node_id).collect();
    let mut problems = Vec::new();
    if !silent.is_empty() {
        problems.push(format!("{} of {} nodes did not answer", silent.len(), m.brokers.len()));
    }
    if !leaderless.is_empty() {
        problems.push(format!("{} partitions have no leader", leaderless.len()));
    }
    if !low.is_empty() {
        problems.push(format!("{} nodes are low on disk", low.len()));
    }
    let text = format!(
        "{} nodes, {} answered; {} resources you may see; {}.",
        m.brokers.len(),
        metrics.len(),
        m.resources.len(),
        if problems.is_empty() { "healthy".to_string() } else { problems.join(", ") }
    );
    done(
        json!({
            "healthy": problems.is_empty(),
            "problems": problems,
            "nodes": nodes,
            "resources": m.resources.len(),
            "partitions_without_leader": leaderless,
            "nodes_low_on_disk": low,
            "cluster_version": cluster_version,
            "cluster_version_error": cluster_version_error,
        }),
        text,
    )
}

fn version(v: u32) -> String {
    format!("{}.{}", lotse_proto::cluster_version::major(v), lotse_proto::cluster_version::minor(v))
}

fn exchange_type(t: u8) -> &'static str {
    match t {
        lotse_proto::exchange_type::DIRECT => "direct",
        lotse_proto::exchange_type::TOPIC => "topic",
        lotse_proto::exchange_type::FANOUT => "fanout",
        lotse_proto::exchange_type::HEADERS => "headers",
        _ => "unknown",
    }
}

pub(crate) async fn list_resources<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let want = match a.opt_str("kind")? {
        None => None,
        Some("stream") => Some(kind::STREAM),
        Some("queue") => Some(kind::QUEUE),
        Some("exchange") => Some(kind::EXCHANGE),
        Some(other) => return Err(ToolError::invalid(format!("\"kind\" is stream, queue or exchange, not {other:?}"))),
    };
    let m = c.metadata(&[]).await?;
    let names: BTreeMap<u64, &str> = m.resources.iter().map(|r| (r.resource_id, r.name.as_str())).collect();
    let mut found: Vec<&ResourceMetadata> =
        m.resources.iter().filter(|r| r.error == ErrorCode::NONE && want.is_none_or(|k| k == r.kind)).collect();
    found.sort_by(|x, y| (&x.tenant, &x.name).cmp(&(&y.tenant, &y.name)));
    let resources: Vec<Value> = found
        .iter()
        .map(|r| {
            let mut v = json!({
                "name": r.name,
                "tenant": r.tenant,
                "kind": kind_name(r.kind),
                "partitions": r.partitions.iter().map(|p| json!({
                    "partition": p.partition,
                    "leader": if p.leader == 0 { Value::Null } else { json!(p.leader) },
                    "voters": p.voters,
                })).collect::<Vec<_>>(),
            });
            if let Some(x) = &r.exchange {
                v["exchange_type"] = json!(exchange_type(x.exchange_type));
                v["bindings"] = x
                    .bindings
                    .iter()
                    .map(|b| {
                        json!({
                            "queue": names.get(&b.queue_id).copied(),
                            "routing_key": b.binding_key,
                        })
                    })
                    .collect();
            }
            v
        })
        .collect();
    let text = format!("{} resources.", resources.len());
    done(json!({ "resources": resources }), text)
}

/// A resource's settings, or null if the principal may not read them, and
/// the error if reading them failed.
async fn configs<R: Net + Clock + Spawn + Rng>(c: &Client<R>, name: &str) -> (Value, Option<String>) {
    settings(c.describe_configs(name).await)
}

pub(crate) async fn describe_stream<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let r = find(&c.metadata(&[name]).await?, name, kind::STREAM)?;
    let (metrics, _) = split(c.node_metrics().await?);
    let mut ends = BTreeMap::new();
    let partitions: Vec<Value> = r
        .partitions
        .iter()
        .map(|p| {
            let mut v = json!({
                "partition": p.partition,
                "leader": if p.leader == 0 { Value::Null } else { json!(p.leader) },
                "voters": p.voters,
            });
            if let Some(m) = led(&metrics, r.resource_id, p.partition) {
                ends.insert(p.partition, m);
                v["log_start"] = json!(m.log_start);
                v["high_watermark"] = json!(m.high_watermark);
                v["last_stable_offset"] = json!(m.last_stable);
                v["size_bytes"] = json!(m.size_bytes);
                v["oldest_timestamp_ms"] = json!(m.oldest_timestamp_ms);
                v["open_transactions"] = json!(m.open_transactions);
                v["replicas_behind"] = m
                    .replicas
                    .iter()
                    .map(|rp| json!({"node_id": rp.node_id, "offsets": m.high_watermark.saturating_sub(rp.log_end)}))
                    .collect();
            }
            v
        })
        .collect();
    let groups: Vec<Value> = group_reports(&metrics)
        .into_iter()
        .filter(|g| {
            g.offsets.iter().any(|o| o.resource_id == r.resource_id)
                || g.members.iter().any(|m| m.owned.iter().any(|o| o.resource_id == r.resource_id))
        })
        .map(|g| {
            let (mut stable, mut high) = (0u64, 0u64);
            for o in g.offsets.iter().filter(|o| o.resource_id == r.resource_id) {
                if let Some(p) = ends.get(&o.partition) {
                    let l = lag(p, o.offset);
                    stable += l.stable;
                    high += l.high_watermark;
                }
            }
            json!({
                "group_id": g.group_id,
                "tenant": g.tenant,
                "members": g.members.len(),
                "lag": stable,
                "lag_against": "last_stable_offset",
                "lag_to_high_watermark": high,
            })
        })
        .collect();
    let text = format!("Stream {name}: {} partitions, {} consumer groups.", partitions.len(), groups.len());
    let (settings, settings_error) = configs(c, name).await;
    done(
        json!({
            "name": r.name,
            "tenant": r.tenant,
            "partitions": partitions,
            "configs": settings,
            "configs_error": settings_error,
            "consumer_groups": groups,
        }),
        text,
    )
}

pub(crate) async fn describe_queue<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let r = find(&c.metadata(&[name]).await?, name, kind::QUEUE)?;
    let (metrics, _) = split(c.node_metrics().await?);
    let (settings, settings_error) = configs(c, name).await;
    let queue = |id: u64| led_queue(&metrics, id);
    let leader = r.partitions.first().map(|p| p.leader).filter(|l| *l != 0);
    let mut v = json!({
        "name": r.name,
        "tenant": r.tenant,
        "leader": leader,
        "configs": settings,
        "configs_error": settings_error,
    });
    match queue(r.resource_id) {
        Some(q) => {
            v["ready"] = json!(q.ready);
            v["in_flight"] = json!(q.in_flight);
            v["in_open_transactions"] = json!(q.in_open_txns);
            v["delivered"] = json!(q.delivered);
            v["acked"] = json!(q.acked);
            if !q.ready_by_level.is_empty() {
                v["ready_by_priority"] = json!(q.ready_by_level);
            }
            v["consumers"] = q
                .consumers
                .iter()
                .map(|s| {
                    json!({
                        "subscription_id": s.subscription_id,
                        "peer": s.peer,
                        "in_flight": s.in_flight,
                        "in_flight_bytes": s.in_flight_bytes,
                        "credit": s.credit,
                        "state": s.state.name(),
                    })
                })
                .collect();
        }
        None => v["note"] = json!("no node reported this queue's counts: it has no leader at the moment"),
    }
    if let Some(p) = led(&metrics, r.resource_id, 0) {
        v["size_bytes"] = json!(p.size_bytes);
    }
    let dlq = settings.get("dead.letter.queue").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string);
    if let Some(dlq) = dlq {
        let ready = match c.metadata(&[&dlq]).await.ok().and_then(|m| find(&m, &dlq, kind::QUEUE).ok()) {
            Some(d) => queue(d.resource_id).map(|q| q.ready),
            None => None,
        };
        v["dead_letter_queue"] = json!({"name": dlq, "ready": ready});
    }
    let text = match (v["ready"].as_u64(), v["in_flight"].as_u64()) {
        (Some(ready), Some(flight)) => format!("Queue {name}: {ready} ready, {flight} in flight."),
        _ => format!("Queue {name}: no counts, it has no leader at the moment."),
    };
    done(v, text)
}

pub(crate) async fn describe_consumer_group<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let group_id = a.str("group_id")?;
    let tenant = a.opt_str("tenant")?;
    let m = c.metadata(&[]).await?;
    let (metrics, _) = split(c.node_metrics().await?);
    let names: BTreeMap<u64, &str> = m.resources.iter().map(|r| (r.resource_id, r.name.as_str())).collect();
    let stream = |id: u64| names.get(&id).map_or_else(|| json!(id), |n| json!(n));
    let groups: Vec<Value> = group_reports(&metrics)
        .into_iter()
        .filter(|g| g.group_id == group_id && tenant.is_none_or(|t| t == g.tenant))
        .map(|g| {
            let members: Vec<Value> = g
                .members
                .iter()
                .map(|mm| {
                    let owned: Vec<Value> = mm
                        .owned
                        .iter()
                        .flat_map(|o| o.partitions.iter().map(move |p| (o.resource_id, *p)))
                        .map(|(id, p)| json!({"stream": stream(id), "partition": p}))
                        .collect();
                    json!({"member_id": mm.member_id, "partitions": owned})
                })
                .collect();
            let mut total = 0u64;
            let mut total_high = 0u64;
            let partitions: Vec<Value> = g
                .offsets
                .iter()
                .map(|o| {
                    let end = led(&metrics, o.resource_id, o.partition);
                    let l = end.map(|p| lag(p, o.offset));
                    total += l.as_ref().map_or(0, |l| l.stable);
                    total_high += l.as_ref().map_or(0, |l| l.high_watermark);
                    json!({
                        "stream": stream(o.resource_id),
                        "partition": o.partition,
                        "committed_offset": o.offset,
                        "last_stable_offset": end.map(|p| p.last_stable),
                        "high_watermark": end.map(|p| p.high_watermark),
                        "lag": l.as_ref().map(|l| l.stable),
                        "lag_to_high_watermark": l.as_ref().map(|l| l.high_watermark),
                    })
                })
                .collect();
            json!({
                "group_id": g.group_id,
                "tenant": g.tenant,
                "epoch": g.epoch,
                "members": members,
                "partitions": partitions,
                "total_lag": total,
                "total_lag_to_high_watermark": total_high,
                "lag_against": "last_stable_offset",
            })
        })
        .collect();
    if groups.is_empty() {
        return Err(ToolError::unknown(format!(
            "there is no consumer group {group_id:?} that you may see, or the node leading the coordinator group did not answer"
        )));
    }
    let text = groups
        .iter()
        .map(|g| {
            format!(
                "Group {} in {}: {} members, lag {} offsets.",
                group_id,
                g["tenant"].as_str().unwrap_or_default(),
                g["members"].as_array().map_or(0, Vec::len),
                g["total_lag"]
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    done(json!({ "groups": groups }), text)
}

pub(crate) async fn node_metrics<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let only = a.opt_u32("node_id")?;
    let all = c.node_metrics().await?;
    if let Some(id) = only
        && !all.iter().any(|(n, _)| *n == id)
    {
        return Err(ToolError::unknown(format!("there is no node {id}")));
    }
    let nodes: Vec<Value> = all
        .into_iter()
        .filter(|(n, _)| only.is_none_or(|id| id == *n))
        .map(|(node_id, r)| match r {
            Err(e) => json!({"node_id": node_id, "answered": false, "error": e.to_string()}),
            Ok(m) => json!({
                "node_id": node_id,
                "answered": true,
                "version": m.version,
                "uptime_ms": m.uptime_ms,
                "connections": m.connections,
                "client_in_bytes": m.client_in_bytes,
                "client_out_bytes": m.client_out_bytes,
                "peer_in_bytes": m.peer_in_bytes,
                "peer_out_bytes": m.peer_out_bytes,
                "commit_latency": m.commit_latency.iter().enumerate().map(|(i, n)| {
                    let under = if i + 1 == m.commit_latency.len() { Value::Null } else { json!(1u64 << i) };
                    json!({"under_ms": under, "count": n})
                }).collect::<Vec<_>>(),
                "host": {
                    "cpu_permille": m.host.cpu_permille,
                    "memory_bytes": m.host.memory_bytes,
                    "memory_limit_bytes": m.host.memory_limit_bytes,
                    "disk_free_bytes": m.host.disk_free_bytes,
                    "disk_total_bytes": m.host.disk_total_bytes,
                },
                "disk": m.disk.map(|d| json!({"free_limit_bytes": d.free_limit_bytes, "low": d.low})),
                "groups_hosted": m.partitions.len(),
                "groups_led": m.partitions.iter().filter(|p| p.leader).count(),
                "queues_led": m.queues.len(),
                "consumer_groups": m.groups.len(),
                "produce_records": m.partitions.iter().map(|p| p.produce_records).sum::<u64>(),
                "produce_bytes": m.partitions.iter().map(|p| p.produce_bytes).sum::<u64>(),
                "fetch_bytes": m.partitions.iter().map(|p| p.fetch_bytes).sum::<u64>(),
            }),
        })
        .collect();
    let answered = nodes.iter().filter(|n| n["answered"] == true).count();
    let text = format!("{} nodes, {answered} answered.", nodes.len());
    done(json!({ "nodes": nodes }), text)
}

fn mechanism(m: u8) -> &'static str {
    use lotse_proto::session_mechanism::*;
    match m {
        NONE => "none",
        CERTIFICATE => "certificate",
        SCRAM_SHA_512 => "scram-sha-512",
        OAUTHBEARER => "oauthbearer",
        TOKEN => "token",
        _ => "unknown",
    }
}

fn principal(k: u8) -> &'static str {
    use lotse_proto::session_principal::*;
    match k {
        ANONYMOUS => "anonymous",
        USER => "user",
        NODE => "node",
        _ => "unknown",
    }
}

/// The token a session signed in with, null for any other credential.
pub(crate) fn session_token_id(id: Option<u64>) -> Value {
    id.map_or(Value::Null, crate::tokens::token_id)
}

pub(crate) async fn list_sessions<R: Net + Clock + Spawn + Rng>(c: &Client<R>) -> Outcome {
    let mut sessions = Vec::new();
    let mut silent = Vec::new();
    for (node_id, r) in c.node_sessions().await? {
        match r {
            Ok(s) => sessions.extend(s.sessions.iter().map(|e| {
                json!({
                    "node_id": node_id,
                    "principal": principal(e.principal_kind),
                    "name": e.name,
                    "groups": e.groups,
                    "mechanism": mechanism(e.mechanism),
                    "token_id": session_token_id(e.token_id),
                    "peer": e.peer,
                    "connected_ms": e.connected_ms,
                    "expires_ms": if e.expires_ms == 0 { Value::Null } else { json!(e.expires_ms) },
                })
            })),
            Err(e) => silent.push(json!({"node_id": node_id, "error": e.to_string()})),
        }
    }
    let text = format!("{} open sessions; {} nodes did not answer.", sessions.len(), silent.len());
    done(json!({ "sessions": sessions, "nodes_not_answering": silent }), text)
}

pub(crate) async fn describe_quotas<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let d = c.describe_quotas(a.opt_str("tenant")?).await?;
    let quotas: Vec<Value> = d
        .quotas
        .iter()
        .map(|q| {
            json!({
                "tenant": q.tenant,
                "prefix": q.prefix,
                "bytes": q.bytes,
                "full": q.full,
                "set_by_cluster_admin": q.set_by_cluster_admin,
                "used_bytes": q.used_bytes,
            })
        })
        .collect();
    let full = d.quotas.iter().filter(|q| q.full).count();
    let limits = d.quotas.iter().filter(|q| q.bytes.is_some()).count();
    let text = match d.counted_at_ms {
        Some(_) => format!("{limits} storage limits, {full} full."),
        None => format!("{limits} storage limits, {full} full; usage is not counted yet."),
    };
    done(json!({"quotas": quotas, "counted_at_ms": d.counted_at_ms}), text)
}

pub(crate) async fn describe_upgrade<R: Net + Clock + Spawn + Rng>(c: &Client<R>) -> Outcome {
    let u = c.describe_upgrade().await?;
    let nodes: Vec<Value> = u
        .nodes
        .iter()
        .map(|n| {
            let reported = !n.release.is_empty();
            json!({
                "node_id": n.node_id,
                "release": if reported { json!(n.release) } else { Value::Null },
                "oldest_cluster_version": if reported { json!(version(n.oldest)) } else { Value::Null },
                "newest_cluster_version": if reported { json!(version(n.newest)) } else { Value::Null },
            })
        })
        .collect();
    let cluster_version = u.cluster_version.map(version);
    let turn = u.turn.map(|t| json!({"node_id": t.node_id, "since_ms": t.since_ms}));
    let text = format!(
        "Cluster version {}; {}{}.",
        cluster_version.as_deref().unwrap_or("not set yet"),
        match u.turn {
            Some(t) => format!("node {} holds the restart turn", t.node_id),
            None => "no node holds the restart turn".to_string(),
        },
        if u.held { "; turns are paused" } else { "" }
    );
    done(json!({"cluster_version": cluster_version, "turn": turn, "paused": u.held, "nodes": nodes}), text)
}
