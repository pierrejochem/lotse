//! Tools for data: reading a stream, producing, publishing and looking up
//! offsets. No tool takes messages from a queue: a consumer that reads
//! takes them out of flight for others, which an agent should not do by
//! accident (ADR 0134).

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use lotse_client::{Client, ClientError, OffsetAt, OutMessage};
use lotse_proto::{ErrorCode, kind};
use lotse_record::{BatchBuilder, BatchOptions, Record};
use lotse_rt::{Clock, Net, Rng, Spawn};
use serde_json::{Map, Value, json};

use crate::{Args, Outcome, ToolError, ToolSpec, done, spec, strings};

/// At most this many records come back from one `read_stream`, and go in
/// one `produce`.
pub(crate) const MAX_RECORDS: u64 = 100;

/// At most this many bytes of record data (keys, values, header names and
/// values, before any base64) come back from one `read_stream`.
pub(crate) const MAX_BYTES: usize = 1 << 20;

fn record_props() -> Value {
    json!({
        "type": "object",
        "description": "One record.",
        "properties": {
            "key": {"type": "string", "description": "The key; absent for none."},
            "value": {"type": "string", "description": "The value; absent for none (a tombstone)."},
            "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Headers, name to value."},
            "encoding": {"type": "string", "enum": ["utf8", "base64"], "description": "How key, value and header values are written: utf8 (default) or base64."},
        },
        "additionalProperties": false,
    })
}

pub(crate) fn specs() -> Vec<ToolSpec> {
    vec![
        spec(
            "read_stream",
            "Reads committed records of one stream partition (read_committed), from an offset or a time, or from \
             the start: at most 100 records or 1 MiB of record data. Records whose key, value or headers are not \
             UTF-8 come back in base64, with encoding base64; header names are text. next_offset is where to read on.",
            json!({
                "stream": {"type": "string", "description": "The stream's name."},
                "partition": {"type": "integer", "minimum": 0, "description": "The partition; 0 by default."},
                "offset": {"type": "integer", "minimum": 0, "description": "The first offset to read. Give this or time_ms, not both."},
                "time_ms": {"type": "integer", "description": "Read from the first record at or after this time, in ms since the Unix epoch."},
                "max_records": {"type": "integer", "minimum": 1, "maximum": MAX_RECORDS, "description": "At most this many records, 1 to 100; 100 by default."},
            }),
            &["stream"],
            true,
            false,
        ),
        spec(
            "produce",
            "Appends records to a stream partition or a queue, idempotently, in one batch of at most 100 records, \
             stamped with the current time. Returns the first record's offset.",
            json!({
                "name": {"type": "string", "description": "The stream's or queue's name."},
                "partition": {"type": "integer", "minimum": 0, "description": "The stream partition; 0 by default, and always 0 for a queue."},
                "records": {"type": "array", "minItems": 1, "maxItems": MAX_RECORDS, "items": record_props(), "description": "The records, 1 to 100."},
            }),
            &["name", "records"],
            false,
            false,
        ),
        spec(
            "publish",
            "Publishes one message to an exchange, which routes it to queues by its routing key (and headers). \
             Returns each queue it went to with the message's offset there.",
            json!({
                "exchange": {"type": "string", "description": "The exchange's name."},
                "routing_key": {"type": "string", "description": "The routing key; empty for a fanout or headers exchange."},
                "key": {"type": "string", "description": "The message key; absent for none."},
                "value": {"type": "string", "description": "The message value; absent for none."},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Headers, name to value; a headers exchange routes by them."},
                "encoding": {"type": "string", "enum": ["utf8", "base64"], "description": "How key, value and header values are written: utf8 (default) or base64."},
                "mandatory": {"type": "boolean", "description": "Refuse a message no queue takes, instead of dropping it; false by default."},
            }),
            &["exchange", "routing_key"],
            false,
            false,
        ),
        spec(
            "list_offsets",
            "A stream's or queue's offsets per partition: the first still stored (earliest), the end a \
             read_committed reader sees (latest), and with time_ms the first offset at or after that time.",
            json!({
                "stream": {"type": "string", "description": "The stream's or queue's name."},
                "partition": {"type": "integer", "minimum": 0, "description": "Only this partition."},
                "time_ms": {"type": "integer", "description": "Also look up this time, in ms since the Unix epoch."},
            }),
            &["stream"],
            true,
            false,
        ),
    ]
}

/// Bytes of a record's data, as the read limit counts them.
pub(crate) fn record_size(r: &Record) -> usize {
    let len = |b: &Option<Bytes>| b.as_ref().map_or(0, Bytes::len);
    len(&r.key) + len(&r.value) + r.headers.iter().map(|h| h.key.len() + len(&h.value)).sum::<usize>()
}

/// A record as JSON: key, value and header values as text when all of
/// them are UTF-8, otherwise all in base64, so one record has one encoding.
/// Header names are always text, as they are JSON object keys when
/// produced.
pub(crate) fn record_json(r: &Record) -> Value {
    let utf8 = |b: &[u8]| std::str::from_utf8(b).is_ok();
    let text = r.key.as_deref().is_none_or(utf8)
        && r.value.as_deref().is_none_or(utf8)
        && r.headers.iter().all(|h| h.value.as_deref().is_none_or(utf8));
    let show = |b: &[u8]| {
        if text { Value::String(String::from_utf8_lossy(b).into_owned()) } else { Value::String(BASE64.encode(b)) }
    };
    let opt = |b: &Option<Bytes>| b.as_deref().map_or(Value::Null, show);
    let headers: Map<String, Value> =
        r.headers.iter().map(|h| (String::from_utf8_lossy(&h.key).into_owned(), opt(&h.value))).collect();
    json!({
        "offset": r.offset,
        "timestamp_ms": r.timestamp,
        "key": opt(&r.key),
        "value": opt(&r.value),
        "headers": headers,
        "encoding": if text { "utf8" } else { "base64" },
    })
}

pub(crate) async fn read_stream<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let stream = a.str("stream")?;
    let partition = a.opt_u32("partition")?.unwrap_or(0);
    let max = a.opt_ranged("max_records", 1, MAX_RECORDS)?.unwrap_or(MAX_RECORDS) as usize;
    let start = match (a.opt_u64("offset")?, a.opt_i64("time_ms")?) {
        (Some(_), Some(_)) => return Err(ToolError::invalid("give \"offset\" or \"time_ms\", not both")),
        (Some(o), None) => o,
        (None, Some(t)) => c.list_offset(stream, partition, OffsetAt::Time(t)).await?,
        (None, None) => c.list_offset(stream, partition, OffsetAt::Earliest).await?,
    };
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut limited_by = None;
    let mut offset = start;
    let mut next = start;
    let mut high_watermark;
    let mut last_stable;
    'reading: loop {
        let f = c.fetch(stream, partition, offset, Duration::ZERO).await?;
        high_watermark = f.high_watermark;
        last_stable = f.last_stable_offset;
        for batch in f.committed_batches()? {
            let decoded = batch.records().map_err(|e| {
                ToolError::from(ClientError::Broker { code: ErrorCode::CORRUPT_BATCH, message: Some(e.to_string()) })
            })?;
            // A batch may begin before the offset asked for.
            for r in decoded.iter().filter(|r| r.offset >= offset) {
                if records.len() == max {
                    limited_by = Some("records");
                    next = r.offset;
                    break 'reading;
                }
                let size = record_size(r);
                if bytes + size > MAX_BYTES {
                    limited_by = Some("bytes");
                    if records.is_empty() {
                        // Alone above the limit: say so and step over it,
                        // or the agent could never read past it.
                        records.push(json!({
                            "offset": r.offset,
                            "timestamp_ms": r.timestamp,
                            "too_large": true,
                            "size_bytes": size,
                        }));
                        next = r.offset + 1;
                    } else {
                        next = r.offset;
                    }
                    break 'reading;
                }
                bytes += size;
                records.push(record_json(r));
                next = r.offset + 1;
            }
        }
        // Past control batches and aborted data too.
        let after = f.next_offset(offset);
        if after <= offset {
            break;
        }
        next = after;
        offset = after;
        if offset >= f.last_stable_offset {
            break;
        }
    }
    let mut text = format!("Read {} records from {stream} partition {partition}", records.len());
    if let (Some(first), Some(last)) = (records.first(), records.last()) {
        text.push_str(&format!(", offsets {} to {}", first["offset"], last["offset"]));
    }
    text.push_str(&format!("; next offset {next}"));
    match limited_by {
        Some("records") => text.push_str(&format!("; stopped at {max} records.")),
        Some(_) => text.push_str("; stopped at 1 MiB of record data."),
        None => text.push('.'),
    }
    done(
        json!({
            "stream": stream,
            "partition": partition,
            "records": records,
            "next_offset": next,
            "high_watermark": high_watermark,
            "last_stable_offset": last_stable,
            "limited_by": limited_by,
        }),
        text,
    )
}

/// Data from the agent: text as UTF-8, or base64.
fn decode(field: &str, s: &str, base64: bool) -> Result<Bytes, ToolError> {
    if !base64 {
        return Ok(Bytes::copy_from_slice(s.as_bytes()));
    }
    BASE64.decode(s).map(Bytes::from).map_err(|e| ToolError::invalid(format!("{field} is not valid base64: {e}")))
}

fn encoding(v: Option<&Value>) -> Result<bool, ToolError> {
    match v {
        None | Some(Value::Null) => Ok(false),
        Some(Value::String(s)) if s == "utf8" => Ok(false),
        Some(Value::String(s)) if s == "base64" => Ok(true),
        Some(other) => Err(ToolError::invalid(format!("\"encoding\" is utf8 or base64, not {other}"))),
    }
}

fn opt_text(field: &str, v: Option<&Value>, base64: bool) -> Result<Option<Bytes>, ToolError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => decode(field, s, base64).map(Some),
        Some(other) => Err(ToolError::invalid(format!("{field} must be a string, not {other}"))),
    }
}

/// One record or message from the agent.
struct Incoming {
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<(Bytes, Option<Bytes>)>,
}

fn incoming(at: &str, m: &Map<String, Value>) -> Result<Incoming, ToolError> {
    let base64 = encoding(m.get("encoding"))?;
    let headers = strings(&format!("{at}.headers"), m.get("headers"))?
        .into_iter()
        .map(|(k, v)| Ok((Bytes::from(k), Some(decode(&format!("{at}.headers"), &v, base64)?))))
        .collect::<Result<Vec<_>, ToolError>>()?;
    Ok(Incoming {
        key: opt_text(&format!("{at}.key"), m.get("key"), base64)?,
        value: opt_text(&format!("{at}.value"), m.get("value"), base64)?,
        headers,
    })
}

pub(crate) async fn produce<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let name = a.str("name")?;
    let partition = a.opt_u32("partition")?.unwrap_or(0);
    let items = a.opt_array("records")?.map(Vec::as_slice).unwrap_or_default();
    if items.is_empty() || items.len() as u64 > MAX_RECORDS {
        return Err(ToolError::invalid(format!("\"records\" holds 1 to {MAX_RECORDS} records, not {}", items.len())));
    }
    let now = c.unix_time_ms();
    let mut b = BatchBuilder::new(BatchOptions::default());
    for (i, item) in items.iter().enumerate() {
        let at = format!("records[{i}]");
        let Value::Object(m) = item else {
            return Err(ToolError::invalid(format!("{at} must be an object, not {item}")));
        };
        if let Some(k) = m.keys().find(|k| !["key", "value", "headers", "encoding"].contains(&k.as_str())) {
            return Err(ToolError::invalid(format!("{at} takes key, value, headers and encoding, not {k:?}")));
        }
        let r = incoming(&at, m)?;
        let headers: Vec<(&[u8], Option<&[u8]>)> = r.headers.iter().map(|(k, v)| (&k[..], v.as_deref())).collect();
        b.push(now, r.key.as_deref(), r.value.as_deref(), &headers);
    }
    let batch = b.finish().map_err(|e| ToolError::invalid(format!("the records do not make a batch: {e}")))?;
    let offset = c.produce(name, partition, batch).await?;
    let text = format!("Wrote {} records to {name} partition {partition} from offset {offset}.", items.len());
    done(json!({"name": name, "partition": partition, "base_offset": offset, "records": items.len()}), text)
}

pub(crate) async fn publish<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let exchange = a.str("exchange")?;
    let routing_key = a.str("routing_key")?;
    let mandatory = a.opt_bool("mandatory")?.unwrap_or(false);
    let mut m = Map::new();
    for k in ["key", "value", "headers", "encoding"] {
        if let Some(v) = a.0.get(k) {
            m.insert(k.to_string(), v.clone());
        }
    }
    let r = incoming("message", &m)?;
    let message = OutMessage { timestamp: c.unix_time_ms(), key: r.key, value: r.value, headers: r.headers };
    let published = c.publish(exchange, routing_key, message, mandatory).await?;
    let routed: Vec<Value> = published.routed.iter().map(|(q, o)| json!({"queue": q, "offset": o})).collect();
    let text = if routed.is_empty() {
        format!("No queue bound to {exchange} took routing key {routing_key:?}; the message was dropped.")
    } else {
        let qs: Vec<&str> = published.routed.iter().map(|(q, _)| q.as_str()).collect();
        format!("Published to {}.", qs.join(", "))
    };
    done(json!({"exchange": exchange, "routed": routed}), text)
}

pub(crate) async fn list_offsets<R: Net + Clock + Spawn + Rng>(c: &Client<R>, a: &Args<'_>) -> Outcome {
    let stream = a.str("stream")?;
    let only = a.opt_u32("partition")?;
    let time = a.opt_i64("time_ms")?;
    let m = c.metadata(&[stream]).await?;
    let r = m
        .resources
        .iter()
        .find(|r| r.name == stream && r.error == ErrorCode::NONE && r.kind != kind::EXCHANGE)
        .ok_or_else(|| ToolError::unknown(format!("there is no stream or queue named {stream:?} that you may see")))?;
    let mut partitions: Vec<u32> = r.partitions.iter().map(|p| p.partition).collect();
    if let Some(p) = only {
        if !partitions.contains(&p) {
            return Err(ToolError::unknown(format!("{stream} has no partition {p}")));
        }
        partitions = vec![p];
    }
    let mut out = Vec::new();
    for p in partitions {
        let mut v = json!({
            "partition": p,
            "earliest": c.list_offset(stream, p, OffsetAt::Earliest).await?,
            "latest": c.list_offset(stream, p, OffsetAt::Latest).await?,
        });
        if let Some(t) = time {
            v["at_time"] = json!(c.list_offset(stream, p, OffsetAt::Time(t)).await?);
        }
        out.push(v);
    }
    let records: u64 =
        out.iter().map(|v| v["latest"].as_u64().unwrap_or(0).saturating_sub(v["earliest"].as_u64().unwrap_or(0))).sum();
    let text = format!("{stream}: {} partitions, {records} offsets stored in all.", out.len());
    done(json!({"stream": stream, "partitions": out}), text)
}
