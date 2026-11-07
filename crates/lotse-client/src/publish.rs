//! Publishing through exchanges: [`Client::publish`] and
//! [`Transaction::publish`].
//!
//! Outside a transaction a node routes ([ADR 0079](../../docs/adr/0079-publish-routed-by-the-node.md)):
//! the client sends `Publish` to any node, which routes by the exchange's
//! bindings as it knows them, checks that the publisher may write every
//! queue the message routes to, and writes it to each with an idempotent
//! producer of its own. Every queue is written on its own, as in RabbitMQ:
//! `publish` returns once every routed queue has the message, and otherwise
//! says which queues have it, which do not and which may. A `Publish` whose
//! answer is lost is not sent again, since the node may have written it.
//!
//! Inside a transaction the client routes (ADR 0032): `Metadata` for an
//! exchange returns its type and bindings with the bound queues, the client
//! picks the queues with `lotse-route` and produces the message to each of
//! them transactionally, so it reaches all its queues or none.

use std::collections::BTreeMap;

use bytes::Bytes;
use lotse_proto::{ErrorCode, PublishEntry, PublishRequest, PublishResult, Request, Response};
use lotse_route::{Binding, Exchange, ExchangeKind};
use lotse_rt::{Clock, Net, Rng, Spawn};

use crate::{CallError, Client, ClientError, Transaction};

/// A message to publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutMessage {
    /// Timestamp in milliseconds, chosen by the publisher.
    pub timestamp: i64,
    /// Key, `None` for null.
    pub key: Option<Bytes>,
    /// Value, `None` for null.
    pub value: Option<Bytes>,
    /// Headers in order; a `None` value is null. A headers exchange routes
    /// by them.
    pub headers: Vec<(Bytes, Option<Bytes>)>,
}

impl OutMessage {
    /// A message with only a value.
    pub fn value(value: impl Into<Bytes>) -> Self {
        OutMessage { value: Some(value.into()), ..OutMessage::default() }
    }

    /// The message as a batch of one record.
    pub(crate) fn batch(&self) -> Bytes {
        let mut b = lotse_record::BatchBuilder::new(lotse_record::BatchOptions::default());
        let headers: Vec<(&[u8], Option<&[u8]>)> = self.headers.iter().map(|(k, v)| (&k[..], v.as_deref())).collect();
        b.push(self.timestamp, self.key.as_deref(), self.value.as_deref(), &headers);
        b.finish().expect("one record")
    }
}

/// Where a published message went: each queue it was routed to and the
/// message's offset there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Published {
    /// `(queue, offset)`, in queue name order.
    pub routed: Vec<(String, u64)>,
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// The exchange as metadata last said, asking for it if unknown.
    pub(crate) async fn exchange(&self, name: &str) -> Result<Exchange, ClientError> {
        let now = self.rt.now();
        if let Some((x, at)) = self.exchanges.lock().expect("exchanges lock").get(name)
            && now - *at < self.cfg.metadata_max_age
        {
            return Ok(x.clone());
        }
        // `learn` keeps the exchange and the queues bound to it.
        let m = self.metadata(&[name]).await?;
        if !m.resources.iter().any(|r| r.name == name && r.error == ErrorCode::NONE) {
            // Declared a moment ago: not every broker knows it yet.
            self.metadata_from_all(&[name]).await;
        }
        self.exchanges
            .lock()
            .expect("exchanges lock")
            .get(name)
            .map(|(x, _)| x.clone())
            .ok_or_else(|| ClientError::UnknownStream(name.into(), 0))
    }

    /// The queues `message` goes to, by name. With `mandatory`, a message no
    /// queue takes is refused.
    pub(crate) async fn route_message(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &OutMessage,
        mandatory: bool,
    ) -> Result<Vec<String>, ClientError> {
        let x = self.exchange(exchange).await?;
        let headers = message.headers.iter().map(|(k, v)| (&k[..], v.as_deref()));
        let ids = x.route(routing_key, headers);
        if ids.is_empty() && mandatory {
            return Err(ClientError::Unroutable { exchange: exchange.into(), routing_key: routing_key.into() });
        }
        let names = self.names.lock().expect("names lock");
        let mut queues: Vec<String> = Vec::new();
        for id in ids {
            match names.get(&id) {
                Some(n) => queues.push(n.clone()),
                None => return Err(ClientError::UnknownStream(format!("queue {id} bound to {exchange}"), 0)),
            }
        }
        queues.sort();
        Ok(queues)
    }

    /// Publishes a message to an exchange, which routes it to queues by
    /// `routing_key` (and headers). Returns once every routed queue holds
    /// it; a message no queue takes is dropped, or refused with `mandatory`.
    /// If some queues could not be written, the error says which hold it.
    /// A node routes it (ADR 0079); if the node's answer is lost, the result
    /// is [`ClientError::Unknown`] rather than a second publish.
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        message: OutMessage,
        mandatory: bool,
    ) -> Result<Published, ClientError> {
        let batch = message.batch();
        // A name deleted and made again has a new id: ask once more.
        for fresh in [false, true] {
            if fresh {
                self.exchanges.lock().expect("exchanges lock").remove(exchange);
            }
            let x = self.exchange(exchange).await?;
            let entry = PublishEntry {
                exchange_id: x.resource_id,
                routing_key: routing_key.into(),
                mandatory,
                batch: batch.clone(),
            };
            let result = self.publish_once(entry).await?;
            match result.error {
                ErrorCode::NONE => return self.published(result),
                ErrorCode::UNKNOWN_RESOURCE if !fresh => continue,
                ErrorCode::UNKNOWN_RESOURCE => return Err(ClientError::UnknownStream(exchange.into(), 0)),
                ErrorCode::UNROUTABLE => {
                    return Err(ClientError::Unroutable { exchange: exchange.into(), routing_key: routing_key.into() });
                }
                code => return Err(ClientError::Broker { code, message: None }),
            }
        }
        unreachable!("the second attempt returns")
    }

    /// One `Publish` to the first node that takes it.
    async fn publish_once(&self, entry: PublishEntry) -> Result<PublishResult, ClientError> {
        let timeout_ms = u32::try_from(self.cfg.produce_timeout.as_millis()).unwrap_or(u32::MAX);
        let req = Request::Publish(PublishRequest { txn_id: None, timeout_ms, entries: vec![entry] });
        let start = self.rt.now();
        let mut why = String::from("no broker known");
        loop {
            let mut addrs: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
            addrs.extend(self.cfg.bootstrap.iter().cloned());
            addrs.dedup();
            for addr in addrs {
                match self.call(&addr, &req).await {
                    Ok(Response::Publish(mut r)) if r.entries.len() == 1 => return Ok(r.entries.remove(0)),
                    Ok(other) => why = format!("unexpected response {other:?}"),
                    Err(CallError::Denied(why)) => return Err(ClientError::Authentication(why)),
                    Err(CallError::Refused(code, why)) => return Err(ClientError::Broker { code, message: Some(why) }),
                    // The node may have written it: not sent again.
                    Err(CallError::Lost(why)) => return Err(ClientError::Unknown(format!("{addr}: {why}"))),
                    Err(e) => why = format!("{addr}: {e}"),
                }
            }
            if self.rt.now() - start >= self.cfg.retry_for {
                return Err(ClientError::GaveUp(self.cfg.retry_for, why));
            }
            self.rt.sleep(self.cfg.backoff).await;
        }
    }

    /// A routed message's outcome by queue name.
    fn published(&self, result: PublishResult) -> Result<Published, ClientError> {
        // The node names each queue: the client may never have seen one
        // bound and deleted between two of its lookups. Only a queue the
        // node no longer knows comes without a name.
        let names = self.names.lock().expect("names lock").clone();
        let name = |q: &lotse_proto::RoutedMessage| {
            if q.name.is_empty() {
                names.get(&q.queue_id).cloned().unwrap_or_else(|| format!("queue {}", q.queue_id))
            } else {
                q.name.clone()
            }
        };
        let mut written = Vec::new();
        let mut failed = Vec::new();
        for q in &result.routed {
            match q.error {
                ErrorCode::NONE => written.push((name(q), q.base_offset)),
                ErrorCode::REQUEST_TIMED_OUT => {
                    failed.push((name(q), ClientError::Unknown("the write's outcome is unknown".into())))
                }
                code => failed.push((name(q), ClientError::Broker { code, message: None })),
            }
        }
        written.sort();
        failed.sort_by(|a, b| a.0.cmp(&b.0));
        if failed.is_empty() {
            Ok(Published { routed: written })
        } else {
            Err(ClientError::NotAllWritten { written, failed })
        }
    }

    /// Remembers the exchanges a metadata response describes.
    pub(crate) fn learn_exchanges(&self, m: &lotse_proto::MetadataResponse) {
        let now = self.rt.now();
        let mut exchanges = self.exchanges.lock().expect("exchanges lock");
        for r in m.resources.iter().filter(|r| r.error == ErrorCode::NONE) {
            let Some(x) = &r.exchange else { continue };
            let Some(kind) = ExchangeKind::from_code(x.exchange_type) else { continue };
            let bindings = x
                .bindings
                .iter()
                .map(|b| Binding {
                    queue_id: b.queue_id,
                    key: b.binding_key.clone(),
                    headers: b.header_match.clone(),
                    match_all: b.match_all,
                })
                .collect();
            let x = Exchange { name: r.name.clone(), resource_id: r.resource_id, kind, bindings };
            exchanges.insert(r.name.clone(), (x, now));
        }
    }
}

impl<R: Net + Clock + Spawn + Rng> Transaction<'_, R> {
    /// Publishes a message within the transaction: it reaches every queue it
    /// is routed to if the transaction commits, and none if it aborts.
    pub async fn publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: OutMessage,
        mandatory: bool,
    ) -> Result<Published, ClientError> {
        let queues = self.client.route_message(exchange, routing_key, &message, mandatory).await?;
        let batch = message.batch();
        let mut routed = Vec::new();
        for q in queues {
            match self.produce(&q, 0, batch.clone()).await {
                Ok(offset) => routed.push((q, offset)),
                Err(e) => {
                    // Some queues may hold it already: committing would
                    // deliver it to those only. Only an abort is left.
                    if !routed.is_empty() {
                        self.poisoned = Some(format!("published to {routed:?} only: {e}"));
                    }
                    return Err(e);
                }
            }
        }
        Ok(Published { routed })
    }
}

/// Summary of a partial publish, for [`ClientError::NotAllWritten`].
pub(crate) fn describe_partial(written: &[(String, u64)], failed: &[(String, ClientError)]) -> String {
    let failed: BTreeMap<&str, String> = failed.iter().map(|(q, e)| (q.as_str(), e.to_string())).collect();
    let written: Vec<&str> = written.iter().map(|(q, _)| q.as_str()).collect();
    format!("written to {written:?}; not, or not known, for {failed:?}")
}
