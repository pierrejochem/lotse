//! Publishing through exchanges: [`Client::publish`] and
//! [`Transaction::publish`].
//!
//! The client routes ([ADR 0032](../../docs/adr/0032-publish-routed-by-the-client.md)):
//! `Metadata` for an exchange returns its type and bindings together with
//! the bound queues, the client picks the queues with `lotse-route` and
//! produces the message to each of them. Each write is an ordinary produce,
//! idempotent per queue, so retries never duplicate a message in a queue.
//!
//! Without a transaction every queue is written on its own, as in RabbitMQ:
//! `publish` returns once every routed queue has the message, and otherwise
//! says which queues have it, which do not and which may. Inside a
//! transaction the message reaches all its queues or none.

use std::collections::BTreeMap;

use bytes::Bytes;
use lotse_proto::ErrorCode;
use lotse_route::{Binding, Exchange, ExchangeKind};
use lotse_rt::{Clock, Net, Spawn};

use crate::{Client, ClientError, Transaction};

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

impl<R: Net + Clock + Spawn> Client<R> {
    /// The exchange as metadata last said, asking for it if unknown.
    pub(crate) async fn exchange(&self, name: &str) -> Result<Exchange, ClientError> {
        if let Some(x) = self.exchanges.lock().expect("exchanges lock").get(name) {
            return Ok(x.clone());
        }
        // `learn` keeps the exchange and the queues bound to it.
        self.metadata(&[name]).await?;
        self.exchanges
            .lock()
            .expect("exchanges lock")
            .get(name)
            .cloned()
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
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        message: OutMessage,
        mandatory: bool,
    ) -> Result<Published, ClientError> {
        let queues = self.route_message(exchange, routing_key, &message, mandatory).await?;
        let batch = message.batch();
        let mut written = Vec::new();
        let mut failed = Vec::new();
        for q in queues {
            match self.produce(&q, 0, batch.clone()).await {
                Ok(offset) => written.push((q, offset)),
                Err(e) => failed.push((q, e)),
            }
        }
        if failed.is_empty() {
            Ok(Published { routed: written })
        } else {
            Err(ClientError::NotAllWritten { written, failed })
        }
    }

    /// Remembers the exchanges a metadata response describes.
    pub(crate) fn learn_exchanges(&self, m: &lotse_proto::MetadataResponse) {
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
            exchanges
                .insert(r.name.clone(), Exchange { name: r.name.clone(), resource_id: r.resource_id, kind, bindings });
        }
    }
}

impl<R: Net + Clock + Spawn> Transaction<'_, R> {
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
