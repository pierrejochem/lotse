//! Exchanges and bindings: which queues a published message goes to.
//!
//! An [`Exchange`] holds its bindings and routes a message by its routing
//! key and headers, with RabbitMQ's semantics per [`ExchangeKind`]:
//!
//! - **direct**: a binding matches if its key equals the routing key;
//! - **topic**: keys are words separated by `.`; in a binding key `*`
//!   matches exactly one word and `#` zero or more;
//! - **fanout**: every binding matches, keys are ignored;
//! - **headers**: a binding lists headers that must be present with equal
//!   values, all of them or any one ([`Binding::match_all`]); keys are
//!   ignored.
//!
//! The result is a set: a queue bound twice gets a message once. Routing is
//! pure, so the client, the broker and later protocol adapters can share it.

#![warn(missing_docs)]

use std::collections::BTreeSet;

use bytes::Bytes;

/// How an exchange matches bindings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeKind {
    /// The binding key equals the routing key.
    Direct,
    /// `.`-separated words with `*` and `#` wildcards.
    Topic,
    /// Every bound queue.
    Fanout,
    /// By message headers.
    Headers,
}

impl ExchangeKind {
    /// The wire code (`DIRECT` 0, `TOPIC` 1, `FANOUT` 2, `HEADERS` 3).
    pub fn code(self) -> u8 {
        match self {
            ExchangeKind::Direct => 0,
            ExchangeKind::Topic => 1,
            ExchangeKind::Fanout => 2,
            ExchangeKind::Headers => 3,
        }
    }

    /// The kind for a wire code.
    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            0 => ExchangeKind::Direct,
            1 => ExchangeKind::Topic,
            2 => ExchangeKind::Fanout,
            3 => ExchangeKind::Headers,
            _ => return None,
        })
    }
}

/// A queue bound to an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The queue.
    pub queue_id: u64,
    /// Matched against the routing key (direct, topic).
    pub key: String,
    /// Headers to match (headers exchange): name and value, `None` for a
    /// null value.
    pub headers: Vec<(Bytes, Option<Bytes>)>,
    /// Headers exchange: all listed headers must match (`true`) or any one.
    pub match_all: bool,
}

impl Binding {
    /// A binding by key.
    pub fn key(queue_id: u64, key: impl Into<String>) -> Self {
        Binding { queue_id, key: key.into(), headers: Vec::new(), match_all: true }
    }
}

/// An exchange and its bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exchange {
    /// Name clients publish to.
    pub name: String,
    /// Id in metadata.
    pub resource_id: u64,
    /// How bindings match.
    pub kind: ExchangeKind,
    /// The bound queues.
    pub bindings: Vec<Binding>,
}

impl Exchange {
    /// The queues a message with `routing_key` and `headers` goes to.
    pub fn route<'a>(
        &self,
        routing_key: &str,
        headers: impl IntoIterator<Item = (&'a [u8], Option<&'a [u8]>)> + Clone,
    ) -> BTreeSet<u64> {
        self.bindings
            .iter()
            .filter(|b| match self.kind {
                ExchangeKind::Direct => b.key == routing_key,
                ExchangeKind::Topic => topic_matches(&b.key, routing_key),
                ExchangeKind::Fanout => true,
                ExchangeKind::Headers => headers_match(b, headers.clone()),
            })
            .map(|b| b.queue_id)
            .collect()
    }
}

/// Whether a topic binding key matches a routing key.
pub fn topic_matches(pattern: &str, key: &str) -> bool {
    let (pattern, key) = (words(pattern), words(key));
    matches(&pattern, &key)
}

/// An empty key has no words (not one empty word).
fn words(s: &str) -> Vec<&str> {
    if s.is_empty() { Vec::new() } else { s.split('.').collect() }
}

fn matches(pattern: &[&str], key: &[&str]) -> bool {
    match pattern.split_first() {
        None => key.is_empty(),
        // `#` takes zero words, or one and stays.
        Some((&"#", rest)) => matches(rest, key) || (!key.is_empty() && matches(pattern, &key[1..])),
        Some((&p, rest)) => match key.split_first() {
            Some((&k, krest)) => (p == "*" || p == k) && matches(rest, krest),
            None => false,
        },
    }
}

fn headers_match<'a>(b: &Binding, headers: impl IntoIterator<Item = (&'a [u8], Option<&'a [u8]>)>) -> bool {
    if b.headers.is_empty() {
        return b.match_all;
    }
    let present: Vec<(&[u8], Option<&[u8]>)> = headers.into_iter().collect();
    let hit = |(k, v): &(Bytes, Option<Bytes>)| present.iter().any(|(pk, pv)| *pk == &k[..] && *pv == v.as_deref());
    if b.match_all { b.headers.iter().all(hit) } else { b.headers.iter().any(hit) }
}

#[cfg(test)]
mod tests;
