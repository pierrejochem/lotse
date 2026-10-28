//! Admin requests: creating streams and queues, declaring exchanges and
//! binding queues to them, at runtime ([ADR 0034](../../docs/adr/0034-metadata-group.md)).
//!
//! They go to the metadata group's leader. Any broker answers `NOT_LEADER`
//! with a hint to it, which the client follows. A create whose outcome is
//! unknown is retried; if the first attempt did create the resource, the
//! retry finds it and the answer is [`ClientError::ResourceExists`] with its
//! id, which the caller can compare with what it expected.

use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    BindRequest, CreateQueueRequest, CreateStreamRequest, DeclareExchangeRequest, ErrorCode, LeaderHint, Request,
    Response,
};
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Net, Spawn};

use crate::{Attempt, Client, ClientError};

impl<R: Net + Clock + Spawn> Client<R> {
    /// Creates a stream with `partitions` partitions, each replicated by
    /// `replication` nodes; returns its id.
    pub async fn create_stream(&self, name: &str, partitions: u32, replication: u8) -> Result<u64, ClientError> {
        let req = Request::CreateStream(CreateStreamRequest {
            name: name.into(),
            partitions,
            replication,
            configs: Vec::new(),
        });
        self.create(name, &req).await
    }

    /// Creates a queue replicated by `replication` nodes; returns its id.
    /// `ack_timeout` overrides the default consumer ack timeout.
    pub async fn create_queue(
        &self,
        name: &str,
        replication: u8,
        ack_timeout: Option<Duration>,
    ) -> Result<u64, ClientError> {
        let configs = ack_timeout.map(|t| vec![("ack.timeout.ms".to_string(), t.as_millis().to_string())]);
        let req = Request::CreateQueue(CreateQueueRequest {
            name: name.into(),
            priority_levels: 1,
            single_active: false,
            dlq: None,
            replication,
            configs: configs.unwrap_or_default(),
        });
        self.create(name, &req).await
    }

    /// Declares an exchange; returns its id.
    pub async fn declare_exchange(&self, name: &str, kind: ExchangeKind) -> Result<u64, ClientError> {
        let req = Request::DeclareExchange(DeclareExchangeRequest {
            name: name.into(),
            exchange_type: kind.code(),
            configs: Vec::new(),
        });
        self.create(name, &req).await
    }

    /// Binds `queue` to `exchange` with a binding key (direct, topic).
    pub async fn bind(&self, exchange: &str, queue: &str, key: &str) -> Result<(), ClientError> {
        self.binding(exchange, queue, key, Vec::new(), true, false).await
    }

    /// Binds `queue` to a headers exchange: all (or any) of `headers` must
    /// match.
    pub async fn bind_headers(
        &self,
        exchange: &str,
        queue: &str,
        headers: Vec<(Bytes, Option<Bytes>)>,
        match_all: bool,
    ) -> Result<(), ClientError> {
        self.binding(exchange, queue, "", headers, match_all, false).await
    }

    /// Removes a binding made by [`Client::bind`].
    pub async fn unbind(&self, exchange: &str, queue: &str, key: &str) -> Result<(), ClientError> {
        self.binding(exchange, queue, key, Vec::new(), true, true).await
    }

    async fn binding(
        &self,
        exchange: &str,
        queue: &str,
        key: &str,
        header_match: Vec<(Bytes, Option<Bytes>)>,
        match_all: bool,
        unbind: bool,
    ) -> Result<(), ClientError> {
        let m = self.metadata_from_all(&[exchange, queue]).await;
        let id = |name: &str| m.resources.iter().find(|r| r.name == name).map(|r| r.resource_id);
        let (Some(exchange_id), Some(queue_id)) = (id(exchange), id(queue)) else {
            return Err(ClientError::UnknownStream(format!("{exchange} or {queue}"), 0));
        };
        let b = BindRequest { exchange_id, queue_id, binding_key: key.into(), header_match, match_all };
        let req = if unbind { Request::Unbind(b) } else { Request::Bind(b) };
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::Outcome(o) => self.admin_answer(o.error, o.leader_hint).map(|_| ()),
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await?;
        // Publishing routes by the bindings it has seen: see the new ones.
        self.exchanges.lock().expect("exchanges lock").remove(exchange);
        Ok(())
    }

    async fn create(&self, name: &str, req: &Request) -> Result<u64, ClientError> {
        let id = self
            .retrying(|| async {
                match self.call_admin(req).await? {
                    Response::Create(c) => match self.admin_answer(c.error, c.leader_hint) {
                        Ok(()) => Ok(c.resource_id),
                        Err(Attempt::Fail(ClientError::Broker { code: ErrorCode::RESOURCE_EXISTS, .. })) => {
                            Err(Attempt::Fail(ClientError::ResourceExists {
                                name: name.into(),
                                resource_id: c.resource_id,
                            }))
                        }
                        Err(Attempt::Fail(ClientError::Broker { code, .. })) => {
                            Err(Attempt::Fail(ClientError::Broker { code, message: c.error_msg.clone() }))
                        }
                        Err(e) => Err(e),
                    },
                    other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
                }
            })
            .await?;
        Ok(id)
    }

    /// Maps an admin answer: retriable errors (following a leader hint) and
    /// failures.
    fn admin_answer(&self, error: ErrorCode, hint: Option<LeaderHint>) -> Result<(), Attempt> {
        match error {
            ErrorCode::NONE => Ok(()),
            code if code.retriable() => {
                *self.metadata_leader.lock().expect("leader lock") = hint.map(|h| h.leader);
                Err(Attempt::RetryMaybeDone(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Sends an admin request to the metadata leader, or any broker if it
    /// is not known.
    async fn call_admin(&self, req: &Request) -> Result<Response, Attempt> {
        let leader = *self.metadata_leader.lock().expect("leader lock");
        let addr = match leader.and_then(|l| self.brokers.lock().expect("brokers lock").get(&l).cloned()) {
            Some(a) => a,
            None => {
                let known: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
                let pool = if known.is_empty() { self.cfg.bootstrap.clone() } else { known };
                let n = self.admin_turn.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;
                pool[n % pool.len().max(1)].clone()
            }
        };
        match self.call(&addr, req).await {
            Ok(r) => Ok(r),
            Err(e) => {
                *self.metadata_leader.lock().expect("leader lock") = None;
                // The request may have arrived: a retry finds what it did.
                Err(Attempt::RetryMaybeDone(e.to_string()))
            }
        }
    }
}
