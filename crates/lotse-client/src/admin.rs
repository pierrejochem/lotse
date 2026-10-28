//! Admin requests: creating, binding, changing and deleting streams,
//! queues and exchanges at runtime ([ADR 0034](../../docs/adr/0034-metadata-group.md),
//! [ADR 0035](../../docs/adr/0035-deleting-resources-and-changing-settings.md)).
//!
//! They go to the metadata group's leader. Any broker answers `NOT_LEADER`
//! with a hint to it, which the client follows. A create whose outcome is
//! unknown is retried; if the first attempt did create the resource, the
//! retry finds it and the answer is [`ClientError::ResourceExists`] with its
//! id, which the caller can compare with what it expected. A delete is done
//! once the resource is gone, whoever deleted it, so a retried delete
//! succeeds too.

use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    AlterConfigsRequest, AlterUserRequest, BindRequest, ConfigEntry, CreateQueueRequest, CreateStreamRequest,
    DeclareExchangeRequest, DeleteResourceRequest, DescribeConfigsRequest, DescribeMetricsRequest,
    DescribeMetricsResponse, DescribeUsersRequest, ErrorCode, LeaderHint, Request, Response, ScramCredentialData,
    UserEntry,
};
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Net, Rng, Spawn};

use crate::{Attempt, CallError, Client, ClientError};

/// PBKDF2 iterations for passwords this client sets (RFC 7677's minimum,
/// Kafka's default).
const SCRAM_ITERATIONS: u32 = lotse_auth::scram::MIN_ITERATIONS;

impl<R: Net + Clock + Spawn + Rng> Client<R> {
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

    /// Deletes a stream, queue or exchange created at runtime, with its
    /// data and bindings; returns its id. Resources from the node
    /// configuration cannot be deleted. Done once the resource is gone,
    /// even if someone else deleted it first.
    pub async fn delete(&self, name: &str) -> Result<u64, ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = Request::DeleteResource(DeleteResourceRequest { resource_id, if_empty: false });
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::Admin(a) => match self.admin_answer(a.error, a.leader_hint) {
                    Err(Attempt::Fail(ClientError::Broker { code: ErrorCode::UNKNOWN_RESOURCE, .. })) => Ok(()),
                    Err(Attempt::Fail(ClientError::Broker { code, .. })) => {
                        Err(Attempt::Fail(ClientError::Broker { code, message: a.error_msg.clone() }))
                    }
                    other => other,
                },
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await?;
        self.exchanges.lock().expect("exchanges lock").remove(name);
        Ok(resource_id)
    }

    /// Changes settings of a resource created at runtime: `set` gives keys
    /// new values, `reset` returns keys to their default. Only a queue's
    /// `ack.timeout.ms` can change so far.
    pub async fn alter_configs(&self, name: &str, set: &[(&str, &str)], reset: &[&str]) -> Result<(), ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = Request::AlterConfigs(AlterConfigsRequest {
            resource_id,
            set: set.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            delete: reset.iter().map(|k| k.to_string()).collect(),
        });
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::Admin(a) => match self.admin_answer(a.error, a.leader_hint) {
                    Err(Attempt::Fail(ClientError::Broker { code, .. })) => {
                        Err(Attempt::Fail(ClientError::Broker { code, message: a.error_msg.clone() }))
                    }
                    other => other,
                },
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// A resource's settings, with where each value comes from
    /// ([`lotse_proto::config_source`]). Read from the metadata leader, so
    /// changes this client made are in it.
    pub async fn describe_configs(&self, name: &str) -> Result<Vec<ConfigEntry>, ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = Request::DescribeConfigs(DescribeConfigsRequest { resource_ids: vec![resource_id] });
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeConfigs(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    match d.resources.into_iter().next() {
                        Some(r) if r.error == ErrorCode::NONE => Ok(r.configs),
                        Some(r) => Err(Attempt::Fail(ClientError::Broker { code: r.error, message: None })),
                        None => Err(Attempt::Retry("no resource in the answer".into())),
                    }
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Sets `user`'s SCRAM-SHA-512 password, creating the user if needed.
    /// Only a salted credential leaves the client; the password does not.
    pub async fn set_user_password(&self, user: &str, password: &str) -> Result<(), ClientError> {
        let mut salt = [0u8; 16];
        self.rt.fill_secret(&mut salt);
        let c = lotse_auth::scram::ScramCredential::new(password, &salt, SCRAM_ITERATIONS);
        let credential = ScramCredentialData {
            iterations: c.iterations,
            salt: Bytes::copy_from_slice(&c.salt),
            stored_key: Bytes::from(c.stored_key),
            server_key: Bytes::from(c.server_key),
        };
        self.alter_user(AlterUserRequest { user: user.into(), credential: Some(credential) }).await
    }

    /// Removes `user`'s SCRAM credential. Sessions it opened end at the
    /// latest when the node's maximum session time is up.
    pub async fn delete_user(&self, user: &str) -> Result<(), ClientError> {
        self.alter_user(AlterUserRequest { user: user.into(), credential: None }).await
    }

    async fn alter_user(&self, req: AlterUserRequest) -> Result<(), ClientError> {
        let req = Request::AlterUser(req);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::Admin(a) => match self.admin_answer(a.error, a.leader_hint) {
                    Err(Attempt::Fail(ClientError::Broker { code, .. })) => {
                        Err(Attempt::Fail(ClientError::Broker { code, message: a.error_msg.clone() }))
                    }
                    other => other,
                },
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// The users with a SCRAM credential, configured and set at runtime.
    pub async fn describe_users(&self) -> Result<Vec<UserEntry>, ClientError> {
        let req = Request::DescribeUsers(DescribeUsersRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeUsers(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d.users)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Every node's metrics, asked of each node directly, for every node
    /// the cluster's metadata names: each with its answer, or why there is
    /// none. Counters grow from each node's start; see `DescribeMetrics`.
    pub async fn node_metrics(&self) -> Result<Vec<(u32, Result<DescribeMetricsResponse, ClientError>)>, ClientError> {
        let m = self.metadata(&[]).await?;
        let mut out = Vec::with_capacity(m.brokers.len());
        for b in &m.brokers {
            let addr = format!("{}:{}", b.host, b.port);
            let res = match self.call(&addr, &Request::DescribeMetrics(DescribeMetricsRequest)).await {
                Ok(Response::DescribeMetrics(r)) if r.error == ErrorCode::NONE => Ok(*r),
                Ok(Response::DescribeMetrics(r)) => Err(ClientError::Broker { code: r.error, message: None }),
                Ok(Response::Error(code)) => Err(ClientError::Broker { code, message: None }),
                Ok(other) => Err(ClientError::NoAnswer(b.node_id, format!("unexpected response {other:?}"))),
                Err(e) => Err(ClientError::NoAnswer(b.node_id, format!("{addr}: {e}"))),
            };
            out.push((b.node_id, res));
        }
        Ok(out)
    }

    /// The id of the resource called `name`, asking every broker before
    /// calling it unknown.
    async fn resource_id(&self, name: &str) -> Result<u64, ClientError> {
        let m = self.metadata_from_all(&[name]).await;
        let found = m.resources.iter().find(|r| r.name == name && r.error == ErrorCode::NONE);
        found.map(|r| r.resource_id).ok_or_else(|| ClientError::UnknownStream(name.into(), 0))
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
            Err(CallError::Denied(why)) => Err(Attempt::Fail(ClientError::Authentication(why))),
            Err(e) => {
                *self.metadata_leader.lock().expect("leader lock") = None;
                // The request may have arrived: a retry finds what it did.
                Err(Attempt::RetryMaybeDone(e.to_string()))
            }
        }
    }
}
