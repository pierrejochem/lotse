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
    AddNodeRequest, AlterConfigsRequest, AlterRoleBindingsRequest, AlterUserRequest, BindRequest, ConfigEntry,
    CreateQueueRequest, CreateStreamRequest, CreateTenantRequest, DeclareExchangeRequest, DeleteResourceRequest,
    DeleteTenantRequest, DescribeConfigsRequest, DescribeMetricsRequest, DescribeMetricsResponse, DescribeNodesRequest,
    DescribeRoleBindingsRequest, DescribeTenantsRequest, DescribeUsersRequest, ErrorCode, LeaderHint, NodeEntry,
    RemoveNodeRequest, Request, Response, RoleBindingData, ScramCredentialData, SettleVotersRequest, TenantEntry,
    UserEntry, WhoAmIRequest, WhoAmIResponse,
};
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Net, Rng, Spawn};

use crate::{Attempt, CallError, Client, ClientError};

/// PBKDF2 iterations for passwords this client sets (RFC 7677's minimum,
/// Kafka's default).
const SCRAM_ITERATIONS: u32 = lotse_auth::scram::MIN_ITERATIONS;

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// Creates a stream with `partitions` partitions, each replicated by
    /// `replication` nodes, in [`crate::ClientConfig::tenant`]; returns its id.
    pub async fn create_stream(&self, name: &str, partitions: u32, replication: u8) -> Result<u64, ClientError> {
        self.create_stream_with(name, partitions, replication, &[]).await
    }

    /// [`Client::create_stream`] with settings of its own, such as
    /// `retention.ms` and `retention.bytes` (`-1`: no limit), or
    /// `cleanup.policy` (`delete`, `compact`, `compact,delete`) and
    /// `delete.retention.ms` for compaction; a setting it does not name
    /// takes the node's default.
    pub async fn create_stream_with(
        &self,
        name: &str,
        partitions: u32,
        replication: u8,
        configs: &[(&str, &str)],
    ) -> Result<u64, ClientError> {
        let req = Request::CreateStream(CreateStreamRequest {
            name: name.into(),
            tenant: self.cfg.tenant.clone(),
            partitions,
            replication,
            configs: configs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
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
        self.create_queue_in(&self.cfg.tenant, name, replication, ack_timeout).await
    }

    /// [`Client::create_queue`] in `tenant` instead of the configured one.
    pub async fn create_queue_in(
        &self,
        tenant: &str,
        name: &str,
        replication: u8,
        ack_timeout: Option<Duration>,
    ) -> Result<u64, ClientError> {
        let configs = ack_timeout.map(|t| vec![("ack.timeout.ms".to_string(), t.as_millis().to_string())]);
        let req = Request::CreateQueue(CreateQueueRequest {
            name: name.into(),
            tenant: tenant.into(),
            priority_levels: 1,
            single_active: false,
            dlq: None,
            replication,
            configs: configs.unwrap_or_default(),
        });
        self.create(name, &req).await
    }

    /// Creates a queue with settings: `ack.timeout.ms`, `max.deliveries` and
    /// `message.ttl.ms` in `configs`, and the name of a queue of the same
    /// tenant that receives the messages the limits retire (without one they
    /// are dropped). Returns its id.
    pub async fn create_queue_with(
        &self,
        name: &str,
        replication: u8,
        configs: &[(&str, &str)],
        dead_letter: Option<&str>,
    ) -> Result<u64, ClientError> {
        self.create_queue_in_with(&self.cfg.tenant, name, replication, configs, dead_letter).await
    }

    /// [`Client::create_queue_with`] in `tenant` instead of the configured
    /// one.
    pub async fn create_queue_in_with(
        &self,
        tenant: &str,
        name: &str,
        replication: u8,
        configs: &[(&str, &str)],
        dead_letter: Option<&str>,
    ) -> Result<u64, ClientError> {
        let req = Request::CreateQueue(CreateQueueRequest {
            name: name.into(),
            tenant: tenant.into(),
            priority_levels: 1,
            single_active: false,
            dlq: dead_letter.map(str::to_string),
            replication,
            configs: configs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        });
        self.create(name, &req).await
    }

    /// Declares an exchange; returns its id.
    pub async fn declare_exchange(&self, name: &str, kind: ExchangeKind) -> Result<u64, ClientError> {
        let req = Request::DeclareExchange(DeclareExchangeRequest {
            name: name.into(),
            tenant: self.cfg.tenant.clone(),
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
        self.delete_with(name, false).await
    }

    /// [`Client::delete`], but only if the stream or queue holds nothing
    /// (ADR 0063): no unsettled message in a queue, no record in a stream.
    /// Otherwise it fails with [`ErrorCode::RESOURCE_NOT_EMPTY`] and
    /// nothing is deleted. An exchange holds no messages and is deleted.
    /// The check guards against mistakes, not against a producer: a
    /// message produced while the delete is under way goes with it.
    pub async fn delete_if_empty(&self, name: &str) -> Result<u64, ClientError> {
        self.delete_with(name, true).await
    }

    /// Settles every message of `queue` that is ready now: not held by a
    /// consumer, not in an open transaction (ADR 0064). Messages produced
    /// while it runs are left alone. Returns how many were purged. Needs the
    /// tenant-admin role.
    pub async fn purge(&self, queue: &str) -> Result<u64, ClientError> {
        self.queue_action(queue, true).await
    }

    /// Moves every ready message of the dead-letter queue `queue` back to
    /// the queue its dead-letter headers name, once each across failovers
    /// (ADR 0064). Messages without such headers, or whose queue is gone,
    /// stay. Returns how many were moved. Needs the tenant-admin role.
    pub async fn requeue(&self, queue: &str) -> Result<u64, ClientError> {
        self.queue_action(queue, false).await
    }

    async fn queue_action(&self, queue: &str, purge: bool) -> Result<u64, ClientError> {
        let mut total = 0;
        let mut up_to = 0;
        loop {
            let (count, bound, more) = self.retrying(|| self.queue_action_once(queue, purge, up_to)).await?;
            total += count;
            up_to = bound;
            if !more {
                return Ok(total);
            }
        }
    }

    async fn queue_action_once(&self, queue: &str, purge: bool, up_to: u64) -> Result<(u64, u64, bool), Attempt> {
        let (route, addr) = self.route(queue, 0).await?;
        let req = lotse_proto::QueueActionRequest { queue_id: route.resource_id, up_to };
        let req = if purge { Request::PurgeQueue(req) } else { Request::Requeue(req) };
        match self.call(&addr, &req).await {
            Ok(Response::QueueAction(r)) => match r.error {
                ErrorCode::NONE => Ok((r.count, r.up_to, r.more)),
                code if code.retriable() => {
                    self.redirect(queue, 0, r.leader_hint);
                    // Acting again is harmless: what was done is settled.
                    Err(Attempt::RetryMaybeDone(code.to_string()))
                }
                code => Err(Attempt::Fail(ClientError::Broker { code, message: r.error_msg })),
            },
            Ok(other) => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                self.forget(queue, 0);
                Err(e.into())
            }
        }
    }

    async fn delete_with(&self, name: &str, if_empty: bool) -> Result<u64, ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = Request::DeleteResource(DeleteResourceRequest { resource_id, if_empty });
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
        self.set_user_credential(user, credential).await
    }

    /// Sets `user`'s SCRAM-SHA-512 credential from one derived elsewhere,
    /// creating the user if needed. For callers that must not see the
    /// password, such as the console, whose browser derives it. The broker
    /// refuses fewer iterations than its minimum.
    pub async fn set_user_credential(&self, user: &str, credential: ScramCredentialData) -> Result<(), ClientError> {
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

    /// Creates a tenant.
    pub async fn create_tenant(&self, name: &str) -> Result<(), ClientError> {
        self.admin_change(Request::CreateTenant(CreateTenantRequest { name: name.into() })).await
    }

    /// Deletes a tenant that holds no resources, and its role bindings.
    pub async fn delete_tenant(&self, name: &str) -> Result<(), ClientError> {
        self.admin_change(Request::DeleteTenant(DeleteTenantRequest { name: name.into() })).await
    }

    /// The tenants this client has a role in (all of them for a cluster
    /// admin).
    pub async fn describe_tenants(&self) -> Result<Vec<TenantEntry>, ClientError> {
        let req = Request::DescribeTenants(DescribeTenantsRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeTenants(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d.tenants)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Adds node `node_id` to the cluster, reached by nodes at `peer` and
    /// by clients at `client`; start it with `join` afterwards. Ids are
    /// never used twice.
    pub async fn add_node(&self, node_id: u64, peer: &str, client: &str) -> Result<(), ClientError> {
        self.add_node_in_zone(node_id, peer, client, None).await
    }

    /// As [`Client::add_node`], with the node's zone: clients in that zone
    /// fetch from its replicas (ADR 0104).
    pub async fn add_node_in_zone(
        &self,
        node_id: u64,
        peer: &str,
        client: &str,
        zone: Option<&str>,
    ) -> Result<(), ClientError> {
        let req = AddNodeRequest { node_id, peer: peer.into(), client: client.into(), zone: zone.map(Into::into) };
        self.admin_change(Request::AddNode(req)).await
    }

    /// Starts draining node `node_id`: its replicas move to other nodes,
    /// then it leaves the cluster. [`Client::describe_nodes`] shows how far.
    pub async fn remove_node(&self, node_id: u64) -> Result<(), ClientError> {
        self.admin_change(Request::RemoveNode(RemoveNodeRequest { node_id })).await
    }

    /// Pins the leader of `partition` of the stream or queue `name` to node
    /// `node`, one of its voters, or unpins it with `None` so that leaders
    /// spread by themselves again (ADR 0065). The group's leader hands over
    /// shortly after; the pin stays until it is removed or the node leaves.
    /// Needs the cluster-admin role.
    pub async fn move_leader(&self, name: &str, partition: u32, node: Option<u64>) -> Result<(), ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = lotse_proto::MoveLeaderRequest { resource_id, partition, node_id: node.unwrap_or(0) };
        self.admin_change(Request::MoveLeader(req)).await
    }

    /// Raises the partition count of the stream `name`, created at runtime,
    /// to `count` (ADR 0074). Existing partitions stay as they are, so a
    /// key may map to another partition afterwards; consumer groups spread
    /// over the new ones on their next heartbeat. Needs the tenant-admin
    /// role.
    pub async fn create_partitions(&self, name: &str, count: u32) -> Result<(), ClientError> {
        let resource_id = self.resource_id(name).await?;
        let req = Request::CreatePartitions(lotse_proto::CreatePartitionsRequest { resource_id, count });
        self.admin_change(req).await
    }

    /// Revokes the sessions of a user (`group` false) or of every member of
    /// an IdP group (ADR 0070): each node ends their sessions that
    /// authenticated before now, and refuses their tokens issued before now.
    /// A SCRAM user who still has the password can sign in again; change
    /// or delete the password to keep them out. Needs the cluster-admin
    /// role.
    pub async fn revoke_sessions(&self, subject: &str, group: bool) -> Result<(), ClientError> {
        let subject_kind = if group { lotse_proto::subject_kind::GROUP } else { lotse_proto::subject_kind::USER };
        let req = lotse_proto::RevokeSessionsRequest { subject_kind, subject: subject.into(), before_ms: 0 };
        self.admin_change(Request::RevokeSessions(req)).await
    }

    /// Revokes one client certificate (ADR 0090): each node ends the
    /// connections that showed it and refuses it from then on, until it
    /// expires at `not_after_ms` (ms since the Unix epoch). `issuer` is the
    /// SHA-256 of the issuer's DER name and authority key identifier (ADR 0096) and `serial` the serial, as
    /// [`lotse_rt::CertInfo`] has them. The CRL file stays as it is. Needs
    /// the cluster-admin role.
    pub async fn revoke_certificate(
        &self,
        issuer: [u8; 32],
        serial: &[u8],
        not_after_ms: u64,
        reason: &str,
    ) -> Result<(), ClientError> {
        let req = lotse_proto::RevokeCertificateRequest {
            issuer: Bytes::copy_from_slice(&issuer),
            serial: Bytes::copy_from_slice(serial),
            not_after_ms,
            reason: reason.into(),
        };
        self.admin_change(Request::RevokeCertificate(req)).await
    }

    /// The cluster's nodes, as the metadata group knows them.
    pub async fn describe_nodes(&self) -> Result<Vec<NodeEntry>, ClientError> {
        self.nodes_answer(DescribeNodesRequest::default()).await
    }

    /// Internal, for nodes (ADR 0105): the cluster's nodes from a metadata
    /// leader that has applied at least `index`, so the answer is at least
    /// as new as the asking node's own metadata state.
    pub async fn describe_nodes_since(&self, index: u64) -> Result<Vec<NodeEntry>, ClientError> {
        self.nodes_answer(DescribeNodesRequest { applied_at_least: Some(index) }).await
    }

    async fn nodes_answer(&self, req: DescribeNodesRequest) -> Result<Vec<NodeEntry>, ClientError> {
        let req = Request::DescribeNodes(req);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeNodes(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d.nodes)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Internal, for nodes: reports that `group` has exactly `voters`.
    pub async fn settle_voters(&self, group: u64, voters: Vec<u64>) -> Result<(), ClientError> {
        self.admin_change(Request::SettleVoters(SettleVotersRequest { group, voters })).await
    }

    /// Internal, for nodes: reports client certificates the node accepted
    /// (ADR 0091).
    pub async fn record_certificates(
        &self,
        certificates: Vec<lotse_proto::SeenCertificate>,
    ) -> Result<(), ClientError> {
        self.admin_change(Request::RecordCertificates(lotse_proto::RecordCertificatesRequest { certificates })).await
    }

    /// The client certificates the cluster has seen or had revoked, by
    /// expiry (ADR 0091). Needs the cluster-admin role.
    pub async fn describe_certificates(&self) -> Result<lotse_proto::DescribeCertificatesResponse, ClientError> {
        let req = Request::DescribeCertificates(lotse_proto::DescribeCertificatesRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeCertificates(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Asks the CA holder for a client certificate for `principal`, with a
    /// PKCS#10 signing request in DER form (ADR 0092). The private key stays
    /// with whoever made the request. Needs the cluster-admin role.
    pub async fn request_certificate(&self, principal: &str, csr: Vec<u8>) -> Result<(), ClientError> {
        self.admin_change(Request::RequestCertificate(lotse_proto::RequestCertificateRequest {
            principal: principal.into(),
            csr: csr.into(),
        }))
        .await
    }

    /// The certificate requests, newest first (ADR 0092). Needs the
    /// cluster-admin role.
    pub async fn describe_certificate_requests(
        &self,
    ) -> Result<lotse_proto::DescribeCertificateRequestsResponse, ClientError> {
        let req = Request::DescribeCertificateRequests(lotse_proto::DescribeCertificateRequestsRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeCertificateRequests(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Records the certificate the CA holder signed for a request (ADR 0092).
    /// Needs the cluster-admin role.
    pub async fn record_certificate(&self, req: lotse_proto::RecordCertificateRequest) -> Result<(), ClientError> {
        self.admin_change(Request::RecordCertificate(req)).await
    }

    /// Adds and removes role bindings; removals first.
    pub async fn alter_role_bindings(
        &self,
        add: Vec<RoleBindingData>,
        remove: Vec<RoleBindingData>,
    ) -> Result<(), ClientError> {
        self.admin_change(Request::AlterRoleBindings(AlterRoleBindingsRequest { add, remove })).await
    }

    /// Every role binding, each with where it comes from
    /// ([`lotse_proto::config_source`]).
    pub async fn describe_role_bindings(&self) -> Result<Vec<(RoleBindingData, u8)>, ClientError> {
        let req = Request::DescribeRoleBindings(DescribeRoleBindingsRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::DescribeRoleBindings(d) => {
                    self.admin_answer(d.error, d.leader_hint)?;
                    Ok(d.bindings)
                }
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Who this client acts as, and the role bindings that apply.
    pub async fn who_am_i(&self) -> Result<WhoAmIResponse, ClientError> {
        let req = Request::WhoAmI(WhoAmIRequest);
        self.retrying(|| async {
            match self.call_admin(&req).await? {
                Response::WhoAmI(w) => Ok(w),
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// An admin change answered with `AdminResponse`.
    async fn admin_change(&self, req: Request) -> Result<(), ClientError> {
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

    /// Learns the address of node `node` from the first broker whose
    /// metadata lists it. Not from the first broker that answers: one that
    /// lags (cut off from the metadata group) may not know the leader the
    /// others name, and would answer first every time. Only that address is
    /// taken; what else the answer says is left alone.
    async fn find_broker(&self, node: u32) {
        let mut addrs: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
        addrs.extend(self.cfg.bootstrap.iter().cloned());
        addrs.sort();
        addrs.dedup();
        for addr in addrs {
            let req = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });
            let Ok(Response::Metadata(m)) = self.call(&addr, &req).await else { continue };
            if let Some(b) = m.brokers.iter().find(|b| b.node_id == node) {
                self.brokers.lock().expect("brokers lock").insert(node, format!("{}:{}", b.host, b.port));
                return;
            }
        }
    }

    /// Sends an admin request to the metadata leader, or any broker if it
    /// is not known.
    async fn call_admin(&self, req: &Request) -> Result<Response, Attempt> {
        let leader = *self.metadata_leader.lock().expect("leader lock");
        // A leader whose address this client has not seen (a node added
        // since it last asked): learn the cluster's nodes first, or it
        // keeps asking the others. If the broker that answers does not know
        // it either (one that lags), ask the others for its address.
        let unknown = |l: &u32| !self.brokers.lock().expect("brokers lock").contains_key(l);
        if let Some(l) = leader.filter(unknown) {
            let _ = self.metadata(&[]).await;
            if unknown(&l) {
                self.find_broker(l).await;
            }
        }
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
            Err(e @ (CallError::Denied(_) | CallError::Refused(..))) => Err(e.into()),
            Err(e) => {
                *self.metadata_leader.lock().expect("leader lock") = None;
                // The request may have arrived: a retry finds what it did.
                Err(Attempt::RetryMaybeDone(e.to_string()))
            }
        }
    }
}
