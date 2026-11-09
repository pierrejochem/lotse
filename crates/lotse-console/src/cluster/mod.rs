//! The console's view of a cluster.
//!
//! Handlers and the live-update hub only talk to [`Cluster`]:
//! [`native::NativeCluster`] serves a real cluster through `lotse-client`,
//! [`demo::DemoCluster`] a built-in one with sample data. Every method
//! takes the caller's [`Identity`] so that the native cluster can
//! authenticate as the user and let the broker's RBAC decide, once brokers
//! authenticate.

pub mod demo;
pub mod native;

use async_trait::async_trait;

use crate::auth::Identity;
use crate::error::ApiResult;
use crate::metrics::{ChartSample, Visibility};
use crate::model::{
    BindingRow, BindingsView, CertificateRequestsView, CertificatesView, ClusterRef, CreateQueue, Credential,
    Isolation, MoveLeader, NodesView, Overview, QueueDetail, QueueRow, RecordView, StreamDetail, StreamRow,
    TenantsView, UsersView,
};

/// One managed cluster.
#[async_trait]
pub trait Cluster: Send + Sync + 'static {
    /// Cluster ID from the config.
    fn id(&self) -> &str;

    /// Name, size and status for the cluster switcher.
    async fn summary(&self, who: &Identity) -> ApiResult<ClusterRef>;

    /// The overview screen.
    async fn overview(&self, who: &Identity) -> ApiResult<Overview>;

    /// Nodes, upgrade and operator events.
    async fn nodes(&self, who: &Identity) -> ApiResult<NodesView>;

    /// All streams the caller may see.
    async fn streams(&self, who: &Identity) -> ApiResult<Vec<StreamRow>>;

    /// One stream.
    async fn stream(&self, who: &Identity, name: &str) -> ApiResult<StreamDetail>;

    /// All queues the caller may see.
    async fn queues(&self, who: &Identity) -> ApiResult<Vec<QueueRow>>;

    /// One queue.
    async fn queue(&self, who: &Identity, name: &str) -> ApiResult<QueueDetail>;

    /// Creates a queue. The request is already validated.
    async fn create_queue(&self, who: &Identity, req: &CreateQueue) -> ApiResult<()>;

    /// Settles the queue's ready messages; returns how many (ADR 0064).
    async fn purge_queue(&self, who: &Identity, name: &str) -> ApiResult<u64>;

    /// Moves the ready messages of the dead-letter queue `name` back to the
    /// queues they came from; returns how many (ADR 0064).
    async fn requeue(&self, who: &Identity, name: &str) -> ApiResult<u64>;

    /// Pins a partition's leader, or unpins it (ADR 0065).
    async fn move_leader(&self, who: &Identity, req: &MoveLeader) -> ApiResult<()>;

    /// Reads records without a consumer group.
    async fn read(
        &self,
        who: &Identity,
        stream: &str,
        partition: u32,
        offset: u64,
        count: u32,
        isolation: Isolation,
    ) -> ApiResult<Vec<RecordView>>;

    /// Tenants with quota use and role bindings.
    async fn tenants(&self, who: &Identity) -> ApiResult<TenantsView>;

    /// SCRAM users (cluster admins only; the broker decides).
    async fn users(&self, who: &Identity) -> ApiResult<UsersView>;

    /// Sets a user's credential, creating the user (ADR 0089).
    async fn set_user_credential(&self, who: &Identity, name: &str, credential: &Credential) -> ApiResult<()>;

    /// Removes a user's credential.
    async fn delete_user(&self, who: &Identity, name: &str) -> ApiResult<()>;

    /// Role bindings with tenant, prefix and source.
    async fn bindings(&self, who: &Identity) -> ApiResult<BindingsView>;

    /// Adds or removes one binding.
    async fn alter_binding(&self, who: &Identity, binding: &BindingRow, add: bool) -> ApiResult<()>;

    /// The client certificates the cluster has seen or had revoked (ADR 0091).
    async fn certificates(&self, who: &Identity) -> ApiResult<CertificatesView>;

    /// The certificate requests (ADR 0092).
    async fn certificate_requests(&self, who: &Identity) -> ApiResult<CertificateRequestsView>;

    /// Asks the CA holder for a client certificate, with a signing request
    /// the requester's browser made (ADR 0092).
    async fn request_certificate(&self, who: &Identity, principal: &str, csr: Vec<u8>) -> ApiResult<()>;

    /// Revokes one client certificate (ADR 0090).
    async fn revoke_certificate(
        &self,
        who: &Identity,
        issuer: [u8; 32],
        serial: &[u8],
        not_after_ms: u64,
        reason: &str,
    ) -> ApiResult<()>;

    /// Revokes the sessions of a user or an IdP group (ADR 0070).
    async fn revoke_sessions(&self, who: &Identity, subject: &str, group: bool) -> ApiResult<()>;

    /// Asks the operator to stop after the node it is upgrading.
    async fn pause_upgrade(&self, who: &Identity) -> ApiResult<()>;

    /// One sample for chart history, as `who` sees the cluster.
    async fn chart_sample(&self, who: &Identity) -> ApiResult<ChartSample>;

    /// What of the chart history `who` may see now.
    async fn visibility(&self, who: &Identity) -> ApiResult<Visibility>;
}

impl std::fmt::Debug for dyn Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cluster({})", self.id())
    }
}
