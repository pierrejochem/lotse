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
    ClusterRef, CreateQueue, Isolation, NodesView, Overview, QueueDetail, QueueRow, RecordView, StreamDetail,
    StreamRow, TenantsView,
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
