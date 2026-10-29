//! REST handlers and the OpenAPI document.
//!
//! Every route is described with `#[utoipa::path]`; `lotse-console
//! --print-openapi` writes the document that `console/web` generates its
//! TypeScript client from.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::AppState;
use crate::auth::Identity;
use crate::cli;
use crate::cluster::Cluster;
use crate::error::{ApiError, ApiResult};
use crate::metrics::{QueryError, SERIES};
use crate::model::*;

/// The OpenAPI document.
#[derive(Debug, OpenApi)]
#[openapi(
    info(title = "Lotse Console API", description = "HTTP gateway and UI backend of lotse-console."),
    paths(
        me,
        clusters,
        overview,
        nodes,
        pause_upgrade,
        streams,
        stream,
        read_records,
        queues,
        queue,
        create_queue,
        tenants,
        metrics
    ),
    components(schemas(ErrorBody, Tone, Chip, PhaseState, ReplicaState, TxnState, Isolation, Range))
)]
pub struct ApiDoc;

fn cluster(st: &AppState, id: &str) -> ApiResult<Arc<dyn Cluster>> {
    st.clusters
        .get(id)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(format!("There is no cluster \"{id}\" in this console.")))
}

/// The signed-in user.
#[utoipa::path(get, path = "/api/v1/me", responses((status = 200, body = Me)))]
pub async fn me(State(st): State<AppState>, who: Identity) -> Json<Me> {
    Json(Me { principal: who.principal, display_name: who.display_name, auth_mode: st.auth.mode().into() })
}

/// Clusters this console manages.
#[utoipa::path(get, path = "/api/v1/clusters", responses((status = 200, body = [ClusterRef])))]
pub async fn clusters(State(st): State<AppState>, who: Identity) -> ApiResult<Json<Vec<ClusterRef>>> {
    let mut out = Vec::with_capacity(st.clusters.len());
    for c in st.clusters.values() {
        out.push(c.summary(&who).await?);
    }
    Ok(Json(out))
}

/// The overview screen.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/overview",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = Overview), (status = 404, body = ErrorBody)))]
pub async fn overview(State(st): State<AppState>, who: Identity, Path(id): Path<String>) -> ApiResult<Json<Overview>> {
    Ok(Json(cluster(&st, &id)?.overview(&who).await?))
}

/// Nodes, rolling upgrade and operator events.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/nodes",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = NodesView), (status = 404, body = ErrorBody)))]
pub async fn nodes(State(st): State<AppState>, who: Identity, Path(id): Path<String>) -> ApiResult<Json<NodesView>> {
    Ok(Json(cluster(&st, &id)?.nodes(&who).await?))
}

/// Stops the rolling upgrade after the node it is on.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/upgrade/pause",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = Done), (status = 403, body = ErrorBody)))]
pub async fn pause_upgrade(State(st): State<AppState>, who: Identity, Path(id): Path<String>) -> ApiResult<Json<Done>> {
    cluster(&st, &id)?.pause_upgrade(&who).await?;
    Ok(Json(Done {
        message: "The upgrade stops after the node it is on.".into(),
        cli: format!("lotse-cli --cluster {id} upgrade pause"),
    }))
}

/// All streams.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/streams",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = [StreamRow])))]
pub async fn streams(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<StreamRow>>> {
    Ok(Json(cluster(&st, &id)?.streams(&who).await?))
}

/// One stream.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/streams/{name}",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "Stream name")),
    responses((status = 200, body = StreamDetail), (status = 404, body = ErrorBody)))]
pub async fn stream(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Json<StreamDetail>> {
    Ok(Json(cluster(&st, &id)?.stream(&who, &name).await?))
}

/// Where and how to read records.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ReadParams {
    /// Partition.
    pub partition: u32,
    /// First offset.
    pub offset: u64,
    /// Records to return, 1–500 (default 50).
    pub count: Option<u32>,
    /// Default `committed`.
    pub isolation: Option<Isolation>,
}

/// Reads records without joining a consumer group.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/streams/{name}/records",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "Stream name"), ReadParams),
    responses((status = 200, body = RecordPage), (status = 400, body = ErrorBody), (status = 404, body = ErrorBody)))]
pub async fn read_records(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
    Query(q): Query<ReadParams>,
) -> ApiResult<Json<RecordPage>> {
    let count = q.count.unwrap_or(50);
    if !(1..=500).contains(&count) {
        return Err(ApiError::BadRequest("count must be between 1 and 500.".into()));
    }
    let isolation = q.isolation.unwrap_or(Isolation::Committed);
    let records = cluster(&st, &id)?.read(&who, &name, q.partition, q.offset, count, isolation).await?;
    let next_offset = records.last().map_or(q.offset, |r| r.offset + 1);
    tracing::info!(principal = %who.principal, cluster = %id, stream = %name, partition = q.partition, offset = q.offset, count, "records read");
    Ok(Json(RecordPage { records, next_offset, cli: cli::read(&name, q.partition, q.offset, count, isolation) }))
}

/// All queues.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/queues",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = [QueueRow])))]
pub async fn queues(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<QueueRow>>> {
    Ok(Json(cluster(&st, &id)?.queues(&who).await?))
}

/// One queue.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/queues/{name}",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "Queue name")),
    responses((status = 200, body = QueueDetail), (status = 404, body = ErrorBody)))]
pub async fn queue(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Json<QueueDetail>> {
    Ok(Json(cluster(&st, &id)?.queue(&who, &name).await?))
}

/// Creates a queue.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/queues",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = CreateQueue,
    responses((status = 201, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody), (status = 409, body = ErrorBody)))]
pub async fn create_queue(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<CreateQueue>,
) -> ApiResult<(StatusCode, Json<Done>)> {
    cli::validate_create_queue(&req)?;
    cluster(&st, &id)?.create_queue(&who, &req).await?;
    tracing::info!(principal = %who.principal, cluster = %id, queue = %req.name, "queue created");
    Ok((
        StatusCode::CREATED,
        Json(Done { message: format!("Created queue {}.", req.name), cli: cli::create_queue(&req) }),
    ))
}

/// Tenants, quotas and role bindings.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/tenants",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = TenantsView)))]
pub async fn tenants(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<TenantsView>> {
    Ok(Json(cluster(&st, &id)?.tenants(&who).await?))
}

/// Chart time range.
#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
pub enum Range {
    /// 15 minutes.
    #[serde(rename = "15m")]
    M15,
    /// 1 hour.
    #[serde(rename = "1h")]
    H1,
    /// 6 hours.
    #[serde(rename = "6h")]
    H6,
    /// 24 hours.
    #[serde(rename = "24h")]
    H24,
}

impl Range {
    fn duration(self) -> Duration {
        Duration::from_secs(match self {
            Range::M15 => 900,
            Range::H1 => 3_600,
            Range::H6 => 21_600,
            Range::H24 => 86_400,
        })
    }
}

/// Which series and how far back.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct MetricsParams {
    /// `in_bytes_per_sec`, `out_bytes_per_sec`, `records_per_sec` or `commit_p99_ms`.
    pub series: String,
    /// Time range.
    pub range: Range,
}

/// Chart history from the console's in-memory buffer (at most 180 points):
/// the sum over the streams and queues the caller may see; the commit
/// latency only for cluster admins.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/metrics",
    params(("cluster" = String, Path, description = "Cluster ID"), MetricsParams),
    responses((status = 200, body = Series), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn metrics(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Query(q): Query<MetricsParams>,
) -> ApiResult<Json<Series>> {
    let c = cluster(&st, &id)?;
    let (name, unit) = SERIES
        .iter()
        .find(|(n, _)| *n == q.series)
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown series \"{}\".", q.series)))?;
    // The resources the user may see now, as the cluster tells them.
    let vis = c.visibility(&who).await?;
    let points = st.metrics.query(&id, name, q.range.duration(), 180, &vis).map_err(|e| match e {
        QueryError::Unknown(_) => ApiError::BadRequest(e.to_string()),
        QueryError::ClusterWide(_) => ApiError::Forbidden(e.to_string()),
    })?;
    Ok(Json(Series { name: (*name).into(), unit: (*unit).into(), points }))
}
