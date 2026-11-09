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
        purge_queue,
        requeue,
        move_leader,
        users,
        create_user,
        set_credential,
        delete_user,
        bindings,
        add_binding,
        remove_binding,
        revoke_sessions,
        revoke_certificate,
        certificates,
        certificate_requests,
        request_certificate,
        tenants,
        metrics
    ),
    components(schemas(ErrorBody, Tone, Chip, PhaseState, ReplicaState, TxnState, Isolation, Range))
)]
pub struct ApiDoc;

/// `n messages`, with thousands grouped as everywhere in the console.
fn messages(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    format!("{out} message{}", if n == 1 { "" } else { "s" })
}

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

/// Settles a queue's ready messages: what consumers hold stays theirs.
/// Destructive: the body names the queue again.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/queues/{name}/purge",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "Queue name")),
    request_body = PurgeQueue,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody), (status = 404, body = ErrorBody)))]
pub async fn purge_queue(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
    Json(req): Json<PurgeQueue>,
) -> ApiResult<Json<Done>> {
    if req.confirm != name {
        return Err(ApiError::BadRequest(format!("Type the queue's name, {name}, to purge it.")));
    }
    let n = cluster(&st, &id)?.purge_queue(&who, &name).await?;
    tracing::info!(principal = %who.principal, cluster = %id, queue = %name, purged = n, "queue purged");
    let message = format!("Purged {} from {name}.", messages(n));
    Ok(Json(Done { message, cli: cli::purge_queue(&name) }))
}

/// Moves the ready messages of a dead-letter queue back to the queues they
/// came from, each once.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/queues/{name}/requeue",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "Dead-letter queue name")),
    responses((status = 200, body = Done), (status = 403, body = ErrorBody), (status = 404, body = ErrorBody)))]
pub async fn requeue(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
) -> ApiResult<Json<Done>> {
    let n = cluster(&st, &id)?.requeue(&who, &name).await?;
    tracing::info!(principal = %who.principal, cluster = %id, queue = %name, requeued = n, "dead letters requeued");
    let message = format!("Requeued {} from {name} to their queues.", messages(n));
    Ok(Json(Done { message, cli: cli::requeue(&name) }))
}

/// Pins a partition's leader to one of its replicas, or unpins it.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/leaders",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = MoveLeader,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody), (status = 404, body = ErrorBody)))]
pub async fn move_leader(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<MoveLeader>,
) -> ApiResult<Json<Done>> {
    cluster(&st, &id)?.move_leader(&who, &req).await?;
    tracing::info!(principal = %who.principal, cluster = %id, resource = %req.resource, partition = req.partition, node = ?req.node, "leader pinned");
    let message = match &req.node {
        Some(node) => format!(
            "Partition {} of {} is pinned to {node}. Its leader moves there within a few seconds.",
            req.partition, req.resource
        ),
        None => format!("Partition {} of {} is unpinned. Leaders spread by themselves.", req.partition, req.resource),
    };
    Ok(Json(Done { message, cli: cli::move_leader(&req) }))
}

/// SCRAM users. The broker lets only cluster admins list them.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/users",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = UsersView), (status = 403, body = ErrorBody)))]
pub async fn users(State(st): State<AppState>, who: Identity, Path(id): Path<String>) -> ApiResult<Json<UsersView>> {
    Ok(Json(cluster(&st, &id)?.users(&who).await?))
}

/// A SCRAM user name the broker and the SCRAM exchange can carry.
fn check_user_name(name: &str) -> ApiResult<()> {
    if name.is_empty() || name.len() > 128 || name.chars().any(|c| c.is_control() || c == ',' || c == '=') {
        return Err(ApiError::BadRequest(
            "A user name has 1 to 128 characters, none of them a comma, an equals sign or a control character.".into(),
        ));
    }
    Ok(())
}

/// Creates a user with a credential the browser derived.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/users",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = CreateUser,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn create_user(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<CreateUser>,
) -> ApiResult<Json<Done>> {
    check_user_name(&req.name)?;
    let c = cluster(&st, &id)?;
    if c.users(&who).await?.users.iter().any(|u| u.name == req.name) {
        return Err(ApiError::Conflict(format!("{} exists already; set a new password instead.", req.name)));
    }
    c.set_user_credential(&who, &req.name, &req.credential).await?;
    tracing::info!(principal = %who.principal, cluster = %id, user = %req.name, "user created");
    Ok(Json(Done { message: format!("Created {}.", req.name), cli: cli::set_user_password(&req.name) }))
}

/// Sets a new password for a user, as a derived credential.
#[utoipa::path(put, path = "/api/v1/clusters/{cluster}/users/{name}/credential",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "User name")),
    request_body = Credential,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn set_credential(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
    Json(req): Json<Credential>,
) -> ApiResult<Json<Done>> {
    check_user_name(&name)?;
    cluster(&st, &id)?.set_user_credential(&who, &name, &req).await?;
    tracing::info!(principal = %who.principal, cluster = %id, user = %name, "user password set");
    Ok(Json(Done {
        message: format!("Set a new password for {name}. Open sessions stay until they end or are revoked."),
        cli: cli::set_user_password(&name),
    }))
}

/// Deletes a user. Destructive: the body names the user again.
#[utoipa::path(delete, path = "/api/v1/clusters/{cluster}/users/{name}",
    params(("cluster" = String, Path, description = "Cluster ID"), ("name" = String, Path, description = "User name")),
    request_body = DeleteUser,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody), (status = 404, body = ErrorBody)))]
pub async fn delete_user(
    State(st): State<AppState>,
    who: Identity,
    Path((id, name)): Path<(String, String)>,
    Json(req): Json<DeleteUser>,
) -> ApiResult<Json<Done>> {
    if req.confirm != name {
        return Err(ApiError::BadRequest(format!("Type the user's name, {name}, to delete it.")));
    }
    cluster(&st, &id)?.delete_user(&who, &name).await?;
    tracing::info!(principal = %who.principal, cluster = %id, user = %name, "user deleted");
    Ok(Json(Done {
        message: format!("Deleted {name}. Open sessions end at the latest when the node's session limit is up."),
        cli: cli::delete_user(&name),
    }))
}

/// Role bindings: all for a cluster admin, one's own for everyone else.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/bindings",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = BindingsView), (status = 403, body = ErrorBody)))]
pub async fn bindings(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<BindingsView>> {
    Ok(Json(cluster(&st, &id)?.bindings(&who).await?))
}

/// Adds a role binding.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/bindings",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = BindingRow,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn add_binding(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<BindingRow>,
) -> ApiResult<Json<Done>> {
    cluster(&st, &id)?.alter_binding(&who, &req, true).await?;
    tracing::info!(principal = %who.principal, cluster = %id, subject = %req.subject, role = %req.role, tenant = %req.tenant, "binding added");
    Ok(Json(Done { message: format!("{} is now {}.", req.subject, binding_text(&req)), cli: cli::bind(&req, true) }))
}

/// Removes a role binding.
#[utoipa::path(delete, path = "/api/v1/clusters/{cluster}/bindings",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = BindingRow,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn remove_binding(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<BindingRow>,
) -> ApiResult<Json<Done>> {
    cluster(&st, &id)?.alter_binding(&who, &req, false).await?;
    tracing::info!(principal = %who.principal, cluster = %id, subject = %req.subject, role = %req.role, tenant = %req.tenant, "binding removed");
    Ok(Json(Done {
        message: format!("{} is no longer {}.", req.subject, binding_text(&req)),
        cli: cli::bind(&req, false),
    }))
}

/// `producer in payments for events.*`, for the messages.
fn binding_text(b: &BindingRow) -> String {
    match (b.tenant.is_empty(), b.prefix.is_empty()) {
        (true, _) => b.role.clone(),
        (false, true) => format!("{} in {}", b.role, b.tenant),
        (false, false) => format!("{} in {} for {}*", b.role, b.tenant, b.prefix),
    }
}

/// Ends the sessions of a user or an IdP group that began before now.
/// Destructive: the body names the subject again.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/sessions/revoke",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = RevokeSessions,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn revoke_sessions(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<RevokeSessions>,
) -> ApiResult<Json<Done>> {
    if req.subject.is_empty() || req.confirm != req.subject {
        return Err(ApiError::BadRequest(format!("Type the name, {}, to revoke its sessions.", req.subject)));
    }
    cluster(&st, &id)?.revoke_sessions(&who, &req.subject, req.group).await?;
    tracing::info!(principal = %who.principal, cluster = %id, subject = %req.subject, group = req.group, "sessions revoked");
    let message = format!(
        "Revoked the sessions of {}. Tokens issued before now are refused; a SCRAM user with the password can sign in again.",
        req.subject
    );
    Ok(Json(Done { message, cli: cli::revoke_sessions(&req.subject, req.group) }))
}

/// The client certificates the cluster has seen or had revoked, by expiry.
/// The broker lets only cluster admins list them.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/certificates",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = CertificatesView), (status = 403, body = ErrorBody)))]
pub async fn certificates(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<CertificatesView>> {
    Ok(Json(cluster(&st, &id)?.certificates(&who).await?))
}

/// The requests for client certificates, newest first, with the certificate
/// of each that was signed. The broker lets only cluster admins list them.
#[utoipa::path(get, path = "/api/v1/clusters/{cluster}/certificate-requests",
    params(("cluster" = String, Path, description = "Cluster ID")),
    responses((status = 200, body = CertificateRequestsView), (status = 403, body = ErrorBody)))]
pub async fn certificate_requests(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
) -> ApiResult<Json<CertificateRequestsView>> {
    Ok(Json(cluster(&st, &id)?.certificate_requests(&who).await?))
}

/// Asks the CA holder for a client certificate. The body carries a signing
/// request made in the browser: the private key never reaches the console,
/// and the CA holder signs with `lotse certs --sign`.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/certificate-requests",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = RequestCertificate,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn request_certificate(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<RequestCertificate>,
) -> ApiResult<Json<Done>> {
    use base64::Engine as _;
    let csr = base64::engine::general_purpose::STANDARD
        .decode(req.csr.trim())
        .map_err(|_| ApiError::BadRequest("The signing request is base64 of its DER form.".into()))?;
    cluster(&st, &id)?.request_certificate(&who, &req.principal, csr).await?;
    tracing::info!(principal = %who.principal, cluster = %id, requested = %req.principal, "certificate requested");
    let message = format!(
        "Asked for a certificate for {}. The CA holder signs it with `lotse certs --sign`; the request lapses after 7 days.",
        req.principal
    );
    Ok(Json(Done { message, cli: cli::sign_certificates() }))
}

/// Decodes hex, or says what is wrong with `what`.
fn hex(what: &str, text: &str) -> ApiResult<Vec<u8>> {
    let bad = || ApiError::BadRequest(format!("The {what} is hexadecimal: pairs of 0-9 and a-f."));
    if text.is_empty() || text.len() % 2 != 0 {
        return Err(bad());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2).ok_or_else(bad)?, 16).map_err(|_| bad()))
        .collect()
}

/// Revokes one client certificate, by issuer and serial. Every node refuses
/// it from then on and ends the connections that showed it. Destructive: the
/// body names the serial again.
#[utoipa::path(post, path = "/api/v1/clusters/{cluster}/certificates/revoke",
    params(("cluster" = String, Path, description = "Cluster ID")),
    request_body = RevokeCertificate,
    responses((status = 200, body = Done), (status = 400, body = ErrorBody), (status = 403, body = ErrorBody)))]
pub async fn revoke_certificate(
    State(st): State<AppState>,
    who: Identity,
    Path(id): Path<String>,
    Json(req): Json<RevokeCertificate>,
) -> ApiResult<Json<Done>> {
    let serial = hex("serial", &req.serial)?;
    if !req.confirm.eq_ignore_ascii_case(&req.serial) {
        return Err(ApiError::BadRequest(format!("Type the serial, {}, to revoke the certificate.", req.serial)));
    }
    let issuer: [u8; 32] = hex("issuer", &req.issuer)?
        .try_into()
        .map_err(|_| ApiError::BadRequest("The issuer is 64 hex digits: the SHA-256 of the issuer's name.".into()))?;
    cluster(&st, &id)?.revoke_certificate(&who, issuer, &serial, req.not_after_ms, &req.reason).await?;
    tracing::info!(principal = %who.principal, cluster = %id, serial = %req.serial, "certificate revoked");
    let message = format!(
        "Revoked the certificate with serial {}. Connections that showed it end; it is refused until it expires.",
        req.serial
    );
    Ok(Json(Done { message, cli: cli::revoke_certificate(&req) }))
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
/// latency only for cluster admins and cluster viewers.
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

#[cfg(test)]
mod tests {
    #[test]
    fn counts_of_messages_read_as_elsewhere() {
        assert_eq!(super::messages(0), "0 messages");
        assert_eq!(super::messages(1), "1 message");
        assert_eq!(super::messages(1_942), "1,942 messages");
        assert_eq!(super::messages(18_442_107), "18,442,107 messages");
    }
}
