//! `lotse-console`: the management console, run on its own node.
//!
//! The browser talks only to this process: REST/JSON (described by OpenAPI)
//! plus one WebSocket for live updates. This process talks to the clusters,
//! always as the signed-in user; only the background metrics sampler uses
//! the console's own read-only identity.

#![warn(missing_docs)]

pub mod api;
pub mod assets;
pub mod auth;
pub mod cli;
pub mod cluster;
pub mod config;
pub mod error;
pub mod hub;
pub mod metrics;
pub mod model;
pub mod time;
pub mod ws;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::Response;
use axum::routing::{get, post};

use crate::auth::{AuthConfig, Sessions};
use crate::cluster::Cluster;
use crate::hub::Hub;
use crate::metrics::MetricsStore;

/// Shared state of all handlers.
#[derive(Clone, Debug)]
pub struct AppState {
    /// Managed clusters by ID.
    pub clusters: Arc<BTreeMap<String, Arc<dyn Cluster>>>,
    /// Sign-in settings.
    pub auth: Arc<AuthConfig>,
    /// Chart history.
    pub metrics: Arc<MetricsStore>,
    /// Live updates.
    pub hub: Arc<Hub>,
    /// Signed-in browsers, in `oidc` mode.
    pub sessions: Option<Arc<Sessions>>,
}

impl AppState {
    /// State for `clusters`, polling live topics every `live`.
    pub fn new(clusters: Vec<Arc<dyn Cluster>>, auth: AuthConfig, metrics: Arc<MetricsStore>, live: Duration) -> Self {
        let clusters: BTreeMap<String, Arc<dyn Cluster>> =
            clusters.into_iter().map(|c| (c.id().to_owned(), c)).collect();
        let hub = Hub::new(clusters.clone(), live);
        AppState { clusters: Arc::new(clusters), auth: Arc::new(auth), metrics, hub, sessions: None }
    }
}

impl AppState {
    /// The same state, signing users in with `sessions`.
    pub fn with_sessions(self, sessions: Arc<Sessions>) -> Self {
        AppState { sessions: Some(sessions), ..self }
    }
}

/// The whole HTTP surface.
pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/me", get(api::me))
        .route("/clusters", get(api::clusters))
        .route("/clusters/{cluster}/overview", get(api::overview))
        .route("/clusters/{cluster}/nodes", get(api::nodes))
        .route("/clusters/{cluster}/upgrade/pause", post(api::pause_upgrade))
        .route("/clusters/{cluster}/streams", get(api::streams))
        .route("/clusters/{cluster}/streams/{name}", get(api::stream))
        .route("/clusters/{cluster}/streams/{name}/records", get(api::read_records))
        .route("/clusters/{cluster}/queues", get(api::queues).post(api::create_queue))
        .route("/clusters/{cluster}/queues/{name}", get(api::queue))
        .route("/clusters/{cluster}/tenants", get(api::tenants))
        .route("/clusters/{cluster}/metrics", get(api::metrics))
        .route("/ws", get(ws::upgrade))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth::session_layer));
    Router::new()
        .nest("/api/v1", api)
        .route("/auth/login", get(auth::login))
        .route("/auth/callback", get(auth::callback))
        .route("/auth/logout", post(auth::logout))
        .route("/api/openapi.json", get(|| async { axum::Json(openapi()) }))
        .fallback(assets::serve)
        .layer(axum::middleware::map_response(security_headers))
        .with_state(state)
}

/// The OpenAPI document as JSON.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <api::ApiDoc as utoipa::OpenApi>::openapi()
}

async fn security_headers(mut res: Response) -> Response {
    let h = res.headers_mut();
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("same-origin"));
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; \
             font-src 'self'; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    res
}
