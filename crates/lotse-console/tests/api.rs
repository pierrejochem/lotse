//! The HTTP surface, end to end through the router.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use lotse_console::auth::AuthConfig;
use lotse_console::cluster::Cluster;
use lotse_console::cluster::demo::DemoCluster;
use lotse_console::metrics::MetricsStore;
use lotse_console::{AppState, router};
use serde_json::Value;
use tower::ServiceExt;

fn app() -> axum::Router {
    let store = Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));
    let demo = DemoCluster::new("prod-hamburg-1");
    demo.backfill(&store, Duration::from_secs(3600), Duration::from_secs(10));
    let clusters: Vec<Arc<dyn Cluster>> = vec![Arc::new(demo)];
    router(AppState::new(clusters, AuthConfig::Dev { user: "pierre".into() }, store, Duration::from_millis(50)))
}

async fn call(method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value, axum::http::HeaderMap) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, json, headers)
}

#[tokio::test]
async fn me_reports_the_dev_user() {
    let (s, v, h) = call("GET", "/api/v1/me", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["principal"], "pierre");
    assert_eq!(v["auth_mode"], "dev");
    assert_eq!(h[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
    assert!(h[header::CONTENT_SECURITY_POLICY].to_str().unwrap().contains("default-src 'self'"));
}

#[tokio::test]
async fn overview_and_details_answer() {
    let (s, v, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/overview", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["nodes"].as_array().unwrap().len(), 3);
    let (s, v, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/streams/orders", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["partitions"].as_array().unwrap().len(), 12);
    let (s, v, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/queues/payments.capture", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["levels"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn unknown_things_are_404_with_a_message() {
    let (s, v, _) = call("GET", "/api/v1/clusters/nope/overview", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(v["code"], "NOT_FOUND");
    let (s, v, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/streams/nope", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(v["message"].as_str().unwrap().contains("nope"));
}

#[tokio::test]
async fn creating_a_queue_validates_and_returns_the_cli_command() {
    let good = serde_json::json!({
        "name": "payments.refund", "tenant": "commerce", "priority_levels": 3, "single_active": true,
        "ttl_ms": 86400000, "dlq": "payments.refund.dlq", "max_deliveries": 5, "create_dlq": true
    });
    let (s, v, _) = call("POST", "/api/v1/clusters/prod-hamburg-1/queues", Some(good.clone())).await;
    assert_eq!(s, StatusCode::CREATED);
    assert!(v["cli"].as_str().unwrap().starts_with("lotse-cli queue create payments.refund"));

    let mut bad = good.clone();
    bad["name"] = "Payments Refund".into();
    let (s, v, _) = call("POST", "/api/v1/clusters/prod-hamburg-1/queues", Some(bad)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(v["message"].as_str().unwrap().contains("lowercase"));

    let mut over = good;
    over["tenant"] = "analytics".into();
    over["name"] = "x".into();
    over["dlq"] = "x.dlq".into();
    let (s, _, _) = call("POST", "/api/v1/clusters/prod-hamburg-1/queues", Some(over)).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn record_reads_check_their_parameters() {
    let (_, d, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/streams/orders", None).await;
    let start = d["partitions"][4]["log_start"].as_u64().unwrap();
    let uri = format!(
        "/api/v1/clusters/prod-hamburg-1/streams/orders/records?partition=4&offset={start}&count=20&isolation=uncommitted"
    );
    let (s, v, _) = call("GET", &uri, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["records"].as_array().unwrap().len(), 20);
    assert_eq!(v["next_offset"].as_u64().unwrap(), start + 20);
    assert!(v["cli"].as_str().unwrap().contains("--isolation uncommitted"));
    let (s, _, _) =
        call("GET", "/api/v1/clusters/prod-hamburg-1/streams/orders/records?partition=4&offset=0&count=900", None)
            .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn metrics_come_from_the_buffer() {
    let (s, v, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/metrics?series=in_bytes_per_sec&range=1h", None).await;
    assert_eq!(s, StatusCode::OK);
    let n = v["points"].as_array().unwrap().len();
    assert!(n > 100 && n <= 180, "{n} points");
    let (s, _, _) = call("GET", "/api/v1/clusters/prod-hamburg-1/metrics?series=nope&range=1h", None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_openapi_document_lists_every_route() {
    let (s, v, _) = call("GET", "/api/openapi.json", None).await;
    assert_eq!(s, StatusCode::OK);
    let paths = v["paths"].as_object().unwrap();
    for p in ["/api/v1/me", "/api/v1/clusters/{cluster}/queues", "/api/v1/clusters/{cluster}/streams/{name}/records"] {
        assert!(paths.contains_key(p), "{p} missing");
    }
}

#[tokio::test]
async fn deep_links_get_the_app_and_missing_files_get_404() {
    let (s, v, h) = call("GET", "/clusters/prod-hamburg-1/streams/orders", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(h[header::CONTENT_TYPE].to_str().unwrap().starts_with("text/html"));
    assert!(v.as_str().unwrap().contains("<"));
    // Queue names contain dots; their pages are routes, not files.
    let (s, _, h) = call("GET", "/clusters/prod-hamburg-1/queues/payments.capture", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(h[header::CONTENT_TYPE].to_str().unwrap().starts_with("text/html"));
    let (s, _, _) = call("GET", "/assets/nope.js", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _, _) = call("GET", "/api/v1/nope", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}
