//! The built React app, embedded in the binary.
//!
//! `console/web` builds into `console/web/dist`; that folder is compiled
//! into `lotse-console` (read from disk in debug builds). Unknown paths that
//! are not files get `index.html`, so React Router handles deep links.

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::Embed)]
#[folder = "../../console/web/dist"]
#[allow_missing = true]
struct Web;

const NOT_BUILT: &str = "<!doctype html><meta charset=utf-8><title>Lotse Console</title>\
<body style=\"font:16px/1.5 system-ui;margin:48px;color:#0F2B3D\"><h1>The web app is not built</h1>\
<p>Run <code>npm ci &amp;&amp; npm run build</code> in <code>console/web</code>, then rebuild <code>lotse-console</code>. \
The API is up at <a href=\"/api/openapi.json\">/api/openapi.json</a>.</p>";

/// Serves a static file, or `index.html` for client-side routes.
pub async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(file) = Web::get(path).filter(|_| !path.is_empty()) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        // Vite puts content hashes in names under assets/, so those never change.
        let cache = if path.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "no-cache" };
        return file_response(file.data.into_owned(), mime.as_ref(), cache);
    }
    // A missing build asset is a real 404. Anything else is a client route,
    // and routes may contain dots: queue names like `payments.capture` do.
    if path.starts_with("assets/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    match Web::get("index.html") {
        Some(index) => file_response(index.data.into_owned(), "text/html; charset=utf-8", "no-cache"),
        None => file_response(NOT_BUILT.as_bytes().to_vec(), "text/html; charset=utf-8", "no-cache"),
    }
}

fn file_response(body: Vec<u8>, mime: &str, cache: &'static str) -> Response {
    let mut res = Response::new(Body::from(body));
    let h = res.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime).unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    res
}
