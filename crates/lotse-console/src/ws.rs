//! The browser's WebSocket: one per tab, many topics.

use std::collections::BTreeMap;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::AppState;
use crate::auth::Identity;
use crate::hub::{ClientMessage, Topic};

/// Upgrades to a WebSocket after checking the origin.
///
/// Browsers send cookies with cross-site WebSocket requests, so a page on
/// another site could otherwise open a socket with the user's session.
pub async fn upgrade(State(st): State<AppState>, who: Identity, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if !same_origin(&headers) {
        return (StatusCode::FORBIDDEN, "cross-origin WebSocket refused").into_response();
    }
    ws.on_upgrade(move |socket| session(st, who, socket))
}

/// `true` when there is no Origin header (non-browser clients) or its host
/// matches the Host header.
pub fn same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else { return true };
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else { return false };
    origin.split_once("://").map(|(_, rest)| rest.trim_end_matches('/')) == Some(host)
}

async fn session(st: AppState, who: Identity, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(64);
    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    let mut subs: BTreeMap<Topic, JoinHandle<()>> = BTreeMap::new();
    while let Some(Ok(msg)) = stream.next().await {
        let Message::Text(text) = msg else { continue };
        match serde_json::from_str::<ClientMessage>(&text) {
            Ok(ClientMessage::Subscribe { topic }) => {
                if subs.contains_key(&topic) {
                    continue;
                }
                match st.hub.subscribe(topic.clone(), who.clone()) {
                    Ok(mut rx) => {
                        let tx = out_tx.clone();
                        let forward = tokio::spawn(async move {
                            loop {
                                let frame = rx.borrow_and_update().clone();
                                if let Some(frame) = frame {
                                    if tx.send(frame.to_string()).await.is_err() {
                                        return;
                                    }
                                }
                                if rx.changed().await.is_err() {
                                    return;
                                }
                            }
                        });
                        subs.insert(topic, forward);
                    }
                    Err(e) => {
                        let body = serde_json::json!({ "topic": topic, "error": { "code": "NOT_FOUND", "message": e.to_string() } });
                        let _ = out_tx.send(body.to_string()).await;
                    }
                }
            }
            Ok(ClientMessage::Unsubscribe { topic }) => {
                if let Some(h) = subs.remove(&topic) {
                    h.abort();
                }
            }
            Err(e) => {
                let body = serde_json::json!({ "error": { "code": "BAD_REQUEST", "message": format!("Unreadable message: {e}") } });
                let _ = out_tx.send(body.to_string()).await;
            }
        }
    }
    for (_, h) in subs {
        h.abort();
    }
    writer.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(origin: Option<&str>, host: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, HeaderValue::from_str(host).unwrap());
        if let Some(o) = origin {
            h.insert(header::ORIGIN, HeaderValue::from_str(o).unwrap());
        }
        h
    }

    #[test]
    fn origin_check() {
        assert!(same_origin(&headers(Some("http://console.lan:8080"), "console.lan:8080")));
        assert!(same_origin(&headers(None, "console.lan:8080")));
        assert!(!same_origin(&headers(Some("https://evil.example"), "console.lan:8080")));
        assert!(!same_origin(&headers(Some("http://console.lan:8081"), "console.lan:8080")));
    }
}
