//! Live updates over one WebSocket per browser.
//!
//! A browser subscribes to topics (the overview of a cluster, one stream,
//! one queue, the nodes screen). For each topic and principal the hub runs a
//! single poller that asks the cluster every `interval` and publishes the
//! result; every tab of that user watching that topic shares it. Pollers run
//! with the user's own identity, so viewers are grouped by principal, not
//! just by topic: the cluster sees one poll per user and topic, however many
//! tabs are open. A poller stops as soon as nobody watches.
//!
//! Wire format, as JSON text frames:
//! - client: `{"op":"subscribe","topic":{"kind":"overview","cluster":"c1"}}`
//!   and the same with `"op":"unsubscribe"`;
//! - server: `{"topic":{…},"data":{…}}` or `{"topic":{…},"error":{"code","message"}}`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::auth::Identity;
use crate::cluster::Cluster;
use crate::error::ApiError;
use crate::model::ErrorBody;

/// Something a browser can watch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Topic {
    /// The overview screen of a cluster.
    Overview {
        /// Cluster ID.
        cluster: String,
    },
    /// The nodes screen of a cluster.
    Nodes {
        /// Cluster ID.
        cluster: String,
    },
    /// One stream.
    Stream {
        /// Cluster ID.
        cluster: String,
        /// Stream name.
        name: String,
    },
    /// One queue.
    Queue {
        /// Cluster ID.
        cluster: String,
        /// Queue name.
        name: String,
    },
}

impl Topic {
    /// Cluster the topic belongs to.
    pub fn cluster(&self) -> &str {
        match self {
            Topic::Overview { cluster } | Topic::Nodes { cluster } => cluster,
            Topic::Stream { cluster, .. } | Topic::Queue { cluster, .. } => cluster,
        }
    }
}

/// A frame from the browser.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Start receiving updates for a topic.
    Subscribe {
        /// What to watch.
        topic: Topic,
    },
    /// Stop receiving updates for a topic.
    Unsubscribe {
        /// What to stop watching.
        topic: Topic,
    },
}

#[derive(Serialize)]
struct Update<'a> {
    topic: &'a Topic,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorBody>,
}

/// Latest frame for a topic; `None` until the first poll finished.
pub type Frame = Option<Arc<str>>;

type Key = (Topic, String);

/// Shares one poller per (topic, principal).
#[derive(Debug)]
pub struct Hub {
    clusters: BTreeMap<String, Arc<dyn Cluster>>,
    interval: Duration,
    pollers: Mutex<BTreeMap<Key, watch::Sender<Frame>>>,
}

impl Hub {
    /// A hub polling every `interval`.
    pub fn new(clusters: BTreeMap<String, Arc<dyn Cluster>>, interval: Duration) -> Arc<Self> {
        Arc::new(Hub { clusters, interval, pollers: Mutex::new(BTreeMap::new()) })
    }

    /// Number of running pollers (for tests and metrics).
    pub fn poller_count(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<Key, watch::Sender<Frame>>> {
        self.pollers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Watches `topic` as `who`. The receiver always holds the latest frame;
    /// a late subscriber gets the current state at once.
    pub fn subscribe(self: &Arc<Self>, topic: Topic, who: Identity) -> Result<watch::Receiver<Frame>, ApiError> {
        let cluster = self
            .clusters
            .get(topic.cluster())
            .cloned()
            .ok_or_else(|| ApiError::NotFound(format!("There is no cluster \"{}\".", topic.cluster())))?;
        let key = (topic.clone(), who.principal.clone());
        let mut pollers = self.lock();
        if let Some(tx) = pollers.get(&key) {
            return Ok(tx.subscribe());
        }
        let (tx, rx) = watch::channel(None);
        pollers.insert(key.clone(), tx.clone());
        drop(pollers);
        let hub = Arc::clone(self);
        tokio::spawn(async move { hub.poll(key, cluster, who, tx).await });
        Ok(rx)
    }

    async fn poll(self: Arc<Self>, key: Key, cluster: Arc<dyn Cluster>, who: Identity, tx: watch::Sender<Frame>) {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            {
                // Decide under the lock, so a subscriber can't join a poller
                // that is about to stop.
                let mut pollers = self.lock();
                if tx.receiver_count() == 0 {
                    pollers.remove(&key);
                    return;
                }
            }
            let frame = fetch(&*cluster, &key.0, &who).await;
            tx.send_replace(Some(frame));
        }
    }
}

async fn fetch(cluster: &dyn Cluster, topic: &Topic, who: &Identity) -> Arc<str> {
    let result = match topic {
        Topic::Overview { .. } => cluster.overview(who).await.map(to_value),
        Topic::Nodes { .. } => cluster.nodes(who).await.map(to_value),
        Topic::Stream { name, .. } => cluster.stream(who, name).await.map(to_value),
        Topic::Queue { name, .. } => cluster.queue(who, name).await.map(to_value),
    };
    let update = match result {
        Ok(data) => Update { topic, data: Some(data), error: None },
        Err(e) => {
            let code = match &e {
                ApiError::NotFound(_) => "NOT_FOUND",
                ApiError::Forbidden(_) => "FORBIDDEN",
                ApiError::Unavailable(_) => "CLUSTER_UNAVAILABLE",
                ApiError::BadRequest(_) => "BAD_REQUEST",
                ApiError::Conflict(_) => "CONFLICT",
            };
            Update { topic, data: None, error: Some(ErrorBody { code: code.into(), message: e.to_string() }) }
        }
    };
    serde_json::to_string(&update).expect("updates serialise").into()
}

fn to_value<T: Serialize>(v: T) -> serde_json::Value {
    serde_json::to_value(v).expect("models serialise")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::demo::DemoCluster;

    fn hub() -> Arc<Hub> {
        let c: Arc<dyn Cluster> = Arc::new(DemoCluster::new("c1"));
        Hub::new(BTreeMap::from([("c1".to_owned(), c)]), Duration::from_millis(20))
    }

    fn user(p: &str) -> Identity {
        Identity { principal: p.into(), display_name: p.into(), token: None }
    }

    fn overview() -> Topic {
        Topic::Overview { cluster: "c1".into() }
    }

    #[tokio::test]
    async fn tabs_of_one_user_share_a_poller_and_users_do_not() {
        let hub = hub();
        let mut a1 = hub.subscribe(overview(), user("alice")).unwrap();
        let _a2 = hub.subscribe(overview(), user("alice")).unwrap();
        let _b = hub.subscribe(overview(), user("bob")).unwrap();
        assert_eq!(hub.poller_count(), 2);
        a1.changed().await.unwrap();
        let frame = a1.borrow().clone().unwrap();
        assert!(frame.contains("\"kind\":\"overview\""));
        assert!(frame.contains("\"raft_groups\":1284"));
    }

    #[tokio::test]
    async fn pollers_stop_when_the_last_viewer_leaves() {
        let hub = hub();
        let rx = hub.subscribe(overview(), user("alice")).unwrap();
        assert_eq!(hub.poller_count(), 1);
        drop(rx);
        for _ in 0..50 {
            if hub.poller_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("poller still running after its last viewer left");
    }

    #[tokio::test]
    async fn errors_are_delivered_as_frames() {
        let hub = hub();
        let mut rx = hub.subscribe(Topic::Stream { cluster: "c1".into(), name: "nope".into() }, user("alice")).unwrap();
        rx.changed().await.unwrap();
        let frame = rx.borrow().clone().unwrap();
        assert!(frame.contains("\"code\":\"NOT_FOUND\""), "{frame}");
    }

    #[tokio::test]
    async fn unknown_clusters_are_refused_up_front() {
        let hub = hub();
        assert!(hub.subscribe(Topic::Overview { cluster: "nope".into() }, user("alice")).is_err());
    }

    #[test]
    fn client_messages_parse() {
        let m: ClientMessage =
            serde_json::from_str(r#"{"op":"subscribe","topic":{"kind":"queue","cluster":"c1","name":"q"}}"#).unwrap();
        assert!(matches!(m, ClientMessage::Subscribe { topic: Topic::Queue { .. } }));
    }
}
