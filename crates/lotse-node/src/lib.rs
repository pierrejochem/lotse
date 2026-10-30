//! The `lotse` binary's library: reading the cluster file, making
//! certificates and running one node (ADR 0047). `main.rs` is only the
//! command line around it; tests start nodes through [`run`].

pub mod certs;
pub mod config;

use std::future::Future;

use bytes::Bytes;
use lotse_record::Batch;
use lotse_rt_tokio::TlsConfig;
use lotse_rt_tokio::TokioRuntime;
use lotse_server::{Node, StateMachine};

use crate::config::{ClusterFile, ConfigError};

/// Why a node did not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The cluster file.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The data directory.
    #[error("{path}: {source}")]
    DataDir {
        /// The directory.
        path: String,
        /// Why.
        source: std::io::Error,
    },
    /// The certificates.
    #[error("TLS: {0}")]
    Tls(#[from] lotse_rt_tokio::TlsError),
    /// The node itself.
    #[error(transparent)]
    Node(#[from] lotse_server::Error),
}

/// Streams, queues and the metadata and coordinator groups keep their state
/// in the replica; a node run from the binary has no state machine of its
/// own.
#[derive(Debug, Default)]
struct NoStateMachine;

impl StateMachine for NoStateMachine {
    fn apply(&mut self, _: &Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// The runtime node `id` runs on: its data directory, and TLS with its
/// certificate or, if the file says so, plain TCP on loopback.
pub fn runtime(file: &ClusterFile, id: u64) -> Result<TokioRuntime, RunError> {
    let dir = file.data_dir(id);
    std::fs::create_dir_all(&dir).map_err(|source| RunError::DataDir { path: dir.display().to_string(), source })?;
    let rt = TokioRuntime::new(dir);
    Ok(match file.tls_paths(id) {
        Some((ca, cert, key)) => rt.with_tls(TlsConfig::from_files(ca, Some((cert, key)))?),
        None => rt.plaintext_on_loopback(),
    })
}

/// Runs node `id` of the cluster in `file` until `shutdown` completes,
/// then stops it and waits for it to finish.
pub async fn run(file: &ClusterFile, id: u64, shutdown: impl Future<Output = ()>) -> Result<(), RunError> {
    let cfg = file.node_config(id)?;
    let rt = runtime(file, id)?;
    tracing::info!(
        node = id,
        peer = %cfg.listen,
        client = cfg.client_listen.as_deref().unwrap_or("-"),
        data = %file.data_dir(id).display(),
        tls = file.tls_paths(id).is_some(),
        "starting"
    );
    let node = Node::start(rt, cfg, |_| NoStateMachine).await?;
    let handle = node.handle();
    let stopped = node.join();
    tokio::pin!(stopped);
    tokio::select! {
        res = &mut stopped => return res.map_err(RunError::from),
        () = shutdown => tracing::info!(node = id, "stopping"),
    }
    handle.stop();
    stopped.await?;
    tracing::info!(node = id, "stopped");
    Ok(())
}
