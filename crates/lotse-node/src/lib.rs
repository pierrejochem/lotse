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
/// then stops it and waits for it to finish. With `join`, a node starting
/// for the first time joins a running cluster (after `lotse node add`)
/// instead of being one of its first voters (ADR 0048).
pub async fn run(file: &ClusterFile, id: u64, join: bool, shutdown: impl Future<Output = ()>) -> Result<(), RunError> {
    let mut cfg = file.node_config(id)?;
    cfg.join = join;
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
        res = &mut stopped => {
            res?;
            if handle.removed() {
                // Exit 0: a supervisor does not start a drained node again.
                tracing::info!(node = id, "drained: no group needs this node any more, stopped");
            }
            return Ok(());
        }
        () = shutdown => tracing::info!(node = id, "stopping"),
    }
    handle.stop();
    stopped.await?;
    tracing::info!(node = id, "stopped");
    Ok(())
}

/// A client acting for an admin, reaching the nodes the file names: with a
/// client certificate (`identity`: certificate and key paths) or, without
/// one, a SCRAM user and password.
pub fn admin_client(
    file: &ClusterFile,
    identity: Option<(std::path::PathBuf, std::path::PathBuf)>,
    scram: Option<(String, String)>,
) -> Result<lotse_client::Client<TokioRuntime>, RunError> {
    let rt = TokioRuntime::new(std::env::temp_dir());
    let rt = match &file.tls.ca {
        Some(ca) => rt.with_tls(TlsConfig::from_files(ca, identity)?),
        None => rt.plaintext_on_loopback(),
    };
    let bootstrap = file.nodes.iter().map(|n| n.client.clone()).collect();
    let credentials = scram.map(|(user, password)| lotse_client::Credentials::Scram { user, password });
    let cfg = lotse_client::ClientConfig {
        retry_for: std::time::Duration::from_secs(15),
        credentials,
        ..lotse_client::ClientConfig::new(bootstrap)
    };
    Ok(lotse_client::Client::new(rt, cfg))
}
