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
use lotse_server::{ClientCas, Node, StateMachine};

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

/// How often a running node looks at its certificate files (ADR 0060).
pub const TLS_RELOAD_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// The runtime node `id` runs on: its data directory, and TLS with its
/// certificate or, if the file says so, plain TCP on loopback.
pub fn runtime(file: &ClusterFile, id: u64) -> Result<TokioRuntime, RunError> {
    let dir = file.data_dir(id);
    std::fs::create_dir_all(&dir).map_err(|source| RunError::DataDir { path: dir.display().to_string(), source })?;
    let rt = TokioRuntime::new(dir);
    Ok(match tls_files(file, id)? {
        Some((_, tls)) => rt.with_tls(tls),
        None => rt.plaintext_on_loopback(),
    })
}

/// The bytes of the files TLS was loaded from, in the order `[tls]` names them.
pub type FileContents = Vec<Vec<u8>>;

/// Node `id`'s TLS from the files `[tls]` names, together with those files'
/// contents, which say whether anything changed since; `None` without TLS.
pub fn tls_files(file: &ClusterFile, id: u64) -> Result<Option<(FileContents, TlsConfig)>, RunError> {
    let Some((ca, cert, key)) = file.tls_paths(id) else { return Ok(None) };
    let mut paths = vec![ca, cert, key];
    paths.extend(file.crl_path());
    let mut contents = Vec::new();
    for p in &paths {
        contents.push(
            std::fs::read(p).map_err(|source| {
                RunError::Tls(lotse_rt_tokio::TlsError::Io { path: p.display().to_string(), source })
            })?,
        );
    }
    let crl = contents.get(3).map(Vec::as_slice);
    let tls = TlsConfig::from_pem_with_crl(&contents[0], Some((&contents[1], &contents[2])), crl)?;
    Ok(Some((contents, tls)))
}

/// The certificates (DER) of a PEM bundle such as the CA file, in order;
/// blocks that are no certificate are left out.
pub fn pem_certificates(pem: &[u8]) -> Vec<Vec<u8>> {
    x509_parser::pem::Pem::iter_from_buffer(pem)
        .filter_map(Result::ok)
        .filter(|p| p.label == "CERTIFICATE")
        .map(|p| p.contents)
        .collect()
}

/// Looks at node `id`'s certificate files every [`TLS_RELOAD_EVERY`] and
/// gives the runtime what changed: a renewed certificate, a new CA bundle,
/// a longer revocation list; a new CA bundle also goes to `cas`, which
/// recorded client certificates are checked against (ADR 0096). Files that
/// do not make a working configuration (half written, a key that does not
/// match) are reported and the old material kept, so a mistake never takes
/// a running node off the network.
pub async fn reload_tls(rt: TokioRuntime, file: ClusterFile, id: u64, cas: ClientCas) {
    reload_tls_every(rt, file, id, cas, TLS_RELOAD_EVERY).await
}

/// [`reload_tls`], looking every `every`.
pub async fn reload_tls_every(
    rt: TokioRuntime,
    file: ClusterFile,
    id: u64,
    cas: ClientCas,
    every: std::time::Duration,
) {
    let mut seen = match tls_files(&file, id) {
        Ok(Some((contents, _))) => contents,
        _ => return,
    };
    let mut failing = false;
    loop {
        tokio::time::sleep(every).await;
        match tls_files(&file, id) {
            Ok(Some((contents, tls))) if contents != seen => {
                let closed = rt.reload_tls(tls);
                cas.set(pem_certificates(&contents[0]));
                tracing::info!(node = id, revoked_connections_closed = closed, "TLS files changed: reloaded");
                seen = contents;
                failing = false;
            }
            Ok(_) => failing = false,
            Err(e) => {
                if !failing {
                    tracing::warn!(node = id, error = %e, "TLS files do not load; keeping the material in use");
                }
                failing = true;
            }
        }
    }
}

/// Runs node `id` of the cluster in `file` until `shutdown` completes,
/// then stops it and waits for it to finish. With `join`, a node starting
/// for the first time joins a running cluster (after `lotse node add`)
/// instead of being one of its first voters (ADR 0048).
pub async fn run(file: &ClusterFile, id: u64, join: bool, shutdown: impl Future<Output = ()>) -> Result<(), RunError> {
    let mut cfg = file.node_config(id)?;
    cfg.join = join;
    let rt = runtime(file, id)?;
    if let Some((contents, _)) = tls_files(file, id)? {
        cfg.auth.client_cas.set(pem_certificates(&contents[0]));
    }
    tracing::info!(
        node = id,
        peer = %cfg.listen,
        client = cfg.client_listen.as_deref().unwrap_or("-"),
        data = %file.data_dir(id).display(),
        tls = file.tls_paths(id).is_some(),
        "starting"
    );
    // Stops with this function, however it returns.
    let _reload =
        AbortOnDrop(tokio::spawn(reload_tls(rt.clone(), file.clone(), id, cfg.auth.client_cas.clone())).abort_handle());
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

/// Aborts a task when dropped.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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
