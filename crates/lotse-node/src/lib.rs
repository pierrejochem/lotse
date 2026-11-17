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
use lotse_rt_tokio::{HttpRequest, HttpResponse};
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
    /// The metrics endpoint could not listen.
    #[error("metrics at {addr}: {source}")]
    Metrics {
        /// Where it should have listened.
        addr: String,
        /// Why it did not.
        source: std::io::Error,
    },
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
    let metrics_at = file.node(id)?.metrics.clone();
    let node = Node::start(rt.clone(), cfg, |_| NoStateMachine).await?;
    let handle = node.handle();
    // The operator's endpoint (ADR 0117); stops with this function too.
    let _metrics = match metrics_at {
        Some(addr) => {
            let listener =
                rt.bind_http(&addr).await.map_err(|source| RunError::Metrics { addr: addr.clone(), source })?;
            tracing::info!(node = id, metrics = %listener.local_addr(), tls = listener.tls(), "serving metrics");
            let handle = handle.clone();
            Some(AbortOnDrop(tokio::spawn(listener.serve(move |req| operator(handle.clone(), req))).abort_handle()))
        }
        None => None,
    };
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
        () = shutdown => tracing::info!(node = id, "stopping: handing the groups this node leads to other voters"),
    }
    // Like Kafka's controlled shutdown: the groups this node leads move
    // before it stops, so clients meet a planned move, not a failover
    // (ADR 0114). Bounded, within systemd's TimeoutStopSec of 60 s.
    handle.leave(LEAVE_WITHIN);
    tokio::select! {
        res = &mut stopped => res?,
        () = tokio::time::sleep(LEAVE_WITHIN + std::time::Duration::from_secs(5)) => {
            handle.stop();
            stopped.await?;
        }
    }
    tracing::info!(node = id, "stopped");
    Ok(())
}

/// How long the operator's endpoint waits for the node's main loop; a loop
/// that does not answer in this time is not healthy.
const OBSERVE_WITHIN: std::time::Duration = std::time::Duration::from_secs(5);
/// How long `/safe-to-stop` waits for the other nodes' answers.
const JUDGE_WITHIN: std::time::Duration = std::time::Duration::from_secs(15);

/// Answers one request to the operator's endpoint (ADR 0117).
///
/// - `/metrics`: OpenMetrics text for Prometheus. It names every tenant's
///   resources: over TLS only for a client certificate whose principal sees
///   the whole cluster.
/// - `/healthz`: 200 while the node's main loop answers.
/// - `/readyz`: 200 once every group on the node has a leader it hears and
///   keeps up with, or leads with a quorum.
/// - `/safe-to-stop`: 200 unless stopping the node now would leave a group
///   without a quorum, as the groups' leaders see it.
///
/// The last three carry group ids and node ids, no names, and are open to
/// probes without a certificate.
pub async fn operator(handle: lotse_server::NodeHandle, req: HttpRequest) -> HttpResponse {
    use lotse_server::ops::{self, Scraper};
    let scraper = match req.tls {
        false => Scraper::Local,
        true => Scraper::Tls { identity: req.identity.clone(), cert: req.cert.clone() },
    };
    let observe =
        |scraper| async { tokio::time::timeout(OBSERVE_WITHIN, handle.observe(scraper)).await.ok().flatten() };
    match req.path.as_str() {
        "/metrics" => match observe(scraper).await {
            None => HttpResponse::text(503, "the node does not answer\n"),
            Some(obs) if !obs.may_scrape => HttpResponse::text(
                403,
                "metrics name every tenant's resources: show a client certificate from the cluster CA whose \
                 principal holds cluster-viewer or cluster-admin\n",
            ),
            Some(obs) => HttpResponse {
                status: 200,
                content_type: ops::OPENMETRICS_CONTENT_TYPE.into(),
                body: ops::render(&obs).into_bytes(),
            },
        },
        "/healthz" => match observe(Scraper::Local).await {
            Some(_) => HttpResponse::text(200, "ok\n"),
            None => HttpResponse::text(503, "the node's main loop does not answer\n"),
        },
        "/readyz" => match observe(Scraper::Local).await {
            None => HttpResponse::text(503, "the node's main loop does not answer\n"),
            Some(obs) => {
                let not_ready = obs.not_ready();
                if not_ready.is_empty() {
                    return HttpResponse::text(200, "ready\n");
                }
                let mut text = String::from("not ready\n");
                for (g, why) in not_ready {
                    text.push_str(&format!("group {g}: {why}\n"));
                }
                HttpResponse::text(503, text)
            }
        },
        "/safe-to-stop" => match tokio::time::timeout(JUDGE_WITHIN, handle.safe_to_stop()).await {
            Err(_) => HttpResponse::text(503, "the other nodes did not answer in time\n"),
            Ok(Err(e)) => HttpResponse::text(503, format!("cannot tell: {e}\n")),
            Ok(Ok(critical)) if critical.is_empty() => HttpResponse::text(200, "safe to stop\n"),
            Ok(Ok(critical)) => {
                let mut text = String::from("not safe to stop\n");
                for c in critical {
                    let voters = c.voters.len();
                    match c.responsive {
                        Some(r) => text.push_str(&format!(
                            "group {}: {} of {voters} voters would remain answering\n",
                            c.group,
                            r.len()
                        )),
                        None => text.push_str(&format!("group {}: no node reports leading it\n", c.group)),
                    }
                }
                HttpResponse::text(503, text)
            }
        },
        _ => HttpResponse::text(404, "not found: /metrics, /healthz, /readyz, /safe-to-stop\n"),
    }
}

/// How long a stopping node may take to hand over the groups it leads.
const LEAVE_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);

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
