use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use lotse_console::auth::{AuthConfig, Identity};
use lotse_console::cluster::Cluster;
use lotse_console::cluster::demo::DemoCluster;
use lotse_console::cluster::native::NativeCluster;
use lotse_console::config::{ClusterConfig, Config};
use lotse_console::metrics::{MetricsStore, run_sampler};
use lotse_console::{AppState, openapi, router};
use lotse_rt_tokio::{TlsConfig, TokioRuntime};

/// Lotse management console.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Config file.
    #[arg(long, short, default_value = "lotse-console.toml")]
    config: PathBuf,
    /// Print the OpenAPI document and exit.
    #[arg(long)]
    print_openapi: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    if args.print_openapi {
        println!("{}", openapi().to_pretty_json().expect("document serialises"));
        return ExitCode::SUCCESS;
    }
    let cfg = match Config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("lotse-console: {e}");
            return ExitCode::FAILURE;
        }
    };

    let store = Arc::new(MetricsStore::new(cfg.metrics.retention(), cfg.metrics.step()));
    let mut clusters: Vec<Arc<dyn Cluster>> = Vec::new();
    for c in &cfg.clusters {
        match c {
            ClusterConfig::Demo { id } => {
                let demo = DemoCluster::new(id.clone());
                demo.backfill(&store, cfg.metrics.retention(), cfg.metrics.step());
                tracing::info!(cluster = %id, "serving the built-in demo cluster");
                clusters.push(Arc::new(demo));
            }
            ClusterConfig::Native { id, bootstrap, ca_file } => {
                let rt = TokioRuntime::new(std::env::temp_dir());
                let rt = match ca_file {
                    Some(ca) => match TlsConfig::from_files(ca, None::<(&str, &str)>) {
                        Ok(tls) => rt.with_tls(tls),
                        Err(e) => {
                            eprintln!("lotse-console: cluster \"{id}\": {e}");
                            return ExitCode::FAILURE;
                        }
                    },
                    None => {
                        tracing::warn!(cluster = %id, "no ca_file: plain TCP, loopback only");
                        rt.plaintext_on_loopback()
                    }
                };
                tracing::info!(cluster = %id, ?bootstrap, "serving a cluster over the native protocol");
                tracing::warn!(cluster = %id, "Lotse has no authentication yet: requests reach the cluster without credentials");
                clusters.push(Arc::new(NativeCluster::with_runtime(id.clone(), bootstrap.clone(), rt)));
            }
        }
    }

    // The sampler is the only thing that uses the console's own identity,
    // a read-only metrics role on the brokers.
    let sampler_identity =
        Identity { principal: cfg.metrics.principal.clone(), display_name: "metrics sampler".into(), token: None };
    tokio::spawn(run_sampler(clusters.clone(), Arc::clone(&store), sampler_identity, cfg.metrics.step()));

    if let AuthConfig::Dev { user } = &cfg.auth {
        tracing::warn!(%user, "dev auth: every request acts as this user");
    }
    let state = AppState::new(clusters, cfg.auth.clone(), store, Duration::from_millis(cfg.live.interval_ms.max(250)));
    let listener = match tokio::net::TcpListener::bind(cfg.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("lotse-console: cannot listen on {}: {e}", cfg.listen);
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(listen = %cfg.listen, "lotse-console ready");
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    if let Err(e) = axum::serve(listener, router(state)).with_graceful_shutdown(shutdown).await {
        eprintln!("lotse-console: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
