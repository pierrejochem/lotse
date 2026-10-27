use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use lotse_console::auth::{AuthConfig, Identity};
use lotse_console::cluster::Cluster;
use lotse_console::cluster::demo::DemoCluster;
use lotse_console::config::{ClusterConfig, Config};
use lotse_console::metrics::{MetricsStore, run_sampler};
use lotse_console::{AppState, openapi, router};

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
            ClusterConfig::Native { .. } => unreachable!("rejected by Config::parse"),
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
