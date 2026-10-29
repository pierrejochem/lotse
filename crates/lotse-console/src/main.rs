use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use lotse_console::auth::{
    AuthConfig, Identity, Login, SamplerLogin, Secret, ServiceCredentials, ServiceLogin, SessionKey, Sessions,
};
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
                clusters.push(Arc::new(NativeCluster::with_runtime(id.clone(), bootstrap.clone(), rt)));
            }
        }
    }

    // The sampler is the only thing that uses the console's own identity,
    // a viewer role on the brokers.
    let http_rt = |ca: Option<&std::path::Path>| -> Result<TokioRuntime, String> {
        let rt = TokioRuntime::new(std::env::temp_dir());
        match ca {
            None => Ok(rt),
            Some(path) => std::fs::read(path)
                .map_err(|e| e.to_string())
                .and_then(|pem| rt.with_http_ca(&pem).map_err(|e| e.to_string()))
                .map_err(|e| format!("{}: {e}", path.display())),
        }
    };
    let oidc = match &cfg.auth {
        AuthConfig::Oidc(o) => Some(o),
        AuthConfig::Dev { .. } => None,
    };
    let idp_rt = match http_rt(oidc.and_then(|o| o.ca_file.as_deref())) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("lotse-console: auth.ca_file {e}");
            return ExitCode::FAILURE;
        }
    };
    let m = &cfg.metrics;
    let sampler = match (&m.client_id, &m.client_secret_file) {
        (Some(client_id), Some(secret_file)) => {
            let secret = match std::fs::read_to_string(secret_file) {
                Ok(s) => s.trim().to_owned(),
                Err(e) => {
                    eprintln!("lotse-console: metrics.client_secret_file {}: {e}", secret_file.display());
                    return ExitCode::FAILURE;
                }
            };
            let issuer = m.issuer.clone().or_else(|| oidc.map(|o| o.issuer.clone())).expect("checked when loaded");
            let creds = ServiceCredentials {
                issuer,
                client_id: client_id.clone(),
                client_secret: Secret::new(secret),
                scope: m.scope.clone(),
            };
            SamplerLogin::Service(Arc::new(ServiceLogin::new(&m.principal, creds, idp_rt.clone())))
        }
        _ => SamplerLogin::Fixed(Identity {
            principal: m.principal.clone(),
            display_name: "metrics sampler".into(),
            login: m.password.clone().map_or(Login::None, |p| Login::Password(Secret::new(p))),
        }),
    };
    tokio::spawn(run_sampler(clusters.clone(), Arc::clone(&store), sampler, cfg.metrics.step()));

    let mut state =
        AppState::new(clusters, cfg.auth.clone(), store, Duration::from_millis(cfg.live.interval_ms.max(250)));
    match &cfg.auth {
        AuthConfig::Dev { user, .. } => tracing::warn!(%user, "dev auth: every request acts as this user"),
        AuthConfig::Oidc(o) => {
            let path = cfg.session_key_file().expect("oidc mode");
            let key = match SessionKey::load_or_create(&path, &idp_rt) {
                Ok((k, true)) => {
                    tracing::info!(path = %path.display(), "made a session key; consoles that share sessions need a copy");
                    k
                }
                Ok((k, false)) => k,
                Err(e) => {
                    eprintln!("lotse-console: session key {e}");
                    return ExitCode::FAILURE;
                }
            };
            tracing::info!(issuer = %o.issuer, "users sign in with OpenID Connect");
            state = state.with_sessions(Arc::new(Sessions::new(o.clone(), idp_rt.clone(), key)));
        }
    }
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
