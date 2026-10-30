//! `lotse`: runs one node of a cluster from a cluster file, makes its
//! certificates, or checks the file (ADR 0047).

use std::path::PathBuf;
use std::process::ExitCode;

use lotse_node::certs::{self, Outcome};
use lotse_node::config::ClusterFile;

const USAGE: &str = "\
lotse: one node of a Lotse cluster

usage:
  lotse run   --config <cluster.toml> --node <id>
  lotse certs --config <cluster.toml>
  lotse certs --config <cluster.toml> --client <principal> --out <dir>
  lotse check --config <cluster.toml>
  lotse --version

run    runs the node; stops on Ctrl-C or SIGTERM. LOTSE_LOG sets the log
       filter (default info).
certs  makes the cluster's CA and every node's certificate where [tls] says,
       keeping those that exist; with --client, a certificate for a
       principal, such as an admin named in auth.admins.
check  reads the file and says what it describes.";

#[derive(Debug, Default)]
struct Args {
    config: Option<PathBuf>,
    node: Option<u64>,
    client: Option<String>,
    out: Option<PathBuf>,
}

fn parse(mut rest: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args::default();
    while let Some(flag) = rest.next() {
        let mut value = || rest.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--config" => a.config = Some(PathBuf::from(value()?)),
            "--node" => {
                let v = value()?;
                a.node = Some(v.parse().map_err(|_| format!("--node {v}: a node id is a number"))?);
            }
            "--client" => a.client = Some(value()?),
            "--out" => a.out = Some(PathBuf::from(value()?)),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(a)
}

fn load(a: &Args) -> Result<ClusterFile, String> {
    let path = a.config.as_ref().ok_or("--config is missing")?;
    ClusterFile::load(path).map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let result = match command.as_str() {
        "--version" | "-V" => {
            println!("lotse {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        "--help" | "-h" | "help" => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        "run" => parse(args).and_then(|a| run(&a)),
        "certs" => parse(args).and_then(|a| certs(&a)),
        "check" => parse(args).and_then(|a| check(&a)),
        other => Err(format!("unknown command {other}\n\n{USAGE}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("lotse: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let id = a.node.ok_or("--node is missing")?;
    file.node(id).map_err(|e| e.to_string())?;
    let filter = std::env::var("LOTSE_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(filter)).init();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(lotse_node::run(&file, id, shutdown())).map_err(|e| e.to_string())
}

/// Ctrl-C, or SIGTERM from a service manager.
async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn certs(a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let outcomes = match &a.client {
        Some(principal) => {
            let out = a.out.as_ref().ok_or("--client needs --out <dir>")?;
            certs::client_cert(&file, principal, out)
        }
        None => certs::node_certs(&file),
    }
    .map_err(|e| e.to_string())?;
    for o in outcomes {
        match o {
            Outcome::Wrote(p) => println!("wrote {}", p.display()),
            Outcome::Kept(p) => println!("kept  {}", p.display()),
        }
    }
    if a.client.is_none() {
        if let Some(ca) = &file.tls.ca {
            println!(
                "\nThe CA key {} signs every certificate of the cluster: keep it off the nodes.",
                certs::ca_key_path(std::path::Path::new(ca)).display()
            );
        }
    }
    Ok(())
}

fn check(a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let ids: Vec<String> = file.nodes.iter().map(|n| n.id.to_string()).collect();
    println!("{} nodes: {}", file.nodes.len(), ids.join(", "));
    println!("TLS: {}", if file.tls_paths(0).is_some() { "certificates from [tls]" } else { "none, loopback only" });
    let c = file.node_config(file.nodes[0].id).map_err(|e| e.to_string())?;
    let limit = |v: Option<u64>, unit: &str| v.map_or("no limit".to_string(), |v| format!("{v} {unit}"));
    println!("retention: {} by age, {} per partition", limit(c.retention.ms, "ms"), limit(c.retention.bytes, "bytes"));
    let auth = &c.auth;
    if auth.required {
        let mut ways = Vec::new();
        if auth.mtls {
            ways.push("certificate");
        }
        if auth.scram {
            ways.push("SCRAM-SHA-512");
        }
        if auth.oidc.is_some() {
            ways.push("OAUTHBEARER");
        }
        println!("clients sign in with: {}; admins: {}", ways.join(", "), auth.admins.join(", "));
    } else {
        println!("clients: no authentication, everyone may do everything");
    }
    Ok(())
}
