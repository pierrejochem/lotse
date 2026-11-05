//! `lotse`: runs one node of a cluster from a cluster file, makes its
//! certificates, checks the file (ADR 0047), and adds and removes nodes
//! (ADR 0048).

use std::path::PathBuf;
use std::process::ExitCode;

use lotse_node::certs::{self, Outcome};
use lotse_node::config::ClusterFile;

const USAGE: &str = "\
lotse: one node of a Lotse cluster

usage:
  lotse run   --config <cluster.toml> --node <id> [--join]
  lotse certs --config <cluster.toml> [--days <n>]
  lotse certs --config <cluster.toml> --client <principal> --out <dir> [--days <n>]
  lotse certs --config <cluster.toml> --revoke <cert.pem>
  lotse check --config <cluster.toml>
  lotse node add    --config <cluster.toml> --node <id> [--peer <host:port> --client-addr <host:port>] <admin>
  lotse node remove --config <cluster.toml> --node <id> <admin>
  lotse node list   --config <cluster.toml> <admin>
  lotse --version

run    runs the node; stops on Ctrl-C or SIGTERM. LOTSE_LOG sets the log
       filter (default info). --join: a new node joining a running cluster,
       after `lotse node add`. In a container or a systemd instance,
       LOTSE_NODE, LOTSE_JOIN=1 and LOTSE_CONFIG stand in for the flags.
certs  makes the cluster's CA and every node's certificate where [tls] says,
       keeping those that exist; with --client, a certificate for a
       principal, such as an admin named in auth.admins. New certificates
       are valid for --days (default 365), a new CA for 3650. --revoke adds
       a certificate to the revocation list [tls] crl names; copy the list
       to every node, which picks it up within 10 s, as it does renewed
       certificates.
check  reads the file and says what it describes, and which certificates
       expire within 30 days.
node   adds a node (addresses from its [[node]] entry unless given), drains
       and removes one, or lists them.
       <admin> is --cert <file> --key <file> (a client certificate) or
       --user <name> with the password in LOTSE_PASSWORD.";

#[derive(Debug, Default)]
struct Args {
    config: Option<PathBuf>,
    node: Option<u64>,
    client: Option<String>,
    out: Option<PathBuf>,
    join: bool,
    peer: Option<String>,
    client_addr: Option<String>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    user: Option<String>,
    days: Option<u32>,
    revoke: Option<PathBuf>,
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
            "--join" => a.join = true,
            "--peer" => a.peer = Some(value()?),
            "--client-addr" => a.client_addr = Some(value()?),
            "--cert" => a.cert = Some(PathBuf::from(value()?)),
            "--key" => a.key = Some(PathBuf::from(value()?)),
            "--user" => a.user = Some(value()?),
            "--days" => {
                let v = value()?;
                let days = v.parse().ok().filter(|d| *d > 0);
                a.days = Some(days.ok_or_else(|| format!("--days {v}: a number of days, at least 1"))?);
            }
            "--revoke" => a.revoke = Some(PathBuf::from(value()?)),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(a)
}

fn load(a: &Args) -> Result<ClusterFile, String> {
    let path = match &a.config {
        Some(p) => p.clone(),
        None => std::env::var_os("LOTSE_CONFIG").map(PathBuf::from).ok_or("--config (or LOTSE_CONFIG) is missing")?,
    };
    ClusterFile::load(&path).map_err(|e| e.to_string())
}

/// The node `run` starts: `--node`, else `LOTSE_NODE` (ADR 0049), and
/// whether it joins: `--join`, else `LOTSE_JOIN` (`1` or `true`).
fn node_and_join(a: &Args) -> Result<(u64, bool), String> {
    let id = match (a.node, std::env::var("LOTSE_NODE")) {
        (Some(id), _) => id,
        (None, Ok(v)) => v.trim().parse().map_err(|_| format!("LOTSE_NODE={v}: a node id is a number"))?,
        (None, Err(_)) => return Err("--node (or LOTSE_NODE) is missing".into()),
    };
    let join = a.join || std::env::var("LOTSE_JOIN").is_ok_and(|v| matches!(v.trim(), "1" | "true"));
    Ok((id, join))
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
        "node" => match args.next().as_deref() {
            Some(what @ ("add" | "remove" | "list")) => {
                let what = what.to_string();
                parse(args).and_then(|a| node(&what, &a))
            }
            _ => Err(format!("lotse node needs add, remove or list\n\n{USAGE}")),
        },
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
    let (id, join) = node_and_join(a)?;
    file.node(id).map_err(|e| e.to_string())?;
    let filter = std::env::var("LOTSE_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(filter)).init();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(lotse_node::run(&file, id, join, shutdown())).map_err(|e| e.to_string())
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
    if let Some(cert) = &a.revoke {
        if a.client.is_some() || a.days.is_some() {
            return Err("--revoke goes alone".into());
        }
        let r = certs::revoke(&file, cert).map_err(|e| e.to_string())?;
        let what = if r.already { "already revoked" } else { "revoked" };
        println!("{what}: serial {} ({})", r.serial, cert.display());
        println!("wrote {}: copy it to every node; each picks it up within 10 s", r.crl.display());
        return Ok(());
    }
    let days = a.days.unwrap_or(certs::LEAF_DAYS);
    let outcomes = match &a.client {
        Some(principal) => {
            let out = a.out.as_ref().ok_or("--client needs --out <dir>")?;
            certs::client_cert(&file, principal, out, days)
        }
        None => certs::node_certs(&file, days),
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
    expiry(&file);
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

/// Within this, `check` names a certificate that is about to expire.
const EXPIRY_WARNING_DAYS: i64 = 30;

/// Says which of the CA and node certificates on this machine expire soon
/// or have expired; files that are not here (on another node) are skipped.
fn expiry(file: &ClusterFile) {
    let mut paths: Vec<PathBuf> = file.tls.ca.iter().map(PathBuf::from).collect();
    for n in &file.nodes {
        paths.extend(file.tls_paths(n.id).map(|(_, cert, _)| cert));
    }
    paths.sort();
    paths.dedup();
    for p in paths.iter().filter(|p| p.exists()) {
        match certs::expires_in(p) {
            Ok(s) if s <= 0 => println!("warning: {} has expired", p.display()),
            Ok(s) if s < EXPIRY_WARNING_DAYS * 86_400 => {
                println!("warning: {} expires in {} days", p.display(), s / 86_400)
            }
            Ok(_) => {}
            Err(e) => println!("warning: {e}"),
        }
    }
}

/// `lotse node add | remove | list`, as the admin the flags name.
fn node(what: &str, a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let identity = match (&a.cert, &a.key) {
        (Some(c), Some(k)) => Some((c.clone(), k.clone())),
        (None, None) => None,
        _ => return Err("--cert and --key go together".into()),
    };
    let scram = match &a.user {
        Some(u) => {
            let password = std::env::var("LOTSE_PASSWORD").map_err(|_| "--user needs LOTSE_PASSWORD")?;
            Some((u.clone(), password))
        }
        None => None,
    };
    let client = lotse_node::admin_client(&file, identity, scram).map_err(|e| e.to_string())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(async {
        match what {
            "add" => {
                let id = a.node.ok_or("--node is missing")?;
                let entry = file.nodes.iter().find(|n| n.id == id);
                let peer = a.peer.clone().or_else(|| entry.map(|n| n.peer.clone()));
                let addr = a.client_addr.clone().or_else(|| entry.map(|n| n.client.clone()));
                let (Some(peer), Some(addr)) = (peer, addr) else {
                    return Err(format!("node {id} is not in the file: give --peer and --client-addr"));
                };
                client.add_node(id, &peer, &addr).await.map_err(|e| e.to_string())?;
                println!("node {id} added; start it with: lotse run --config <file> --node {id} --join");
            }
            "remove" => {
                let id = a.node.ok_or("--node is missing")?;
                client.remove_node(id).await.map_err(|e| e.to_string())?;
                println!("node {id}: draining; `lotse node list` shows how far, and it leaves once empty");
            }
            _ => {
                let nodes = client.describe_nodes().await.map_err(|e| e.to_string())?;
                println!(
                    "{:<6} {:<24} {:<24} {:<9} {:>8} {:>7}",
                    "node", "peer", "client", "state", "replicas", "moving"
                );
                for n in nodes {
                    let state = if n.state == lotse_proto::node_state::DRAINING { "draining" } else { "active" };
                    println!(
                        "{:<6} {:<24} {:<24} {:<9} {:>8} {:>7}",
                        n.node_id, n.peer, n.client, state, n.replicas, n.moving
                    );
                }
            }
        }
        Ok(())
    })
}
