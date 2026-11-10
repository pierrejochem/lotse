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
  lotse certs --config <cluster.toml> --sign [--request <id>] [--yes] [--out <dir>] [--days <n>] <admin>
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
       certificates. --sign, on the machine with the CA key, lists the
       certificate requests cluster admins made in the console, shows each
       one's principal and key fingerprint, signs those you confirm (--yes
       with --request <id>, the id or its first digits, signs without
       asking) and records the certificates in the cluster; --out also
       writes each as <principal>.pem.
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
    sign: bool,
    request: Option<String>,
    yes: bool,
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
            "--sign" => a.sign = true,
            "--request" => a.request = Some(value()?.to_lowercase()),
            "--yes" => a.yes = true,
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
    if a.sign {
        if a.client.is_some() || a.revoke.is_some() {
            return Err("--sign goes alone".into());
        }
        return sign(&file, a);
    }
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
    if a.client.is_none()
        && let Some(ca) = &file.tls.ca
    {
        println!(
            "\nThe CA key {} signs every certificate of the cluster: keep it off the nodes.",
            certs::ca_key_path(std::path::Path::new(ca)).display()
        );
    }
    Ok(())
}

/// `lotse certs --sign`: the pending requests, signed as the CA holder
/// confirms and recorded in the cluster as the admin the flags name.
fn sign(file: &ClusterFile, a: &Args) -> Result<(), String> {
    if a.yes && a.request.is_none() {
        return Err("--yes needs --request <id>: it signs that request without asking, never all of them".into());
    }
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
    let days = a.days.unwrap_or(certs::LEAF_DAYS);
    let client = lotse_node::admin_client(file, identity, scram).map_err(|e| e.to_string())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(async {
        let listed = client.describe_certificate_requests().await.map_err(|e| e.to_string())?;
        let hex = |b: &[u8]| b.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let pending: Vec<_> = listed
            .requests
            .iter()
            .filter(|r| r.state == lotse_proto::certificate_request_state::PENDING)
            .filter(|r| a.request.as_ref().is_none_or(|p| hex(&r.id).starts_with(p.as_str())))
            .collect();
        if pending.is_empty() {
            println!("no certificate requests are waiting");
            return Ok(());
        }
        if a.request.is_some() && pending.len() > 1 {
            return Err(format!("--request matches {} requests: give more digits of the id", pending.len()));
        }
        let mut signed = 0;
        for r in pending {
            let id = hex(&r.id);
            let info = match certs::read_csr(&r.csr, &r.principal) {
                Ok(info) => info,
                Err(e) => {
                    println!("{}  {}: skipped, {e}", &id[..12], r.principal);
                    continue;
                }
            };
            println!(
                "request {}  principal {}  asked by {}\n  key fingerprint {}",
                &id[..12],
                r.principal,
                r.requested_by,
                info.key_fingerprint
            );
            if !a.yes && !confirm(&format!("sign a client certificate for {} valid for {days} days?", r.principal))? {
                println!("  left for later");
                continue;
            }
            let made = certs::sign_csr(file, &r.csr, &r.principal, days).map_err(|e| e.to_string())?;
            client
                .record_certificate(lotse_proto::RecordCertificateRequest {
                    id: r.id.clone(),
                    certificate: made.der.clone().into(),
                    issuer: made.issuer.to_vec().into(),
                    serial: made.serial.clone().into(),
                    not_before_ms: made.not_before_ms,
                    not_after_ms: made.not_after_ms,
                })
                .await
                .map_err(|e| e.to_string())?;
            println!("  signed: serial {}; recorded, the requester can download it in the console", hex(&made.serial));
            if let Some(dir) = &a.out {
                let path = certs::write_certificate(dir, &r.principal, &made.pem).map_err(|e| e.to_string())?;
                println!("  wrote {}", path.display());
            }
            signed += 1;
        }
        println!("{signed} signed");
        Ok(())
    })
}

/// Asks on the terminal; anything but `y` or `yes` is no.
fn confirm(question: &str) -> Result<bool, String> {
    use std::io::Write as _;
    print!("  {question} [y/N] ");
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| e.to_string())?;
    Ok(matches!(line.trim().to_lowercase().as_str(), "y" | "yes"))
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
