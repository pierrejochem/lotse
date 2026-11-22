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
  lotse certs --config <cluster.toml> --renewal-ca [--days <n>]
  lotse certs --config <cluster.toml> --renew [--if-due] --cert <file> --key <file>
  lotse certs --config <cluster.toml> --sign [--request <id>] [--yes] [--out <dir>] [--days <n>] <admin>
  lotse check --config <cluster.toml>
  lotse node add    --config <cluster.toml> --node <id> [--peer <host:port> --client-addr <host:port> --zone <zone>] <admin>
  lotse node remove --config <cluster.toml> --node <id> <admin>
  lotse node list   --config <cluster.toml> <admin>
  lotse turn take    --config <cluster.toml> --node <id> [--wait <time>]
  lotse turn release --config <cluster.toml> --node <id>
  lotse upgrade status   --config <cluster.toml> (--node <id> | <admin>)
  lotse upgrade hold     --config <cluster.toml> <admin>
  lotse upgrade resume   --config <cluster.toml> <admin>
  lotse upgrade finalize --config <cluster.toml> [--version <major.minor>] <admin>
  lotse upgrade release  --config <cluster.toml> --node <id> <admin>
  lotse probe <healthz|readyz|safe-to-stop> --config <cluster.toml> --node <id>
  lotse mcp (--config <cluster.toml> | --bootstrap <host:port,...> --ca <file>) [--cert <file> --key <file>]
            [--plaintext-on-loopback]
            [--listen <host:port> [--tls-cert <file> --tls-key <file>] [--allow-origin <origin>]...]
  lotse --version

run    runs the node; stops on Ctrl-C or SIGTERM. LOTSE_LOG sets the log
       filter (default info). --join: a new node joining a running cluster,
       after `lotse node add`. In a container or a systemd instance,
       LOTSE_NODE, LOTSE_JOIN=1 and LOTSE_CONFIG stand in for the flags;
       in a StatefulSet, LOTSE_NODE_INDEX (the pod index) makes the node
       id the index plus one.
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
       writes each as <principal>.pem. --renewal-ca makes the renewal CA
       at [tls] renewal_ca, under the CA; copy it and its key to every
       node. --renew renews the client certificate in --cert over a
       connection made with it, once it is in its last 30 days or past two
       thirds of its validity, and writes the new key and certificate over
       --key and --cert; with --if-due it does nothing before then, for a
       timer to run daily.
check  reads the file and says what it describes, and which certificates
       expire within 30 days.
node   adds a node (addresses from its [[node]] entry unless given), drains
       and removes one, or lists them.
turn   on a node's machine, with its certificate (ADR 0127): take waits
       until the node holds the cluster's restart turn, which it gets once
       no other node holds it, turns are not paused, and no group of its
       would lose its quorum without it; then restart the node. The
       restarted node gives the turn back once it is ready. --wait bounds
       the wait (30m by default; s, m or h); exit 1 if it ends first.
       release gives the turn back if the restart did not happen.
upgrade shows the cluster version, the turn and each node's release;
       pauses or resumes turns; raises the cluster version once every node
       runs a release with it (--version, by default the newest all of
       them run with); or gives back the turn a node holds.
probe  asks the node's operator endpoint ([[node]] metrics, on loopback)
       for /healthz, /readyz or /safe-to-stop; exit 0 for 200. For exec
       probes in a container without a shell.
mcp    speaks the Model Context Protocol on stdin and stdout, for an AI
       agent that starts it (ADR 0134): MCP 2026-07-28, and 2025-11-25 for
       clients that open a session. It acts as one principal, from
       LOTSE_TOKEN (an access token), LOTSE_USER and LOTSE_PASSWORD, or
       --cert and --key (a client certificate); without one it exits 2.
       It reaches the nodes the cluster file names, or those --bootstrap
       names, over TLS with the CA of [tls] ca or --ca; plain TCP only with
       --plaintext-on-loopback, to a loopback address. Logs go to stderr
       (LOTSE_LOG, default info). With --listen it speaks MCP over
       Streamable HTTP instead, at POST /mcp, for many agents: it holds no
       credential of its own (giving it one is refused) and every request
       signs in with its own Authorization header, Bearer <access token>
       or Basic <user:password>; without one it gets 401. Plain HTTP only
       on a loopback address; elsewhere --tls-cert and --tls-key (PEM)
       make it HTTPS, else it exits 2. A request with an Origin header
       (from a browser) is refused unless --allow-origin names it.
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
    zone: Option<String>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    user: Option<String>,
    days: Option<u32>,
    revoke: Option<PathBuf>,
    sign: bool,
    renewal_ca: bool,
    renew: bool,
    if_due: bool,
    request: Option<String>,
    yes: bool,
    wait: Option<std::time::Duration>,
    version: Option<String>,
    bootstrap: Option<Vec<String>>,
    ca: Option<PathBuf>,
    plaintext_on_loopback: bool,
    listen: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    allow_origin: Vec<String>,
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
            "--zone" => a.zone = Some(value()?),
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
            "--renewal-ca" => a.renewal_ca = true,
            "--renew" => a.renew = true,
            "--if-due" => a.if_due = true,
            "--request" => a.request = Some(value()?.to_lowercase()),
            "--yes" => a.yes = true,
            "--wait" => {
                let v = value()?;
                a.wait = Some(duration(&v).ok_or_else(|| format!("--wait {v}: a time such as 90s, 30m or 2h"))?);
            }
            "--version" => a.version = Some(value()?),
            "--bootstrap" => {
                let v = value()?;
                let addrs: Vec<String> =
                    v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect();
                if addrs.is_empty() {
                    return Err(format!("--bootstrap {v:?}: one or more host:port, separated by commas"));
                }
                a.bootstrap = Some(addrs);
            }
            "--ca" => a.ca = Some(PathBuf::from(value()?)),
            "--plaintext-on-loopback" => a.plaintext_on_loopback = true,
            "--listen" => a.listen = Some(value()?),
            "--tls-cert" => a.tls_cert = Some(PathBuf::from(value()?)),
            "--tls-key" => a.tls_key = Some(PathBuf::from(value()?)),
            "--allow-origin" => a.allow_origin.push(value()?),
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

/// The node `run` starts: `--node`, else `LOTSE_NODE` (ADR 0049), else
/// `LOTSE_NODE_INDEX` plus one (a StatefulSet's pod index, ADR 0128), and
/// whether it joins: `--join`, else `LOTSE_JOIN` (`1` or `true`).
fn node_and_join(a: &Args) -> Result<(u64, bool), String> {
    let id = node_id(a.node, std::env::var("LOTSE_NODE").ok(), std::env::var("LOTSE_NODE_INDEX").ok())?;
    let join = a.join || std::env::var("LOTSE_JOIN").is_ok_and(|v| matches!(v.trim(), "1" | "true"));
    Ok((id, join))
}

/// The node id from the flag, `LOTSE_NODE` or `LOTSE_NODE_INDEX`, in that
/// order. Pod indexes count from 0 and node ids from 1, so index 0 is node 1.
fn node_id(flag: Option<u64>, node: Option<String>, index: Option<String>) -> Result<u64, String> {
    if let Some(id) = flag {
        return Ok(id);
    }
    if let Some(v) = node {
        return v.trim().parse().map_err(|_| format!("LOTSE_NODE={v}: a node id is a number"));
    }
    if let Some(v) = index {
        let index: u64 = v.trim().parse().map_err(|_| format!("LOTSE_NODE_INDEX={v}: a pod index is a number"))?;
        return index.checked_add(1).ok_or_else(|| format!("LOTSE_NODE_INDEX={v}: too large"));
    }
    Err("--node (or LOTSE_NODE, or LOTSE_NODE_INDEX) is missing".into())
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
        "turn" => match args.next().as_deref() {
            Some(what @ ("take" | "release")) => {
                let what = what.to_string();
                parse(args).and_then(|a| turn(&what, &a))
            }
            _ => Err(format!("lotse turn needs take or release\n\n{USAGE}")),
        },
        "upgrade" => match args.next().as_deref() {
            Some(what @ ("status" | "hold" | "resume" | "finalize" | "release")) => {
                let what = what.to_string();
                parse(args).and_then(|a| upgrade(&what, &a))
            }
            _ => Err(format!("lotse upgrade needs status, hold, resume, finalize or release\n\n{USAGE}")),
        },
        "probe" => match args.next().as_deref() {
            Some(path @ ("healthz" | "readyz" | "safe-to-stop")) => {
                let path = format!("/{path}");
                return match parse(args).and_then(|a| probe(&path, &a)) {
                    Ok(true) => ExitCode::SUCCESS,
                    Ok(false) => ExitCode::FAILURE,
                    Err(e) => {
                        eprintln!("lotse: {e}");
                        ExitCode::FAILURE
                    }
                };
            }
            _ => Err(format!("lotse probe needs healthz, readyz or safe-to-stop\n\n{USAGE}")),
        },
        "mcp" => {
            return match parse(args).and_then(|a| mcp_setup(&a)) {
                Err(e) => {
                    eprintln!("lotse: {e}");
                    ExitCode::from(2)
                }
                Ok(setup) => match mcp(setup) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => {
                        eprintln!("lotse: {e}");
                        ExitCode::FAILURE
                    }
                },
            };
        }
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
    if a.renew {
        if a.client.is_some() || a.revoke.is_some() || a.sign || a.renewal_ca {
            return Err("--renew goes alone, with --cert and --key".into());
        }
        return renew(&file, a);
    }
    if a.if_due {
        return Err("--if-due goes with --renew".into());
    }
    if a.renewal_ca {
        if a.client.is_some() || a.revoke.is_some() || a.sign {
            return Err("--renewal-ca goes alone".into());
        }
        let outcomes = certs::renewal_ca(&file, a.days.unwrap_or(certs::LEAF_DAYS)).map_err(|e| e.to_string())?;
        for o in outcomes {
            match o {
                Outcome::Wrote(p) => println!("wrote {}", p.display()),
                Outcome::Kept(p) => println!("kept  {}", p.display()),
            }
        }
        println!("\nCopy the renewal CA and its key to every node; each takes them up within 10 s.");
        return Ok(());
    }
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

/// `lotse certs --renew`: renews the client certificate in `--cert` over a
/// connection made with it (ADR 0123) and writes the new key and chain over
/// the old files.
fn renew(file: &ClusterFile, a: &Args) -> Result<(), String> {
    let (Some(cert), Some(key)) = (&a.cert, &a.key) else {
        return Err("--renew needs --cert <file> --key <file>: the certificate to renew and its key".into());
    };
    let client = lotse_node::admin_client(file, Some((cert.clone(), key.clone())), None).map_err(|e| e.to_string())?;
    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    let renewed = if a.if_due {
        let pem = std::fs::read(cert).map_err(|e| format!("{}: {e}", cert.display()))?;
        let Some(leaf) = lotse_node::pem_certificates(&pem).into_iter().next() else {
            return Err(format!("{}: no certificate", cert.display()));
        };
        match runtime.block_on(client.renew_certificate_if_due(&leaf)).map_err(|e| e.to_string())? {
            Some(r) => r,
            None => {
                let days = certs::expires_in(cert).map_err(|e| e.to_string())?.max(0) / 86_400;
                println!("not due: {} is valid for {days} more days, before its renewal window", cert.display());
                return Ok(());
            }
        }
    } else {
        runtime.block_on(client.renew_certificate()).map_err(|e| e.to_string())?
    };
    // The key first: a certificate without its key would not load, a key
    // without its certificate is noticed at once.
    certs::replace(key, &renewed.key_pem, true).map_err(|e| e.to_string())?;
    certs::replace(cert, &renewed.chain_pem, false).map_err(|e| e.to_string())?;
    let days = certs::expires_in(cert).map_err(|e| e.to_string())?.max(0) / 86_400;
    println!("renewed: wrote {} and {}; valid for {days} more days", cert.display(), key.display());
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
                let zone = a.zone.clone().or_else(|| entry.and_then(|n| n.zone.clone()));
                client.add_node_in_zone(id, &peer, &addr, zone.as_deref()).await.map_err(|e| e.to_string())?;
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

/// A time such as `90s`, `30m`, `2h`, or seconds.
fn duration(v: &str) -> Option<std::time::Duration> {
    let (n, unit) = match v.trim().char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, _)) => v.trim().split_at(i),
        None => (v.trim(), "s"),
    };
    let n: u64 = n.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(3600)?,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

/// A client certificate and key, and a SCRAM user and password.
type AdminCredentials = (Option<(PathBuf, PathBuf)>, Option<(String, String)>);

/// The admin a command acts as: a client certificate, or a SCRAM user.
fn admin(a: &Args) -> Result<AdminCredentials, String> {
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
    Ok((identity, scram))
}

/// The node a command on a node's machine is for: `--node`, else
/// `LOTSE_NODE`.
fn this_node(a: &Args) -> Result<u64, String> {
    node_and_join(a).map(|(id, _)| id)
}

fn turn(what: &str, a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let id = this_node(a)?;
    let node_id = u32::try_from(id).map_err(|_| format!("node {id}: too large an id"))?;
    let client = lotse_node::node_client(&file, id).map_err(|e| e.to_string())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(async {
        if what == "release" {
            client.release_turn(node_id).await.map_err(|e| e.to_string())?;
            println!("node {id}: holds no restart turn");
            return Ok(());
        }
        let wait = a.wait.unwrap_or(std::time::Duration::from_secs(30 * 60));
        let said = std::sync::Mutex::new(String::new());
        let waiting = async {
            loop {
                let why = match client.take_turn(node_id).await {
                    Ok(()) => {
                        println!("node {id}: holds the restart turn; restart it now");
                        return Ok(());
                    }
                    Err(lotse_client::ClientError::Broker {
                        code: lotse_proto::ErrorCode::TURN_UNAVAILABLE,
                        message,
                    }) => message.unwrap_or_else(|| "the turn is not free".into()),
                    Err(lotse_client::ClientError::Broker { code, message }) if !code.retriable() => {
                        return Err(message.unwrap_or_else(|| code.to_string()));
                    }
                    Err(e) => e.to_string(),
                };
                {
                    let mut said = said.lock().expect("said lock");
                    if why != *said {
                        eprintln!("node {id}: waiting for the restart turn: {why}");
                        *said = why;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        };
        match tokio::time::timeout(wait, waiting).await {
            Ok(done) => done,
            Err(_) => {
                let said = said.lock().expect("said lock");
                Err(format!("node {id} did not get the restart turn within {}s: {said}", wait.as_secs()))
            }
        }
    })
}

fn upgrade(what: &str, a: &Args) -> Result<(), String> {
    let file = load(a)?;
    let (identity, scram) = admin(a)?;
    let client = if what == "status" && identity.is_none() && scram.is_none() {
        lotse_node::node_client(&file, this_node(a)?)
    } else {
        lotse_node::admin_client(&file, identity, scram)
    }
    .map_err(|e| e.to_string())?;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    let show = lotse_server::version::display;
    rt.block_on(async {
        let change = match what {
            "status" => {
                let d = client.describe_upgrade().await.map_err(|e| e.to_string())?;
                let version = d.cluster_version.map_or_else(|| "none yet".to_string(), show);
                println!("cluster version: {version}");
                match d.turn {
                    Some(t) => println!("restart turn:    node {}", t.node_id),
                    None => println!("restart turn:    free"),
                }
                println!("turns:           {}", if d.held { "paused" } else { "running" });
                println!("{:<6} {:<12} cluster versions", "node", "release");
                for n in d.nodes {
                    let (release, range) = if n.release.is_empty() {
                        ("-".to_string(), "not reported".to_string())
                    } else {
                        (n.release, format!("{} to {}", show(n.oldest), show(n.newest)))
                    };
                    println!("{:<6} {release:<12} {range}", n.node_id);
                }
                return Ok(());
            }
            "hold" => lotse_proto::AlterUpgradeRequest { hold: Some(true), ..Default::default() },
            "resume" => lotse_proto::AlterUpgradeRequest { hold: Some(false), ..Default::default() },
            "release" => {
                let id = a.node.ok_or("--node is missing")?;
                lotse_proto::AlterUpgradeRequest { release_turn: Some(id), ..Default::default() }
            }
            _ => {
                let target = match &a.version {
                    Some(v) => lotse_server::version::parse(v).ok_or_else(|| format!("--version {v}: major.minor"))?,
                    None => {
                        let d = client.describe_upgrade().await.map_err(|e| e.to_string())?;
                        if d.nodes.iter().any(|n| n.release.is_empty()) {
                            return Err(
                                "not every node has reported its version yet; `lotse upgrade status` shows which"
                                    .to_string(),
                            );
                        }
                        d.nodes.iter().map(|n| n.newest).min().ok_or("the cluster lists no nodes")?
                    }
                };
                println!("raising the cluster version to {}", show(target));
                lotse_proto::AlterUpgradeRequest { cluster_version: Some(target), ..Default::default() }
            }
        };
        client.alter_upgrade(change).await.map_err(|e| e.to_string())?;
        println!("done");
        Ok(())
    })
}

fn probe(path: &str, a: &Args) -> Result<bool, String> {
    let file = load(a)?;
    let (status, body) = lotse_node::probe(&file, this_node(a)?, path)?;
    print!("{body}");
    Ok(status == 200)
}

/// What `lotse mcp` was asked for: where the cluster is and who to sign in
/// as.
struct McpSetup {
    reach: lotse_node::mcp::Reach,
    mode: McpMode,
}

/// Stdio as one principal, or HTTP with a credential per request.
enum McpMode {
    Stdio(lotse_node::mcp::SignIn),
    Http(lotse_node::mcp::http::Listen),
}

/// Reads the flags and the environment of `lotse mcp`; an error here is a
/// usage error, exit code 2.
fn mcp_setup(a: &Args) -> Result<McpSetup, String> {
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let mode = match &a.listen {
        None => {
            if a.tls_cert.is_some() || a.tls_key.is_some() || !a.allow_origin.is_empty() {
                return Err("--tls-cert, --tls-key and --allow-origin go with --listen".into());
            }
            McpMode::Stdio(lotse_node::mcp::SignIn::choose(
                env("LOTSE_TOKEN"),
                env("LOTSE_USER"),
                env("LOTSE_PASSWORD"),
                a.cert.clone(),
                a.key.clone(),
            )?)
        }
        Some(listen) => {
            let own = ["LOTSE_TOKEN", "LOTSE_USER", "LOTSE_PASSWORD"].iter().any(|v| env(v).is_some())
                || a.cert.is_some()
                || a.key.is_some();
            McpMode::Http(lotse_node::mcp::http::Listen::new(
                listen.clone(),
                a.tls_cert.clone(),
                a.tls_key.clone(),
                a.allow_origin.clone(),
                own,
            )?)
        }
    };
    let reach = match &a.bootstrap {
        Some(bootstrap) => {
            if a.config.is_some() {
                return Err("--config and --bootstrap exclude each other".into());
            }
            lotse_node::mcp::Reach {
                bootstrap: bootstrap.clone(),
                ca: a.ca.clone(),
                plaintext_on_loopback: a.plaintext_on_loopback,
            }
        }
        None => {
            let file = load(a).map_err(|e| format!("{e}; or give --bootstrap <host:port,...> --ca <file>"))?;
            lotse_node::mcp::Reach {
                bootstrap: file.nodes.iter().map(|n| n.client.clone()).collect(),
                ca: a.ca.clone().or_else(|| file.tls.ca.as_ref().map(PathBuf::from)),
                plaintext_on_loopback: a.plaintext_on_loopback,
            }
        }
    };
    Ok(McpSetup { reach, mode })
}

/// `lotse mcp`: signs in, then serves MCP on stdin and stdout; or serves
/// it over HTTP, signing in per request. Logs go to stderr, since stdout
/// carries only JSON-RPC.
fn mcp(setup: McpSetup) -> Result<(), String> {
    let filter = std::env::var("LOTSE_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    let sign_in = match setup.mode {
        McpMode::Stdio(s) => s,
        McpMode::Http(listen) => {
            return rt.block_on(lotse_node::mcp::http::serve_http(setup.reach, listen, shutdown()));
        }
    };
    rt.block_on(async {
        let client = std::sync::Arc::new(lotse_node::mcp::client(&setup.reach, sign_in)?);
        // Sign in before serving, so a wrong credential or an unreachable
        // cluster shows when the agent starts the server, not at its first
        // tool call.
        let me = client.who_am_i().await.map_err(|e| format!("signing in to the cluster: {e}"))?;
        tracing::info!(principal = %me.name, "signed in; serving MCP on stdio");
        lotse_node::mcp::serve_stdio(client).await
    })
}

#[cfg(test)]
mod tests {
    use super::node_id;

    #[test]
    fn the_flag_then_lotse_node_then_the_pod_index_name_the_node() {
        assert_eq!(node_id(Some(4), Some("5".into()), Some("6".into())), Ok(4));
        assert_eq!(node_id(None, Some(" 5 ".into()), Some("6".into())), Ok(5));
        assert_eq!(node_id(None, None, Some("0".into())), Ok(1), "pod index 0 is node 1");
        assert_eq!(node_id(None, None, Some("2".into())), Ok(3));
        assert!(node_id(None, None, Some("x".into())).unwrap_err().contains("LOTSE_NODE_INDEX=x"));
        assert!(node_id(None, None, Some(u64::MAX.to_string())).is_err());
        assert!(node_id(None, None, None).unwrap_err().contains("missing"));
    }
}
