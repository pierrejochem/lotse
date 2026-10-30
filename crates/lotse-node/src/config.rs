//! The cluster file: one TOML file, the same on every node, that names
//! every node, how they speak TLS, how clients authenticate and what
//! streams keep by default (ADR 0047). Streams, queues and exchanges are
//! not in it: clients create them at runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// The metadata group of every cluster run from a cluster file.
pub const METADATA_GROUP: u64 = 1;
/// The coordinator group (producer ids, transactions, consumer groups).
pub const COORDINATOR_GROUP: u64 = 2;

/// What is wrong with a cluster file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("{path}: {source}")]
    Read {
        /// The file.
        path: String,
        /// Why.
        source: std::io::Error,
    },
    /// The file is not valid TOML of the expected shape.
    #[error("{path}: {message}")]
    Parse {
        /// The file.
        path: String,
        /// What the parser said.
        message: String,
    },
    /// The file parses but describes no cluster that can run.
    #[error("{0}")]
    Invalid(String),
}

/// The whole file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterFile {
    /// Settings every node shares.
    pub cluster: Cluster,
    /// The nodes, each with its addresses.
    #[serde(rename = "node")]
    pub nodes: Vec<NodeEntry>,
    /// How nodes and clients speak TLS.
    pub tls: Tls,
    /// What streams keep unless they say otherwise.
    #[serde(default)]
    pub retention: Retention,
    /// How clients authenticate.
    #[serde(default)]
    pub auth: Auth,
}

/// Settings every node shares.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Where a node keeps its data; `{id}` becomes the node's id, so
    /// several nodes can share a machine.
    pub data_dir: String,
    /// The Raft tick, ms (default 10).
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    /// Log segment size, bytes (default 128 MiB). Retention deletes whole
    /// segments.
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,
    /// Consumer groups: a member without a heartbeat for this long is
    /// removed, ms.
    #[serde(default)]
    pub group_session_timeout_ms: Option<u64>,
    /// Consumer groups: how often members send heartbeats, ms.
    #[serde(default)]
    pub group_heartbeat_ms: Option<u64>,
}

fn default_tick_ms() -> u64 {
    10
}

fn default_segment_bytes() -> u64 {
    lotse_log::LogConfig::default().segment_bytes
}

/// One node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeEntry {
    /// Its id, 1 or more, unique.
    pub id: u64,
    /// The address other nodes dial (`host:port`).
    pub peer: String,
    /// The address clients dial (`host:port`).
    pub client: String,
    /// Where it listens for nodes, if not on `peer` (such as `0.0.0.0:7000`).
    #[serde(default)]
    pub peer_bind: Option<String>,
    /// Where it listens for clients, if not on `client`.
    #[serde(default)]
    pub client_bind: Option<String>,
}

/// TLS: the CA and each node's certificate and key. `{id}` in a path
/// becomes the node's id.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// The CA certificate, PEM.
    #[serde(default)]
    pub ca: Option<String>,
    /// The node's certificate chain, PEM.
    #[serde(default)]
    pub cert: Option<String>,
    /// The node's private key, PEM.
    #[serde(default)]
    pub key: Option<String>,
    /// No TLS: allowed only when every address is on loopback, for trying
    /// a cluster on one machine.
    #[serde(default)]
    pub plaintext_on_loopback: bool,
}

/// What streams keep: `-1` for no limit, a key left out for the default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    /// Records older than this go, ms (default 7 days).
    #[serde(default)]
    pub ms: Option<i64>,
    /// Each partition keeps at least this much, bytes (default no limit).
    #[serde(default)]
    pub bytes: Option<i64>,
}

/// How clients authenticate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    /// Clients must authenticate (default on).
    #[serde(default = "yes")]
    pub required: bool,
    /// A client certificate names its principal (default on).
    #[serde(default = "yes")]
    pub mtls: bool,
    /// SASL SCRAM-SHA-512 with users set at runtime (default on).
    #[serde(default = "yes")]
    pub scram: bool,
    /// Principals that are cluster admins from the start.
    #[serde(default)]
    pub admins: Vec<String>,
    /// Tenants besides `default` that exist from the start.
    #[serde(default)]
    pub tenants: Vec<String>,
    /// Sessions end after this at the latest, seconds.
    #[serde(default)]
    pub max_session_s: Option<u64>,
    /// SASL OAUTHBEARER with tokens from an identity provider.
    #[serde(default)]
    pub oidc: Option<Oidc>,
}

impl Default for Auth {
    fn default() -> Self {
        Auth {
            required: true,
            mtls: true,
            scram: true,
            admins: Vec::new(),
            tenants: Vec::new(),
            max_session_s: None,
            oidc: None,
        }
    }
}

fn yes() -> bool {
    true
}

/// Tokens from an identity provider.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oidc {
    /// The issuer, as in the tokens' `iss`.
    pub issuer: String,
    /// The audience tokens must be for.
    pub audience: String,
    /// The claim naming the principal (default `sub`).
    #[serde(default)]
    pub principal_claim: Option<String>,
    /// The claim listing the user's groups (default `groups`).
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// The key set's URL, if not from the issuer's discovery document.
    #[serde(default)]
    pub jwks_url: Option<String>,
}

impl ClusterFile {
    /// Reads and checks the file at `path`.
    pub fn load(path: &Path) -> Result<ClusterFile, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Read { path: path.display().to_string(), source })?;
        ClusterFile::parse(&text, &path.display().to_string())
    }

    /// Parses and checks `text`; `name` says where it came from.
    pub fn parse(text: &str, name: &str) -> Result<ClusterFile, ConfigError> {
        let file: ClusterFile =
            toml::from_str(text).map_err(|e| ConfigError::Parse { path: name.into(), message: e.to_string() })?;
        file.check()?;
        Ok(file)
    }

    fn check(&self) -> Result<(), ConfigError> {
        let invalid = |m: String| Err(ConfigError::Invalid(m));
        if self.nodes.is_empty() {
            return invalid("the file names no [[node]]".into());
        }
        let mut ids = BTreeSet::new();
        let mut addrs = BTreeSet::new();
        for n in &self.nodes {
            if n.id == 0 {
                return invalid("node ids start at 1".into());
            }
            if !ids.insert(n.id) {
                return invalid(format!("node {} is named twice", n.id));
            }
            for a in [&n.peer, &n.client] {
                if host_of(a).is_none() {
                    return invalid(format!("node {}: {a} is not host:port", n.id));
                }
                if !addrs.insert(a.as_str()) {
                    return invalid(format!("node {}: the address {a} is used twice", n.id));
                }
            }
        }
        if self.cluster.tick_ms == 0 {
            return invalid("cluster.tick_ms must be at least 1".into());
        }
        if self.cluster.segment_bytes < 4096 {
            return invalid("cluster.segment_bytes must be at least 4096".into());
        }
        if self.tls.plaintext_on_loopback {
            if self.tls.ca.is_some() || self.tls.cert.is_some() || self.tls.key.is_some() {
                return invalid("[tls] names certificates and plaintext_on_loopback; choose one".into());
            }
            for n in &self.nodes {
                for a in [&n.peer, &n.client].into_iter().chain(n.peer_bind.iter()).chain(n.client_bind.iter()) {
                    if !is_loopback(a) {
                        return invalid(format!(
                            "node {}: {a} is not on loopback; without TLS every address must be",
                            n.id
                        ));
                    }
                }
            }
            if self.auth.required {
                return invalid(
                    "auth.required needs TLS: nodes prove who they are by their certificates; \
                     set auth.required = false to try a cluster without TLS"
                        .into(),
                );
            }
        } else if self.tls.ca.is_none() || self.tls.cert.is_none() || self.tls.key.is_none() {
            return invalid(
                "[tls] needs ca, cert and key (make them with `lotse certs`), or plaintext_on_loopback = true".into(),
            );
        }
        for (key, v) in [("retention.ms", self.retention.ms), ("retention.bytes", self.retention.bytes)] {
            if v.is_some_and(|v| v != -1 && v <= 0) {
                return invalid(format!("{key} must be positive, or -1 for no limit"));
            }
        }
        if self.auth.required && self.auth.admins.is_empty() {
            return invalid("auth.admins is empty: nobody could create tenants or grant roles".into());
        }
        if self.auth.required && !self.auth.mtls && !self.auth.scram && self.auth.oidc.is_none() {
            return invalid("auth.required with mtls, scram and oidc all off: nobody could sign in".into());
        }
        Ok(())
    }

    /// The node with id `id`.
    pub fn node(&self, id: u64) -> Result<&NodeEntry, ConfigError> {
        self.nodes.iter().find(|n| n.id == id).ok_or_else(|| {
            let ids: Vec<String> = self.nodes.iter().map(|n| n.id.to_string()).collect();
            ConfigError::Invalid(format!("there is no node {id} in the file; it names {}", ids.join(", ")))
        })
    }

    /// Where node `id` keeps its data.
    pub fn data_dir(&self, id: u64) -> PathBuf {
        PathBuf::from(with_id(&self.cluster.data_dir, id))
    }

    /// The node configuration for node `id`.
    pub fn node_config(&self, id: u64) -> Result<lotse_server::NodeConfig, ConfigError> {
        let me = self.node(id)?;
        let peers: BTreeMap<u64, String> = self.nodes.iter().map(|n| (n.id, n.peer.clone())).collect();
        let clients: BTreeMap<u64, String> = self.nodes.iter().map(|n| (n.id, n.client.clone())).collect();
        let pick = |v: Option<i64>, default: Option<u64>| match v {
            None => default,
            Some(-1) => None,
            Some(v) => Some(v as u64),
        };
        let defaults = lotse_server::RetentionConfig::default();
        let a = &self.auth;
        let auth = lotse_server::AuthConfig {
            required: a.required,
            mtls: a.mtls,
            scram: a.scram,
            oidc: a.oidc.as_ref().map(|o| {
                let mut c = lotse_server::OidcConfig::new(&o.issuer, &o.audience);
                if let Some(p) = &o.principal_claim {
                    c.principal_claim = p.clone();
                }
                if let Some(g) = &o.groups_claim {
                    c.groups_claim = g.clone();
                }
                c.jwks_url = o.jwks_url.clone();
                c
            }),
            max_session: a.max_session_s.map_or(lotse_server::DEFAULT_MAX_SESSION, Duration::from_secs),
            users: BTreeMap::new(),
            admins: a.admins.clone(),
            tenants: a.tenants.clone(),
            resource_tenants: BTreeMap::new(),
        };
        Ok(lotse_server::NodeConfig {
            id,
            listen: me.peer_bind.clone().unwrap_or_else(|| me.peer.clone()),
            peers,
            groups: vec![METADATA_GROUP, COORDINATOR_GROUP],
            tick: Duration::from_millis(self.cluster.tick_ms),
            pre_vote: true,
            log: lotse_log::LogConfig { segment_bytes: self.cluster.segment_bytes, ..lotse_log::LogConfig::default() },
            client_listen: Some(me.client_bind.clone().unwrap_or_else(|| me.client.clone())),
            client_addrs: clients,
            streams: Vec::new(),
            queues: Vec::new(),
            exchanges: Vec::new(),
            group_session_timeout: self
                .cluster
                .group_session_timeout_ms
                .map_or(lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT, Duration::from_millis),
            group_heartbeat_interval: self
                .cluster
                .group_heartbeat_ms
                .map_or(lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL, Duration::from_millis),
            metadata: Some(METADATA_GROUP),
            coordinator: Some(COORDINATOR_GROUP),
            max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
            auth,
            retention: lotse_server::RetentionConfig {
                ms: pick(self.retention.ms, defaults.ms),
                bytes: pick(self.retention.bytes, defaults.bytes),
            },
            join: false,
        })
    }

    /// Node `id`'s CA, certificate and key paths; `None` without TLS.
    pub fn tls_paths(&self, id: u64) -> Option<(PathBuf, PathBuf, PathBuf)> {
        let t = &self.tls;
        Some((
            PathBuf::from(t.ca.as_deref()?),
            PathBuf::from(with_id(t.cert.as_deref()?, id)),
            PathBuf::from(with_id(t.key.as_deref()?, id)),
        ))
    }
}

/// `template` with `{id}` replaced by `id`.
pub fn with_id(template: &str, id: u64) -> String {
    template.replace("{id}", &id.to_string())
}

/// The host part of `host:port` (`[::1]:7000` included).
pub fn host_of(addr: &str) -> Option<&str> {
    let (host, port) = addr.rsplit_once(':')?;
    port.parse::<u16>().ok()?;
    let host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    (!host.is_empty()).then_some(host)
}

fn is_loopback(addr: &str) -> bool {
    match host_of(addr) {
        Some("localhost") => true,
        Some(h) => h.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

#[cfg(test)]
mod tests;
