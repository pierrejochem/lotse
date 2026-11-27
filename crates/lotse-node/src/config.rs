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
    /// How compacted streams are compacted.
    #[serde(default)]
    pub compaction: Compaction,
    /// How partitions keep idempotent producers.
    #[serde(default)]
    pub producers: Producers,
    /// How much free space nodes keep on their data volume.
    #[serde(default)]
    pub disk: DiskSection,
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
    /// A runtime stream's or queue's active segment closes once it is this
    /// old, ms, unless the resource sets `segment.ms` (default an hour,
    /// at least a minute; ADR 0146).
    #[serde(default = "default_segment_ms")]
    pub segment_ms: u64,
    /// Consumer groups: a member without a heartbeat for this long is
    /// removed, ms.
    #[serde(default)]
    pub group_session_timeout_ms: Option<u64>,
    /// Consumer groups: how often members send heartbeats, ms.
    #[serde(default)]
    pub group_heartbeat_ms: Option<u64>,
    /// How long a follower may go without having applied what its leader
    /// said was committed and still serve `Fetch` and `ListOffsets`, ms
    /// (default 10,000; ADR 0116), like Kafka's `replica.lag.time.max.ms`.
    #[serde(default)]
    pub follower_max_lag_ms: Option<u64>,
    /// A group that hears no leader for this long, up to twice as long,
    /// elects another, ms (default 1,000; ADR 0144). Shorter fails over
    /// sooner, but a node whose disk or CPU stalls that long loses its
    /// groups' leadership.
    #[serde(default)]
    pub election_timeout_ms: Option<u64>,
    /// How often a leader sends heartbeats, ms (default 100; ADR 0144). At
    /// most half the election timeout.
    #[serde(default)]
    pub heartbeat_ms: Option<u64>,
}

impl Cluster {
    /// The election timeout, ms, as set or by default (ADR 0144).
    pub fn election_timeout_ms(&self) -> u64 {
        self.election_timeout_ms.unwrap_or(lotse_server::DEFAULT_ELECTION_TIMEOUT.as_millis() as u64)
    }

    /// The heartbeat interval, ms, as set or by default (ADR 0144).
    pub fn heartbeat_ms(&self) -> u64 {
        self.heartbeat_ms.unwrap_or(lotse_server::DEFAULT_HEARTBEAT_INTERVAL.as_millis() as u64)
    }
}

fn default_tick_ms() -> u64 {
    10
}

fn default_segment_ms() -> u64 {
    lotse_server::DEFAULT_SEGMENT_MS
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
    /// Its zone (a data centre or availability zone): clients that name
    /// the same zone fetch from its replicas (ADR 0104).
    #[serde(default)]
    pub zone: Option<String>,
    /// Where it serves `/metrics`, `/healthz`, `/readyz` and
    /// `/safe-to-stop` over HTTP (ADR 0117), if anywhere: plain on a
    /// loopback address, TLS with the node's certificate elsewhere.
    #[serde(default)]
    pub metrics: Option<String>,
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
    /// The CA's certificate revocation list, PEM (ADR 0060): a certificate
    /// it names is refused in every handshake. `lotse certs --revoke`
    /// writes it; the same file goes to every node.
    #[serde(default)]
    pub crl: Option<String>,
    /// The renewal CA's certificate, PEM (ADR 0123): an intermediate under
    /// the CA that `lotse certs --renewal-ca` makes. With it, the metadata
    /// leader renews client certificates over connections made with them,
    /// and a certificate it issued works only while the cluster records it.
    /// The same file goes to every node.
    #[serde(default)]
    pub renewal_ca: Option<String>,
    /// The renewal CA's private key, PEM; default: `renewal_ca` with
    /// `.key`. Every node holds it.
    #[serde(default)]
    pub renewal_ca_key: Option<String>,
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

/// How compacted streams are compacted (ADR 0098).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compaction {
    /// Bytes the map of keys of the node's one compaction sweep may take
    /// (default 64 MiB, about 1.4 million keys).
    #[serde(default)]
    pub map_bytes: Option<u64>,
}

/// How much free space nodes keep on their data volume (ADR 0122).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskSection {
    /// Below this much free space on its data volume a node takes no new
    /// data in the groups it votes in: bytes (`2147483648`, `"2GiB"`,
    /// `"500MB"`), a share of the volume (`"5%"`), or `0` for none. Default:
    /// the larger of 1 GiB and 5 %.
    #[serde(default)]
    pub free_limit: Option<SizeOrShare>,
    /// How often a node reads its data volume's free space, ms (default
    /// 1,000; ADR 0124). A node that crosses the limit tells the metadata
    /// group at its next read, so this is most of the delay before the
    /// leaders of its groups refuse new data.
    #[serde(default)]
    pub check_interval_ms: Option<u64>,
}

/// A number of bytes, or text with a unit or a percent sign.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum SizeOrShare {
    /// Bytes.
    Bytes(u64),
    /// `"2GiB"`, `"500MB"`, `"5%"`.
    Text(String),
}

impl SizeOrShare {
    /// The limit it names.
    pub fn free_limit(&self) -> Result<lotse_server::FreeLimit, String> {
        match self {
            SizeOrShare::Bytes(b) => Ok(lotse_server::FreeLimit { bytes: *b, permille: 0 }),
            SizeOrShare::Text(t) => parse_free_limit(t),
        }
    }
}

/// `"2GiB"`, `"500 MB"`, `"1073741824"` or `"5%"`, `"2.5%"`.
fn parse_free_limit(text: &str) -> Result<lotse_server::FreeLimit, String> {
    let t = text.trim();
    let bad = || format!("disk.free_limit: {text:?} is neither bytes (\"2GiB\", \"500MB\") nor a share (\"5%\")");
    if let Some(share) = t.strip_suffix('%') {
        let share = share.trim();
        let (whole, tenth) = match share.split_once('.') {
            Some((w, f)) if f.len() == 1 => (w, f),
            Some(_) => return Err(format!("disk.free_limit: {text:?} has more than one decimal")),
            None => (share, "0"),
        };
        let (Ok(whole), Ok(tenth)) = (whole.parse::<u32>(), tenth.parse::<u32>()) else { return Err(bad()) };
        let permille = whole.checked_mul(10).and_then(|p| p.checked_add(tenth)).filter(|p| *p <= 1000);
        let Some(permille) = permille else { return Err(format!("disk.free_limit: {text:?} is more than 100 %")) };
        return Ok(lotse_server::FreeLimit { bytes: 0, permille });
    }
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (number, unit) = t.split_at(split);
    let number: u64 = number.parse().map_err(|_| bad())?;
    let factor: u64 = match unit.trim() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err(bad()),
    };
    let bytes = number.checked_mul(factor).ok_or_else(|| format!("disk.free_limit: {text:?} is too large"))?;
    Ok(lotse_server::FreeLimit { bytes, permille: 0 })
}

/// How partitions keep idempotent producers (ADR 0110).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Producers {
    /// Producers each partition admits (default 10,000); while it holds this
    /// many, a new one is refused until one is forgotten.
    #[serde(default)]
    pub max: Option<usize>,
    /// How long a producer may write nothing to a partition before it is
    /// forgotten (default 86,400,000 ms, a day).
    #[serde(default)]
    pub expiration_ms: Option<u64>,
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
    /// Signing in with an access token, `LOTSE-TOKEN` (default on).
    #[serde(default = "yes")]
    pub tokens: bool,
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
            tokens: true,
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
            for a in [&n.peer, &n.client].into_iter().chain(n.metrics.iter()) {
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
        if self.cluster.segment_ms < 60_000 {
            return invalid("cluster.segment_ms must be at least 60000 (a minute)".into());
        }
        if self.cluster.segment_bytes < 4096 {
            return invalid("cluster.segment_bytes must be at least 4096".into());
        }
        if let Some(l) = &self.disk.free_limit {
            l.free_limit().map_err(ConfigError::Invalid)?;
        }
        if self.tls.renewal_ca_key.is_some() && self.tls.renewal_ca.is_none() {
            return invalid("[tls] names renewal_ca_key without renewal_ca".into());
        }
        if self.producers.max == Some(0) {
            return invalid("producers.max must be at least 1".into());
        }
        let election = self.cluster.election_timeout_ms();
        let heartbeat = self.cluster.heartbeat_ms();
        if heartbeat < self.cluster.tick_ms {
            return invalid(format!(
                "cluster.heartbeat_ms ({heartbeat}) must be at least cluster.tick_ms ({})",
                self.cluster.tick_ms
            ));
        }
        if election < 2 * heartbeat {
            return invalid(format!(
                "cluster.election_timeout_ms ({election}) must be at least twice cluster.heartbeat_ms ({heartbeat}): \
                 followers would elect between two heartbeats"
            ));
        }
        if self.cluster.follower_max_lag_ms == Some(0) {
            return invalid("cluster.follower_max_lag_ms must be at least 1".into());
        }
        if let Some(ms) = self.disk.check_interval_ms
            && !(10..=60_000).contains(&ms)
        {
            return invalid(format!("disk.check_interval_ms is {ms}: give 10 to 60000 (10 ms to a minute)"));
        }
        if self.producers.expiration_ms == Some(0) {
            return invalid("producers.expiration_ms must be at least 1".into());
        }
        if self.tls.plaintext_on_loopback {
            if self.tls.ca.is_some()
                || self.tls.cert.is_some()
                || self.tls.key.is_some()
                || self.tls.crl.is_some()
                || self.tls.renewal_ca.is_some()
            {
                return invalid("[tls] names certificates and plaintext_on_loopback; choose one".into());
            }
            for n in &self.nodes {
                let binds = n.peer_bind.iter().chain(n.client_bind.iter()).chain(n.metrics.iter());
                for a in [&n.peer, &n.client].into_iter().chain(binds) {
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
            tokens: a.tokens,
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
            // Filled from `[tls] ca` when the node runs.
            client_cas: lotse_server::ClientCas::default(),
            renewal: lotse_server::RenewalSlot::default(),
        };
        Ok(lotse_server::NodeConfig {
            id,
            listen: me.peer_bind.clone().unwrap_or_else(|| me.peer.clone()),
            peers,
            groups: vec![METADATA_GROUP, COORDINATOR_GROUP],
            tick: Duration::from_millis(self.cluster.tick_ms),
            election_timeout: Duration::from_millis(self.cluster.election_timeout_ms()),
            heartbeat_interval: Duration::from_millis(self.cluster.heartbeat_ms()),
            pre_vote: true,
            log: lotse_log::LogConfig { segment_bytes: self.cluster.segment_bytes, ..lotse_log::LogConfig::default() },
            client_listen: Some(me.client_bind.clone().unwrap_or_else(|| me.client.clone())),
            client_addrs: clients,
            zones: self.nodes.iter().filter_map(|n| Some((n.id, n.zone.clone()?))).collect(),
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
            max_producers: self.producers.max.unwrap_or(lotse_server::DEFAULT_MAX_PRODUCERS),
            producer_expiration: self
                .producers
                .expiration_ms
                .map_or(lotse_server::DEFAULT_PRODUCER_EXPIRATION, Duration::from_millis),
            leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
            follower_max_lag: self
                .cluster
                .follower_max_lag_ms
                .map_or(lotse_server::DEFAULT_FOLLOWER_MAX_LAG, Duration::from_millis),
            disk_free_limit: match &self.disk.free_limit {
                Some(l) => l.free_limit().map_err(ConfigError::Invalid)?,
                None => lotse_server::FreeLimit::default(),
            },
            disk_check_interval: self
                .disk
                .check_interval_ms
                .map_or(lotse_server::DEFAULT_DISK_CHECK_INTERVAL, Duration::from_millis),
            versions: lotse_server::version::Versions::default(),
            auth,
            retention: lotse_server::RetentionConfig {
                ms: pick(self.retention.ms, defaults.ms),
                bytes: pick(self.retention.bytes, defaults.bytes),
                compaction_map_bytes: self.compaction.map_bytes.unwrap_or(lotse_server::DEFAULT_COMPACTION_MAP_BYTES),
                segment_ms: self.cluster.segment_ms,
            },
            join: false,
        })
    }

    /// The revocation list's path, if `[tls]` names one.
    pub fn crl_path(&self) -> Option<PathBuf> {
        self.tls.crl.as_deref().map(PathBuf::from)
    }

    /// The renewal CA's certificate and key paths (ADR 0123), if `[tls]`
    /// names one.
    pub fn renewal_paths(&self) -> Option<(PathBuf, PathBuf)> {
        let cert = PathBuf::from(self.tls.renewal_ca.as_deref()?);
        let key = match &self.tls.renewal_ca_key {
            Some(k) => PathBuf::from(k),
            None => cert.with_extension("key"),
        };
        Some((cert, key))
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
