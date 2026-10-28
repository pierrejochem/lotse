//! `lotse-console.toml`.
//!
//! ```toml
//! listen = "127.0.0.1:8080"
//!
//! [auth]
//! mode = "dev"          # "oidc" once implemented
//! user = "pierre"
//!
//! [metrics]
//! principal = "console-metrics"   # read-only broker role, used only by the sampler
//! sample_secs = 10
//! retention_hours = 24
//!
//! [live]
//! interval_ms = 2000
//!
//! [[clusters]]
//! id = "prod-hamburg-1"
//! kind = "native"
//! bootstrap = ["broker-1:9000", "broker-2:9000"]
//!
//! [[clusters]]
//! id = "demo"
//! kind = "demo"
//! ```

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::auth::AuthConfig;

/// The whole config file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to serve the UI and API on.
    pub listen: SocketAddr,
    /// How users sign in.
    pub auth: AuthConfig,
    /// Background sampling for chart history.
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Live updates.
    #[serde(default)]
    pub live: LiveConfig,
    /// Clusters this console manages; at least one.
    pub clusters: Vec<ClusterConfig>,
}

/// Chart history settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Principal of the console's read-only metrics identity. The sampler
    /// uses it; nothing a user does ever does.
    pub principal: String,
    /// Seconds between samples.
    pub sample_secs: u64,
    /// Hours of history to keep in memory.
    pub retention_hours: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig { principal: "console-metrics".into(), sample_secs: 10, retention_hours: 24 }
    }
}

impl MetricsConfig {
    /// Sample interval.
    pub fn step(&self) -> Duration {
        Duration::from_secs(self.sample_secs.max(1))
    }
    /// History kept.
    pub fn retention(&self) -> Duration {
        Duration::from_secs(self.retention_hours.max(1) * 3600)
    }
}

/// Live update settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveConfig {
    /// Milliseconds between polls of a watched topic.
    pub interval_ms: u64,
}

impl Default for LiveConfig {
    fn default() -> Self {
        LiveConfig { interval_ms: 2000 }
    }
}

/// One managed cluster.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterConfig {
    /// The built-in sample cluster.
    Demo {
        /// Cluster ID.
        id: String,
    },
    /// A real cluster over the native protocol.
    Native {
        /// Cluster ID.
        id: String,
        /// Client addresses of some of its nodes (`host:port`); the others
        /// are learnt from them.
        bootstrap: Vec<String>,
    },
}

impl ClusterConfig {
    /// Cluster ID.
    pub fn id(&self) -> &str {
        match self {
            ClusterConfig::Demo { id } | ClusterConfig::Native { id, .. } => id,
        }
    }
}

/// A config that cannot be used, with a message saying what to fix.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// File.
        path: String,
        /// Cause.
        source: std::io::Error,
    },
    /// The file is not valid TOML or has unknown keys.
    #[error("{0}")]
    Parse(#[from] toml::de::Error),
    /// The values contradict each other or are not supported yet.
    #[error("{0}")]
    Invalid(String),
}

impl Config {
    /// Reads and checks a config file.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Read { path: path.display().to_string(), source })?;
        Config::parse(&text)
    }

    /// Parses and checks config text.
    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        let cfg: Config = toml::from_str(text)?;
        if cfg.clusters.is_empty() {
            return Err(ConfigError::Invalid("Add at least one [[clusters]] entry.".into()));
        }
        let mut ids: Vec<&str> = cfg.clusters.iter().map(ClusterConfig::id).collect();
        ids.sort_unstable();
        if let Some(w) = ids.windows(2).find(|w| w[0] == w[1]) {
            return Err(ConfigError::Invalid(format!("Cluster ID \"{}\" is used twice.", w[0])));
        }
        if let Some(c) =
            cfg.clusters.iter().find(|c| matches!(c, ClusterConfig::Native { bootstrap, .. } if bootstrap.is_empty()))
        {
            return Err(ConfigError::Invalid(format!(
                "Cluster \"{}\" has no bootstrap addresses. Add the client address of at least one node, e.g. bootstrap = [\"broker-1:9000\"].",
                c.id()
            )));
        }
        if matches!(cfg.auth, AuthConfig::Oidc { .. }) {
            return Err(ConfigError::Invalid(
                "auth.mode = \"oidc\" is not implemented yet. Use mode = \"dev\" on a machine only you can reach."
                    .into(),
            ));
        }
        if !cfg.listen.ip().is_loopback() && matches!(cfg.auth, AuthConfig::Dev { .. }) {
            tracing::warn!(listen = %cfg.listen, "dev auth on a non-loopback address: anyone who can reach it acts as the dev user");
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"
        listen = "127.0.0.1:8080"
        [auth]
        mode = "dev"
        user = "pierre"
        [[clusters]]
        id = "prod-hamburg-1"
        kind = "demo"
    "#;

    #[test]
    fn minimal_config_gets_defaults() {
        let c = Config::parse(OK).unwrap();
        assert_eq!(c.metrics.sample_secs, 10);
        assert_eq!(c.metrics.retention(), Duration::from_secs(86_400));
        assert_eq!(c.live.interval_ms, 2000);
        assert_eq!(c.clusters[0].id(), "prod-hamburg-1");
    }

    #[test]
    fn refuses_what_does_not_work_yet() {
        let native = OK.replace("kind = \"demo\"", "kind = \"native\"\nbootstrap = [\"b1:9000\"]");
        assert!(
            matches!(&Config::parse(&native).unwrap().clusters[0], ClusterConfig::Native { bootstrap, .. } if bootstrap.len() == 1)
        );
        let empty = OK.replace("kind = \"demo\"", "kind = \"native\"\nbootstrap = []");
        assert!(Config::parse(&empty).unwrap_err().to_string().contains("bootstrap"));
        let oidc = OK.replace(
            "mode = \"dev\"\n        user = \"pierre\"",
            "mode = \"oidc\"\nissuer = \"https://id\"\nclient_id = \"c\"",
        );
        assert!(Config::parse(&oidc).unwrap_err().to_string().contains("oidc"));
    }

    #[test]
    fn refuses_duplicates_and_typos() {
        let dup = format!("{OK}\n[[clusters]]\nid = \"prod-hamburg-1\"\nkind = \"demo\"");
        assert!(Config::parse(&dup).unwrap_err().to_string().contains("twice"));
        assert!(Config::parse(&OK.replace("listen", "listn")).is_err());
    }
}
