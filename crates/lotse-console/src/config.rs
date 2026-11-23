//! `lotse-console.toml`.
//!
//! ```toml
//! listen = "127.0.0.1:8080"   # plain HTTP: loopback only
//!
//! # listen = "0.0.0.0:8443"   # anywhere else: HTTPS
//! # [https]
//! # cert = "/etc/lotse-console/console.pem"   # chain, PEM
//! # key = "/etc/lotse-console/console.key"
//!
//! [auth]
//! mode = "dev"          # or "oidc", see below
//! user = "pierre"
//! password = "…"        # the user's SCRAM password, if the clusters require authentication
//!
//! # [auth]
//! # mode = "oidc"
//! # issuer = "https://id.example.com/realms/ops"
//! # client_id = "lotse-console"
//! # redirect_url = "https://console.example.com/auth/callback"
//!
//! [metrics]
//! principal = "console-metrics"   # viewer role on the brokers, used only by the sampler
//! password = "…"                  # its SCRAM password, if the clusters require authentication
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
//! ca_file = "/etc/lotse/ca.pem"   # the cluster's CA; without it, plain TCP on loopback only
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
    /// The certificate and key to serve HTTPS with. Without them the
    /// console serves plain HTTP, and only on a loopback address.
    #[serde(default)]
    pub https: Option<HttpsConfig>,
    /// Where the console keeps what must outlive a restart (the session
    /// key); relative paths are relative to the config file. Default:
    /// `data` next to the config file.
    #[serde(default)]
    pub data_dir: Option<std::path::PathBuf>,
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

/// HTTPS (ADR 0137): TLS 1.3 with a certificate chain and its key, PEM.
/// Relative paths are relative to the config file. Read at start.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpsConfig {
    /// The certificate chain, the console's own certificate first.
    pub cert: std::path::PathBuf,
    /// Its private key.
    pub key: std::path::PathBuf,
}

/// Chart history settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Principal of the console's read-only metrics identity. The sampler
    /// uses it; nothing a user does ever does.
    pub principal: String,
    /// Its SCRAM password, where the clusters require authentication.
    #[serde(default)]
    pub password: Option<String>,
    /// Or: its client id at the issuer, to sign in with the OAuth client
    /// credentials grant.
    #[serde(default)]
    pub client_id: Option<String>,
    /// The file that holds that client's secret.
    #[serde(default)]
    pub client_secret_file: Option<std::path::PathBuf>,
    /// The issuer; by default the one users sign in at.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Scopes to ask for, space-separated, if the issuer wants any.
    #[serde(default)]
    pub scope: Option<String>,
    /// Seconds between samples.
    pub sample_secs: u64,
    /// Hours of history to keep in memory.
    pub retention_hours: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            principal: "console-metrics".into(),
            password: None,
            client_id: None,
            client_secret_file: None,
            issuer: None,
            scope: None,
            sample_secs: 10,
            retention_hours: 24,
        }
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
        /// The cluster's CA certificate (PEM), to reach it over TLS. Without
        /// it the console speaks plain TCP, which only works on loopback.
        #[serde(default)]
        ca_file: Option<std::path::PathBuf>,
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
        let mut cfg = Config::parse(&text)?;
        let base = path.parent().unwrap_or(Path::new("."));
        let data = cfg.data_dir.take().unwrap_or_else(|| "data".into());
        cfg.data_dir = Some(if data.is_absolute() { data } else { base.join(data) });
        if let Some(h) = &mut cfg.https {
            for f in [&mut h.cert, &mut h.key] {
                if f.is_relative() {
                    *f = base.join(&*f);
                }
            }
        }
        Ok(cfg)
    }

    /// The data directory: as configured, or `data` here.
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.data_dir.clone().unwrap_or_else(|| "data".into())
    }

    /// The session key's file: as configured, or `session.key` in the data
    /// directory.
    pub fn session_key_file(&self) -> Option<std::path::PathBuf> {
        match &self.auth {
            AuthConfig::Oidc(o) => {
                Some(o.session_key_file.clone().unwrap_or_else(|| self.data_dir().join("session.key")))
            }
            AuthConfig::Dev { .. } => None,
        }
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
        // No plaintext by accident: plain HTTP only where nobody else can
        // listen in (AGENTS.md, rule 7).
        if cfg.https.is_none() && !cfg.listen.ip().is_loopback() {
            return Err(ConfigError::Invalid(format!(
                "listen = \"{}\" is not a loopback address, and the console serves plain HTTP only on loopback. Add [https] with cert and key (PEM) to serve HTTPS there.",
                cfg.listen
            )));
        }
        if let AuthConfig::Oidc(o) = &cfg.auth {
            if !o.redirect_url.ends_with("/auth/callback") {
                return Err(ConfigError::Invalid(format!(
                    "auth.redirect_url must be this console's /auth/callback, e.g. \"https://console.example.com/auth/callback\"; it is \"{}\".",
                    o.redirect_url
                )));
            }
            if !o.redirect_url.starts_with("https://") && (cfg.https.is_some() || !cfg.listen.ip().is_loopback()) {
                return Err(ConfigError::Invalid(
                    "auth.redirect_url must use https:// unless the console serves plain HTTP on loopback: the session cookie would travel in the clear.".into(),
                ));
            }
        }
        let m = &cfg.metrics;
        if m.client_id.is_some() {
            if m.password.is_some() {
                return Err(ConfigError::Invalid("metrics: give either a password or a client_id, not both.".into()));
            }
            if m.client_secret_file.is_none() {
                return Err(ConfigError::Invalid(
                    "metrics.client_id needs metrics.client_secret_file, the file that holds the client's secret."
                        .into(),
                ));
            }
            if m.issuer.is_none() && !matches!(cfg.auth, AuthConfig::Oidc(_)) {
                return Err(ConfigError::Invalid(
                    "metrics.client_id needs an issuer: set metrics.issuer, or sign users in with auth.mode = \"oidc\".".into(),
                ));
            }
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
    fn the_packaged_example_is_a_valid_config() {
        let c = Config::parse(include_str!("../../../packaging/lotse-console.toml.example")).unwrap();
        assert!(c.https.is_some() && !c.listen.ip().is_loopback(), "HTTPS on every address");
        assert!(matches!(c.auth, AuthConfig::Oidc(_)));
        Config::parse(include_str!("../../../console/lotse-console.example.toml")).unwrap();
    }

    #[test]
    fn plain_http_only_on_loopback() {
        let public = OK.replace("127.0.0.1:8080", "0.0.0.0:8443");
        let e = Config::parse(&public).unwrap_err().to_string();
        assert!(e.contains("loopback") && e.contains("[https]"), "{e}");
        let v6 = OK.replace("127.0.0.1:8080", "[::]:8443");
        assert!(Config::parse(&v6).is_err(), "nor on every IPv6 address");
        assert!(Config::parse(&OK.replace("127.0.0.1:8080", "[::1]:8080")).is_ok(), "IPv6 loopback is loopback");

        let https = format!("{public}\n[https]\ncert = \"c.pem\"\nkey = \"/abs/c.key\"\n");
        let c = Config::parse(&https).unwrap();
        let h = c.https.as_ref().unwrap();
        assert_eq!((h.cert.as_path(), h.key.as_path()), (Path::new("c.pem"), Path::new("/abs/c.key")));
        assert!(Config::parse(&https.replace("key = ", "ky = ")).is_err(), "unknown keys are refused");

        // Relative paths are the config file's.
        let dir = std::env::temp_dir().join(format!("lotse-console-https-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("lotse-console.toml");
        std::fs::write(&file, &https).unwrap();
        let loaded = Config::load(&file).unwrap();
        let h = loaded.https.unwrap();
        assert_eq!((h.cert, h.key), (dir.join("c.pem"), "/abs/c.key".into()));
        let _ = std::fs::remove_dir_all(&dir);

        // With HTTPS, the sign-in comes back over HTTPS too.
        let oidc = https.replace(
            "mode = \"dev\"\n        user = \"pierre\"",
            "mode = \"oidc\"\nissuer = \"https://id\"\nclient_id = \"c\"\nredirect_url = \"REDIRECT\"",
        );
        assert!(Config::parse(&oidc.replace("REDIRECT", "https://console:8443/auth/callback")).is_ok());
        let e = Config::parse(&oidc.replace("REDIRECT", "http://console:8443/auth/callback")).unwrap_err();
        assert!(e.to_string().contains("https://"), "{e}");
    }

    #[test]
    fn checks_clusters_and_sign_in() {
        let native = OK.replace("kind = \"demo\"", "kind = \"native\"\nbootstrap = [\"b1:9000\"]");
        assert!(
            matches!(&Config::parse(&native).unwrap().clusters[0], ClusterConfig::Native { bootstrap, .. } if bootstrap.len() == 1)
        );
        let empty = OK.replace("kind = \"demo\"", "kind = \"native\"\nbootstrap = []");
        assert!(Config::parse(&empty).unwrap_err().to_string().contains("bootstrap"));
        let oidc = |redirect: &str| {
            OK.replace(
                "mode = \"dev\"\n        user = \"pierre\"",
                &format!("mode = \"oidc\"\nissuer = \"https://id\"\nclient_id = \"c\"\nredirect_url = \"{redirect}\""),
            )
        };
        let c = Config::parse(&oidc("https://console/auth/callback")).unwrap();
        assert!(
            matches!(&c.auth, AuthConfig::Oidc(o) if o.scopes.contains(&"openid".to_string()) && o.principal_claim == "sub")
        );
        assert!(Config::parse(&oidc("https://console/elsewhere")).unwrap_err().to_string().contains("/auth/callback"));
        // Plain HTTP only where nobody else can listen in.
        assert!(Config::parse(&oidc("http://127.0.0.1:8080/auth/callback")).is_ok());
        let public = oidc("http://console/auth/callback").replace("127.0.0.1:8080", "0.0.0.0:8080");
        assert!(Config::parse(&public).unwrap_err().to_string().contains("https"));
        assert!(matches!(&c.auth, AuthConfig::Oidc(o) if o.session_hours == 12 && o.session_key_file.is_none()));
        assert_eq!(c.session_key_file(), Some("data/session.key".into()), "the key's default place");

        // The sampler with client credentials: a secret, and an issuer.
        let sampler = |auth: &str, extra: &str| {
            format!(
                "{auth}\n[metrics]\nprincipal = \"m\"\nsample_secs = 10\nretention_hours = 1\nclient_id = \"m\"\n{extra}"
            )
        };
        let with_oidc = oidc("https://console/auth/callback");
        assert!(Config::parse(&sampler(&with_oidc, "client_secret_file = \"/s\"")).is_ok());
        assert!(Config::parse(&sampler(&with_oidc, "")).unwrap_err().to_string().contains("client_secret_file"));
        assert!(Config::parse(&sampler(OK, "client_secret_file = \"/s\"")).unwrap_err().to_string().contains("issuer"));
        assert!(Config::parse(&sampler(OK, "client_secret_file = \"/s\"\nissuer = \"https://id\"")).is_ok());
        let both = sampler(&with_oidc, "client_secret_file = \"/s\"\npassword = \"p\"");
        assert!(Config::parse(&both).unwrap_err().to_string().contains("not both"));
    }

    #[test]
    fn the_data_dir_is_relative_to_the_config_file() {
        let dir = std::env::temp_dir().join(format!("lotse-console-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("console.toml");
        std::fs::write(&path, OK).unwrap();
        assert_eq!(Config::load(&path).unwrap().data_dir(), dir.join("data"));
        std::fs::write(&path, format!("data_dir = \"state\"\n{OK}")).unwrap();
        assert_eq!(Config::load(&path).unwrap().data_dir(), dir.join("state"));
        std::fs::write(&path, format!("data_dir = \"/var/lib/lotse-console\"\n{OK}")).unwrap();
        assert_eq!(Config::load(&path).unwrap().data_dir(), std::path::PathBuf::from("/var/lib/lotse-console"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn refuses_duplicates_and_typos() {
        let dup = format!("{OK}\n[[clusters]]\nid = \"prod-hamburg-1\"\nkind = \"demo\"");
        assert!(Config::parse(&dup).unwrap_err().to_string().contains("twice"));
        assert!(Config::parse(&OK.replace("listen", "listn")).is_err());
    }
}
