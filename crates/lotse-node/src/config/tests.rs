use super::*;

const THREE: &str = r#"
[cluster]
data_dir = "/var/lib/lotse/node-{id}"

[[node]]
id = 1
peer = "10.0.0.1:7000"
client = "10.0.0.1:9000"
peer_bind = "0.0.0.0:7000"

[[node]]
id = 2
peer = "10.0.0.2:7000"
client = "broker-2.example:9000"

[[node]]
id = 3
peer = "[fd00::3]:7000"
client = "[fd00::3]:9000"

[tls]
ca = "/etc/lotse/ca.pem"
cert = "/etc/lotse/node-{id}.pem"
key = "/etc/lotse/node-{id}.key"

[retention]
bytes = 1073741824

[auth]
admins = ["alice"]
tenants = ["shop"]
"#;

fn err(text: &str) -> String {
    ClusterFile::parse(text, "test.toml").unwrap_err().to_string()
}

#[test]
fn a_cluster_file_becomes_each_nodes_configuration() {
    let f = ClusterFile::parse(THREE, "test.toml").unwrap();
    let c = f.node_config(1).unwrap();
    assert_eq!(c.listen, "0.0.0.0:7000", "it listens where peer_bind says");
    assert_eq!(c.peers[&1], "10.0.0.1:7000", "and is dialled at its peer address");
    assert_eq!(c.peers.len(), 3);
    assert_eq!(c.client_listen.as_deref(), Some("10.0.0.1:9000"));
    assert_eq!(c.client_addrs[&2], "broker-2.example:9000");
    assert_eq!(c.groups, vec![METADATA_GROUP, COORDINATOR_GROUP]);
    assert_eq!((c.metadata, c.coordinator), (Some(METADATA_GROUP), Some(COORDINATOR_GROUP)));
    assert!(c.streams.is_empty() && c.queues.is_empty() && c.exchanges.is_empty(), "resources come at runtime");
    assert_eq!(c.tick, Duration::from_millis(10));
    assert_eq!(c.retention.ms, lotse_server::RetentionConfig::default().ms, "the default age");
    assert_eq!(c.retention.bytes, Some(1 << 30));
    assert!(c.auth.required && c.auth.mtls && c.auth.scram, "secure unless the file says otherwise");
    assert_eq!(c.auth.admins, vec!["alice".to_string()]);
    assert_eq!(f.data_dir(2), PathBuf::from("/var/lib/lotse/node-2"));
    let (ca, cert, key) = f.tls_paths(3).unwrap();
    assert_eq!(
        (ca.to_str().unwrap(), cert.to_str().unwrap(), key.to_str().unwrap()),
        ("/etc/lotse/ca.pem", "/etc/lotse/node-3.pem", "/etc/lotse/node-3.key")
    );
    assert!(f.node_config(4).unwrap_err().to_string().contains("1, 2, 3"));
}

#[test]
fn minus_one_means_no_limit() {
    let f = ClusterFile::parse(&THREE.replace("bytes = 1073741824", "ms = -1"), "t").unwrap();
    let c = f.node_config(2).unwrap();
    assert_eq!((c.retention.ms, c.retention.bytes), (None, None));
}

#[test]
fn files_that_could_not_run_are_refused_with_the_reason() {
    assert!(err(&THREE.replace("id = 3", "id = 2")).contains("node 2 is named twice"));
    assert!(err(&THREE.replace("id = 3", "id = 0")).contains("start at 1"));
    assert!(err(&THREE.replace("10.0.0.2:7000", "10.0.0.1:7000")).contains("used twice"));
    assert!(err(&THREE.replace("10.0.0.2:7000", "10.0.0.2")).contains("not host:port"));
    assert!(err(&THREE.replace("admins = [\"alice\"]", "")).contains("nobody could create tenants"));
    assert!(err(&THREE.replace("key = \"/etc/lotse/node-{id}.key\"", "")).contains("lotse certs"));
    assert!(err(&THREE.replace("bytes = 1073741824", "bytes = 0")).contains("retention.bytes"));
    assert!(err(&THREE.replace("[auth]", "[auth]\nmtls = false\nscram = false")).contains("nobody could sign in"));
    assert!(err(&format!("{THREE}\nsurprise = 1")).contains("surprise"), "unknown keys are refused");
    let empty = "node = []\n[cluster]\ndata_dir = \"d\"\n[tls]\nplaintext_on_loopback = true\n[auth]\nrequired = false";
    assert!(err(empty).contains("[[node]]"));
}

#[test]
fn without_tls_only_loopback_and_no_authentication() {
    let local = r#"
[cluster]
data_dir = "data/{id}"
[[node]]
id = 1
peer = "127.0.0.1:7001"
client = "localhost:9001"
[tls]
plaintext_on_loopback = true
[auth]
required = false
"#;
    let f = ClusterFile::parse(local, "t").unwrap();
    assert_eq!(f.tls_paths(1), None);
    assert!(!f.node_config(1).unwrap().auth.required);
    assert!(err(&local.replace("127.0.0.1:7001", "10.0.0.1:7001")).contains("not on loopback"));
    assert!(err(&local.replace("required = false", "admins = [\"a\"]")).contains("auth.required needs TLS"));
    assert!(
        err(&local.replace("plaintext_on_loopback = true", "plaintext_on_loopback = true\nca = \"x\""))
            .contains("choose one")
    );
}

#[test]
fn hosts_and_ports() {
    assert_eq!(host_of("10.0.0.1:7000"), Some("10.0.0.1"));
    assert_eq!(host_of("[::1]:7000"), Some("::1"));
    assert_eq!(host_of("broker:7000"), Some("broker"));
    assert_eq!(host_of("broker"), None);
    assert_eq!(host_of(":7000"), None);
    assert!(is_loopback("[::1]:1") && is_loopback("127.0.0.2:1") && !is_loopback("0.0.0.0:1"));
}
