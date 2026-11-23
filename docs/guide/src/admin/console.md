# The console

The management console is a separate program, `lotse-console`, with the web app built into it. It runs on a host of its own, reaches one or more clusters over the native protocol, and does everything as the user who signed in: it has no rights of its own ([ADR 0019](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0019-management-console-on-a-dedicated-node-with-the-user-s-identity.md)). What users see in it is in [The management console](../user/console.md).

Every release has it as a static binary for x86_64 and aarch64, a tarball, a `.deb`, an `.rpm`, an `.apk` and the image `ghcr.io/pierrejochem/lotse-console`, built and checked like the node's ([ADR 0137](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0137-the-console-ships-in-the-release-with-https-of-its-own.md)).

## Install

| System | Package | Runs the console with |
| --- | --- | --- |
| Debian, Ubuntu | `lotse-console_<version>-1_amd64.deb` | systemd: `lotse-console.service` |
| Debian, Devuan without systemd | the same `.deb` | sysvinit: `/etc/init.d/lotse-console` |
| Fedora, RHEL, SUSE | `lotse-console-<version>-1.x86_64.rpm` | systemd: `lotse-console.service` |
| Alpine | `lotse-console_<version>-r1_x86_64.apk` | OpenRC: `/etc/init.d/lotse-console` |

```sh
sudo apt install ./lotse-console_<version>-1_amd64.deb
sudo install -m 0640 -g lotse-console /usr/share/doc/lotse-console/lotse-console.toml.example \
  /etc/lotse-console/lotse-console.toml          # then edit it
sudo systemctl enable --now lotse-console
```

A package creates the user `lotse-console`, the data directory `/var/lib/lotse-console` and `/etc/lotse-console`, and starts nothing: the console needs its config first. An upgrade restarts a running console. Removing the package, even with purge, leaves the config and the session key. Put the files the config names (the HTTPS certificate and key, the cluster's CA, secrets) in `/etc/lotse-console`, readable by the group `lotse-console`.

For any other Linux, `lotse-console-<version>-linux-amd64.tar.gz` holds the binary, the scripts and the example config; its `INSTALL.md` lists where each goes. Check every download against the release's `SHA256SUMS`, as in [Install](../setup/install.md).

## The config file

The packages read `/etc/lotse-console/lotse-console.toml`. Relative paths in it are relative to the file. The example the packages install:

```toml
listen = "0.0.0.0:8443"
data_dir = "/var/lib/lotse-console"

[https]
cert = "/etc/lotse-console/console.pem"
key = "/etc/lotse-console/console.key"

[auth]
mode = "oidc"
issuer = "https://id.example.com/realms/ops"
client_id = "lotse-console"
redirect_url = "https://console.example.com:8443/auth/callback"
principal_claim = "sub"

[metrics]
principal = "console-metrics"
client_id = "lotse-console-metrics"
client_secret_file = "/etc/lotse-console/metrics-secret"
sample_secs = 10
retention_hours = 24

[[clusters]]
id = "production"
kind = "native"
bootstrap = ["broker-1.example.com:9000", "broker-2.example.com:9000"]
ca_file = "/etc/lotse-console/cluster-ca.pem"
```

### HTTPS

With `[https]` the console serves TLS 1.3 with the certificate chain in `cert` (its own certificate first) and the key in `key`, both PEM. The certificate names the host users reach the console by; it may come from any CA their browsers trust, not the cluster's. The files are read at start: restart the console after renewing them.

Without `[https]` the console serves plain HTTP, and it starts only if `listen` is a loopback address. That fits a machine only you use, or a reverse proxy on the same host that terminates TLS and forwards to `127.0.0.1`.

### Sign-in

With `mode = "oidc"` users sign in at the identity provider the nodes trust for OAUTHBEARER, and the console passes their access token on ([Authentication and roles](authentication-and-roles.md)). Register the console there as a client with the redirect URL `https://<host>/auth/callback`; with `[https]` it must start with `https://`. `principal_claim` must be the claim the nodes take the principal from. The console keeps its session key in `data_dir`, so sessions survive a restart.

`mode = "dev"` signs every request in as one configured user, with `password` as that user's SCRAM password if the cluster requires authentication. Use it only where nobody else can reach the console.

### The chart sampler

The charts come from a sampler that runs whether or not anyone is signed in. It is the only thing that uses an identity of the console's own: `metrics.principal`, signed in with OAuth client credentials (`client_id`, `client_secret_file`) or a SCRAM `password`. Bind that principal the cluster-viewer role; each user's chart still sums only what that user may see ([ADR 0041](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0041-console-sessions-sampler-sign-in-and-charts-per-user.md)).

### Clusters

Each `[[clusters]]` entry is one cluster the console manages: `bootstrap` lists client addresses of some of its nodes, `ca_file` is the cluster's CA. Without `ca_file` the console speaks plain TCP to the cluster, which works only on loopback. `kind = "demo"` adds the built-in sample cluster instead.

## Container image

```sh
docker run -v /etc/lotse-console:/etc/lotse-console:ro \
  -v lotse-console:/var/lib/lotse-console -p 8443:8443 ghcr.io/pierrejochem/lotse-console
```

The image reads `/etc/lotse-console/lotse-console.toml`. Its `listen` must not be loopback inside a container, so it needs `[https]`; `data_dir` should be `/var/lib/lotse-console`.

## Kubernetes

The Helm chart runs the console next to the cluster when `console.enabled` is true: one pod, a Service on port 8443, and a config the chart renders. It sets `listen`, `data_dir`, `[https]` from the `kubernetes.io/tls` Secret `console.tlsSecretName`, and a `[[clusters]]` entry for its own cluster with the cluster's CA. The rest of the config, `[auth]` and `[metrics]`, goes in `console.config`; files it names go in a Secret `console.secretName`, mounted at `/etc/lotse-console/secrets`.

```sh
kubectl create secret tls lotse-console-tls --cert=console.pem --key=console.key
helm upgrade lotse packaging/helm/lotse --reuse-values \
  --set console.enabled=true --set-file console.config=console-auth.toml
```

The pod keeps its session key and chart history in the pod: when it restarts, users sign in again and the charts start afresh.
