# Running a cluster

The short form. The [guide](https://pierrejochem.github.io/lotse/) has each step in full: [install](guide/src/setup/install.md), [a first cluster](guide/src/setup/first-cluster.md), [the cluster file](guide/src/admin/cluster-file.md), [certificates](guide/src/admin/certificates.md), [nodes](guide/src/admin/nodes.md) and [monitoring](guide/src/admin/monitoring.md).

`lotse` runs one node from a cluster file that is the same on every node (ADR 0047). Streams, queues and exchanges are not in the file; clients create them.

```toml
# cluster.toml
[cluster]
data_dir = "/var/lib/lotse"

[[node]]
id = 1
peer = "10.0.0.1:7000"     # where the other nodes reach it
client = "10.0.0.1:9000"   # where clients reach it
[[node]]
id = 2
peer = "10.0.0.2:7000"
client = "10.0.0.2:9000"
[[node]]
id = 3
peer = "10.0.0.3:7000"
client = "10.0.0.3:9000"

[tls]
ca = "/etc/lotse/ca.pem"
cert = "/etc/lotse/node-{id}.pem"
key = "/etc/lotse/node-{id}.key"
crl = "/etc/lotse/crl.pem"  # revoked certificates, written by `lotse certs --revoke`

[retention]
ms = 604800000             # 7 days, the default; -1 keeps everything

[auth]
admins = ["alice"]         # signs in with the certificate below
```

```sh
cargo build --release -p lotse-node                               # target/release/lotse
lotse check --config cluster.toml                                 # what the file describes
lotse certs --config cluster.toml                                 # a CA, a certificate per node (one year) and an empty revocation list
lotse certs --config cluster.toml --client alice --out ./alice    # the first admin's client certificate
lotse run --config cluster.toml --node 1                          # on each machine, its own id
```

`lotse certs` keeps what exists, so it can run again after a node is added to the file; keep `ca.key` off the nodes. Certificates last a year (`--days` for another length), the CA ten; `lotse check` warns 30 days before one ends. A running node reads its certificate files every 10 s and takes up a renewed certificate or a new revocation list without a restart (ADR 0060):

```sh
rm /etc/lotse/node-2.pem /etc/lotse/node-2.key && lotse certs --config cluster.toml   # renew node 2's certificate
lotse certs --config cluster.toml --revoke alice/alice.pem         # then copy crl.pem to every node
```

Client certificates renew themselves once the cluster has a renewal CA: an intermediate under the cluster's CA that every node holds, which signs a new certificate for the same principal over a connection made with the old one, in its last 30 days or past two thirds of its validity. A certificate from it signs in only while the cluster has recorded it (ADR 0123):

```sh
lotse certs --config cluster.toml --renewal-ca                    # then add [tls] renewal_ca and copy both files to every node
lotse certs --config cluster.toml --renew --if-due --cert alice/alice.pem --key alice/alice.key   # from a daily timer
```

To add a node, give it a `[[node]]` entry, make its certificate, announce it and start it with `--join`; to remove one, drain it. Its replicas move to other nodes, then it leaves the list and stops by itself (ADR 0048, ADR 0051). A new node fills up by itself: replicas and leadership move onto it until the nodes are even (ADR 0050).

```sh
lotse certs --config cluster.toml
lotse node add --config cluster.toml --node 4 --cert alice/alice.pem --key alice/alice.key
lotse run --config cluster.toml --node 4 --join                         # on the new machine
lotse node remove --config cluster.toml --node 2 --cert alice/alice.pem --key alice/alice.key
lotse node list --config cluster.toml --cert alice/alice.pem --key alice/alice.key
```

Larger setups put certificates from their own CA at the same paths. Clients authenticate with a certificate, SCRAM-SHA-512 (users set at runtime) or OAUTHBEARER (`[auth.oidc]`); `auth.required = false` with `[tls] plaintext_on_loopback = true` runs a cluster on one machine without either. `LOTSE_LOG` sets the log filter; the node stops cleanly on Ctrl-C or SIGTERM. With `metrics = "host:port"` in its `[[node]]` entry a node serves `/metrics` (OpenMetrics, for Prometheus), `/healthz`, `/readyz` and `/safe-to-stop` over HTTP: plain on loopback, TLS elsewhere, metrics only for a certificate whose principal holds `cluster-viewer` (ADR 0117).

## Packages, images and systemd

Each release (ADR 0049, ADR 0126) has, for x86_64 and aarch64, a static binary, a tarball, a `.deb` (systemd, and sysvinit without it), an `.rpm` (systemd) and an `.apk` (OpenRC), and the image `ghcr.io/pierrejochem/lotse`; the management console has the same, as `lotse-console` and `ghcr.io/pierrejochem/lotse-console`, serving HTTPS with a certificate of its own (ADR 0137, [the guide](guide/src/admin/console.md)). Releases are reproducible: built twice and compared before they are published, and checkable by anyone. A node takes its id from `--node` or `LOTSE_NODE`, its cluster file from `--config` or `LOTSE_CONFIG`, and joins with `--join` or `LOTSE_JOIN=1`.

```sh
# Debian or Ubuntu: the binary, lotse@.service, the lotse user
sudo apt install ./lotse_<version>_amd64.deb
sudo install -d /etc/lotse
sudo cp /usr/share/doc/lotse/cluster.toml.example /etc/lotse/cluster.toml   # then edit it
sudo lotse certs --config /etc/lotse/cluster.toml && sudo chown -R lotse: /etc/lotse
sudo systemctl enable --now lotse@1

# Fedora, RHEL or SUSE: the same, with systemd
sudo dnf install ./lotse-<version>-1.x86_64.rpm

# Alpine: OpenRC runs node 1 as lotse.1
apk add --allow-untrusted ./lotse_<version>-r1_x86_64.apk
ln -s lotse /etc/init.d/lotse.1 && rc-update add lotse.1 default && rc-service lotse.1 start

# Debian or Devuan without systemd: set LOTSE_NODE=1 in /etc/default/lotse, then
sudo /etc/init.d/lotse start

# A container: the cluster file and certificates read-only, the data in a volume
docker run -e LOTSE_NODE=1 -v /etc/lotse:/etc/lotse:ro -v lotse-1:/var/lib/lotse \
  -p 7000:7000 -p 9000:9000 ghcr.io/pierrejochem/lotse
```

Any other Linux takes the tarball, whose `INSTALL.md` covers systemd, OpenRC and sysvinit. In the image and under every init script the data directory is `/var/lib/lotse`, so the cluster file's `data_dir` should be that path or one below it.

### Upgrading

Nodes upgrade one at a time while the cluster runs, one minor release at a time ([ADR 0127](adr/0127-rolling-upgrades-with-a-cluster-version-and-restart-turns.md)). A package upgrade restarts the node it had running once the node holds the cluster's restart turn; elsewhere take the turn yourself. When every node runs the new release, a cluster admin raises the cluster version. On Kubernetes, the Helm chart in `packaging/helm/lotse` does the same per pod ([ADR 0128](adr/0128-a-helm-chart-whose-pods-take-the-restart-turn.md)).

```sh
lotse turn take --config cluster.toml --node 2 && systemctl restart lotse@2   # by hand
lotse upgrade status --config cluster.toml --node 1                           # where it stands
lotse upgrade finalize --config cluster.toml --cert admin.pem --key admin.key # once every node runs it
```

### AI agents

`lotse mcp` lets an AI agent work with a cluster as the user who signed in, over stdio on a developer's machine or over HTTP as one shared process ([ADR 0134](adr/0134-lotse-mcp-a-separate-command-for-ai-agents.md)); see the guide's [Using Lotse with AI agents](guide/src/user/ai-agents.md).

### Building and checking a release

`packaging/` holds the Dockerfile, the init scripts, the package definition (`nfpm.yaml`) and the release script. A release builds on any Linux with the tools `packaging/release.env` pins:

```sh
make release-tools                                   # Rust, zig, cargo-zigbuild and nfpm at the pinned versions
make release VERSION=0.4.0                           # dist/: binaries, tarballs, .deb, .rpm, .apk, BUILDINFO, SHA256SUMS
git checkout v0.4.0 && make release-verify VERSION=0.4.0 SUMS=~/Downloads/SHA256SUMS   # the same bytes as published?
docker build -f packaging/Dockerfile -t lotse .      # the image from source
```

The same commit with the same tools gives the same bytes. Every timestamp is the commit's, no path of the build machine stays in a binary, and the release workflow builds twice on two runners and publishes only when every file matches. `packaging/image.sh` builds the release image the same way (ADR 0126).
