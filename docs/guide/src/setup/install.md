# Install

Lotse is one binary, `lotse`. It runs one node of a cluster, makes the cluster's certificates and checks the cluster file. There are four ways to get it: a package, the tarball, the container image, or a build from source.

## What you need

- Linux on x86_64 or aarch64. The release binaries are static.
- Three machines for a cluster that survives the loss of one. One machine is enough to try Lotse ([a one-machine trial](one-machine-trial.md)).
- Two TCP ports per node. The examples use 7000 for the other nodes (`peer`) and 9000 for clients (`client`). Both are set per node in the cluster file.
- A disk for the data directory. Retention keeps 7 days by default, so the size follows your traffic.

Each release has, for x86_64 and aarch64, a static binary, a tarball, a `.deb`, an `.rpm`, an `.apk` and a container image ([ADR 0049](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0049-packaging.md), [ADR 0126](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0126-reproducible-releases-built-by-one-script-for-every-init-system.md)). Replace `<version>` below with the release you downloaded. The next chapter, [A first cluster](first-cluster.md), continues from here.

## Packages

Every package installs the binary, the system user `lotse`, the data directory `/var/lib/lotse` and an example cluster file at `/usr/share/doc/lotse/cluster.toml.example`. Removing a package, even with purge, leaves data and configuration.

| System | Package | Runs a node with |
| --- | --- | --- |
| Debian, Ubuntu | `lotse_<version>-1_amd64.deb` | systemd: `lotse@<id>.service` |
| Debian, Devuan without systemd | the same `.deb` | sysvinit: `/etc/init.d/lotse`, `LOTSE_NODE` in `/etc/default/lotse` |
| Fedora, RHEL, SUSE | `lotse-<version>-1.x86_64.rpm` | systemd: `lotse@<id>.service` |
| Alpine | `lotse_<version>-r1_x86_64.apk` | OpenRC: `/etc/init.d/lotse.<id>`, a link to `lotse` |

```sh
sudo apt install ./lotse_<version>-1_amd64.deb             # Debian, Ubuntu, Devuan
sudo dnf install ./lotse-<version>-1.x86_64.rpm            # Fedora, RHEL
sudo zypper install ./lotse-<version>-1.x86_64.rpm         # SUSE
apk add --allow-untrusted ./lotse_<version>-r1_x86_64.apk  # Alpine
lotse --version
```

The packages are not signed, so Alpine needs `--allow-untrusted`. Check a download against the release's `SHA256SUMS` first (below).

## Tarball

For any other Linux, `lotse-<version>-linux-amd64.tar.gz` holds the static binary, the systemd unit, the OpenRC and sysvinit scripts and the example cluster file. Its `INSTALL.md` lists where each goes.

```sh
tar -xzf lotse-<version>-linux-amd64.tar.gz
sudo install -m 0755 lotse-<version>-linux-amd64/bin/lotse /usr/bin/lotse
```

## Check a download

```sh
sha256sum --check --ignore-missing SHA256SUMS
```

Releases are reproducible: the release workflow builds every file twice, on two machines, and publishes only when the two builds match byte for byte. You can do the same from the source tree at the release's tag. This needs the pinned tools, which `make release-tools` installs:

```sh
git checkout v<version>
make release-verify VERSION=<version> SUMS=path/to/SHA256SUMS
```

It lists every file as `same` or `differs`.

## Container image

The image `ghcr.io/pierrejochem/lotse` is a static build on a distroless base. A node takes its id and cluster file from the environment:

| Variable | Same as | Meaning |
| --- | --- | --- |
| `LOTSE_NODE` | `--node` | the node's id |
| `LOTSE_CONFIG` | `--config` | the cluster file |
| `LOTSE_JOIN` | `--join` | `1` or `true`: a new node joining a running cluster |
| `LOTSE_NODE_INDEX` | | without `LOTSE_NODE`: a StatefulSet pod index; the node id is the index plus one |

```sh
docker run -e LOTSE_NODE=1 \
  -v /etc/lotse:/etc/lotse:ro -v lotse-1:/var/lib/lotse \
  -p 7000:7000 -p 9000:9000 ghcr.io/pierrejochem/lotse
```

Mount the cluster file and the certificates read-only and the data in a volume. In the image the data directory is `/var/lib/lotse`, so `data_dir` in the cluster file should be that path or one below it. The image sets `LOTSE_CONFIG=/etc/lotse/cluster.toml`, so mount the cluster file there. It exposes ports 7000 and 9000.

## Kubernetes

The Helm chart in `packaging/helm/lotse` runs a cluster as a StatefulSet, one node per pod, and rolls upgrades one node at a time ([ADR 0128](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0128-a-helm-chart-whose-pods-take-the-restart-turn.md)). Its [README](https://github.com/pierrejochem/lotse/blob/main/packaging/helm/lotse/README.md) shows the install: render the cluster file, make the certificates for it, store them in a Secret, `helm install`. It needs Kubernetes 1.28 or later.

## Build from source

You need a current stable Rust toolchain.

```sh
git clone https://github.com/pierrejochem/lotse
cd lotse
cargo build --release -p lotse-node      # target/release/lotse
```

`make release-tools && make release VERSION=<version>` builds the whole release into `dist/`: binaries, tarballs and packages for both architectures. `docker build -f packaging/Dockerfile -t lotse .` builds the image.

## Check the install

```sh
lotse --version
```

prints the version, for example `lotse 0.0.1`. `lotse` with no command prints the usage of `run`, `certs`, `check` and `node`.
