# Install

Lotse is one binary, `lotse`. It runs one node of a cluster, makes the cluster's certificates and checks the cluster file. There are three ways to get it.

## What you need

- Linux on x86_64 or aarch64. The release binaries are static.
- Three machines for a cluster that survives the loss of one. One machine is enough to try Lotse ([a one-machine trial](one-machine-trial.md)).
- Two TCP ports per node. The examples use 7000 for the other nodes (`peer`) and 9000 for clients (`client`). Both are set per node in the cluster file.
- A disk for the data directory. Retention keeps 7 days by default, so the size follows your traffic.

Each release has static binaries, Debian packages and a container image ([ADR 0049](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0049-packaging.md)).

## Debian or Ubuntu

The package installs the binary, the unit `lotse@.service`, the system user `lotse` and an example cluster file at `/usr/share/doc/lotse/cluster.toml.example`.

```sh
sudo apt install ./lotse_<version>_amd64.deb
lotse --version
```

Replace `<version>` with the release you downloaded. The next chapter, [A first cluster](first-cluster.md), continues from here.

## Container image

The image `ghcr.io/pierrejochem/lotse` is a static build on a distroless base. A node takes its id and cluster file from the environment:

| Variable | Same as | Meaning |
| --- | --- | --- |
| `LOTSE_NODE` | `--node` | the node's id |
| `LOTSE_CONFIG` | `--config` | the cluster file |
| `LOTSE_JOIN` | `--join` | `1` or `true`: a new node joining a running cluster |

```sh
docker run -e LOTSE_NODE=1 \
  -v /etc/lotse:/etc/lotse:ro -v lotse-1:/var/lib/lotse \
  -p 7000:7000 -p 9000:9000 ghcr.io/pierrejochem/lotse
```

Mount the cluster file and the certificates read-only and the data in a volume. In the image the data directory is `/var/lib/lotse`, so `data_dir` in the cluster file should be that path or one below it. The image sets `LOTSE_CONFIG=/etc/lotse/cluster.toml`, so mount the cluster file there. It exposes ports 7000 and 9000.

## Build from source

You need a current stable Rust toolchain.

```sh
git clone https://github.com/pierrejochem/lotse
cd lotse
cargo build --release -p lotse-node      # target/release/lotse
```

`cargo deb -p lotse-node` builds the Debian package from the repository (install the tool with `cargo install cargo-deb`), and `docker build -f packaging/Dockerfile -t lotse .` builds the image.

## Check the install

```sh
lotse --version
```

prints the version, for example `lotse 0.0.1`. `lotse` with no command prints the usage of `run`, `certs`, `check` and `node`.
