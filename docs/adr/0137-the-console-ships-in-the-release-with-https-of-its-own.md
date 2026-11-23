# 137. The console ships in the release, with HTTPS of its own

- Status: Accepted
- Date: 2026-11-23

## Context

The release (ADR 0126) held only the `lotse` binary: its packages, tarballs, image and the Helm chart (ADR 0128). The management console (ADR 0019) had to be built from source, with the web app built first and then compiled into `lotse-console`. It served plain HTTP only, so a console on its own host, as ADR 0019 places it, needed a reverse proxy for TLS, and nothing stopped it from serving plain HTTP on a public address in dev mode. A first release should give operators the console the same way it gives them the nodes.

## Decision

- **Its own package.** Every release holds, for x86_64 and aarch64, a static `lotse-console` binary with the web app built in, a tarball `lotse-console-<version>-linux-<arch>.tar.gz`, a `.deb`, an `.rpm` and an `.apk` named `lotse-console`, and the image `ghcr.io/<owner>/lotse-console`. A broker host does not carry the web UI, and a console host does not carry a node. The packages create the user `lotse-console`, `/var/lib/lotse-console` and `/etc/lotse-console`, install `lotse-console.service` (systemd, `.deb` and `.rpm`), `/etc/init.d/lotse-console` (sysvinit in the `.deb`, OpenRC in the `.apk`) and an example config in `/usr/share/doc/lotse-console`, start nothing until the config exists and the service is enabled, restart a running console on upgrade, and leave config and session key on removal.
- **Reproducible like the rest.** `packaging/release.sh` builds the web app once with the Node.js version `packaging/release.env` pins (`npm ci` from the lock file, then Vite), then both binaries per architecture. The console embeds the files with fixed timestamps (rust-embed `deterministic-timestamps`), so the two builds of the release workflow match byte for byte, as do the two builds of each image. `BUILDINFO` names the Node.js and npm versions.
- **HTTPS of its own.** `lotse-console.toml` takes `[https]` with `cert` (the chain) and `key`, PEM, relative to the config file and read at start. Without it the console starts only on a loopback address; with it, the OIDC redirect URL must be `https://`. The TLS listener is the one `lotse mcp --listen` uses, now in `lotse-rt-tokio` (feature `https`): TLS 1.3, handshakes in their own tasks, at most 64 at once, 10 s each. SIGTERM stops the console cleanly.
- **Optional in the Helm chart.** `console.enabled` (off by default) adds a ConfigMap, a Deployment of one pod and a Service. The chart renders `listen`, `data_dir`, `[https]` from the `kubernetes.io/tls` Secret `console.tlsSecretName`, and a `[[clusters]]` entry for its own cluster at its pods' names (the node certificates name the pods, not the client Service) with the cluster's CA, mounting only `ca.pem` of the nodes' Secret; `console.config` holds the rest (`[auth]`, `[metrics]`, `[live]`), and `console.secretName` mounts files it names. The console's pods carry the name label `lotse-console`, so the nodes' Services never select them.
- **No MCP tools.** This changes how the console is delivered and served, not what a cluster does; `lotse mcp` already reaches everything the console shows.

## Consequences

- An operator installs the console from the same release as the nodes, with the same checks (`SHA256SUMS`, `release.sh --verify`).
- A release build needs Node.js and npm's registry; CI takes Node.js from `actions/setup-node` at the pinned version.
- A console listening anywhere but on loopback needs a certificate. Behind a reverse proxy on the same host it can still listen on loopback with plain HTTP.
- The certificate is read at start; a renewed one needs a restart. In Kubernetes the console keeps its session key and chart history in the pod, so a restarted pod signs users out and starts its charts afresh.
- CI installs the console's `.deb` under systemd and sysvinit, the `.apk` under OpenRC and the `.rpm`, starts it under each, builds its image from source and runs it in the chart on kind.
