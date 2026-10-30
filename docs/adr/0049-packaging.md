# 49. Packaging: an image, a systemd unit, a Debian package and releases

- Status: Accepted
- Date: 2026-10-30

## Context

The `lotse` binary (ADR 0047) had to be built from source and started by hand, with the node's id as a flag. Running it on servers or in a container platform meant writing one's own image, unit file or init script, and there were no published builds.

## Decision

**The image is a static binary on distroless** (maintainer's choice). `packaging/Dockerfile` builds `lotse` on Alpine, where Rust targets musl and links statically, and copies it onto `gcr.io/distroless/static-debian12:nonroot`: no shell, no package manager, a non-root user (65532). The cluster file is expected at `/etc/lotse/cluster.toml`, the data in the volume `/var/lib/lotse`; ports 7000 (nodes) and 9000 (clients) are exposed as in the examples. `packaging/Dockerfile.prebuilt` makes the same image from binaries built already, for releases.

**A node takes its id from a flag or the environment** (maintainer's choice). `lotse run` reads `--node`, else `LOTSE_NODE`; `--join`, else `LOTSE_JOIN` (`1` or `true`); every command reads `--config`, else `LOTSE_CONFIG`. Nothing is guessed from the host name.

**A systemd instance unit per node.** `lotse@.service` runs `lotse run` with `LOTSE_NODE=%i`, as the `lotse` user (`lotse.sysusers`), with `/var/lib/lotse` as its state directory and everything else read-only, no capabilities, a system-call filter and the other sandboxing systemd offers; it stops on SIGTERM with 60 s to close its groups. `systemctl enable --now lotse@2` runs node 2.

**Files, a release workflow and a Debian package** (maintainer's choice). `cargo deb -p lotse-node` makes `lotse_<version>_<arch>.deb` with the binary, the unit, the sysusers file and an example cluster file; installing it creates the user and the data directory, and removing it, even with purge, leaves data and configuration. On a `v*` tag, `.github/workflows/release.yml` builds static binaries for x86_64 and aarch64 (cargo-zigbuild), Debian packages for both, a GitHub release with them and a `SHA256SUMS` file, and pushes `ghcr.io/<owner>/lotse:<tag>` and `:latest` for both architectures. CI builds the package, installs it and checks the unit, and builds the image from source and starts its binary.

## Consequences

- A node runs from a package, a unit or an image without writing anything but the cluster file and certificates.
- The image has no shell: debugging inside it needs an ephemeral debug container. Static musl builds use musl's allocator, which is slower than glibc's under heavy allocation; nothing measured yet says whether it matters here.
- Releases and the image depend on GitHub Actions; until it runs again, nothing is published.
- The Debian package targets systemd systems; other init systems need their own unit.
- Verified here: the package builds, installs (user, data directory, `lotse --version`), passes `systemd-analyze verify`, scores 1.4 ("OK") in `systemd-analyze security`, and removes cleanly; `lotse run` started from the environment alone in the binary's test. Not verified here, because this machine has no Docker daemon and cannot download the musl target: the image build and the static release builds. CI and the release workflow run them.
