# 126. Reproducible releases, built by one script, for every init system

- Status: Accepted
- Date: 2026-11-20
- Changes [ADR 0049](0049-packaging.md) in part: how releases are built, and which packages there are

## Context

ADR 0049 put the release build into `.github/workflows/release.yml`: steps written inline built static binaries with cargo-zigbuild, Debian packages with cargo-deb, checksums and the image. Two limits followed, and `docs/status.md` listed both:

- **Only GitHub Actions could build a release.** Nobody could make one elsewhere, and nobody could check that the published files came from the tagged source. The steps had never run either, because there was no tag yet. Tool versions were not pinned: `pip install ziglang` took whatever was newest.
- **The only package targeted systemd.** The `.deb` held `lotse@.service` and nothing for other init systems. Alpine, the common Linux without systemd, had nothing. Fedora, RHEL and SUSE could not install a `.deb`.

## Decision

**A release is built by one script, `packaging/release.sh`, and it is reproducible: the same commit built with the same tools gives the same bytes. Two builds in CI must match before anything is published. Releases add a tarball, an `.rpm` and an `.apk`, and the `.deb` also runs under sysvinit** (maintainer's choices: script plus reproducible; tarball, OpenRC with an Alpine package, RPM, and sysvinit in the `.deb`).

- **Tools.** `packaging/release.env` pins Rust, zig, cargo-zigbuild and nfpm. `packaging/tools.sh` installs them and checks nfpm against its checksum. The script refuses other versions unless `LOTSE_RELEASE_UNPINNED=1`; the build then works but does not give the published bytes.
- **The script.** `packaging/release.sh [--arch amd64|arm64|all] [--out DIR] VERSION` builds into `dist/`:
  - for each architecture, the static musl binary `lotse-<version>-linux-<arch>` (checked for having no program interpreter);
  - the tarball `lotse-<version>-linux-<arch>.tar.gz`;
  - the `.deb`, `.rpm` and `.apk`;
  - `BUILDINFO`: version, commit, whether the tree had changes, the epoch, and the tool versions including zlib;
  - `SHA256SUMS`.
  `make release VERSION=…` runs it.
- **Reproducible.**
  - Every timestamp is `SOURCE_DATE_EPOCH`: the commit's time.
  - Sources map to `/lotse` and `/cargo` in Rust (`--remap-path-prefix`) and in C (`-ffile-prefix-map`).
  - Debug info is stripped and symbols stay, for backtraces.
  - The tarball is written by `packaging/tarball.py`: entries sorted, owned by root, and a gzip header without a name or time.
  - nfpm writes the packages with the epoch as every time, and the `.rpm` with a fixed build host (`lotse`), since its header otherwise records the machine's name.
- **Checking a release.** `packaging/release.sh --verify SHA256SUMS VERSION`, at the release's tag, builds again and compares file by file with a published `SHA256SUMS` (`make release-verify`).
- **Packages, all made by nfpm from `packaging/nfpm.yaml`.** nfpm replaces cargo-deb: one tool and one file for three formats, and it writes reproducible packages.
  - **deb.** systemd as before. `/etc/init.d/lotse` and `/etc/default/lotse` serve systems without systemd: one node per machine, the one `LOTSE_NODE` names; nothing starts until it is set. The node runs as `lotse` under `start-stop-daemon` and logs to `/var/log/lotse/lotse.log`. On systemd, the unit systemd would generate from the init script is masked (`lotse.service` → `/dev/null`), so only `lotse@<id>` runs nodes. Without systemd the package registers the script with `update-rc.d` and stops the node before removal.
  - **rpm.** systemd: the unit and sysusers file. Its scriptlets create the user and the data directory.
  - **apk.** OpenRC: `/etc/init.d/lotse` under `supervise-daemon`, a node per link `lotse.<id>` like `lotse@<id>`, settings in `/etc/conf.d/lotse` and `/etc/conf.d/lotse.<id>`, log in `/var/log/lotse/lotse.<id>.log`, 60 s for SIGTERM like the unit.
  - **tar.gz.** The binary, the systemd unit and sysusers file, the OpenRC and sysvinit scripts, the example cluster file, the licence and `INSTALL.md`, for any Linux.
  - Each package creates the user `lotse` and `/var/lib/lotse`. Removing one, even with purge, leaves data and configuration, as before.
  - The packages are not signed; `SHA256SUMS` and reproducibility are what a user checks. Alpine installs an unsigned package with `--allow-untrusted`.
- **The release workflow.** After the simulations (ADR 0088):
  - `build` runs the script. `reproduce` runs it again on another runner, from another directory with another `CARGO_HOME`. `compare` fails on any byte that differs.
  - The image is built twice the same way (`image`, `image-again`): by `packaging/image.sh` with buildx, from the compared binaries, on the distroless base named by the digest `image` resolved, with rewritten timestamps and no attestations. The two index digests must be equal.
  - On a tag, the release publishes the compared files with BUILDINFO and the image digest in its notes. `publish-image` pushes the compared archive with `skopeo --preserve-digests` and checks the pushed digest.
  - Run by hand on a branch, the workflow runs the simulations and both comparisons, and publishes nothing.
- **CI on pull requests.** The packaging job runs the script for x86_64 and installs the packages:
  - the `.deb` on the runner, with `systemd-analyze verify` and the masked `lotse.service`;
  - the `.deb` in a Debian container without systemd, where the init script starts a one-node cluster that answers `/healthz` and stops it;
  - the `.apk` in an Alpine container, where OpenRC starts `lotse.1` and stops it;
  - the `.rpm` in a Fedora container, where the binary runs a node as `lotse`.
  It still builds the image from source.

Rejected options:

- **Steps only in the workflow, written up for people.** This is the cheapest, but the document and the workflow drift apart, and nothing could be checked.
- **A script without reproducibility.** It would allow building elsewhere, but not checking what was published.
- **Pinning the base image's digest in the repository.** It goes stale, and updating it is a commit for each distroless release. Resolving the digest at release time and recording it gives the same check.
- **cargo-deb and cargo-generate-rpm.** One tool per format, and neither makes an `.apk`.

## Consequences

- Anyone with the pinned tools builds a release, and anyone can check that the published files came from the tag.
- A release builds the binaries twice, which adds about one build's time to the release workflow. Nothing is published unless the two builds match.
- A tool update changes every file of the next release. The versions live in one file, so the change is one commit.
- Builds are byte-identical only with the same tool versions. The tarball's gzip also depends on zlib, which `BUILDINFO` records; CI builds on one runner image, `ubuntu-24.04`.
- Alpine, Devuan and other systems without systemd run a node from a package. Fedora, RHEL and SUSE install an `.rpm`, and any other Linux uses the tarball.
- sysvinit runs one node per machine; systemd and OpenRC run several.
- `cargo deb -p lotse-node` is gone; `make release` takes its place.
- Verified here:
  - Two builds by the script, from two checkouts in different directories with different `CARGO_HOME`, gave identical `SHA256SUMS`. This machine cannot download the musl targets, so that check built for `x86_64-unknown-linux-gnu`.
  - The packages' contents and scripts were inspected.
  - The sysvinit script started and stopped a node here.
- The first run of the release workflow on the branch built everything twice on two runners. The binaries, tarballs, `.deb` and `.apk` files matched; the `.rpm` files did not, because their header held each runner's host name. With a fixed build host, packages made under two host names match here in all three formats.
- Not verified here: the musl builds, the OpenRC and RPM runs, and the image. This machine has no musl target, no Docker daemon and no container registry. The CI packaging job and the release workflow run by hand on the branch check them.
