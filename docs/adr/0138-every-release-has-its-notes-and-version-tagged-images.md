# 138. Every release has its notes, and images are tagged by version

- Status: Accepted
- Date: 2026-11-23

## Context

The release workflow (ADR 0126) wrote the GitHub release's text from the build alone: how it was checked, the image digests, `BUILDINFO`, and the list of merged pull requests. It said nothing about what a release contains, what it lacks or what it promises. Before the first release it also turned out that the images were pushed as `v<version>` and `latest` while the Helm chart's `appVersion` names the plain version, so `helm install` with its defaults would ask for an image tag that does not exist. And nothing stopped a tag whose version differs from `Cargo.toml` or the chart.

## Decision

- **Notes in the repository.** Each release has its notes in `docs/releases/<version>.md`, written and reviewed in a pull request like any other change: what it contains, what you get, how to check it, what is not in it, what it promises for upgrades and compatibility. The release workflow puts them at the top of the GitHub release, above the build section it writes and GitHub's list of pull requests.
- **Checked first.** A job `notes`, which every build waits for, refuses a tag `v<version>` unless `docs/releases/<version>.md` exists and `Cargo.toml`, the chart's `version` and its `appVersion` all say `<version>`. It takes seconds, so a tag that lacks them fails before the simulations' results matter and before anything is built.
- **Images tagged by version.** Both images are pushed as `<version>`, `v<version>` and `latest`, so the chart's defaults find theirs; the release text names the plain version first.

## Consequences

- A release cannot go out without notes, and its notes go through review.
- Raising the version means changing `Cargo.toml`, the chart and writing the notes in the same pull request, before the tag.
- Existing references to `v<version>` image tags keep working.
