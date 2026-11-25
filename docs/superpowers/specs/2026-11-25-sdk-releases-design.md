# SDK releases: packages on GitHub, public registries later

Status: draft for the maintainer's approval. The ADR (0142) is written from it once it is approved.

## Intent

The four SDKs (Java, Kotlin, C#, Node.js) have never been released. ADR 0056 and ADR 0083 kept publishing out of scope and called the package names placeholders, so every user today builds an SDK from source. The maintainer wants installable releases: first on GitHub, with Maven Central, nuget.org and npm planned for later.

The maintainer's choices:

1. **GitHub first.** Packages go to GitHub Packages and to the GitHub release. The public registries come later, planned now so that adding them takes only accounts, secrets and switching a step on.
2. **The broker's version, on the same tag.** A `v0.2.0` tag releases the broker, the console and all four SDKs as 0.2.0. SDK 0.2.x speaks to broker 0.2.x and its neighbours, the same window as rolling upgrades (ADR 0127). An SDK-only fix also needs a broker release with the same number.
3. **A separate SDK workflow** that runs after the release, not jobs inside `release.yml`.

## Goals

- A `v*` tag that passes the release workflow also releases all four SDKs at that version, without another manual step.
- Anyone can download every SDK package from the GitHub release without a token. A user who has a GitHub token can depend on the packages from GitHub Packages with a normal dependency line.
- The later move to the public registries changes only secrets, repository variables and docs. No new workflow is needed.

Non-goals:

- Publishing to Maven Central, nuget.org or npm now.
- Releasing an SDK on its own tag.
- Signing the packages beyond what each registry asks for. GitHub Packages asks for nothing; Maven Central's PGP signatures come with that step.

Anti-goal: **an SDK package published at a version whose broker release failed or was never made.** The SDK workflow runs only after a successful release run for that tag.

## Approaches considered

- **(a) SDK jobs inside `release.yml`,** gated by the simulations like the binaries. Nothing is published unless everything passed, and there is one workflow to read.
- **(b) A separate SDK workflow after the release (chosen).** The broker's release workflow stays as it is, and the SDKs have their own file to read and run. The cost: the broker can be out while its SDKs failed. The fix then is a manual re-run of the SDK workflow for that tag.

**How (b) starts.** It cannot start on the `release: published` event: the release workflow creates the GitHub release with the workflow's own token, and GitHub does not start other workflows on events caused by that token. Instead it starts on `workflow_run` for the `release` workflow, type `completed`, and goes on only when:

- the run succeeded;
- its event was `push`;
- its `head_branch` is a `v*` tag.

That event is allowed to start a workflow. It can also be started by hand (`workflow_dispatch`, input `tag`), to re-run a tag or to try a branch.

## Design

### 1. Names, on GitHub Packages and later on the public registries

| SDK | GitHub Packages | Later |
| --- | --- | --- |
| Java | `dev.lotse:lotse-client`, feature `compression` (ADR 0061) | Maven Central, same coordinates once the namespace is verified (needs the domain `lotse.dev` or another group id) |
| Kotlin | `dev.lotse:lotse-client-kotlin` | Maven Central, same |
| C# | `Lotse.Client`, `Lotse.Client.Codecs` | nuget.org, same ids if free |
| Node.js | `@pierrejochem/lotse-client` | npm, `@lotse/client` if the scope can be claimed |

GitHub Packages requires an npm package's scope to be the repository owner, so the Node.js name differs on GitHub. The names stop being placeholders: ADR 0142 supersedes those lines of ADR 0056 and ADR 0083.

### 2. Versions

- Each SDK's version in the repository equals `Cargo.toml`'s:
  - `sdk/java/build.gradle.kts`;
  - `sdk/kotlin/build.gradle.kts`, including its dependency on the Java core;
  - a `<Version>` for both C# packages;
  - `sdk/node/package.json`.
- The release workflow's existing version check compares them with the tag too. A tag whose SDK versions differ fails before any simulation runs, so the anti-goal cannot happen through a forgotten bump.
- `docs/releases/<version>.md` names the SDK packages once they ship. The workflow requires those notes already (ADR 0138), so this is a convention, not a check.

### 3. The SDK workflow (`.github/workflows/sdk-release.yml`)

One job per SDK. Each job:

1. Checks out the tag.
2. Runs the SDK's tests. The real-node tests run against the `lotse` from the release for that tag (downloaded from the GitHub release), so the packages are tested against the broker they claim to match.
3. Builds the packages twice in fresh directories and compares them byte for byte:
   - Gradle: jars with fixed timestamps and file order;
   - `dotnet pack` deterministic;
   - `npm pack` with fixed times.

   This is the same promise ADR 0126 makes for the binaries. A package that does not reproduce fails the job.
4. On a tag:
   - publishes to GitHub Packages with the workflow token (`packages: write`);
   - uploads the packages to the tag's GitHub release with `gh release upload`, together with a `SHA256SUMS-sdk` file.
5. Started by hand on a branch: steps 1 to 3 only, so nothing is published.

The public registries are steps present in each job, skipped unless a repository variable switches them on and the secret exists:
- `PUBLISH_MAVEN_CENTRAL` with the Sonatype token and the PGP key;
- `PUBLISH_NUGET` with the API key;
- `PUBLISH_NPM` with the npm token.

Turning one on later is a settings change and a docs line.

Re-running for a tag whose packages GitHub Packages already holds: the registry refuses the same version again. The job treats an identical existing package as done and a different one as a failure.

### 4. Docs

- A guide page, *Installing the SDKs*:
  - downloading from the GitHub release;
  - depending on GitHub Packages for Gradle, Maven, NuGet and npm, with the token each needs, since GitHub Packages asks for one even for public packages;
  - the version rule.
- `docs/status.md`: the public registries are not there yet.
- AGENTS.md: the repository map and the release paragraph.
- `docs/design.md`, in German: a short section on SDK releases.
- ADR 0142 records all of the above.

### 5. MCP

No tools. Releasing is a maintainer's CI step, not something an operator or developer does against a cluster. ADR 0142 says so, per the rule in AGENTS.md.

## Tests

- The workflow, run by hand on the branch, builds, compares and tests all four SDKs and publishes nothing. That run is the check before merging.
- The version check: a branch where one SDK version differs from `Cargo.toml` fails the check job. Tried once by hand, then reverted, as a mutation check of the gate.
- A real publish can only be seen on the next `v*` tag. The first tag after merging is the end-to-end test; the release notes say the SDKs are new.

## Left to the builder

- How each build sets fixed timestamps.
- The exact step that downloads the `lotse` binary for the real-node tests.
- Whether the four jobs share a matrix.
