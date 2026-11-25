# SDK Releases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every `v*` tag that passes the release workflow also releases the Java, Kotlin, C# and Node.js SDKs at that version: to GitHub Packages and as assets on the GitHub release. Steps for Maven Central, nuget.org and npm are present but switched off.

**Architecture:**
- **One script per SDK, the same for local runs and CI.** `packaging/sdk-package.sh <java|kotlin|csharp|node> <outdir>` builds an SDK's release packages reproducibly. A second script compares two builds.
- **A new workflow, `.github/workflows/sdk-release.yml`.** On pull requests it only builds and compares, so the change itself is checked. On `workflow_run` of `release`, or started by hand, it tests, builds twice, compares, then has one publish job.
- **The version gate.** The release workflow's `notes` job also checks that the SDK versions match the tag.

**Tech Stack:** GitHub Actions, Gradle (`maven-publish`), `dotnet pack`, `npm pack`, bash.

**Spec:** `docs/superpowers/specs/2026-11-25-sdk-releases-design.md`

## Global Constraints

- **Versions:** each SDK's version equals `Cargo.toml`'s; today that is `0.1.1`.
- **Names on GitHub Packages:**
  - `dev.lotse:lotse-client` (feature `compression`, ADR 0061);
  - `dev.lotse:lotse-client-kotlin`;
  - `Lotse.Client` and `Lotse.Client.Codecs`;
  - `@pierrejochem/lotse-client`.

  A name is never rewritten at publish time.
- **Package metadata:**
  - every package names the license `Apache-2.0`, the repository `https://github.com/pierrejochem/lotse` and the maintainer Pierre Jochem;
  - Java and Kotlin ship sources and javadoc jars and a full POM.
- **Exact dependency pins:**
  - the Kotlin POM depends on `dev.lotse:lotse-client` at exactly the same version, with no composite-build path in the POM or the jars;
  - `Lotse.Client.Codecs` depends on `Lotse.Client` at `[x.y.z]`.
- **Publishing:**
  - publish only after all four SDKs built, tested and compared;
  - Java before Kotlin;
  - release assets (`gh release upload` with `SHA256SUMS-sdk`) before GitHub Packages.
- **Re-runs:** compare against an existing `SHA256SUMS-sdk` on the release (a mismatch fails); a registry's "already exists" counts as done once the checksums matched.
- **Public registries:** a registry step runs only when `vars.PUBLISH_MAVEN_CENTRAL`, `vars.PUBLISH_NUGET` or `vars.PUBLISH_NPM` is `true` and its secret exists.
- **Repository rules:**
  - Brand voice in docs (no exclamation marks, no emoji).
  - The design document is German.
  - A changed decision gets an ADR (0142).
  - Commits are authored by Pierre Jochem, with no AI trailers.
- **This machine:** the proxy blocks Gradle, Maven Central and NuGet, so the Java, Kotlin and C# packaging is verified by the workflow's pull-request run, not locally.

## Review Focus

- **A tag whose SDK versions were not bumped:** expect the `notes` job to fail before any simulation (Task 1, mutation-checked).
- **The Kotlin POM leaking the composite build:** expect the published POM to name `dev.lotse:lotse-client:<version>` and no path (Task 2 test).
- **Two builds of the same commit differing:** expect `sdk-compare.sh` to fail (Tasks 2–4: each runs it).
- **A re-run after a partial publish:** expect the assets checked against `SHA256SUMS-sdk` and an existing package counted as done (Task 5 test of the script's logic).
- **Packages accidentally published from a pull request or a branch:** expect publish steps that run only on a tag (Task 5).

---

### Task 1: Versions in lockstep and the gate

**Files:**
- Modify: `sdk/java/build.gradle.kts`, `sdk/kotlin/build.gradle.kts` (its version and `api("dev.lotse:lotse-client:…")`), `sdk/csharp/src/Lotse.Client/Lotse.Client.csproj`, `sdk/csharp/src/Lotse.Client.Codecs/Lotse.Client.Codecs.csproj`, `sdk/node/package.json` (and its lock file's own version)
- Create: `packaging/sdk-versions.sh`
- Modify: `.github/workflows/release.yml` (`notes` job)

**Interfaces:**
- Produces: `packaging/sdk-versions.sh` prints one line per SDK, `<sdk> <version>`, with `sdk` one of `java kotlin kotlin-core-dependency csharp csharp-codecs node`. It exits 1 when an argument version is given and any line differs from it.

- [ ] **Step 1:** Write `packaging/sdk-versions.sh`; run it and see the current mismatch (`0.0.1`, `0.0.0`) against `0.1.1`: exit 1.
- [ ] **Step 2:** Set every SDK version to `0.1.1`. Add `<Version>0.1.1</Version>` to both csproj files. Run the script with `0.1.1`: exit 0.
- [ ] **Step 3:** In the `notes` job, after the existing check, run `packaging/sdk-versions.sh "$version"`, and add the SDK versions to the echoed line.
- [ ] **Step 4: Mutation check.** Set one SDK version to `0.1.0`; the script with `0.1.1` must exit 1 and name that SDK. Restore it.
- [ ] **Step 5:** `make sdk-node-test` still passes, except the known ZSTD test on this machine's Node.js 22. Commit: "Keep the SDK versions in step with the release".

### Task 2: Java and Kotlin packages

**Files:**
- Modify: `sdk/java/build.gradle.kts`, `sdk/kotlin/build.gradle.kts`
- Create: `packaging/sdk-package.sh` (the `java` and `kotlin` cases), `packaging/sdk-compare.sh`

**Interfaces:**
- Produces:
  - `packaging/sdk-package.sh <sdk> <outdir>` writes the release files of that SDK flat into `<outdir>`:
    - `java`: `lotse-client-<v>.jar`, `-sources.jar`, `-javadoc.jar`, `.pom`, `.module`;
    - `kotlin`: the same for `lotse-client-kotlin`.
  - Gradle publishing is configured with a repository named `GitHubPackages` (url `https://maven.pkg.github.com/pierrejochem/lotse`, credentials from `GITHUB_ACTOR` and `GITHUB_TOKEN`). The publish task is `publishAllPublicationsToGitHubPackagesRepository`.
  - `packaging/sdk-compare.sh <dirA> <dirB>` exits 0 only when both hold the same file names with identical bytes, and prints the differing names otherwise.

- [ ] **Step 1:** Add `maven-publish`, `withSourcesJar()` and `withJavadocJar()`, and the POM (name, description, url, license Apache-2.0, developer Pierre Jochem, scm). Make the archives reproducible (`isPreserveFileTimestamps = false`, `isReproducibleFileOrder = true`) for the jars and the Kotlin layer. Keep the `compression` feature published as ADR 0061 says.
- [ ] **Step 2:** In Kotlin, keep `includeBuild` for tests, but make the published POM and module name `dev.lotse:lotse-client:<the same version>`. A check task, `verifyPublishedPom`, wired into `check`, fails when the generated POM lacks that dependency at that version or contains a filesystem path.
- [ ] **Step 3:** `sdk-package.sh java|kotlin` runs `publishToMavenLocal` (or `generatePomFileForMavenPublication` with the jar tasks) into a temporary Maven repository and copies the files into `<outdir>`.
- [ ] **Step 4:** Verification here is limited (Gradle is blocked by the proxy). Check the scripts with `shellcheck` if it is available, and run `sdk-compare.sh` on two hand-made directories, one identical and one differing: exit 0 and exit 1. The real build is verified in Task 5's pull-request run.
- [ ] **Step 5:** Commit: "Package the Java SDK and the Kotlin layer for release".

### Task 3: C# packages

**Files:**
- Modify: both csproj files (or a new `sdk/csharp/src/Directory.Build.props` for the shared metadata)
- Modify: `packaging/sdk-package.sh` (the `csharp` case)

**Interfaces:**
- Produces: `sdk-package.sh csharp <outdir>` writes `Lotse.Client.<v>.nupkg`, `Lotse.Client.Codecs.<v>.nupkg` and their `.snupkg`.

- [ ] **Step 1:** Add the package metadata:
  - `PackageLicenseExpression` `Apache-2.0`;
  - `Authors`, `RepositoryUrl`, `PackageProjectUrl`, `Description`;
  - `Deterministic`, `ContinuousIntegrationBuild` when `CI=true`;
  - `IncludeSymbols` with `SymbolPackageFormat` `snupkg`.
- [ ] **Step 2:** Make `Lotse.Client.Codecs` reference `Lotse.Client` so that the nuspec dependency is `[0.1.1]`, an exact range, for example through `ProjectReference` with a version range property or a `PackageReference` in pack. Add a check, `packaging/sdk-package.sh csharp` failing when the packed nuspec's dependency is not `[<v>]`.
- [ ] **Step 3:** `sdk-package.sh csharp` runs `dotnet pack -c Release -o <outdir>`. Its .nupkg zip entries must not carry build-time timestamps. If `dotnet pack` leaves the zip times varying, normalise them in the script and say how in a comment.
- [ ] **Step 4:** Verification: same limits as Task 2, with CI checking it in Task 5. Commit: "Package the C# SDK for release".

### Task 4: Node.js package

**Files:**
- Modify: `sdk/node/package.json`, `sdk/node/package-lock.json`
- Modify: `packaging/sdk-package.sh` (the `node` case)

**Interfaces:**
- Produces: `sdk-package.sh node <outdir>` writes `pierrejochem-lotse-client-<v>.tgz`.

- [ ] **Step 1:** Change `package.json`:
  - `name` `@pierrejochem/lotse-client`, remove `private`;
  - `license` `Apache-2.0`, `author`, `repository`;
  - `files` limited to `dist/src` plus README and LICENSE;
  - `publishConfig.registry` `https://npm.pkg.github.com`.

  Update every reference to `@lotse/client` in the SDK's examples, README and guide page.
- [ ] **Step 2:** `sdk-package.sh node` runs `npm ci`, `npm run build` and `npm pack --pack-destination <outdir>`. `npm pack` writes fixed times; confirm by packing twice.
- [ ] **Step 3:** Run `sdk-package.sh node` twice into two directories, then `sdk-compare.sh`: exit 0. Run `make sdk-node-test`: passes, except the known ZSTD test.
- [ ] **Step 4:** Commit: "Package the Node.js SDK for release".

### Task 5: The SDK release workflow

**Files:**
- Create: `.github/workflows/sdk-release.yml`
- Create: `packaging/sdk-publish.sh`

**Interfaces:**
- Consumes: `sdk-package.sh`, `sdk-compare.sh` and `sdk-versions.sh` (Tasks 1–4).
- Produces: `packaging/sdk-publish.sh <tag> <dir>`, where `<dir>` holds all packages. It:
  - writes `SHA256SUMS-sdk`;
  - if the release already has that asset, compares (a mismatch exits 1);
  - otherwise uploads all packages and the sums with `gh release upload`;
  - publishes to GitHub Packages: Gradle publish for Java and then Kotlin, `dotnet nuget push --skip-duplicate`, and `npm publish`, treating "already exists" (409, or `E409`/`EPUBLISHCONFLICT`) as done;
  - runs the public-registry steps only when their variable is `true` and their secret is set.

Workflow:
- **Triggers:**
  - `pull_request` with paths `sdk/**`, `packaging/sdk-*.sh` and the workflow: build and compare only;
  - `workflow_run` of `release`, `completed`: continue only if `conclusion == 'success'`, `event == 'push'` and `head_branch` starts with `v`;
  - `workflow_dispatch` with input `tag`.
- **The ref:** `workflow_run.head_branch`, or `inputs.tag`, or the pull request's head.
- **Build jobs** (`java`, `kotlin`, `csharp`, `node`; a matrix is fine):
  - set up the tool versions CI already pins;
  - when releasing, download the tag's x86_64 tarball and set `LOTSE_BIN`; otherwise build `lotse`, as `ci.yml` does;
  - run the SDK's tests (the matching `make` target);
  - run `sdk-package.sh` twice into `a/` and `b/`, then `sdk-compare.sh a b`;
  - upload `a/` as an artifact.
- **The publish job:**
  - `needs` all build jobs;
  - runs only for `workflow_run` or `workflow_dispatch` with a `v*` tag;
  - permissions `contents: write` and `packages: write`;
  - downloads the artifacts into one directory and runs `sdk-publish.sh`.

- [ ] **Step 1:** Write `sdk-publish.sh`, with a `--dry-run` that prints what it would upload and publish. Test its logic locally against a fake `gh` on `PATH` (a stub script):
  - (a) no existing asset: upload, then publish;
  - (b) an asset with the same sums: no upload, publish counts as done;
  - (c) an asset with different sums: exit 1.
- [ ] **Step 2:** Write the workflow. Lint it with `actionlint` if it can be installed (go install or a release binary through the allowed registries); otherwise say so.
- [ ] **Step 3:** Commit, push and open a draft PR. The pull-request run must build, test and compare all four SDKs green, with no publish job running. Fix until green: this is the check for Tasks 2–4.
- [ ] **Step 4:** Commit: "Release the SDKs after the broker release".

### Task 6: ADR 0142 and the docs

**Files:**
- Create: `docs/adr/0142-the-sdks-are-released-with-the-broker-on-github-first.md`, `docs/guide/src/developer/installing-sdks.md`
- Modify:
  - `docs/adr/README.md`;
  - `docs/guide/src/SUMMARY.md`;
  - `docs/design.md` (German, a short section);
  - `docs/status.md`: the public registries are not there yet;
  - `AGENTS.md`: the repository map (`packaging/sdk-*.sh`, `sdk-release.yml`) and the release paragraph;
  - the four SDK guide pages and READMEs: installing, the version rule;
  - `docs/releases/README.md` or the release notes convention, if it exists: name the SDK packages.
- Delete: the spec and this plan.

- [ ] **Step 1:** Write ADR 0142 from the spec: the three choices, the names, the `workflow_run` reason, the two stages, re-runs, the registries switched off, and no MCP tools with the reason. Status "Accepted (supersedes the publishing lines of ADR 0056 and ADR 0083)".
- [ ] **Step 2:** Write the guide page:
  - the download from the release, with `SHA256SUMS-sdk`;
  - GitHub Packages for Gradle, Maven, NuGet and npm, with the token each needs (a classic token with `read:packages`);
  - the version rule.
- [ ] **Step 3:** Update the other files. `mdbook build docs/guide` succeeds.
- [ ] **Step 4:** Commit: "Record SDK releases with the broker (ADR 0142)".

### Task 7: Verify, PR and merge

- [ ] **Step 1:** `cargo fmt --all --check`, `make sdk-node-test`, and `packaging/sdk-versions.sh 0.1.1`.
- [ ] **Step 2:** Mark the PR ready. The five CI jobs and the `sdk-release` pull-request run must be green. Run the release workflow by hand on the branch: the `notes` check has nothing to check without a tag, so this confirms nothing else broke.
- [ ] **Step 3:** Merge with `gh api -X PUT repos/pierrejochem/lotse/pulls/N/merge -f merge_method=merge`. Tell the maintainer that the first real publish happens on the next `v*` tag, which needs `docs/releases/<version>.md` naming the SDKs.
