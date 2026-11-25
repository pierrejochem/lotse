# 142. The SDKs are released with the broker, on GitHub first

- Status: Accepted (supersedes the lines of [ADR 0056](0056-client-sdks-for-the-jvm.md) and [ADR 0083](0083-client-sdks-for-csharp-and-nodejs.md) that keep publishing out of scope and call the package names placeholders)
- Date: 2026-11-25

## Context

The Java SDK with its Kotlin layer, the C# SDK and the Node.js SDK cover the whole native protocol and pass the golden vectors, but none had ever been released. ADR 0056 and ADR 0083 left publishing out of scope, the package names were placeholders and the versions `0.0.1` or `0.0.0`, so every user built an SDK from source.

There are two homes for packages:

- **The public registries** (Maven Central, nuget.org, npm). They need accounts and secrets only the maintainer can create, and Maven Central grants the group `dev.lotse` only to the owner of `lotse.dev`.
- **GitHub Packages and the GitHub release.** They need nothing new, but Maven, NuGet and npm users must sign in with a GitHub token to download from GitHub Packages, even for a public repository.

The release workflow (ADR 0126, ADR 0138) builds the broker and the console on a `v*` tag after the simulations pass. It creates the GitHub release with the workflow's own token, and GitHub starts no workflow on an event that token caused.

## Decision

**GitHub first, the public registries later.** On every release the SDK packages go to GitHub Packages and to the GitHub release. The steps for Maven Central, nuget.org and npm are present but run only when a repository variable (`PUBLISH_MAVEN_CENTRAL`, `PUBLISH_NUGET`, `PUBLISH_NPM`) is `true` and the matching secret is set:

- Maven Central: `MAVEN_CENTRAL_TOKEN`, `MAVEN_GPG_KEY`, `MAVEN_GPG_PASSPHRASE`;
- nuget.org: `NUGET_API_KEY`;
- npm: `NPM_TOKEN`.

**The broker's version, on the same tag.** A tag `v<version>` releases the broker, the console and all four SDKs as `<version>`. SDK `x.y` speaks to broker `x.y` and its neighbours, the window of rolling upgrades (ADR 0127). Every SDK's version in the repository equals `Cargo.toml`'s (`packaging/sdk-versions.sh` prints them), and the release workflow's `notes` job refuses a tag whose SDK versions differ, before anything is built.

**The names.**

| SDK | Package |
| --- | --- |
| Java | `dev.lotse:lotse-client`, the codecs as the Gradle feature `compression` (ADR 0061), optional dependencies in the POM |
| Kotlin | `dev.lotse:lotse-client-kotlin`, depending on `dev.lotse:lotse-client` at exactly its own version |
| C# | `Lotse.Client`; `Lotse.Client.Codecs`, depending on `Lotse.Client` at exactly `[<version>]` |
| Node.js | `@pierrejochem/lotse-client` (GitHub Packages takes an npm package only under the owner's scope) |

Every package names the license Apache-2.0, the repository and the maintainer. Java and Kotlin ship sources and javadoc jars and a full POM, which Maven Central asks for. A name is never rewritten at publish time. A later move of the Node.js package to `@lotse/client` on npm is a rename in the change that turns npm on.

**A workflow of its own, after the release.** `.github/workflows/sdk-release.yml` runs on `workflow_run` of `release` and goes on only when that run succeeded for a pushed `v*` tag. The `release` event cannot be used, for the reason above. By then the GitHub release and the broker's binaries exist. It also runs by hand: with a `v*` tag it re-runs that release's SDKs, and without one it publishes nothing.

**Two stages.**

1. One build job per SDK:
   - runs the SDK's tests against the tag's released `lotse`;
   - builds its packages twice with `packaging/sdk-package.sh`, the second time from scratch;
   - compares the two builds byte for byte with `packaging/sdk-compare.sh`, as ADR 0126 does for the binaries.
2. Only when all four passed, one job publishes with `packaging/sdk-publish.sh`:
   - first the packages and `SHA256SUMS-sdk` to the GitHub release;
   - then GitHub Packages, Java before Kotlin, whose POM names the Java package;
   - then the registries that are switched on.

**Deterministic packages.**

- **Gradle:** archives without timestamps and in a fixed order.
- **npm:** `npm pack` writes fixed times by itself.
- **.NET:** `dotnet pack` on .NET 10 names the package's metadata part at random, gives its relationships random ids and stamps the zip with the build time, while the assemblies are already deterministic. `packaging/nupkg-normalise.py` replaces those three with values derived from the package and the commit. .NET 11 does this by itself, and the script goes when the SDK moves there.

**Re-runs.** The release's `SHA256SUMS-sdk` is the record. A re-run whose packages differ from it is refused. A registry that already holds the version counts as done, since the sums matched. The publish job rebuilds the Maven packages with Gradle, which cannot upload files it did not build, and checks them against the compared files first.

**On pull requests** that touch the SDKs or these scripts, the workflow tests, builds and compares, and checks `sdk-publish.sh` against stand-ins (`packaging/test-sdk-publish.sh`); it publishes nothing.

**No MCP tools.** Releasing is the maintainer's CI step, not something an operator or developer does against a cluster with `lotse mcp`.

## Consequences

- An SDK release needs no step beyond the tag. If the SDK workflow fails after the broker release, the fix is to start it by hand with that tag.
- Users without a GitHub token download the packages from the release and check them against `SHA256SUMS-sdk`. Users with one depend on GitHub Packages with a normal dependency line and a repository entry.
- A fix in one SDK alone needs a release of everything with a new number.
- Turning on a public registry takes an account, a secret and a variable. Maven Central also needs a verified namespace: the domain `lotse.dev`, or another group id, which would be a rename.
- The Maven Central, nuget.org and npm steps have not run yet; the first time each is switched on is its test.
- The first real publish to GitHub Packages happens on the first `v*` tag after this change.
