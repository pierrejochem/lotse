# Installing the SDKs

The Java SDK, its Kotlin layer, the C# SDK and the Node.js SDK are released with the broker, at its version, from the same tag ([ADR 0142](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0142-the-sdks-are-released-with-the-broker-on-github-first.md)). SDK `x.y` speaks to broker `x.y` and to the minor releases next to it, the same window a rolling upgrade uses.

| SDK | Package |
| --- | --- |
| Java | `dev.lotse:lotse-client` |
| Kotlin | `dev.lotse:lotse-client-kotlin` |
| C# | `Lotse.Client`, and `Lotse.Client.Codecs` for zstd and LZ4 |
| Node.js | `@pierrejochem/lotse-client` |

They are on GitHub, in two places: as files on each [GitHub release](https://github.com/pierrejochem/lotse/releases), and in GitHub Packages. Maven Central, nuget.org and npm are not there yet.

## From the release, without a token

Every release lists the SDK packages next to the broker's, with their checksums in `SHA256SUMS-sdk`:

- the Java and Kotlin `.jar`, `-sources.jar`, `-javadoc.jar`, `.pom` and `.module` files;
- the C# `.nupkg` and `.snupkg` files;
- the Node.js `.tgz`.

Download what you need and check it:

```sh
sha256sum --ignore-missing -c SHA256SUMS-sdk
```

Then point your build at the files:

- **npm:** `npm install ./pierrejochem-lotse-client-<version>.tgz`.
- **NuGet:** add the folder as a source: `dotnet nuget add source ./packages --name lotse`.
- **Gradle or Maven:** install the jar and POM into a local repository, or use a `flatDir` or `files(...)` dependency.

## From GitHub Packages, with a token

GitHub Packages asks for a token even for public packages. Make a classic personal access token with the scope `read:packages`, and keep it out of the repository: an environment variable or your user-level configuration.

**Gradle**

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/pierrejochem/lotse")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("dev.lotse:lotse-client:<version>")
    // Kotlin: implementation("dev.lotse:lotse-client-kotlin:<version>")
}
```

For zstd and LZ4, ask for the feature: `implementation("dev.lotse:lotse-client:<version>") { capabilities { requireCapability("dev.lotse:lotse-client-compression") } }`.

**Maven**: a `<server>` with id `github` and your token in `~/.m2/settings.xml`, a `<repository>` with that id and the URL `https://maven.pkg.github.com/pierrejochem/lotse`, and the dependency `dev.lotse:lotse-client`. For zstd and LZ4, add `com.github.luben:zstd-jni` and `at.yawk.lz4:lz4-java` yourself; the POM lists them as optional.

**NuGet**

```sh
dotnet nuget add source https://nuget.pkg.github.com/pierrejochem/index.json \
  --name lotse --username <your GitHub user> --password <token> --store-password-in-clear-text
dotnet add package Lotse.Client --version <version>
dotnet add package Lotse.Client.Codecs --version <version>   # zstd and LZ4
```

**npm**: in `.npmrc` (your home directory's, not the project's):

```
@pierrejochem:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=<token>
```

Then:

```sh
npm install @pierrejochem/lotse-client@<version>
```
