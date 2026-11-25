import java.util.zip.ZipFile

plugins {
    kotlin("jvm") version "2.4.21"
    // The Javadoc of a Kotlin source set: javadoc reads Java only, so without it the javadoc jar would be empty.
    id("org.jetbrains.dokka-javadoc") version "2.3.0"
    `java-library`
    `maven-publish`
}

group = "dev.lotse"
version = "0.1.1"

repositories {
    mavenCentral()
}

dependencies {
    // The Java core (substituted by the included build) and coroutines: the layer's only dependencies.
    api("dev.lotse:lotse-client:0.1.1")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    // Java 17 byte code, as the core, whatever JDK runs the build.
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    // Maven Central asks for both (ADR 0142); GitHub Packages takes them too.
    withSourcesJar()
    withJavadocJar()
}

kotlin {
    explicitApi()
    compilerOptions {
        allWarningsAsErrors = true
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17
    }
}

tasks.test {
    useJUnitPlatform()
}

// Examples (src/examples): compiled by `check` so they keep up with the API.
val examples: SourceSet by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
    runtimeClasspath += output + compileClasspath
}

// The javadoc jar holds Dokka's Javadoc of the Kotlin API, which Maven Central asks for (ADR 0142).
tasks.named<Jar>("javadocJar") {
    from(tasks.named("dokkaGeneratePublicationJavadoc"))
}

// Release packages (ADR 0142): the same bytes from the same commit, so two builds can be compared.
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "lotse-client-kotlin"
            pom {
                name = "Lotse client for Kotlin"
                description = "Kotlin layer on the Lotse Java client: suspend functions, flows and transactions."
                url = "https://github.com/pierrejochem/lotse"
                licenses {
                    license {
                        name = "Apache-2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                developers {
                    developer {
                        id = "pierrejochem"
                        name = "Pierre Jochem"
                    }
                }
                scm {
                    url = "https://github.com/pierrejochem/lotse"
                    connection = "scm:git:https://github.com/pierrejochem/lotse.git"
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/pierrejochem/lotse")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}

// The core comes from the included build during development; the published layer must name the core's release at
// exactly this version and nothing local, or a user would get a dependency no registry holds (ADR 0142).
val verifyPublishedPom by tasks.registering {
    description = "Fails when the published POM or module does not name dev.lotse:lotse-client at this version."
    group = "verification"
    val pom = tasks.named<GenerateMavenPom>("generatePomFileForMavenPublication")
    val module = tasks.named<GenerateModuleMetadata>("generateMetadataFileForMavenPublication")
    dependsOn(pom, module)
    val pomFile = layout.buildDirectory.file("publications/maven/pom-default.xml")
    val moduleFile = layout.buildDirectory.file("publications/maven/module.json")
    val expected = project.version.toString()
    val local = rootDir.parentFile.canonicalPath
    doLast {
        val pomText = pomFile.get().asFile.readText()
        val core = Regex(
            "<groupId>dev\\.lotse</groupId>\\s*<artifactId>lotse-client</artifactId>\\s*<version>([^<]*)</version>"
        ).find(pomText) ?: throw GradleException("the POM does not depend on dev.lotse:lotse-client")
        if (core.groupValues[1] != expected) {
            throw GradleException("the POM names lotse-client ${core.groupValues[1]}, not $expected")
        }
        for (text in listOf(pomText, moduleFile.get().asFile.readText())) {
            if (text.contains(local) || text.contains("../java")) {
                throw GradleException("the published metadata names a local path")
            }
        }
    }
}

// An empty javadoc jar passes every registry, so check it documents the API: its index and the page of the entry point.
val verifyJavadocJar by tasks.registering {
    description = "Fails when the javadoc jar lacks index.html or the page of dev.lotse.kotlin.Lotse."
    group = "verification"
    val jar = tasks.named<Jar>("javadocJar")
    dependsOn(jar)
    val jarFile = jar.flatMap { it.archiveFile }
    doLast {
        val names = ZipFile(jarFile.get().asFile).use { zip -> zip.entries().toList().map { it.name } }
        val missing = listOf("index.html", "dev/lotse/kotlin/Lotse.html").filter { it !in names }
        if (missing.isNotEmpty()) {
            throw GradleException("the javadoc jar lacks ${missing.joinToString()}; it holds ${names.size} entries")
        }
    }
}

tasks.check {
    dependsOn(tasks.named(examples.classesTaskName), verifyPublishedPom, verifyJavadocJar)
}
