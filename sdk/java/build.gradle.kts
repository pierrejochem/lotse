plugins {
    `java-library`
}

group = "dev.lotse"
version = "0.0.1"

repositories {
    mavenCentral()
}

// zstd and LZ4 batches (ADR 0061): optional. Published as the feature `compression`, so a build that wants it
// asks for it (Gradle: `requireCapability("dev.lotse:lotse-client-compression")`; Maven: the two artifacts
// below, which the POM lists as optional). Without them the client works, and refuses compressed batches with
// an error that names the missing library.
java {
    registerFeature("compression") {
        usingSourceSet(sourceSets["main"])
    }
}

val zstdJni = "com.github.luben:zstd-jni:1.5.7-25"
// The maintained fork of lz4-java, with the fixes for CVE-2025-12183 and CVE-2025-66566.
val lz4Java = "at.yawk.lz4:lz4-java:1.13.0"

dependencies {
    // Nothing else: the client uses the JDK only.
    "compressionImplementation"(zstdJni)
    "compressionImplementation"(lz4Java)
    testImplementation(zstdJni)
    testImplementation(lz4Java)
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 17
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
}

tasks.withType<Test>().configureEach {
    // The golden vectors live next to the Rust code that writes them.
    systemProperty("lotse.conformance", rootDir.resolve("../conformance").canonicalPath)
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.test {
    useJUnitPlatform { excludeTags("without-codecs") }
}

// The same tests' classes with the codec libraries left out, as a user without them runs the client.
val testWithoutCodecs by tasks.registering(Test::class) {
    description = "Runs the tests tagged without-codecs with zstd-jni and lz4-java off the class path."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath.filter {
        !it.name.startsWith("zstd-jni") && !it.name.startsWith("lz4-java")
    }
    useJUnitPlatform { includeTags("without-codecs") }
}

// Examples (src/examples): programs to run against a cluster, compiled by `check` so they keep up with the API.
val examples: SourceSet by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
    runtimeClasspath += output + compileClasspath
}

tasks.check {
    dependsOn(testWithoutCodecs, tasks.named(examples.classesTaskName))
}
