plugins {
    `java-library`
}

group = "dev.lotse"
version = "0.0.1"

repositories {
    mavenCentral()
}


dependencies {
    // No runtime dependencies: the client uses the JDK only.
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
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

tasks.test {
    useJUnitPlatform()
    // The golden vectors live next to the Rust code that writes them.
    systemProperty("lotse.conformance", rootDir.resolve("../conformance").canonicalPath)
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
