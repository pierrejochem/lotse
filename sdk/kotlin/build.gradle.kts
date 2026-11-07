plugins {
    kotlin("jvm") version "2.2.20"
    `java-library`
}

group = "dev.lotse"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    // The Java core (substituted by the included build) and coroutines: the layer's only dependencies.
    api("dev.lotse:lotse-client:0.0.1")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    // Java 17 byte code, as the core, whatever JDK runs the build.
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
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
