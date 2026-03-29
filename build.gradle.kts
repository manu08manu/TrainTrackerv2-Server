import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group   = "com.traintracker"
version = "2.0.0"

application {
    mainClass.set("com.traintracker.server.ApplicationKt")
}

repositories {
    mavenCentral()
}

val ktorVersion   = "2.3.10"
val exposedVersion = "0.50.0"

dependencies {
    // ── Ktor server ────────────────────────────────────────────────────────
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
    implementation("io.ktor:ktor-server-compression:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")

    // ── Kafka ──────────────────────────────────────────────────────────────
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    // ── Database — Exposed ORM ─────────────────────────────────────────────
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")

    // Oracle JDBC (Autonomous DB / Free Tier)
    implementation("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09")
    implementation("com.oracle.database.jdbc:ucp:23.3.0.23.09")

    // SQLite — used as fallback when ORACLE_DB_URL is not set
    implementation("org.xerial:sqlite-jdbc:3.45.3.0")

    // Connection pool
    implementation("com.zaxxer:HikariCP:5.1.0")

    // ── HTTP client for CIF download ───────────────────────────────────────
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-auth:$ktorVersion")

    implementation("org.json:json:20240303")

    // ── Serialization + logging ────────────────────────────────────────────
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("ch.qos.logback:logback-classic:1.5.6")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

// Shadow JAR — single fat jar for deployment on Oracle VM
tasks.shadowJar {
    archiveBaseName.set("traintracker-backend")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    mergeServiceFiles()
    // Required for Kafka SASL
    append("META-INF/services/org.apache.kafka.common.security.auth.AuthenticateCallbackHandler")
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
