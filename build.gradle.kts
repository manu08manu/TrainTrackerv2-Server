plugins {
    kotlin("jvm") version "2.4.21"
    kotlin("plugin.serialization") version "2.4.21"
    id("com.gradleup.shadow") version "9.6.1"
    application
}

group = "com.traintracker"
version = "2.0.0"

val ktorVersion = "3.6.1"
val exposedVersion = "1.3.1"
val kafkaVersion = "4.4.0"

repositories { mavenCentral() }

kotlin {
    jvmToolchain(25)
}

application { mainClass.set("com.traintracker.server.ApplicationKt") }

dependencies {
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("io.ktor:ktor-server-compression:$ktorVersion")
    implementation("io.ktor:ktor-server-caching-headers:$ktorVersion")
    implementation("io.ktor:ktor-server-conditional-headers:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-auth:$ktorVersion")

    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-migration-jdbc:$exposedVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    implementation("com.zaxxer:HikariCP:7.1.0")
    implementation("org.xerial:sqlite-jdbc:3.54.1.0")
    implementation("org.json:json:20260814")
    implementation("org.mindrot:jbcrypt:0.4")
    implementation("ch.qos.logback:logback-classic:1.6.6")
    implementation("org.slf4j:slf4j-api:2.0.20")
}

tasks.shadowJar {
    archiveFileName.set("traintracker-backend-2.0.0.jar")
    manifest {
        attributes["Main-Class"] = "com.traintracker.server.ApplicationKt"
        attributes["Enable-Native-Access"] = "ALL-UNNAMED"
    }

    // Allow the Kotlin metadata transformer to process all matching entries.
    val transformedPaths = listOf("META-INF/*.kotlin_module")

    filesMatching(transformedPaths) {
        duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.INCLUDE
    }

    // Include this configuration in Gradle's task inputs.
    inputs.property("transformedPathsDuplicatesStrategy", "$transformedPaths=INCLUDE")

    // Fail if duplicate ZIP entries remain in the resulting JAR.
    failOnDuplicateEntries.set(true)
}

tasks.build { dependsOn(tasks.shadowJar) }
