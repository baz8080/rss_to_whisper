plugins {
    kotlin("jvm") version "2.1.0"
    application
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2"
}

group = "com.rsstowhisper"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("com.rsstowhisper.MainKt")
    applicationDefaultJvmArgs = listOf("-Xmx4g")
}

repositories {
    mavenCentral()
}

dependencies {
    // Whisper.cpp JNA bindings
    implementation("io.github.ggerganov:whispercpp:1.4.0")
    implementation("net.java.dev.jna:jna:5.13.0")

    // RSS parsing
    implementation("com.rometools:rome:2.1.0")
    implementation("com.rometools:rome-modules:2.1.0")

    // HTTP client
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // Elasticsearch 8.x Java client
    implementation("co.elastic.clients:elasticsearch-java:8.17.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")

    // YAML config (Jackson)
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.2")

    // MP3 decoding (MP3 -> PCM float[])
    implementation("com.googlecode.soundlibs:mp3spi:1.9.5.4")

    // Environment variable loading (.env)
    implementation("io.github.cdimascio:dotenv-kotlin:6.4.2")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.5.15")
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:1.13.16")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.11.4")
}

tasks.test {
    useJUnitPlatform()
}
