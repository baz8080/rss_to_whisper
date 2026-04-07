plugins {
    application
}

val ktorVersion = "3.1.3"

application {
    mainClass.set("com.rsstowhisper.web.ApplicationKt")
}

dependencies {
    // Ktor server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-html-builder:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")

    // Kotlinx HTML DSL
    implementation("org.jetbrains.kotlinx:kotlinx-html-jvm:0.12.0")

    // SQLite JDBC
    implementation("org.xerial:sqlite-jdbc:3.49.1.0")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.5.32")
    implementation("org.slf4j:slf4j-api:2.0.17")

    // Testing
    testImplementation(kotlin("test"))
}
