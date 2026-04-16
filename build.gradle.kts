plugins {
    kotlin("jvm") version "2.3.20" apply false
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2" apply false
    id("io.quarkus") version "3.33.1" apply false
}

allprojects {
    group = "com.rsstowhisper"
    version = "2.0.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    // :web applies kotlin("jvm"), ktlint, and io.quarkus declaratively in its own
    // plugins {} block (order matters for Quarkus's allopen/noarg configuration).
    // Applying them here via the imperative API on top of that causes conflicts.
    if (project.name != "web") {
        apply(plugin = "org.jetbrains.kotlin.jvm")
        apply(plugin = "org.jlleitschuh.gradle.ktlint")
    }

    // Configure the Java toolchain lazily so it works whether the Java plugin
    // arrives from the imperative apply() above or from a declarative plugins {}
    // block in the subproject (as is the case for :web).
    pluginManager.withPlugin("java") {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
