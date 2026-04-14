plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "rss-to-whisper"

include(":pipeline")
include(":web")
