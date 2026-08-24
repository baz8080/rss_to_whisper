package com.rsstowhisper

internal val USAGE =
    """
    Usage: pipeline [options]

    Options:
      --config <path>        Path to pods.yaml                  (PIPELINE_CONFIG_PATH)
      --data-dir <path>      Where audio and transcripts go     (PIPELINE_DATA_DIRECTORY)
      --whisper-url <url>    Base URL of the whisper.cpp server (PIPELINE_WHISPER_SERVER_URL)
      --verbose              Enable debug logging               (PIPELINE_VERBOSE)
      --no-verbose           Force debug logging off
      -h, --help             Show this message

    Options override .env, which overrides pods.yaml. Give a second instance its
    own --config and --whisper-url to run two feeds against two whisper servers.
    """.trimIndent()

/** Null means "not given", so each field can fall through to .env and then pods.yaml. */
internal data class Args(
    val configPath: String? = null,
    val dataDirectory: String? = null,
    val whisperServerUrl: String? = null,
    val verbose: Boolean? = null,
    val help: Boolean = false,
)

internal fun parseArgs(argv: Array<String>): Args {
    var args = Args()
    var i = 0
    while (i < argv.size) {
        val flag = argv[i]
        args =
            when (flag) {
                "--config" -> args.copy(configPath = valueFor(flag, argv, ++i))
                "--data-dir" -> args.copy(dataDirectory = valueFor(flag, argv, ++i))
                "--whisper-url" -> args.copy(whisperServerUrl = valueFor(flag, argv, ++i))
                "--verbose" -> args.copy(verbose = true)
                "--no-verbose" -> args.copy(verbose = false)
                "-h", "--help" -> args.copy(help = true)
                else ->
                    if (flag.startsWith("-")) {
                        error("Unknown option: $flag (try --help)")
                    } else {
                        error("Unexpected argument: $flag (try --help)")
                    }
            }
        i++
    }
    return args
}

private fun valueFor(
    flag: String,
    argv: Array<String>,
    index: Int,
): String =
    argv.getOrNull(index)?.takeUnless { it.startsWith("-") }
        ?: error("$flag needs a value (try --help)")
