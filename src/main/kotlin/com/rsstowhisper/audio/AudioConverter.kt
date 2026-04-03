package com.rsstowhisper.audio

import java.nio.file.Path

class AudioConverter {
    companion object {
        private const val WHISPER_SAMPLE_RATE = 16000
    }

    fun mp3ToWav(mp3Path: Path): Path {
        val wavPath = mp3Path.resolveSibling("audio.wav")
        convertWithFfmpeg(mp3Path, wavPath)
        return wavPath
    }

    private fun convertWithFfmpeg(
        mp3Path: Path,
        wavPath: Path,
    ) {
        val process =
            ProcessBuilder(
                "ffmpeg",
                "-i", mp3Path.toString(),
                "-ar", WHISPER_SAMPLE_RATE.toString(),
                "-ac", "1",
                "-y",
                wavPath.toString(),
            )
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start()

        // Read stderr concurrently to prevent pipe buffer from filling and deadlocking
        val stderrFuture =
            java.util.concurrent.CompletableFuture.supplyAsync {
                process.errorStream.bufferedReader().readText()
            }

        val exitCode = process.waitFor()
        val errorOutput = stderrFuture.get()

        if (exitCode != 0) {
            throw RuntimeException("ffmpeg failed with exit code $exitCode: $errorOutput")
        }
    }
}
