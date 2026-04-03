package com.rsstowhisper.audio

import org.slf4j.LoggerFactory
import java.nio.file.Path
import javax.sound.sampled.AudioFileFormat
import javax.sound.sampled.AudioFormat
import javax.sound.sampled.AudioSystem

class AudioConverter {
    private val logger = LoggerFactory.getLogger(AudioConverter::class.java)

    companion object {
        private const val WHISPER_SAMPLE_RATE = 16000f
    }

    fun mp3ToWav(mp3Path: Path): Path {
        val wavPath = mp3Path.resolveSibling("audio.wav")

        return try {
            convertWithJavaxSound(mp3Path, wavPath)
            wavPath
        } catch (e: Exception) {
            logger.warn("MP3SPI failed, falling back to ffmpeg: ${e.message}")
            convertWithFfmpeg(mp3Path, wavPath)
            wavPath
        }
    }

    private fun convertWithJavaxSound(mp3Path: Path, wavPath: Path) {
        val mp3Stream = AudioSystem.getAudioInputStream(mp3Path.toFile())
        val decodedFormat =
            AudioFormat(
                AudioFormat.Encoding.PCM_SIGNED,
                WHISPER_SAMPLE_RATE,
                16,
                1,
                2,
                WHISPER_SAMPLE_RATE,
                false,
            )
        val decodedStream = AudioSystem.getAudioInputStream(decodedFormat, mp3Stream)
        AudioSystem.write(decodedStream, AudioFileFormat.Type.WAVE, wavPath.toFile())
        decodedStream.close()
        mp3Stream.close()
    }

    private fun convertWithFfmpeg(mp3Path: Path, wavPath: Path) {
        val process =
            ProcessBuilder(
                "ffmpeg",
                "-i", mp3Path.toString(),
                "-ar", WHISPER_SAMPLE_RATE.toInt().toString(),
                "-ac", "1",
                "-y",
                wavPath.toString(),
            ).redirectErrorStream(false).start()

        val exitCode = process.waitFor()

        if (exitCode != 0) {
            val errorOutput = process.errorStream.bufferedReader().readText()
            throw RuntimeException("ffmpeg failed with exit code $exitCode: $errorOutput")
        }
    }
}
