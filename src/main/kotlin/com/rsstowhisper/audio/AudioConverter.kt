package com.rsstowhisper.audio

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Path
import javax.sound.sampled.AudioFormat
import javax.sound.sampled.AudioSystem

class AudioConverter {
    private val logger = LoggerFactory.getLogger(AudioConverter::class.java)

    companion object {
        private const val WHISPER_SAMPLE_RATE = 16000f
    }

    fun mp3ToFloatPcm(mp3Path: Path): FloatArray {
        return try {
            convertWithJavaxSound(mp3Path)
        } catch (e: Exception) {
            logger.warn("MP3SPI failed, falling back to ffmpeg: ${e.message}")
            convertWithFfmpeg(mp3Path)
        }
    }

    private fun convertWithJavaxSound(mp3Path: Path): FloatArray {
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
        val pcmBytes = decodedStream.readAllBytes()
        decodedStream.close()
        mp3Stream.close()

        return bytesToFloatArray(pcmBytes)
    }

    private fun convertWithFfmpeg(mp3Path: Path): FloatArray {
        val process =
            ProcessBuilder(
                "ffmpeg",
                "-i",
                mp3Path.toString(),
                "-ar",
                WHISPER_SAMPLE_RATE.toInt().toString(),
                "-ac",
                "1",
                "-f",
                "s16le",
                "-",
            ).redirectErrorStream(false).start()

        val pcmBytes = process.inputStream.readAllBytes()
        val exitCode = process.waitFor()

        if (exitCode != 0) {
            val errorOutput = process.errorStream.bufferedReader().readText()
            throw RuntimeException("ffmpeg failed with exit code $exitCode: $errorOutput")
        }

        return bytesToFloatArray(pcmBytes)
    }

    private fun bytesToFloatArray(pcmBytes: ByteArray): FloatArray {
        val shortBuffer = ByteBuffer.wrap(pcmBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        val samples = FloatArray(shortBuffer.remaining())
        for (i in samples.indices) {
            samples[i] = shortBuffer.get(i).toFloat() / 32768.0f
        }
        return samples
    }
}
