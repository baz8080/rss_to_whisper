package com.rsstowhisper.transcription

import com.sun.jna.Pointer
import io.github.ggerganov.whispercpp.WhisperCppJnaLibrary
import io.github.ggerganov.whispercpp.params.CBool
import io.github.ggerganov.whispercpp.params.WhisperFullParams
import io.github.ggerganov.whispercpp.params.WhisperSamplingStrategy
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path

class TranscriptionService(private val modelPath: String) {
    private val logger = LoggerFactory.getLogger(TranscriptionService::class.java)
    private val lib: WhisperCppJnaLibrary = WhisperCppJnaLibrary.instance
    private var ctx: Pointer? = null

    private fun ensureContext() {
        if (ctx == null) {
            logger.debug("Loading whisper model from $modelPath")
            if (!Files.exists(Path.of(modelPath))) {
                throw FileNotFoundException("Model file not found: $modelPath")
            }
            ctx =
                lib.whisper_init_from_file(modelPath)
                    ?: throw RuntimeException("Failed to initialize whisper context from $modelPath")
        }
    }

    fun transcribe(audioData: FloatArray): List<TranscriptSegment> {
        ensureContext()
        val context = ctx!!

        val paramsPointer = lib.whisper_full_default_params_by_ref(WhisperSamplingStrategy.WHISPER_SAMPLING_GREEDY.ordinal)
        val params = WhisperFullParams(paramsPointer)
        params.read()
        params.language = "en"
        params.print_progress = CBool.FALSE
        params.print_realtime = CBool.FALSE
        params.print_timestamps = CBool.FALSE
        params.write()

        val result = lib.whisper_full(context, params, audioData, audioData.size)
        if (result != 0) {
            throw RuntimeException("whisper_full failed with code $result")
        }

        val nSegments = lib.whisper_full_n_segments(context)
        val segments = mutableListOf<TranscriptSegment>()

        for (i in 0 until nSegments) {
            // whisper.cpp returns timestamps in centiseconds (10ms units), convert to ms
            val t0 = lib.whisper_full_get_segment_t0(context, i) * 10
            val t1 = lib.whisper_full_get_segment_t1(context, i) * 10
            val text = lib.whisper_full_get_segment_text(context, i) ?: ""

            segments.add(TranscriptSegment(startMs = t0, endMs = t1, text = text))
        }

        return segments
    }

    fun close() {
        ctx?.let {
            lib.whisper_free(it)
            ctx = null
        }
    }
}

data class TranscriptSegment(
    val startMs: Long,
    val endMs: Long,
    val text: String,
)
