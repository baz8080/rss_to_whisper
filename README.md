# rss_to_whisper

Transcribe podcast episodes from RSS feeds using [whisper.cpp](https://github.com/ggml-org/whisper.cpp) and index them into a local SQLite FTS5 database for full-text search.

## Prerequisites

- JDK 21+
- `ffmpeg` on `PATH` (used to decode downloaded MP3s to WAV)
- `whisper-cli` on `PATH` (from [whisper.cpp](https://github.com/ggml-org/whisper.cpp); invoked per episode)
- A whisper.cpp compatible model file (see below)
- Python 3 (for the indexing script; uses only stdlib modules)

On macOS both are available via Homebrew: `brew install ffmpeg whisper-cpp`.

## Building

```bash
./gradlew build
```

## Transcribing

```bash
./gradlew run --args="-c pods.yaml"
```

Or build and run the distribution:

```bash
./gradlew installDist
./build/install/rss-to-whisper/bin/rss-to-whisper -c pods.yaml
```

## Indexing

Indexing is a standalone Python script that reads `transcript.json` files and writes them to a SQLite FTS5 database. It is designed to run directly on the machine hosting the data directory to avoid network filesystem overhead.

```bash
python3 index.py /path/to/data_directory

# Specify a custom database path
python3 index.py /path/to/data_directory --db /path/to/podcasts.db
```

The database defaults to `podcasts.db` inside the data directory.

## Configuration

Copy and edit `pods.yaml` to configure:

- `data_directory` — where episode files are stored
- `whisper_model` — path to the whisper.cpp model file (`.bin`)
- `verbose` — enable debug logging (optional, default `false`)
- `skip_after_consecutive` — stop walking a feed once this many consecutive already-transcribed episodes are seen (optional, default `20`)
- `podcasts` — list of RSS feeds to process, each with `name`, `url`, optional `collections`, and optional `excludes`

### Skip heuristic

Feeds are typically ordered newest-first. Rather than stat'ing every episode directory (expensive for feeds with thousands of entries), the transcriber walks the feed and stops on a podcast once it sees `skip_after_consecutive` transcribed episodes in a row. The counter resets on any gap, so a cancelled run that left untranscribed holes will be picked up on the next invocation.

## Downloading Whisper Models

whisper.cpp uses its own GGML model format (`.bin` files), **not** the OpenAI Python `.pt` files.

Download models from HuggingFace:

```bash
# Tiny model (~75 MB) - fastest, lowest quality
curl -L -o ggml-tiny.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.bin

# Base model (~142 MB)
curl -L -o ggml-base.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.bin

# Small model (~466 MB)
curl -L -o ggml-small.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-small.bin

# Medium model (~1.5 GB)
curl -L -o ggml-medium.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-medium.bin

# Large v3 turbo (~809 MB) - recommended balance of speed and quality
curl -L -o ggml-large-v3-turbo.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3-turbo.bin
```

Set `whisper_model` in `pods.yaml` to the path of your downloaded model file.

## Output Structure

For each episode, the following files are created:

```
{data_directory}/{podcast_name}/{YYYY-MM-DD-episode-title}/
    audio.wav                      # Decoded audio (mp3 is removed after transcription)
    transcript_with_timing.tsv     # Sentence-grouped with timestamps
    transcript.json                # Full metadata + transcript
```

## Testing

```bash
./gradlew test
```

## Linting

```bash
./gradlew ktlintCheck
```

To auto-format:

```bash
./gradlew ktlintFormat
```

## Platform Support

- **macOS** — Metal acceleration (Apple Silicon), CPU fallback
- **Linux** — CUDA acceleration (requires a `whisper-cli` built with CUDA), CPU fallback

Acceleration is determined by how your `whisper-cli` binary was built, so install/build the variant that matches your hardware.
