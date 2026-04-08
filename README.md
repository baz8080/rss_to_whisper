# rss_to_whisper

Transcribe podcast episodes from RSS feeds using [whisper.cpp](https://github.com/ggml-org/whisper.cpp) and index them into a local SQLite FTS4 database for full-text search.

## Modules

### `pipeline` (Kotlin)
Walks one or more RSS feeds, downloads each episode, decodes the audio with `ffmpeg`, transcribes it with `whisper-cli`, and writes a `transcript.json` file alongside the audio. Designed to run on a schedule (e.g. cron) to keep transcripts up to date.

### `web` (Kotlin)
A Ktor HTTP server that serves a full-text search interface over the SQLite database produced by `index.py`. Supports filtering by podcast, tag, and date range. Runs on port 8080 by default.

## Python scripts

### `index.py`
Reads all `transcript.json` files in the data directory and writes them into a SQLite FTS4 database. Run this after the pipeline to make new transcripts searchable. Requires only Python 3 stdlib.

### `fix_tags.py`
One-off utility to clean up `all_tags` in existing `transcript.json` files — trims whitespace, lowercases, removes duplicates, and drops tags of 2 characters or fewer. Run with `--dry-run` first to preview changes.

```bash
python3 fix_tags.py /path/to/data_directory --dry-run
python3 fix_tags.py /path/to/data_directory
```

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

```bash
python3 index.py /path/to/data_directory

# Specify a custom database path
python3 index.py /path/to/data_directory --db /path/to/podcasts.db
```

The database defaults to `podcasts.db` inside the data directory. Designed to run directly on the machine hosting the files to avoid network filesystem overhead.

## Serving the web UI

```bash
./gradlew :web:run --args="/path/to/podcasts.db"

# With a custom audio base URL and port
./gradlew :web:run --args="/path/to/podcasts.db https://audio.example.com 9090"
```

Or build and run the distribution:

```bash
./gradlew installDist
./build/install/rss-to-whisper/bin/web /path/to/podcasts.db
```

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
