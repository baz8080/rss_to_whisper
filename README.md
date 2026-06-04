# rss_to_whisper

Transcribe podcast episodes from RSS feeds using the [whisper.cpp](https://github.com/ggml-org/whisper.cpp) HTTP server and index them into a local SQLite FTS5 database for full-text search.

## Modules

### `pipeline` (Kotlin)
Walks one or more RSS feeds, downloads each episode, decodes the audio with `ffmpeg`, and POSTs it to a running whisper.cpp HTTP server. The server returns a [WebVTT](https://www.w3.org/TR/webvtt1/) transcript, which is stored in a `transcript.json` file alongside the audio. Designed to run on a schedule (e.g. cron) to keep transcripts up to date.

### `web` (Kotlin)
A Quarkus HTTP server that serves a full-text search interface over the SQLite database produced by `index.py`. Supports filtering by podcast, collection, episode type, and duration. Search results are ranked by BM25 relevance. The episode detail page shows a clickable transcript synced to the audio player. Runs on port 8080 by default.

## Python scripts

### `index.py`
Reads all `transcript.json` files in the data directory and writes them into a SQLite FTS5 database. Run this after the pipeline to make new transcripts searchable.

Requires Python 3 and `pysqlite3` with FTS5 support:

```bash
pip install pysqlite3
```

> **Note:** The default system `sqlite3` on some platforms (e.g. Synology NAS) does not include FTS5. `pysqlite3` bundles a SQLite build that does.


## Prerequisites

- JDK 21+
- `ffmpeg` on `PATH` (used to decode downloaded MP3s to WAV)
- A running [whisper.cpp HTTP server](#running-the-whispercpp-server)
- Python 3 + `pysqlite3` (for the indexing script)

On macOS `ffmpeg` is available via Homebrew: `brew install ffmpeg`.

## Building

```bash
./gradlew build
```

## Running the whisper.cpp server

The pipeline POSTs audio to the whisper.cpp `/inference` endpoint. Start the server before running the pipeline:

```bash
./server \
  --model /path/to/models/ggml-large-v3-turbo.bin \
  --host 0.0.0.0 \
  --port 8080
```

The `server` binary is built alongside `whisper-cli` when you compile whisper.cpp. Set `PIPELINE_WHISPER_SERVER_URL` in `pipeline/.env` to the server's base URL (e.g. `http://localhost:8080`).

### Choosing a model

whisper.cpp uses its own GGML model format (`.bin` files), **not** the OpenAI Python `.pt` files.

Download models from HuggingFace:

```bash
# Tiny model (~75 MB) - fastest, lowest quality
curl -L -o ggml-tiny.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.bin

# Large v3 turbo (~809 MB) - recommended balance of speed and quality
curl -L -o ggml-large-v3-turbo.bin https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3-turbo.bin
```

You will need the corresponding .mlmodelc file for your model, for example: 

```
# For the large v3 turbo model. Download, unzip, and put int the same directory as the bin model.
curl -L -o ggml-large-v3-turbo-encoder.mlmodelc.zip https://huggingface.co/ggerganov/whisper.cpp/blob/main/ggml-large-v3-turbo-encoder.mlmodelc.zip
```

### Platform acceleration

Acceleration is determined by how the whisper.cpp `server` binary was compiled:

- **macOS (Apple Silicon)** — Metal acceleration, built in by default
- **Linux** — CUDA acceleration if built with `WHISPER_CUDA=1`; CPU fallback otherwise

## Transcribing

The pipeline requires a config file path, supplied either via `--config` or via `PIPELINE_CONFIG_PATH` in `pipeline/.env` (see [Pipeline configuration](#pipeline-configuration) below).

```bash
./gradlew :pipeline:run --args="-c /path/to/pods.yaml"
```

Or build and run the distribution:

```bash
./gradlew :pipeline:installDist
./pipeline/build/install/pipeline/bin/pipeline -c /path/to/pods.yaml
```

## Indexing

```bash
python3 index.py /path/to/data_directory

# Specify a custom database path
python3 index.py /path/to/data_directory --db /path/to/podcasts.db
```

The database defaults to `podcasts.db` inside the data directory. Designed to run directly on the machine hosting the files to avoid network filesystem overhead.

## Serving the web UI

Copy `.env.example` to `.env` and fill in your values (`.env` is gitignored):

```bash
cd web
cp .env.example .env
```

```ini
APP_DB_PATH=/path/to/podcasts.db
APP_AUDIO_BASE_URL=http://your-nas:9280
```

Quarkus picks up `.env` automatically. Alternatively, override properties inline:

**Development** (live reload on template/code changes):

```bash
./gradlew :web:quarkusDev
```

**Production** — build a runnable JAR then launch it:

```bash
./gradlew :web:build
java -jar web/build/quarkus-app/quarkus-run.jar
```

Configuration properties can also be overridden at launch without editing the file:

```bash
java -Dapp.db.path=/data/podcasts.db \
     -Dapp.audio.base-url=http://nas:9280 \
     -jar web/build/quarkus-app/quarkus-run.jar
```

## Pipeline configuration

### Environment (`.env`)

Copy `pipeline/.env.example` to `pipeline/.env` and fill in your values (`.env` is gitignored):

```ini
PIPELINE_DATA_DIRECTORY=/path/to/download-directory
PIPELINE_WHISPER_SERVER_URL=http://localhost:8080
PIPELINE_CONFIG_PATH=/path/to/pods.yaml
```

`PIPELINE_CONFIG_PATH` can be overridden at runtime with `--config` / `-c`.

### `pods.yaml`

- `verbose` — enable debug logging (optional, default `false`)
- `skip_after_consecutive` — stop walking a feed once this many consecutive already-transcribed episodes are seen (optional, default `20`)
- `podcasts` — list of RSS feeds to process, each with `name`, `url`, optional `collections`, and optional `excludes`

### Skip heuristic

Feeds are typically ordered newest-first. Rather than stat'ing every episode directory (expensive for feeds with thousands of entries), the transcriber walks the feed and stops on a podcast once it sees `skip_after_consecutive` transcribed episodes in a row. The counter resets on any gap, so a cancelled run that left untranscribed holes will be picked up on the next invocation.

## Output structure

For each episode, the following files are created:

```
{data_directory}/{podcast_name}/{YYYY-MM-DD-episode-title}/
    audio.wav                      # Decoded audio (mp3 is removed after transcription)
    transcript.json                # Full metadata + WebVTT transcript
```

The `episode_transcript` field in `transcript.json` is a raw WebVTT string, as returned by the whisper.cpp server.

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
