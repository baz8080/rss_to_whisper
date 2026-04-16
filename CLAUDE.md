# CLAUDE.md

## Project overview

Three-stage pipeline for podcast transcription and full-text search:

1. **`pipeline`** (Kotlin) — downloads RSS episodes, decodes audio with ffmpeg, transcribes with whisper-cli. Writes `transcript.json` alongside each audio file.
2. **`index.py`** (Python 3) — walks the data directory, reads `transcript.json` files, and loads them into a SQLite FTS5 database.
3. **`web`** (Kotlin / Quarkus) — REST server on port 8080 serving a full-text search UI over the SQLite database.

## Build

```bash
./gradlew build          # build everything
./gradlew ktlintCheck    # lint check (ktlint is enforced)
./gradlew ktlintFormat   # auto-fix lint issues
```

## Running

### Pipeline (transcription)

```bash
./gradlew :pipeline:run --args="-c pods.yaml"
```

Configure `pods.yaml` with:
- `data_directory` — where audio and transcripts are stored
- `whisper_model` — path to a GGML model file (e.g. `ggml-large-v3-turbo.bin`)
- `podcasts` — list of RSS feeds with `name`, `url`, and `collections` tags

External tools required on `PATH`: `ffmpeg`, `whisper-cli`

### Indexer

```bash
python3 index.py /path/to/data_directory [--db /path/to/podcasts.db]
```

Requires `pysqlite3` (the system `sqlite3` may lack FTS5 support).

Run this after the pipeline has produced new transcripts.

### Web server

Development (hot reload):
```bash
./gradlew :web:quarkusDev
```

Production:
```bash
./gradlew :web:build
java -jar web/build/quarkus-app/quarkus-run.jar
```

The web module reads `web/.env`:
```
APP_DB_PATH=/path/to/podcasts.db
APP_AUDIO_BASE_URL=http://your-audio-server:port
```

## Code style

- Kotlin throughout (JVM 21), ktlint enforced — run `./gradlew ktlintFormat` before committing
- `pipeline` JVM heap is set to 4GB by default (`-Xmx4g`) to handle large whisper model loads
- Thymeleaf is used as a plain library in `web` (no Quarkiverse extension) via a hand-rolled CDI producer
