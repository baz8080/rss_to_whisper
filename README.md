# rss_to_whisper

Transcribe podcast episodes from RSS feeds using the [whisper.cpp](https://github.com/ggml-org/whisper.cpp) HTTP server and index them into a local SQLite FTS5 database for full-text search.

## Modules

### `pipeline` (Kotlin)
Walks one or more RSS feeds, downloads each episode's MP3, and POSTs it to a running whisper.cpp HTTP server. The server decodes and resamples the audio itself, so no local transcoding step is needed and the MP3 is what gets kept on disk. The server returns a [WebVTT](https://www.w3.org/TR/webvtt1/) transcript, which is stored in a `transcript.json` file alongside the audio. Designed to run on a schedule (e.g. cron) to keep transcripts up to date.

### `web` (Kotlin)
A Quarkus HTTP server that serves a full-text search interface over the SQLite database produced by `index.py`. Supports filtering by podcast, collection, episode type, and duration. Search results are ranked by BM25 relevance. The episode detail page shows a clickable transcript synced to the audio player. Runs on port 8080 by default.

## Python scripts

### `index.py`
Reads all `transcript.json` files in the data directory and writes them into a SQLite FTS5 database. Run this after the pipeline to make new transcripts searchable.

Requires Python 3 with an FTS5-capable SQLite. The script prefers `pysqlite3` when installed and falls back to the standard library `sqlite3` module otherwise, exiting with a clear error if neither has FTS5:

```bash
pip install pysqlite3
```

> **Note:** The default system `sqlite3` on some platforms (e.g. Synology NAS) does not include FTS5. `pysqlite3` bundles a SQLite build that does.


## Prerequisites

- JDK 21+
- A running [whisper.cpp HTTP server](#running-the-whispercpp-server)
- Python 3 with an FTS5-capable SQLite (for the indexing script; `pip install pysqlite3` if the system build lacks FTS5)

No `ffmpeg` is required. MP3s are uploaded as-is and the whisper.cpp server decodes them with its
built-in miniaudio decoder, resampling to 16 kHz mono internally. The one exception is starting the
server with `--convert`, which shells out to `ffmpeg` on the server host — don't use that flag.

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

It is highly recommended to also download a [VAD model](https://github.com/ggml-org/whisper.cpp?ref=shadowfinder.com#voice-activity-detection-vad). 

```bash
whisper-server \
  -m models/ggml-large-v3.bin \
  --vad -vm models/ggml-silero-v5.1.2.bin
```

#### Model: large-v3, not large-v3-turbo

Turbo prunes the **decoder** and keeps the encoder, and with Core ML the encoder
runs on the Neural Engine — which is where most of the wall clock goes. Measured
over 13 episodes, large-v3 costs **1.36x** turbo's decode time, not the 2-3x that
"turbo for speed" assumes, and it produced a usable transcript on 10 of 13
episodes that turbo could not decode at all.

Put `ggml-large-v3-encoder.mlmodelc` next to the `.bin` or the encoder silently
falls back off the ANE and the speed gap gets much worse.

#### Why

Whisper occasionally settles into a degraded decoding mode for a whole episode and produces text with **no punctuation and no capitals at all**. The words are mostly right, but Whisper segments on sentence structure, and with no full stops there is nothing to break on, so cue
boundaries stop tracking speech. Anything downstream that needs a timestamp to land in the right place is then working from fiction.

VAD also reduces cue fragmentation on music-heavy shows. Treat it as a fix for non-speech-driven
fragmentation, not for fragmentation in general.

Leave `vad_threshold` at the 0.5 default

This is worth stating because tuning it is the obvious next move and it is a trap. A fair A/B — 14 random healthy episodes, same audio, both thresholds, nothing written — found **zero failures at either**. On typical material the two
are indistinguishable. (n=14, so this shows equivalence between thresholds, not that failures are rare.)

The threshold only matters in the tail, and **the optimum is episode-dependent**:

|                        | 0.2        | 0.5 (default) |
|------------------------|------------|---------------|
| loop-damaged episodes  | **better** | worse         |
| unpunctuated episodes  | worse — 87 of 654 stayed broken | **better** |

It is not VAD as such. On one episode, threshold 0.2 gave 0.0000 punctuation-per-word and 0.5 gave 0.1982 — and VAD *off* also gave 0.0000. More non-speech reaching the decoder makes the degraded mode likelier, and how much non-speech an episode contains varies.

**Do not tune per show.** 

#### Set an initial prompt

Whisper intermittently decodes an entire episode with **no punctuation and no
capitals**. It is not cosmetic: whisper segments on sentence structure, so with
no full stops the cue boundaries stop tracking speech and every timestamp
derived from them is unreliable. 654 episodes were hit, and 13 resisted every
attempt to re-decode them.

An `initial_prompt` of ordinary punctuated prose fixed **all 13**:

| approach | fixed |
|---|---|
| **initial_prompt** | **13 / 13** |
| whisper large-v3 | 10 / 13 |
| best VAD parameter | 9 / 13 |
| Silero VAD v6.2.0 | 6 / 13 |
| re-decode at another threshold | 0 / 13 |

This is a **decoder** mode, not a segmentation problem, which is why no VAD
setting touched it — VAD only changes what audio reaches the decoder, while a
prompt conditions the decoder itself, and punctuation is a style.

Send `carry_initial_prompt=true` as well. Without it the prompt conditions only
the first window and an episode that degrades later still degrades (13/13 with,
12/13 without).

Keep the prompt generic. An initial prompt biases **vocabulary** as well as
style, so anything domain-specific will contaminate transcripts. The default in
`Transcriber.kt` was checked on a repaired episode: zero occurrences of any
prompt fragment, word count within 5% of the original.

#### TODO: Detect and retry instead

Since there is no correct threshold, do not pick one — decode, measure, and retry the failures at the other value, keeping whichever verifies better. That is threshold-agnostic, self-correcting, and about fifteen lines.

Both `vad` and `vad_threshold` are **per-request form fields**, so the retry needs no server restart and no second server.

Gist for `Transcriber.kt` after the POST returns:

```kotlin
val vtt = response.body
if (punctPerWord(vtt) < 0.03) {
    val alt = transcribe(audio, vadThreshold = 0.2)   // the other value
    if (punctPerWord(alt) > punctPerWord(vtt)) return alt
}
return vtt
```

Rewrite only when the new decode **beats** the old on an ordered test. Looping is worse than unreadable, which is worse than losing a few words. A retry that  overwrites unconditionally will eventually replace a good decode with a bad one.

### Platform acceleration

Acceleration is determined by how the whisper.cpp `server` binary was compiled:

- **macOS (Apple Silicon)** — Metal acceleration, built in by default
- **Linux** — CUDA acceleration if built with `WHISPER_CUDA=1`; CPU fallback otherwise

## Transcribing

All pipeline configuration comes from `pipeline/.env` — there are no command-line arguments (see [Pipeline configuration](#pipeline-configuration) below).

```bash
./gradlew :pipeline:run
```

Or build and run the distribution:

```bash
./gradlew :pipeline:installDist
./pipeline/build/install/pipeline/bin/pipeline
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
#PIPELINE_VERBOSE=true
```

`PIPELINE_VERBOSE` is optional; when set to a non-blank value it overrides the `verbose` value from
`pods.yaml`. Leave it commented out (or blank) to let `pods.yaml` decide — note that any value other
than `true` counts as `false`, so `PIPELINE_VERBOSE=false` will override `verbose: true`.

The `.env` file is resolved relative to the working directory: `pipeline/.env` is tried
first (running from the repo root), then `./.env` (running from inside `pipeline/`, or
next to an installed distribution).

### `pods.yaml`

- `verbose` — enable debug logging (optional, default `false`; overridden by `PIPELINE_VERBOSE` when that is set)
- `skip_after_consecutive` — stop walking a feed once this many consecutive already-transcribed episodes are seen (optional, default `20`)
- `podcasts` — list of RSS feeds to process, each with `name`, `url`, optional `collections`, and optional `excludes`

### Skip heuristic

Feeds are typically ordered newest-first. Rather than stat'ing every episode directory (expensive for feeds with thousands of entries), the transcriber walks the feed and stops on a podcast once it sees `skip_after_consecutive` transcribed episodes in a row. The counter resets on any gap, so a cancelled run that left untranscribed holes will be picked up on the next invocation.

## Output structure

For each episode, the following files are created:

```
{data_directory}/{podcast_name}/{YYYY-MM-DD-episode-title}/
    audio.mp3                      # Downloaded audio, kept as-is and served by the web module
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
