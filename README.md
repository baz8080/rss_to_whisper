# rss_to_whisper

Transcribe podcast episodes from RSS feeds using the [whisper.cpp](https://github.com/ggml-org/whisper.cpp) HTTP server and index them into a local SQLite FTS5 database for full-text search.

## Modules

### `pipeline` (Kotlin)
Walks one or more RSS feeds, downloads each episode's MP3, and POSTs it to a running whisper.cpp HTTP server. The server decodes and resamples the audio itself, so no local transcoding step is needed and the MP3 is what gets kept on disk. It returns `verbose_json`, from which the pipeline renders a [WebVTT](https://www.w3.org/TR/webvtt1/) transcript into `transcript.json` and writes the per-word timings alongside as `words.jsonl.gz`. Designed to run on a schedule (e.g. cron) to keep transcripts up to date.

### `web` (Kotlin)
A Quarkus HTTP server that serves a full-text search interface over the SQLite database produced by `index.py`. Supports filtering by podcast, collection, episode type, and duration. Search results are ranked by BM25 relevance. The episode detail page shows a clickable transcript synced to the audio player. Runs on port 8080 by default.

> **Note:** whisper-server also defaults to 8080. They are rarely up at the same time, but if they are, move one — `--port` on whisper-server, `quarkus.http.port` on the web module.

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
built-in miniaudio decoder, resampling to 16 kHz mono internally.

## Building

```bash
./gradlew build
```

## Running the whisper.cpp server

The pipeline POSTs audio to the whisper.cpp `/inference` endpoint. It does not
start or manage the server — start it yourself and leave it up for the run:

```bash
whisper-server \
  -m /path/to/models/ggml-large-v3.bin \
  --port 8080
```

The `whisper-server` binary is built alongside `whisper-cli` when you compile
whisper.cpp. Set `PIPELINE_WHISPER_SERVER_URL` in `pipeline/.env` to its base
URL.

**Only the model is chosen at launch.** Everything else that matters is a
per-request form field, and `server.cpp` overrides launch defaults with
whatever a request actually sends. So the pipeline's own settings win, and the
only knob you pick when starting the server is `-m`.

Do not pass `--convert` (it shells out to `ffmpeg` on the server host — MP3s are
uploaded as-is and decoded internally), and do not pass `-nt`, which would
suppress the timestamps the pipeline exists to capture.

### Choosing a model

whisper.cpp uses its own GGML model format (`.bin` files), **not** the OpenAI
Python `.pt` files. Download from HuggingFace:

```bash
curl -L -o ggml-large-v3.bin \
  https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3.bin
```

Put the matching `ggml-large-v3-encoder.mlmodelc` next to the `.bin`, or the
encoder silently falls off the Neural Engine and everything gets much slower.

#### large-v3, not large-v3-turbo

Turbo prunes the **decoder** and keeps the encoder, and under Core ML the
encoder is where most of the wall clock goes. Measured over 13 episodes,
large-v3 costs **1.36x** turbo's decode time — not the 2-3x that "turbo for
speed" implies — and produced a usable transcript on 10 of 13 episodes turbo
could not decode at all.

It also fixes a failure nothing else touches. Turbo shreds some episodes into
runs of one- and two-word cues: 1,256 episodes in one corpus. An A/B on 4
shredded episodes across 4 arms took fragmentation to zero in **all sixteen
cells, including the no-prompt no-VAD control**. That is a model difference,
and neither the prompt nor VAD substitutes for it.

## What the pipeline sends, and why

These are the request fields in `Transcriber.kt`. Each one is load-bearing.

### `prompt` and `carry_initial_prompt` — the single biggest lever

Whisper intermittently decodes an entire episode with **no punctuation and no
capitals**. That is not cosmetic: whisper segments on sentence structure, so
with no full stops the cue boundaries stop tracking speech and every timestamp
derived from them is fiction. 654 episodes were hit, and 13 resisted every
attempt to re-decode them.

An `initial_prompt` of ordinary punctuated prose fixed **all 13**:

| approach | fixed |
|---|---|
| **initial_prompt** | **13 / 13** |
| large-v3 | 10 / 13 |
| best VAD parameter | 9 / 13 |
| Silero VAD v6.2.0 | 6 / 13 |
| re-decode at another VAD threshold | 0 / 13 |

It works because this is a **decoder** mode, not a segmentation problem. VAD
only changes what audio reaches the decoder; a prompt conditions the decoder
itself, and punctuation is a style. It also prevents runaway repetition loops —
without it, 4 of 4 test decodes collapsed, one returning 539 usable words out of
10,660, and ran 2.5x slower because a looping decode burns tokens.

`carry_initial_prompt=true` matters: without it only the first window is
conditioned, and an episode that degrades later still degrades (13/13 with,
12/13 without).

Keep the prompt generic. It biases **vocabulary** as well as style, so anything
domain-specific contaminates transcripts. The default in `Transcriber.kt` was
checked against a repaired episode — zero occurrences of any prompt fragment,
word count within 5% of the original.

### `vad=false` — sent explicitly, and off

Not merely omitted. A request that omits `vad` inherits whatever the server was
launched with, silently, which is how one corpus ended up with no record of its
own VAD state.

It has to be off because VAD breaks word timestamps. whisper.cpp keeps token
timestamps in VAD-compressed time while remapping segment timestamps to real
time, so the two drift apart by however much silence VAD removed:

| | offset, first word vs its own segment |
|---|---|
| VAD on | −1.79s at the start, **−6.51s** by the end |
| VAD off | +0.00s to +0.20s |

Same episode, same model, same fields — on a *six-minute* episode. Every word
time would be early by a growing, episode-dependent, invisible amount.

Nothing is lost by turning it off. VAD's real value was suppressing the
unpunctuated collapse, and the prompt does that better (13/13 against 9/13).

### `response_format=verbose_json`

The server can return WebVTT directly. It is derived from the JSON here instead,
because per-word `start`/`end` come only from `verbose_json` and the decode
computes them either way — rendering to VTT throws them away.

Both artifacts come from one parse of one response. Asking for both formats
would mean two decodes, and whisper is not deterministic across runs, so their
cues and words could disagree in ways nothing downstream could detect.

Output per episode:

```
transcript.json     metadata plus the WebVTT string
words.jsonl.gz      {"w":" Doritos","s":1423.44,"e":1423.79,"p":0.94,"seg":118}
```

`p` is the decoder's own confidence and `seg` the cue the word came from, so the
sidecar joins back to the WebVTT without re-alignment. Roughly 274 KB per
episode before compression.

### `token_timestamps`, `max_len`, `split_on_word`

whisper.cpp only applies `max_len` when `token_timestamps` is on — the wrap call
is nested inside `if (params.token_timestamps)` in `whisper_full`, so sending
`max_len` alone is silently ignored. Without them a whole episode can come back
as a single cue; 139 episodes in one corpus did, the worst covering over 7,200
seconds. A cue that long cannot carry a usable timestamp.

`split_on_word` cuts at word boundaries rather than mid-token.

### No `beam_size` — the decode is greedy

whisper-server defaults to greedy and whisper-cli to `beam_size=5`; both run
`strategy = beam_size > 1 ? BEAM_SEARCH : GREEDY`. Greedy is deliberate here.
Beam is worth reaching for when repairing an already-collapsed decode, but it
has not been shown to help a healthy one, and changing it during a bulk run
means never knowing which change did what.

## Platform acceleration

Acceleration is determined by how the whisper.cpp `server` binary was compiled:

- **macOS (Apple Silicon)** — Metal acceleration, built in by default
- **Linux** — CUDA acceleration if built with `WHISPER_CUDA=1`; CPU fallback otherwise

## Transcribing

With `pipeline/.env` filled in (see [Pipeline configuration](#pipeline-configuration) below), no arguments are needed:

```bash
./gradlew :pipeline:run
```

Or build and run the distribution:

```bash
./gradlew :pipeline:installDist
./pipeline/build/install/pipeline/bin/pipeline
```

Every `.env` setting also has a flag, which takes precedence over it — run
`pipeline --help` for the list.

### Running two instances at once

Give each instance its own `pods.yaml` and its own whisper.cpp server. Use the installed
distribution rather than `./gradlew :pipeline:run` twice: two concurrent Gradle
invocations in one checkout serialise on the project lock.

```bash
./gradlew :pipeline:installDist
```

Then, in one terminal:

```bash
./pipeline/build/install/pipeline/bin/pipeline --config ~/pods-a.yaml --whisper-url http://localhost:8081
```

and in another:

```bash
./pipeline/build/install/pipeline/bin/pipeline --config ~/pods-b.yaml --whisper-url http://localhost:8082
```

Both can share one data directory, but nothing coordinates them: list a show in only one
of the two files. If both walk the same feed they will download the same episode to the
same `audio.mp3.part` staging file, and whichever finishes first deletes the other's.

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
next to an installed distribution). A missing `.env` is fine as long as the arguments
below supply the three required values.

### Arguments

| Flag | Overrides |
| --- | --- |
| `--config <path>` | `PIPELINE_CONFIG_PATH` |
| `--data-dir <path>` | `PIPELINE_DATA_DIRECTORY` |
| `--whisper-url <url>` | `PIPELINE_WHISPER_SERVER_URL` |
| `--verbose` / `--no-verbose` | `PIPELINE_VERBOSE` |

Precedence is argument, then `.env`, then `pods.yaml`. A flag that is not passed falls
through, so `--whisper-url` alone leaves everything else coming from `.env`.

Under Gradle the flags go through `--args`. Gradle splits that string itself rather than
handing it to a shell, so write `$HOME` or an absolute path — a `~` inside the quotes
arrives at the pipeline literally and the config file is not found:

```bash
./gradlew :pipeline:run --args="--config $HOME/pods-a.yaml --whisper-url http://localhost:8081"
```

A bad flag exits `2`, unusable configuration exits `1`, and both messages go to stderr, so
a wrapper script can tell a failed launch from a run that found nothing to do.

### `pods.yaml`

- `verbose` — enable debug logging (optional, default `false`; overridden by `PIPELINE_VERBOSE` when that is set)
- `skip_after_consecutive` — stop walking a feed once this many consecutive already-transcribed episodes are seen (optional, default `20`)
- `podcasts` — list of RSS feeds to process, each with `name`, `url`, optional `collections`, and optional `excludes`

`name` becomes the show's directory name, so changing it moves every episode of
that feed. Re-capitalising it used to create a *second* directory for the same
show; the pipeline now reuses a directory that differs only by case, and logs
when it does. Renaming it any other way still starts a fresh directory and
re-transcribes the feed.

### Skip heuristic

Feeds are typically ordered newest-first. Rather than stat'ing every episode directory (expensive for feeds with thousands of entries), the transcriber walks the feed and stops on a podcast once it sees `skip_after_consecutive` transcribed episodes in a row. The counter resets on any gap, so a cancelled run that left untranscribed holes will be picked up on the next invocation.

## Output structure

For each episode, the following files are created:

```
{data_directory}/{podcast_name}/{YYYY-MM-DD-<hex8>-episode-title}/
    audio.mp3                      # Downloaded audio, kept as-is and served by the web module
    transcript.json                # Full metadata + WebVTT transcript
    words.jsonl.gz                 # One line per word, with its own start/end
```

The `episode_transcript` field in `transcript.json` is a raw WebVTT string,
rendered from the same `verbose_json` response that produced `words.jsonl.gz`.

`words.jsonl.gz` is written **before** `transcript.json`, because the latter
existing is what marks an episode done — so a crash between the two leaves the
episode to be redone rather than permanently without its sidecar. A sidecar
write failure is logged and not fatal: the transcript is the artifact the
pipeline exists to produce.

The `<hex8>` in the directory name is `md5(entry.uri)` truncated to 8
characters. It is part of a path, not a unique key: date and title slug
disambiguate it.

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
