# rss_to_whisper

Transcribe podcast episodes from RSS feeds using [whisper.cpp](https://github.com/ggml-org/whisper.cpp) and index them into Elasticsearch for full-text search.

## Prerequisites

- JDK 21+
- A whisper.cpp compatible model file (see below)
- Elasticsearch 8.x (for indexing)

## Building

```bash
./gradlew build
```

## Running

```bash
./gradlew run --args="-c pods.yaml"
```

Or build and run the distribution:

```bash
./gradlew installDist
./build/install/rss-to-whisper/bin/rss-to-whisper -c pods.yaml
```

## Configuration

Copy and edit `pods.yaml` to configure:

- `data_directory` - where episode files are stored
- `whisper_model` - path to the whisper.cpp model file (`.bin`)
- `require_cuda` - whether CUDA is required (set `false` for CPU/Metal)
- `database_config` - Elasticsearch connection settings
- `podcasts` - list of podcast RSS feeds to process

### Environment Variables

Create a `.env` file with:

```
ELASTIC_API_KEY=your_api_key_here
```

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
    audio.mp3                      # Downloaded episode audio
    transcript.txt                 # Plain text transcription
    transcript.tsv                 # Tab-separated (start_ms, end_ms, text)
    transcript_with_timing.tsv     # Sentence-grouped with timestamps
    transcript.json                # Full metadata + transcript
    transcribed                    # Marker file (signals completion)
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

- **macOS** - Metal acceleration (Apple Silicon), CPU fallback
- **Linux** - CUDA acceleration (requires whisper.cpp compiled with CUDA), CPU fallback

The bundled whisper.cpp native libraries support macOS and Linux. For CUDA support on Linux, you may need to compile whisper.cpp from source with CUDA enabled and set `jna.library.path` to point to your custom build.
