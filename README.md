# rss_to_whisper
Transcribe podcasts from rss feeds with whisper

## Install

```commandline
pip install -r requirements.txt
```

## Run

Results are written to `~/rss_to_whisper`. Uses the `medium` size model by default.

```commandline
rss_to_whisper --feed "http://feeds.libsyn.com/60664"
```

Run other models, and be verbose:

```commandline
rss_to_whisper --feed "http://feeds.libsyn.com/60664" --verbose --model-name "tiny.en"
```