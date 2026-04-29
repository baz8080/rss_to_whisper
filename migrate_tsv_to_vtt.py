#!/usr/bin/env python3
"""One-shot migration: convert episode_transcript from TSV to WebVTT format.

For each transcript.json in the data directory, if episode_transcript is in
the old TSV format (lines of "startMs<TAB>text"), it is converted to WebVTT
and the file is rewritten in place.

End times are approximated as the start time of the next cue. For the final
cue, episode_duration (seconds) is used if available, otherwise a 30-second
fallback is added.

Usage:
    python3 migrate_tsv_to_vtt.py /path/to/data_directory [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def ms_to_vtt_timestamp(ms: int) -> str:
    total_s = ms // 1000
    millis = ms % 1000
    hours = total_s // 3600
    minutes = (total_s % 3600) // 60
    seconds = total_s % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def is_vtt(transcript: str) -> bool:
    return transcript.lstrip().startswith("WEBVTT")


def tsv_to_vtt(transcript: str, duration_s: float | None) -> str | None:
    """Convert a TSV transcript to WebVTT. Returns None if no valid cues found."""
    cues = []
    for line in transcript.splitlines():
        line = line.strip()
        if not line:
            continue
        tab = line.find("\t")
        if tab < 0:
            continue
        try:
            start_ms = int(line[:tab])
        except ValueError:
            continue
        text = line[tab + 1:].strip()
        if text:
            cues.append((start_ms, text))

    if not cues:
        return None

    vtt_parts = ["WEBVTT", ""]
    for i, (start_ms, text) in enumerate(cues):
        if i + 1 < len(cues):
            end_ms = cues[i + 1][0]
        elif duration_s is not None:
            end_ms = int(duration_s * 1000)
        else:
            end_ms = start_ms + 30_000

        # Guard against end <= start (e.g. last cue at exactly duration)
        if end_ms <= start_ms:
            end_ms = start_ms + 30_000

        vtt_parts.append(f"{ms_to_vtt_timestamp(start_ms)} --> {ms_to_vtt_timestamp(end_ms)}")
        vtt_parts.append(text)
        vtt_parts.append("")

    return "\n".join(vtt_parts)


def collect_json_paths(data_dir: str):
    for podcast_name in sorted(os.listdir(data_dir)):
        podcast_path = os.path.join(data_dir, podcast_name)
        if not os.path.isdir(podcast_path):
            continue
        for episode_name in os.listdir(podcast_path):
            episode_path = os.path.join(podcast_path, episode_name)
            if not os.path.isdir(episode_path):
                continue
            json_path = os.path.join(episode_path, "transcript.json")
            if os.path.isfile(json_path):
                yield json_path


def main():
    parser = argparse.ArgumentParser(description="Migrate episode_transcript from TSV to WebVTT")
    parser.add_argument("data_dir", help="Path to the podcast data directory")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing files")
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"ERROR: Data directory does not exist: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("DRY RUN — no files will be written")

    converted = 0
    skipped_vtt = 0
    skipped_empty = 0
    errors = 0

    for json_path in collect_json_paths(args.data_dir):
        try:
            with open(json_path) as f:
                episode = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  ERROR reading {json_path}: {e}", file=sys.stderr)
            errors += 1
            continue

        transcript = episode.get("episode_transcript") or ""
        if not transcript.strip():
            skipped_empty += 1
            continue

        if is_vtt(transcript):
            skipped_vtt += 1
            continue

        try:
            duration_s = float(episode.get("episode_duration") or 0) or None
        except (ValueError, TypeError):
            duration_s = None
        vtt = tsv_to_vtt(transcript, duration_s)
        if vtt is None:
            print(f"  WARNING: no valid cues in {json_path}", file=sys.stderr)
            errors += 1
            continue

        print(f"  {'[dry-run] ' if args.dry_run else ''}Converting: {json_path}")
        if not args.dry_run:
            episode["episode_transcript"] = vtt
            with open(json_path, "w") as f:
                json.dump(episode, f, indent=4, sort_keys=True)

        converted += 1

    print(
        f"\nDone. Converted: {converted}  Already VTT: {skipped_vtt}  "
        f"Empty: {skipped_empty}  Errors: {errors}"
    )
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
