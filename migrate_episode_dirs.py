#!/usr/bin/env python3
"""
One-off migration: rename episode directories from {DATE}-{TITLE} to
{DATE}-{HASH8}-{TITLE} and update _id in transcript.json to
MD5(episode_audio_link)[:8]. Also removes the obsolete _index field.

Usage:
    python3 migrate_episode_dirs.py /path/to/data_dir [--dry-run] [--verbose]
"""

import argparse
import hashlib
import json
import os
import re
import sys


def escape_filename(name: str) -> str:
    """Replicate Kotlin escapeFilename logic."""
    escaped = "".join(c if c.isalnum() else "-" for c in name)
    escaped = re.sub(r"-+", "-", escaped)
    return escaped.rstrip("-")


def audio_link_hash(audio_link: str) -> str:
    return hashlib.md5(audio_link.encode()).hexdigest()[:8]


def is_already_migrated(dir_name: str, expected_hash: str) -> bool:
    """Return True if the dir name already has the expected hash fragment."""
    # New format: YYYY-MM-DD-{8hexchars}-...
    match = re.match(r"^\d{4}-\d{2}-\d{2}-([0-9a-f]{8})-", dir_name)
    if not match:
        # unknown-date format
        match = re.match(r"^unknown-date-([0-9a-f]{8})-", dir_name)
    return match is not None and match.group(1) == expected_hash


def migrate(data_dir: str, dry_run: bool, verbose: bool) -> None:
    totals = {"migrated": 0, "already_done": 0, "skipped": 0, "errors": 0}

    for podcast_dir in sorted(os.listdir(data_dir)):
        podcast_path = os.path.join(data_dir, podcast_dir)
        if not os.path.isdir(podcast_path):
            continue

        podcast_counts = {"migrated": 0, "already_done": 0, "skipped": 0, "errors": 0}

        for episode_dir in sorted(os.listdir(podcast_path)):
            episode_path = os.path.join(podcast_path, episode_dir)
            if not os.path.isdir(episode_path):
                continue

            transcript_path = os.path.join(episode_path, "transcript.json")
            if not os.path.isfile(transcript_path):
                print(f" skipped {transcript_path}")
                podcast_counts["skipped"] += 1
                continue

            try:
                with open(transcript_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"  ERROR reading {transcript_path}: {e}", file=sys.stderr)
                podcast_counts["errors"] += 1
                continue

            audio_link = data.get("episode_audio_link")
            if not audio_link:
                print(f"  WARNING: no episode_audio_link in {transcript_path}", file=sys.stderr)
                podcast_counts["errors"] += 1
                continue

            new_id = audio_link_hash(audio_link)

            if is_already_migrated(episode_dir, new_id):
                podcast_counts["already_done"] += 1
                continue

            # Parse date prefix from dir name
            date_match = re.match(r"^(\d{4}-\d{2}-\d{2}|unknown-date)-", episode_dir)
            if not date_match:
                print(f"  WARNING: cannot parse dir name '{episode_dir}', skipping", file=sys.stderr)
                podcast_counts["skipped"] += 1
                continue

            date_part = date_match.group(1)
            episode_title = data.get("episode_title") or data.get("episode_published_on") or "unknown"
            sanitized_title = escape_filename(episode_title)
            new_dir_name = f"{date_part}-{new_id}-{sanitized_title}"
            new_episode_path = os.path.join(podcast_path, new_dir_name)

            # Build updated transcript.json content
            updated = {k: v for k, v in data.items() if k != "_index"}
            updated["_id"] = new_id

            if "episode_relative_mp3_path" in updated and updated["episode_relative_mp3_path"]:
                old_rel = updated["episode_relative_mp3_path"]
                old_prefix = f"{podcast_dir}/{episode_dir}/"
                new_prefix = f"{podcast_dir}/{new_dir_name}/"
                if old_rel.startswith(old_prefix):
                    updated["episode_relative_mp3_path"] = new_prefix + old_rel[len(old_prefix):]

            if verbose:
                print(f"  {episode_dir}")
                print(f"  -> {new_dir_name}")

            if not dry_run:
                with open(transcript_path, "w", encoding="utf-8") as f:
                    json.dump(updated, f, indent=4, ensure_ascii=False)
                os.rename(episode_path, new_episode_path)

            podcast_counts["migrated"] += 1

        if any(v > 0 for v in podcast_counts.values()):
            parts = []
            if podcast_counts["migrated"]:
                parts.append(f"{podcast_counts['migrated']} migrated")
            if podcast_counts["already_done"]:
                parts.append(f"{podcast_counts['already_done']} already done")
            if podcast_counts["skipped"]:
                parts.append(f"{podcast_counts['skipped']} skipped")
            if podcast_counts["errors"]:
                parts.append(f"{podcast_counts['errors']} errors")
            prefix = "[DRY RUN] " if dry_run else ""
            print(f"{prefix}{podcast_dir}: {', '.join(parts)}")

        for k in totals:
            totals[k] += podcast_counts[k]

    print()
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"{prefix}Total: {totals['migrated']} migrated, {totals['already_done']} already done, "
          f"{totals['skipped']} skipped, {totals['errors']} errors")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate episode directory names to stable hash format")
    parser.add_argument("data_dir", help="Path to the podcast data directory")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--verbose", action="store_true", help="Print each directory rename")
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"ERROR: not a directory: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("DRY RUN — no changes will be made\n")

    migrate(args.data_dir, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
