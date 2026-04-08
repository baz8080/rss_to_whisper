#!/usr/bin/env python3
"""One-off script to clean up all_tags in existing transcript.json files.

Removes duplicate tags and tags with 2 or fewer characters, in-place.

Usage:
    python3 fix_tags.py /path/to/data_directory [--dry-run]
"""

import argparse
import json
import os
import sys


def clean_tags(tags):
    """Deduplicate tags and remove those with 2 or fewer characters."""
    if not isinstance(tags, list):
        return tags
    seen = set()
    result = []
    for tag in tags:
        tag = tag.strip()
        if tag and len(tag) > 2 and tag not in seen:
            seen.add(tag)
            result.append(tag)
    return result


def fix_transcript(json_path, dry_run):
    with open(json_path, "r") as f:
        episode = json.load(f)

    original_tags = episode.get("all_tags", [])
    cleaned_tags = clean_tags(original_tags)

    if original_tags == cleaned_tags:
        return False

    removed = [t for t in original_tags if t not in cleaned_tags]
    duplicates_removed = len(original_tags) - len(set(original_tags))
    short_removed = [t for t in set(original_tags) if t not in cleaned_tags]

    print(f"  {json_path}")
    if duplicates_removed:
        print(f"    duplicates removed: {duplicates_removed}")
    if short_removed:
        print(f"    short tags removed: {short_removed}")

    if not dry_run:
        episode["all_tags"] = cleaned_tags
        with open(json_path, "w") as f:
            json.dump(episode, f, indent=2)

    return True


def main():
    parser = argparse.ArgumentParser(description="Fix all_tags in transcript.json files")
    parser.add_argument("data_dir", help="Path to the podcast data directory")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without modifying files")
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"ERROR: Directory does not exist: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("DRY RUN — no files will be modified\n")

    changed = 0
    total = 0

    for podcast_name in sorted(os.listdir(args.data_dir)):
        podcast_path = os.path.join(args.data_dir, podcast_name)
        if not os.path.isdir(podcast_path):
            continue

        for episode_name in os.listdir(podcast_path):
            episode_path = os.path.join(podcast_path, episode_name)
            json_path = os.path.join(episode_path, "transcript.json")
            if not os.path.isfile(json_path):
                continue

            total += 1
            try:
                if fix_transcript(json_path, args.dry_run):
                    changed += 1
            except (json.JSONDecodeError, OSError) as e:
                print(f"  WARNING: Failed to process {json_path}: {e}", file=sys.stderr)

    action = "would be" if args.dry_run else "were"
    print(f"\n{changed}/{total} files {action} modified.")


if __name__ == "__main__":
    main()
