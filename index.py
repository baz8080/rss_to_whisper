#!/usr/bin/env python3
"""Index podcast transcripts into a SQLite FTS5 database.

Usage:
    python3 index.py /path/to/data_directory [--db podcasts.db]

Walks the data directory for transcript.json files and inserts them into
a SQLite database with full-text search. Designed to run directly on the
machine hosting the files to avoid network filesystem overhead.
"""

import argparse
import json
import os
import re
import pysqlite3 as sqlite3
import sys
import time


SCHEMA_EPISODES = """
CREATE TABLE IF NOT EXISTS episodes (
    id TEXT PRIMARY KEY,
    podcast_title TEXT,
    podcast_link TEXT,
    podcast_language TEXT,
    podcast_copyright TEXT,
    podcast_author TEXT,
    podcast_image TEXT,
    podcast_type TEXT,
    podcast_collections TEXT,
    episode_title TEXT,
    episode_published_on TEXT,
    episode_audio_link TEXT,
    episode_web_link TEXT,
    episode_image TEXT,
    episode_summary TEXT,
    episode_subtitle TEXT,
    episode_authors TEXT,
    episode_number INTEGER,
    episode_season INTEGER,
    episode_type TEXT,
    episode_duration INTEGER,
    episode_transcript TEXT,
    episode_transcript_plain TEXT,
    episode_relative_audio_path TEXT,
    all_tags TEXT
)
"""

# FTS indexes episode_transcript_plain (VTT timing lines stripped) rather than
# episode_transcript (raw VTT), so searches never match on timestamps.
SCHEMA_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(
    episode_title,
    episode_transcript_plain,
    podcast_title,
    all_tags,
    content='episodes',
    content_rowid='rowid'
)
"""

INSERT_SQL = """
INSERT OR REPLACE INTO episodes (
    id, podcast_title, podcast_link, podcast_language, podcast_copyright,
    podcast_author, podcast_image, podcast_type, podcast_collections,
    episode_title, episode_published_on, episode_audio_link, episode_web_link,
    episode_image, episode_summary, episode_subtitle, episode_authors,
    episode_number, episode_season, episode_type, episode_duration,
    episode_transcript, episode_transcript_plain, episode_relative_audio_path, all_tags
) VALUES (
    :id, :podcast_title, :podcast_link, :podcast_language, :podcast_copyright,
    :podcast_author, :podcast_image, :podcast_type, :podcast_collections,
    :episode_title, :episode_published_on, :episode_audio_link, :episode_web_link,
    :episode_image, :episode_summary, :episode_subtitle, :episode_authors,
    :episode_number, :episode_season, :episode_type, :episode_duration,
    :episode_transcript, :episode_transcript_plain, :episode_relative_audio_path, :all_tags
)
"""

_VTT_TIMING_RE = re.compile(r"\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}")


def strip_vtt(text):
    """Return plain text from a WebVTT string, suitable for FTS indexing."""
    words = []
    for line in text.splitlines():
        s = line.strip()
        if s and s != "WEBVTT" and not _VTT_TIMING_RE.match(s):
            words.append(s)
    return " ".join(words)


def join_list(value):
    """Join a list into a comma-separated string, or return None."""
    if not isinstance(value, list):
        return None
    return ", ".join(str(item) for item in value if item is not None)


def collect_episodes(data_dir):
    """Walk the data directory and yield episode dicts."""
    count = 0

    for podcast_name in sorted(os.listdir(data_dir)):
        podcast_path = os.path.join(data_dir, podcast_name)
        if not os.path.isdir(podcast_path):
            continue

        for episode_name in os.listdir(podcast_path):
            episode_path = os.path.join(podcast_path, episode_name)
            if not os.path.isdir(episode_path):
                continue

            json_path = os.path.join(episode_path, "transcript.json")
            if not os.path.isfile(json_path):
                continue

            try:
                with open(json_path, "r") as f:
                    episode = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                print(f"  WARNING: Failed to read {json_path}: {e}", file=sys.stderr)
                continue

            transcript = episode.get("episode_transcript")
            if not transcript:
                continue

            episode_id = episode.get("_id")

            yield {
                "id": episode_id,
                "podcast_title": episode.get("podcast_title"),
                "podcast_link": episode.get("podcast_link"),
                "podcast_language": episode.get("podcast_language"),
                "podcast_copyright": episode.get("podcast_copyright"),
                "podcast_author": episode.get("podcast_author"),
                "podcast_image": episode.get("podcast_image"),
                "podcast_type": episode.get("podcast_type"),
                "podcast_collections": join_list(episode.get("podcast_collections")),
                "episode_title": episode.get("episode_title"),
                "episode_published_on": episode.get("episode_published_on"),
                "episode_audio_link": episode.get("episode_audio_link"),
                "episode_web_link": episode.get("episode_web_link"),
                "episode_image": episode.get("episode_image"),
                "episode_summary": episode.get("episode_summary"),
                "episode_subtitle": episode.get("episode_subtitle"),
                "episode_authors": join_list(episode.get("episode_authors")),
                "episode_number": episode.get("episode_number"),
                "episode_season": episode.get("episode_season"),
                "episode_type": episode.get("episode_type"),
                "episode_duration": episode.get("episode_duration"),
                "episode_transcript": transcript,
                "episode_transcript_plain": strip_vtt(transcript),
                "episode_relative_audio_path": episode.get("episode_relative_audio_path"),
                "all_tags": join_list(episode.get("all_tags")),
            }
            count += 1

    print(f"Found {count} episodes to index")


def main():
    parser = argparse.ArgumentParser(description="Index podcast transcripts into SQLite FTS5")
    parser.add_argument("data_dir", help="Path to the podcast data directory")
    parser.add_argument("--db", default=None, help="Path to SQLite database (default: <data_dir>/podcasts.db)")
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"ERROR: Data directory does not exist: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    db_path = args.db or os.path.join(args.data_dir, "podcasts.db")
    print(f"Data directory: {args.data_dir}")
    print(f"Database: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Use raw sqlite_master to drop FTS table safely — DROP TABLE on a
    # virtual table requires the module to be loaded, which may not be
    # available if the table was created with a different FTS version.
    fts_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='episodes_fts'"
    ).fetchone()
    if fts_exists:
        try:
            conn.execute("DROP TABLE IF EXISTS episodes_fts")
        except sqlite3.OperationalError:
            # FTS module not available; delete shadow tables and master entry manually.
            # FTS5 shadow suffixes: data, idx, content, docsize, config
            # FTS4 shadow suffixes (kept for legacy dbs): segments, segdir, stat
            for suffix in ["data", "idx", "content", "docsize", "config",
                           "segments", "segdir", "stat"]:
                conn.execute(f"DROP TABLE IF EXISTS episodes_fts_{suffix}")
            conn.execute("DELETE FROM sqlite_master WHERE name='episodes_fts'")
    conn.execute("DROP TABLE IF EXISTS episodes")
    conn.execute(SCHEMA_EPISODES)
    conn.commit()

    episodes = list(collect_episodes(args.data_dir))

    if not episodes:
        print("Nothing to index")
        conn.close()
        return

    t0 = time.time()
    print(f"Inserting {len(episodes)} episodes...")
    conn.executemany(INSERT_SQL, episodes)
    conn.commit()
    print(f"  Insert: {time.time() - t0:.1f}s")

    t0 = time.time()
    print("Creating indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_published ON episodes(episode_published_on DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_podcast ON episodes(podcast_title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_type ON episodes(episode_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_episodes_duration ON episodes(episode_duration)")
    conn.commit()
    print(f"  Indexes: {time.time() - t0:.1f}s")

    # Build FTS index after all rows are in place — much faster than
    # inserting into FTS incrementally or rebuilding at the end.
    t0 = time.time()
    print("Building FTS index...")
    conn.execute(SCHEMA_FTS)
    conn.execute("INSERT INTO episodes_fts(episodes_fts) VALUES('rebuild')")
    conn.commit()
    print(f"  FTS build: {time.time() - t0:.1f}s")
    print("Done")

    conn.close()


if __name__ == "__main__":
    main()
