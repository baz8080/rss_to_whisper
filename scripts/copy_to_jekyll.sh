#!/usr/bin/env bash

POSTS_DIR="$HOME/Code/podscripts_site/_posts/"
SOURCE_DIR="$HOME/rss_to_whisper"
rm -f "$POSTS_DIR/*md"
find "$SOURCE_DIR" -type f -name "*md" -exec cp {} "$POSTS_DIR" \;