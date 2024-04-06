#!/usr/bin/env bash

export SITE_DIR="$HOME/Code/podscripts_site"
export SOURCE_DIR="/Volumes/rss_to_whisper"

export POSTS_DIR="$SITE_DIR/_posts"

export PODS_DIR="$SOURCE_DIR/pods"
export BUILD_DIR="$SOURCE_DIR/build"

set +e

if [ -z "$SITE_DIR" ] || [ -z "$SOURCE_DIR" ]; then
    echo "SITE_DIR and / or SOURCE_DIR variables are missing. Exiting"
    exit 1
fi
