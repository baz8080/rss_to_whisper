#!/usr/bin/env bash

source ./scripts/common.sh

rm -f "$POSTS_DIR/*md"
find "$PODS_DIR" -type f -name "*md" -exec cp {} "$POSTS_DIR" \;