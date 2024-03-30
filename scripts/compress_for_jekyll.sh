#!/usr/bin/env bash

source ./scripts/common.sh

if [ -d "$BUILD_DIR" ]; then
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"

find "$PODS_DIR" -type f -name "*md" -exec cp {} "$BUILD_DIR" \;
cd "$BUILD_DIR" || exit 1
tar -czf _posts.tgz ./*md
