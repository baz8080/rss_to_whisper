#!/usr/bin/env bash

while read -r url title; do wget "$url" -O pods/"$title"; done < pods/astronomy.txt
