#!/bin/bash

INPUT_FILE="./videoplayback.mp4"
OUTPUT_DIR="./dash"
MPD_NAME="playlist_1080p.mpd"

mkdir -p "$OUTPUT_DIR"

rm -f "$OUTPUT_DIR"/*

ffmpeg \
-re \
-stream_loop -1 \
-i videoplayback.mp4 \
-map 0:v:0 \
-vf scale=1280:720 \
-c:v libx264 \
-preset ultrafast \
-tune zerolatency \
-g 48 \
-keyint_min 48 \
-sc_threshold 0 \
-seg_duration 2 \
-window_size 20 \
-extra_window_size 20 \
-use_template 1 \
-use_timeline 0 \
-adaptation_sets "id=0,streams=v" \
-f dash \
dash/playlist_1080p.mpd