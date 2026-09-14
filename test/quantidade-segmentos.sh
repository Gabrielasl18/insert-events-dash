ffprobe -v error \
  -show_entries format=duration \
  -of default=noprint_wrappers=1:nokey=1 \
  teste1.mp4


# ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 novela.mp4

# ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 videoplayback.mp4