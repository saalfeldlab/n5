#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=/tmp/write-lock-test.zarr

[ -d "$TEST_DIR" ] && rm -r -- "$TEST_DIR"
#mvn test-compile

mvn -e -q exec:java \
  -Dexec.mainClass=org.janelia.saalfeldlab.n5.WriteLockExp \
  -Dexec.classpathScope=test \
  -Dexec.args="/tmp/write-lock-test.zarr 0" &
pid1=$!

mvn -e -q exec:java \
  -Dexec.mainClass=org.janelia.saalfeldlab.n5.WriteLockExp \
  -Dexec.classpathScope=test \
  -Dexec.args="/tmp/write-lock-test.zarr 1" &
pid2=$!

trap 'kill "$pid1" "$pid2" 2>/dev/null || true' EXIT INT TERM

wait "$pid1"
wait "$pid2"