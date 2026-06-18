#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <mountpoint> [NATS_URL] [eventfs-fuse args...]" >&2
  exit 1
fi

MOUNTPOINT="$1"
NATS_URL="${2:-nats://127.0.0.1:4222}"
NATS_CREDS_FILE="${NATS_CREDS_FILE:-}"

clean_url() {
  if [[ -n "$NATS_CREDS_FILE" ]]; then
    echo "$1" | sed -E 's#(nats://)[^/@]+@#\1#'
  else
    echo "$1"
  fi
}

if [[ -n "$NATS_CREDS_FILE" ]]; then
  echo "[run-fuse] using NATS credentials file: $NATS_CREDS_FILE" >&2
  NATS_CREDS_FILE="$NATS_CREDS_FILE" cargo run -p eventfs-fuse -- "$MOUNTPOINT" "$(clean_url "$NATS_URL")" "${@:3}"
else
  cargo run -p eventfs-fuse -- "$MOUNTPOINT" "$(clean_url "$NATS_URL")" "${@:3}"
fi
