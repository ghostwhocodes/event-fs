#!/usr/bin/env bash
set -euo pipefail

NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
MOUNT_NAME="${MOUNT_NAME:-eventfs-smoke}"

if [[ -n "${MOUNTPOINT:-}" ]]; then
  MOUNTPOINT_CREATED=0
else
  MOUNTPOINT="$(mktemp -d)"
  MOUNTPOINT_CREATED=1
fi

if [[ -n "${CACHE_DIR:-}" ]]; then
  CACHE_DIR_CREATED=0
else
  CACHE_DIR="$(mktemp -d)"
  CACHE_DIR_CREATED=1
fi

cleanup() {
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "$MOUNTPOINT" || true
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$MOUNTPOINT" || true
    else
      umount "$MOUNTPOINT" || true
    fi
  fi
  [[ -n "${FUSE_PID:-}" ]] && kill "$FUSE_PID" 2>/dev/null || true
  if [[ "$MOUNTPOINT_CREATED" == "1" ]]; then
    rm -rf "$MOUNTPOINT"
  fi
  if [[ "$CACHE_DIR_CREATED" == "1" ]]; then
    rm -rf "$CACHE_DIR"
  fi
}
trap cleanup EXIT

if [[ ! -e /dev/fuse ]]; then
  echo "[smoke] /dev/fuse is unavailable; cannot run real FUSE smoke validation" >&2
  exit 1
fi

echo "[smoke] mounting EventFS at $MOUNTPOINT"
cargo run -p eventfs-fuse -- "$MOUNTPOINT" "$NATS_URL" \
  --mount-name "$MOUNT_NAME" \
  --cache-dir "$CACHE_DIR" \
  >/tmp/eventfs-fuse.log 2>&1 &
FUSE_PID=$!

for _ in $(seq 1 60); do
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    break
  fi
  sleep 0.25
done

if ! mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
  echo "[smoke] mount did not become ready" >&2
  cat /tmp/eventfs-fuse.log >&2 || true
  exit 1
fi

mkdir -p "$MOUNTPOINT/kv/smoke"
printf '{"hello":"kv"}' >"$MOUNTPOINT/kv/smoke/greeting.json"
grep -q '"hello":"kv"' "$MOUNTPOINT/kv/smoke/greeting.json"

printf '{"kind":"event","n":1}\n' >>"$MOUNTPOINT/events/system.jsonl"
printf '{"kind":"stream","n":1}\n' >>"$MOUNTPOINT/streams/system/subjects/events.system.jsonl"

mkdir -p "$MOUNTPOINT/objects/smoke"
printf 'object payload' >"$MOUNTPOINT/objects/smoke/payload.txt"
grep -q 'object payload' "$MOUNTPOINT/objects/smoke/payload.txt"

mkdir -p "$MOUNTPOINT/tasks/demo"
rm -f "$MOUNTPOINT/tasks/demo/render-001.json"
printf '{"task":"render","state":"new"}' >"$MOUNTPOINT/tasks/demo/render-001.json"
grep -q '"state":"new"' "$MOUNTPOINT/tasks/demo/render-001.json"

test -f "$MOUNTPOINT/.eventfs/status.json"
test -f "$MOUNTPOINT/.eventfs/capabilities.json"

echo "[smoke] verifying broker state independently"
cargo run -p eventfs-transport --bin eventfs-probe -- "$NATS_URL"

echo "[smoke] EventFS smoke completed"
