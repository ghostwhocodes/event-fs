#!/usr/bin/env bash
set -euo pipefail

mode="local"

usage() {
  cat <<'EOF'
Usage: scripts/release-check.sh [--local|--strict]

Modes:
  --local    Run local convenience checks. Missing Docker or /dev/fuse skips the
             affected broker/FUSE checks with a warning. This is the default.
  --strict   Run release checks. Missing Docker, Docker daemon access, Compose,
             or /dev/fuse is a hard failure.
EOF
}

while (($#)); do
  case "$1" in
    --local)
      mode="local"
      shift
      ;;
    --strict)
      mode="strict"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

is_strict() {
  [[ "${mode}" == "strict" ]]
}

skipped_checks=()
broker_available=0
fuse_device="${EVENTFS_FUSE_DEVICE:-/dev/fuse}"

skip_or_fail() {
  local check_name="$1"
  local reason="$2"

  if is_strict; then
    echo "ERROR: ${check_name} cannot run in strict release mode: ${reason}" >&2
    exit 1
  fi

  echo "WARNING: skipping ${check_name} in local mode: ${reason}" >&2
  skipped_checks+=("${check_name}: ${reason}")
}

docker_ready() {
  command -v docker >/dev/null 2>&1 &&
    docker compose version >/dev/null 2>&1 &&
    docker info >/dev/null 2>&1
}

echo "================================================================"
echo "EventFS Release Check"
echo "Mode: ${mode}"
echo "================================================================"

echo ""
echo ">>> [1/3] Running workspace checks..."
just check

echo ""
echo ">>> [2/3] Running broker-backed integration tests..."
if docker_ready; then
  NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}" just integration
  broker_available=1
else
  skip_or_fail "broker-backed integration tests" \
    "Docker with Compose and daemon access is required"
fi

echo ""
echo ">>> [3/3] Running FUSE smoke..."
if [[ ! -e "${fuse_device}" ]]; then
  skip_or_fail "FUSE smoke" "${fuse_device} is unavailable"
elif [[ "${broker_available}" != "1" && -z "${NATS_URL:-}" ]]; then
  skip_or_fail "FUSE smoke" \
    "broker-backed integration was skipped and NATS_URL is not set"
else
  NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}" ./smoke-eventfs.sh
fi

echo ""
echo "================================================================"
if ((${#skipped_checks[@]})); then
  echo "Release Check Complete With Local Skips"
  for skipped in "${skipped_checks[@]}"; do
    echo "- ${skipped}"
  done
  echo "Use scripts/release-check.sh --strict for release gating."
else
  echo "Release Check Complete"
fi
echo "================================================================"
