#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
mkdir -p "${repo_root}/target"
lock_path="${repo_root}/target/.managed-command.lock"

child_pid=""

terminate_child_group() {
  if [[ -z "${child_pid}" ]]; then
    return
  fi
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    return
  fi

  kill -TERM -- "-${child_pid}" 2>/dev/null || kill -TERM "${child_pid}" 2>/dev/null || true

  for _ in 1 2 3 4 5; do
    if ! kill -0 "${child_pid}" 2>/dev/null; then
      return
    fi
    sleep 0.2
  done

  kill -KILL -- "-${child_pid}" 2>/dev/null || kill -KILL "${child_pid}" 2>/dev/null || true
}

handle_signal() {
  local signal_exit="${1}"
  terminate_child_group
  exit "${signal_exit}"
}

cleanup_on_exit() {
  terminate_child_group
}

trap 'handle_signal 130' INT
trap 'handle_signal 129' HUP
trap 'handle_signal 143' TERM
trap cleanup_on_exit EXIT

exec 9>"${lock_path}"
flock 9

setsid "$@" &
child_pid=$!

set +e
wait "${child_pid}"
status=$?
set -e

trap - EXIT HUP INT TERM
exit "${status}"
