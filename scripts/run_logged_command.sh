#!/usr/bin/env bash
set -euo pipefail

if (($# < 2)); then
  echo "Usage: bash scripts/run_logged_command.sh <label> <command> [args...]" >&2
  exit 1
fi

label="$1"
shift

safe_label="$(printf '%s' "${label}" | tr -c 'A-Za-z0-9._-' '-')"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
log_dir="${repo_root}/target/validation"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_file="${log_dir}/${safe_label}-${timestamp}.log"
status_file="${log_dir}/${safe_label}-${timestamp}.status"

mkdir -p "${log_dir}"
cd "${repo_root}"

status_written=0

write_status() {
  local exit_status="$1"
  if ((status_written == 0)); then
    printf '%s' "${exit_status}" >"${status_file}"
    status_written=1
  fi
}

on_signal() {
  local signal="$1"
  local signal_status=128
  case "${signal}" in
    HUP) signal_status=129 ;;
    INT) signal_status=130 ;;
    TERM) signal_status=143 ;;
  esac
  write_status "${signal_status}"
  printf 'Interrupted by %s\n' "${signal}" >&2
  exit "${signal_status}"
}

trap 'on_signal HUP' HUP
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

printf 'Running:'
for arg in "$@"; do
  printf ' %q' "${arg}"
done
printf '\n'
printf 'Log: %s\n' "${log_file}"

set +e
"$@" >"${log_file}" 2>&1
status=$?
set -e

write_status "${status}"
printf 'Exit: %s\n' "${status}"
printf 'Status file: %s\n' "${status_file}"

if ((status != 0)); then
  echo 'Last 120 log lines:'
  tail -n 120 "${log_file}" || true
fi

exit "${status}"
