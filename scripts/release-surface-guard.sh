#!/usr/bin/env bash
set -euo pipefail

failures=0

fail() {
  echo "ERROR: $*" >&2
  failures=1
}

reject_text() {
  local label="$1"
  local content="$2"
  local pattern="$3"
  local reason="$4"
  local matches

  matches="$(printf '%s\n' "${content}" | grep -E -n "${pattern}" || true)"
  if [[ -n "${matches}" ]]; then
    fail "${label} ${reason}: ${matches//$'\n'/; }"
  fi
}

reject_retired_root_docs() {
  local label="$1"
  local content="$2"
  local matches

  matches="$(
    python3 -c '
import re
import sys

pattern = re.compile(
    r"(?<![A-Za-z0-9_.-])"
    r"(?P<path>(?:[A-Za-z0-9_.~/-]+/)?(?:DEVELOPMENT|COMMANDS|TESTING)\.md)"
    r"(?![A-Za-z0-9_.-])"
)

for line_number, line in enumerate(sys.stdin, 1):
    for match in pattern.finditer(line):
        path = match.group("path")
        if path.startswith("docs/") and path.count("/") == 1:
            continue
        print(f"{line_number}:{path}")
' <<<"${content}" || true
  )"

  if [[ -n "${matches}" ]]; then
    fail "${label} references retired root docs: ${matches//$'\n'/; }"
  fi
}

collect_local_shell_paths() {
  local content="$1"

  python3 -c '
import re
import sys
from pathlib import Path

root = Path.cwd().resolve()
pattern = re.compile(
    r"(?<![A-Za-z0-9_.-])"
    r"(?P<path>(?:\./[A-Za-z0-9_.-]+\.sh|(?:\./)?(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+\.sh))"
    r"(?![A-Za-z0-9_.-])"
)
seen = set()

for line in sys.stdin:
    for match in pattern.finditer(line):
        raw_path = match.group("path")
        resolved = (root / raw_path).resolve()
        try:
            relative = resolved.relative_to(root).as_posix()
        except ValueError:
            continue
        if not resolved.is_file() or relative in seen:
            continue
        seen.add(relative)
        print(relative)
' <<<"${content}"
}

for root_file in CLAUDE.md DEVELOPMENT.md COMMANDS.md TESTING.md; do
  if [[ -e "${root_file}" ]]; then
    fail "retired root release-surface file still exists: ${root_file}"
  fi
done

codex_path_pattern='(^|[^[:alnum:]_.-])(\./)?codex/'
task_path_pattern='(^|[^[:alnum:]_.-])ai/(tasks|policies)/'
claude_pattern='CLAUDE(\.md)?|Claude'
maintainer_recipe_pattern='(^|[[:space:]:;|&(){}])codex-test([[:space:]#;|&(){}]|$)'

collect_recipe_closure() {
  python3 - "$@" <<'PY'
import json
import subprocess
import sys

roots = sys.argv[1:]

try:
    dumped = subprocess.run(
        ["just", "--dump", "--dump-format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
except subprocess.CalledProcessError as error:
    sys.stderr.write(error.stderr or error.stdout)
    raise SystemExit(error.returncode)

recipes = json.loads(dumped.stdout)["recipes"]
seen = set()
ordered = []
queue = list(roots)

while queue:
    name = queue.pop(0)
    if name in seen:
        continue

    recipe = recipes.get(name)
    if recipe is None:
        sys.stderr.write(f"missing product recipe: {name}\n")
        raise SystemExit(1)

    seen.add(name)
    ordered.append(name)

    for dependency in recipe.get("dependencies", []):
        dependency_name = dependency.get("recipe")
        if dependency_name:
            queue.append(dependency_name)

for name in ordered:
    print(name)
PY
}

product_files=(
  README.md
  docs/ARCHITECTURE.md
  docs/COMMANDS.md
  docs/DEVELOPMENT.md
  docs/PROTOCOL.md
  docs/SESSION_LAYOUT_AND_CLI.md
  docs/TESTING.md
  docs/USAGE.md
  .github/workflows/ci.yml
  run-fuse.sh
  smoke-eventfs.sh
  smoke.sh
  scripts/release-check.sh
  scripts/run_full_tests.sh
)

product_command_files=(
  .github/workflows/ci.yml
  run-fuse.sh
  smoke-eventfs.sh
  smoke.sh
  scripts/release-check.sh
  scripts/run_full_tests.sh
)

declare -a queued_product_files=()
declare -A queued_product_file_set=()
declare -A command_product_file_set=()

enqueue_product_file() {
  local path="$1"

  if [[ -n "${queued_product_file_set[${path}]+set}" ]]; then
    return
  fi

  queued_product_file_set["${path}"]=1
  queued_product_files+=("${path}")
}

enqueue_product_scripts_from_text() {
  local content="$1"
  local path

  while IFS= read -r path; do
    [[ -n "${path}" ]] || continue
    command_product_file_set["${path}"]=1
    enqueue_product_file "${path}"
  done < <(collect_local_shell_paths "${content}")
}

scan_product_file() {
  local path="$1"
  local content
  local is_command_surface=0

  if [[ ! -f "${path}" ]]; then
    fail "missing expected product surface: ${path}"
    return
  fi

  content="$(cat "${path}")"
  reject_text "${path}" "${content}" "${codex_path_pattern}" \
    "references Codex workflow paths"
  reject_text "${path}" "${content}" "${task_path_pattern}" \
    "references task workflow paths"
  reject_text "${path}" "${content}" "${claude_pattern}" \
    "references Claude-specific files"
  reject_retired_root_docs "${path}" "${content}"

  if [[ "${path}" == *.sh || -n "${command_product_file_set[${path}]+set}" ]]; then
    is_command_surface=1
    reject_text "${path}" "${content}" "${maintainer_recipe_pattern}" \
      "references maintainer-only recipe names"
  fi

  if ((is_command_surface)); then
    enqueue_product_scripts_from_text "${content}"
  fi
}

for path in "${product_files[@]}"; do
  enqueue_product_file "${path}"
done

for path in "${product_command_files[@]}"; do
  command_product_file_set["${path}"]=1
done

product_entry_recipes=(
  check
  ci
  integration
  smoke
  release-check
  release-check-strict
)

if ! product_recipe_closure="$(collect_recipe_closure "${product_entry_recipes[@]}" 2>&1)"; then
  fail "unable to inspect product recipe dependency closure: ${product_recipe_closure//$'\n'/; }"
  product_recipe_closure=""
fi

while IFS= read -r recipe; do
  [[ -n "${recipe}" ]] || continue

  reject_text "just ${recipe}" "${recipe}" "${maintainer_recipe_pattern}" \
    "is a maintainer-only recipe in the product dependency closure"

  recipe_body="$(just --show "${recipe}")"
  reject_text "just ${recipe}" "${recipe_body}" "${codex_path_pattern}" \
    "references Codex workflow paths"
  reject_text "just ${recipe}" "${recipe_body}" "${task_path_pattern}" \
    "references task workflow paths"
  reject_text "just ${recipe}" "${recipe_body}" "${claude_pattern}" \
    "references Claude-specific files"
  reject_retired_root_docs "just ${recipe}" "${recipe_body}"
  reject_text "just ${recipe}" "${recipe_body}" "${maintainer_recipe_pattern}" \
    "references maintainer-only recipe names"
  enqueue_product_scripts_from_text "${recipe_body}"
done <<<"${product_recipe_closure}"

scan_index=0
while ((scan_index < ${#queued_product_files[@]})); do
  scan_product_file "${queued_product_files[scan_index]}"
  scan_index=$((scan_index + 1))
done

if ((failures)); then
  exit 1
fi

echo "release surface guard passed"
