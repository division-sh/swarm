#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

latest_spec_file="$(
  find "${ROOT_DIR}/docs/specs" -maxdepth 2 -type f -name 'empireai-v2_0_*.md' \
    | sort -V \
    | tail -n 1
)"

if [[ -z "${latest_spec_file}" ]]; then
  echo "No empireai-v2_0_*.md found under docs/specs" >&2
  exit 1
fi

target_rel="${latest_spec_file#${ROOT_DIR}/}"
link_path="${ROOT_DIR}/empireai-current.md"

ln -sfn "${target_rel}" "${link_path}"
echo "Updated empireai-current.md -> ${target_rel}"
