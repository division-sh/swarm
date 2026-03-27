#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

latest_spec_file="$(
  find "${ROOT_DIR}/docs/specs" -maxdepth 2 -type f -name 'swarm-v2_0_*.md' \
    | sort -V \
    | tail -n 1
)"

if [[ -z "${latest_spec_file}" ]]; then
  echo "No swarm-v2_0_*.md found under docs/specs" >&2
  exit 1
fi

target_rel="${latest_spec_file#${ROOT_DIR}/}"
link_path="${ROOT_DIR}/swarm-current.md"

ln -sfn "${target_rel}" "${link_path}"
echo "Updated swarm-current.md -> ${target_rel}"
