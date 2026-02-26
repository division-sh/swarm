#!/usr/bin/env bash
set -euo pipefail

profile="${1:-}"
min="${2:-}"

if [[ -z "${profile}" || -z "${min}" ]]; then
  echo "usage: $0 <coverprofile> <min_percent>" >&2
  exit 2
fi
if [[ ! -f "${profile}" ]]; then
  echo "coverage profile not found: ${profile}" >&2
  exit 2
fi

total="$(
  go tool cover -func="${profile}" \
    | awk '/^total:/ { gsub(/%/, "", $NF); print $NF }'
)"

if [[ -z "${total}" ]]; then
  echo "failed to parse total coverage from ${profile}" >&2
  exit 2
fi

awk -v total="${total}" -v min="${min}" 'BEGIN {
  if ((total + 0) + 1e-9 < (min + 0)) {
    printf("coverage gate failed: total %.1f%% < min %.1f%%\n", total + 0, min + 0) > "/dev/stderr";
    exit 1;
  }
  printf("coverage gate ok: total %.1f%% >= min %.1f%%\n", total + 0, min + 0);
}'
