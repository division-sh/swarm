#!/usr/bin/env bash
set -euo pipefail
plan=$1
dir=$(dirname "${BASH_SOURCE[0]}")
# This witness is extracted from the frozen owner's plan, not a second planner.
actual=$(jq -cS '{unit:(.units[]|select(.id=="serveapp-runtime")),batch:(.batches[]|select(.id=="serveapp-runtime"))}' "$plan" | sha256sum | cut -d' ' -f1)
[[ "$actual" = "$(jq -r .unit_and_batch_sha256 "$dir/frozen-selection.json")" ]]
jq -e --arg sha "$(git rev-parse HEAD)" '.head_sha==$sha and .profile=="full" and
  ([.units[]|select(.id=="serveapp-runtime")]|length)==1 and
  ([.batches[]|select(.id=="serveapp-runtime")]|length)==1' "$plan" >/dev/null
