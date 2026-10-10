#!/usr/bin/env bash
set -euo pipefail

plan=$1
unit=$2
inputs=$3
jq -e 'type == "object" and length > 0 and all(to_entries[];
  (.key | startswith("github.com/division-sh/swarm/")) and
  (.value | type == "array" and length > 0 and length == (unique | length) and
    all(.[]; type == "string" and test("^[0-9a-f]{40}$"))))' "$inputs" >/dev/null
commits=$(jq -er --arg id "$unit" --slurpfile inputs "$inputs" '
  [.units[] | select(.id == $id)] |
  if length != 1 then error("expected exactly one selected unit") else .[0] end |
  [.selected_roots[]?.package | $inputs[0][.][]?] | unique | join("\n")' "$plan")
if [ -z "$commits" ]; then exit 0; fi
head=$(git rev-parse HEAD)
while IFS= read -r commit; do
  # Tree-only historical oracles need these exact objects, not all ancestry.
  if ! git cat-file -e "$commit^{commit}" 2>/dev/null || ! git cat-file -e "$commit^{tree}" 2>/dev/null; then
    git fetch --no-tags --depth=1 origin "$commit"
  fi
  git cat-file -e "$commit^{commit}"
  git cat-file -e "$commit^{tree}"
done <<<"$commits"
test "$(git rev-parse HEAD)" = "$head"
