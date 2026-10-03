#!/usr/bin/env bash
set -euo pipefail

plan=test-results/plan/proof-plan.json
mkdir -p test-results/evidence
go run ./cmd/swarm-test-timing -assert-execution-sha -plan "$plan" -execution-sha "$(git rev-parse HEAD)"
mapfile -t units < <(jq -er --arg id "$BATCH_ID" '.batches[] | select(.id == $id) | .units[]' "$plan")
test "${#units[@]}" -gt 0
failed=0
for UNIT_ID in "${units[@]}"; do
  primary_json="test-results/evidence/${UNIT_ID}-primary.json"
  primary_evidence="test-results/evidence/${UNIT_ID}-primary-evidence.json"
  unit_tmp=$(mktemp -d)
  start=$(date +%s)
  set +e
  TMPDIR="$unit_tmp" go run ./cmd/swarm-test --planned "$plan" "$UNIT_ID" | tee "$primary_json"
  statuses=("${PIPESTATUS[@]}")
  set -e
  elapsed=$(( $(date +%s) - start ))
  status=${statuses[0]}
  if [ "${statuses[1]}" -ne 0 ]; then status=1; fi
  # Go module downloads can leave read-only directories inside this owned temp.
  # Do not follow symlinks or let cleanup suppress this or later receipts.
  if ! find "$unit_tmp" -type d -exec chmod u+rwx {} +; then status=1; fi
  if ! rm -rf "$unit_tmp"; then status=1; fi
  # A failed member does not suppress the next member's independent receipt.
  if ! go run ./cmd/swarm-test-timing -record-evidence \
    -workflow-run-id "$GITHUB_RUN_ID" -workflow-attempt "$GITHUB_RUN_ATTEMPT" \
    -plan "$plan" -unit "$UNIT_ID" -attempt primary \
    -input "$primary_json" -evidence "$primary_evidence" \
    -elapsed-seconds "$elapsed" -exit-code "$status"; then failed=1; fi
  if ! go run ./cmd/swarm-test-timing -input "$primary_json" -markdown "test-results/evidence/${UNIT_ID}-timing.md"; then failed=1; fi
  if [ -f "test-results/evidence/${UNIT_ID}-timing.md" ]; then
    cat "test-results/evidence/${UNIT_ID}-timing.md" >> "$GITHUB_STEP_SUMMARY"
  fi
  if [ "$status" -ne 0 ]; then failed=1; fi
done
exit "$failed"
