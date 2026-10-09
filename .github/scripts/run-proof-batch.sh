#!/usr/bin/env bash
set -euo pipefail

plan=test-results/plan/proof-plan.json
mkdir -p test-results/evidence
go run ./cmd/swarm-test-timing -assert-execution-sha -plan "$plan" -execution-sha "$(git rev-parse HEAD)"
mapfile -t units < <(jq -er --arg id "$BATCH_ID" '.batches[] | select(.id == $id) | .units[]' "$plan")
test "${#units[@]}" -gt 0
failed=0
for UNIT_ID in "${units[@]}"; do
  # Prepare the fixture, never let the admission observer pull an image.
  if jq -e --arg id "$UNIT_ID" '.units[] | select(.id == $id) | any(.required_tests[]?; .package == "github.com/division-sh/swarm/internal/runtime/workspace" and .name == "TestVerifyCLIImageProbeLifecycleRealDocker")' "$plan" >/dev/null; then
    docker pull golang:1.25-bookworm
  fi
  primary_json="test-results/evidence/${UNIT_ID}-primary.json"
  primary_evidence="test-results/evidence/${UNIT_ID}-primary-evidence.json"
  unit_tmp=$(mktemp -d)
  command=(go run ./cmd/swarm-test --planned "$plan" "$UNIT_ID")
  if [ "$UNIT_ID" = persistence-authority-debt-census ]; then
    command=(env "SWARM_DEBT_CACHE_DIR=$RUNNER_TEMP/debt-analysis" "${command[@]}")
  fi
  if jq -e --arg id "$UNIT_ID" '.units[] | select(.id == $id) | .budget_class == "soak"' "$plan" >/dev/null; then
    unit=$(jq -cer --arg id "$UNIT_ID" '.units[] | select(.id == $id)' "$plan")
    test "$(jq -r '.count_mode' <<<"$unit")" = count-1
    test -z "$(jq -r '.skip // empty' <<<"$unit")"
    command=(timeout --signal=TERM --kill-after=30s 1500s "${command[@]}")
  fi
  start=$(date +%s)
  set +e
  TMPDIR="$unit_tmp" "${command[@]}" | tee "$primary_json"
  statuses=("${PIPESTATUS[@]}")
  set -e
  elapsed=$(( $(date +%s) - start ))
  status=${statuses[0]}
  if [ "${statuses[1]}" -ne 0 ]; then status=1; fi
  # Go module downloads can leave read-only directories inside this owned temp.
  # Do not follow symlinks or let cleanup suppress this or later receipts.
  if ! find "$unit_tmp" -type d -exec chmod u+rwx {} +; then status=1; fi
  if ! rm -rf "$unit_tmp"; then status=1; fi
  # Retain the failed receipt before fail-fast leaves later members unproven.
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
  if [ "$failed" -ne 0 ] && [ "${KEEP_GOING:-false}" != true ]; then break; fi
done
exit "$failed"
