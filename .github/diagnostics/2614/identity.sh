#!/usr/bin/env bash
set -euo pipefail
[[ "$GITHUB_EVENT_NAME" = workflow_dispatch ]]
[[ "$GITHUB_RUN_ATTEMPT" = 1 ]]
[[ "$GITHUB_REF" = refs/heads/agent-g/diagnostic-2614-deadlock-cycle ]]
[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$EXPECTED_SHA" = "$GITHUB_SHA" && "$GITHUB_SHA" = "$(git rev-parse HEAD)" ]]
git merge-base --is-ancestor f2e0d031a06d3f8e5a6ec487c8f90c3e212860d8 HEAD
sha256sum --check .github/diagnostics/2614/capture-files.sha256
sha256sum --check .github/diagnostics/2614/validation.sha256
mkdir -p test-results/diagnostic-plan
jq -n --arg run "$GITHUB_RUN_ID" --arg attempt "$GITHUB_RUN_ATTEMPT" \
  --arg sha "$GITHUB_SHA" --arg tree "$(git rev-parse HEAD^{tree})" \
  '{purpose:"DIAGNOSTIC ONLY",qualification_credit:false,run_id:$run,run_attempt:$attempt,
    head_sha:$sha,tree:$tree,base:"f2e0d031a06d3f8e5a6ec487c8f90c3e212860d8"}' \
  > test-results/diagnostic-plan/identity.json
printf 'DIAGNOSTIC ONLY; one invocation; missing evidence is INCOMPLETE; no qualification credit.\n' \
  > test-results/diagnostic-plan/DIAGNOSTIC-ONLY.txt
cat test-results/diagnostic-plan/DIAGNOSTIC-ONLY.txt >> "$GITHUB_STEP_SUMMARY"
