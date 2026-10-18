#!/usr/bin/env bash
set -euo pipefail
dir=$(dirname "${BASH_SOURCE[0]}")
out=${DIAGNOSTIC_EVIDENCE_DIR:-test-results/diagnostic-evidence}
plan=${DIAGNOSTIC_PLAN_DIR:-test-results/plan}
evidence=${DIAGNOSTIC_COMMAND_DIR:-test-results/evidence}
private=${DIAGNOSTIC_PRIVATE_DIR:-$RUNNER_TEMP/diagnostic-2614-private}
mkdir -p "$out"
printf '%s\n' '{"purpose":"DIAGNOSTIC ONLY","evidence":"INCOMPLETE","causal_evidence":"INCOMPLETE","qualification_credit":false}' > "$out/result.json"
printf 'DIAGNOSTIC ONLY. Never use this artifact for qualification.\n' > "$out/DIAGNOSTIC-ONLY.txt"
missing=()
for file in proof-plan.json proof-matrix.json identity.json; do
  if [ -f "$plan/$file" ]; then cp "$plan/$file" "$out/$file"; else missing+=("$file"); fi
done
if ! jq -e --arg run "$GITHUB_RUN_ID" --arg attempt "$GITHUB_RUN_ATTEMPT" \
  --arg sha "$GITHUB_SHA" --arg tree "$(git rev-parse HEAD^{tree})" '
  .purpose=="DIAGNOSTIC ONLY" and .qualification_credit==false and
  .run_id==$run and .run_attempt==$attempt and .head_sha==$sha and .tree==$tree and
  .base=="f2e0d031a06d3f8e5a6ec487c8f90c3e212860d8"
  ' "$plan/identity.json" >/dev/null 2>&1; then missing+=("valid_plan_identity"); fi
cp "$dir/frozen-selection.json" "$dir/capture-files.sha256" "$dir/validation.sha256" \
  "$dir/capture-validation.log" "$dir/provenance.json" "$out/"
cp "$dir/sites.jq" "$dir/sites-validation.txt" "$out/"
receipt="$evidence/serveapp-runtime-primary-evidence.json"
raw="$evidence/serveapp-runtime-primary.json"
if [ -f "$receipt" ]; then
  cp "$receipt" "$out/command-evidence.json"
  # Transport/header checks only; run-proof-batch.sh retains the existing
  # record-evidence owner and its complete canonical admission checks.
  if ! jq -e --slurpfile p "$plan/proof-plan.json" --arg sha "$GITHUB_SHA" \
    --argjson run "$GITHUB_RUN_ID" --argjson attempt "$GITHUB_RUN_ATTEMPT" '
    $p[0] as $p | ($p.units[]|select(.id=="serveapp-runtime")) as $u |
    .head_sha==$sha and .head_sha==$p.head_sha and .plan_digest==$p.digest and
    .workflow_run_id==$run and .workflow_attempt==$attempt and
    .unit_id==$u.id and .surface==$u.id and .attempt=="primary" and
    .profile==$p.profile and .build_context==$p.build_context and
    .workload_profile==$u.workload_profile and .execution_tier==$u.execution_tier and
    .packages==$u.packages and .environment_id==$u.environment_id and
    .count_mode==$u.count_mode and (.run//"")==($u.run//"") and
    (.skip//"")==($u.skip//"") and (.go_timeout//"")==($u.go_timeout//"")
  ' "$receipt" >/dev/null 2>&1; then missing+=("valid_command_identity"); fi
else missing+=("command_evidence"); fi
if [ -f "$raw" ]; then
  sha256sum "$raw" | cut -d' ' -f1 > "$out/raw-json.sha256"
  jq -Rrc 'fromjson? | select(.Action=="pass" or .Action=="fail" or .Action=="skip") |
    {Time,Action,Package,Test,Elapsed} | with_entries(select(.value!=null))' "$raw" > "$out/terminal-records.jsonl"
  if ! jq -Rnc -f "$dir/trace.jq" "$raw" > "$out/redacted-trace.jsonl" 2>/dev/null; then
    missing+=("valid_redacted_trace")
  fi
  if ! jq -Rnc -f "$dir/sites.jq" "$raw" > "$out/failure-sites.jsonl" 2>/dev/null; then
    missing+=("valid_failure_site_stream")
  fi
  if ! jq -es 'any(.[]; .Package=="github.com/division-sh/swarm/internal/serveapp" and
    (.Test//"")=="" and (.Action=="pass" or .Action=="fail"))' "$out/terminal-records.jsonl" >/dev/null; then
    missing+=("terminal_package_record")
  fi
  if [ -f "$plan/proof-plan.json" ]; then
    if ! jq -es --slurpfile p "$plan/proof-plan.json" '
      ([.[]|select(.Test!=null)|.Package+"::"+.Test]|unique) as $actual |
      ([$p[0].units[]|select(.id=="serveapp-runtime")|.selected_roots[]|.package+"::"+.name] - $actual)|length==0
    ' "$out/terminal-records.jsonl" >/dev/null; then missing+=("all_selected_root_terminals"); fi
  fi
else missing+=("raw_test_stream"); fi
code=null
if [ -f "$private/wrapper-exit.txt" ] && grep -Eq '^[0-9]{1,3}$' "$private/wrapper-exit.txt"; then
  code=$(cat "$private/wrapper-exit.txt")
else missing+=("wrapper_exit"); fi
cleanup_error=false
if [ -f "$private/stderr.log" ]; then
  sha256sum "$private/stderr.log" | cut -d' ' -f1 > "$out/raw-stderr.sha256"
  if grep -Eq 'remove runner-owned Postgres:|release test slot:' "$private/stderr.log"; then cleanup_error=true; fi
else missing+=("runner_stderr"); fi
if [ -f "$private/stdout.log" ]; then
  sha256sum "$private/stdout.log" | cut -d' ' -f1 > "$out/raw-stdout.sha256"
else missing+=("runner_stdout"); fi
for file in started.txt finished.txt; do
  if [ -f "$private/$file" ]; then cp "$private/$file" "$out/$file"; else missing+=("$file"); fi
done
for file in go-environment.json runner-environment.json docker-version.txt; do
  [ -s "$out/$file" ] || missing+=("$file")
done
if [ -f "$out/terminal-records.jsonl" ] && [ -f "$out/failure-sites.jsonl" ]; then
  jq -n --slurpfile records "$out/terminal-records.jsonl" --slurpfile sites "$out/failure-sites.jsonl" '
    [$records[]|select(.Action=="fail" and .Test!=null)|.Test] as $failed |
    [$failed[]|. as $test|select(any($failed[];startswith($test+"/"))|not)] as $leaves |
    [$sites[]|select(.validation=="valid" and .kind=="site" and .prior_failure==false)|.test]|unique as $located |
    ([$sites[]|select(.validation=="rejected")]|length) as $rejected |
    ($leaves-$located) as $missing |
    {causal_evidence:(if ($missing|length)>0 or $rejected>0 then "INCOMPLETE"
      elif ($leaves|length)==0 then "NEGATIVE" else "SITE_OBSERVED" end),
      failed_leaves_without_site:$missing,rejected_site_records:$rejected}
  ' > "$out/site-attribution.json"
else
  printf '%s\n' '{"causal_evidence":"INCOMPLETE","failed_leaves_without_site":[],"rejected_site_records":0}' > "$out/site-attribution.json"
fi
missing_json=$(printf '%s\n' "${missing[@]}" | jq -Rsc 'split("\n")|map(select(length>0))')
jq -n --slurpfile attribution "$out/site-attribution.json" --argjson missing "$missing_json" --argjson code "$code" --argjson cleanup_error "$cleanup_error" '
  {purpose:"DIAGNOSTIC ONLY",qualification_credit:false,
    evidence:(if ($missing|length)>0 then "INCOMPLETE" else "OBSERVED" end),
    missing:$missing,wrapper_exit:$code,cleanup_error_reported:$cleanup_error,
    cleanup:(if $code==null then "unobserved" elif $cleanup_error then "runner_reported_error"
      else "runner_returned_no_cleanup_error_reported" end)} + $attribution[0]' > "$out/result.json"
(cd "$out" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$out/SHA256SUMS"
cat "$out/result.json"
[ "${#missing[@]}" -eq 0 ]
jq -e '.causal_evidence!="INCOMPLETE"' "$out/result.json" >/dev/null
