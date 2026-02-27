# Spec Writer Feedback — v2.0.35 (Implementation + Gate Runner)

## Scope
This feedback covers contract/gate issues found while implementing and validating v2.0.35 against the live Go runtime.

## Executive Summary
- Runtime-side blockers discovered during implementation have been fixed locally.
- The remaining issues are mostly in spec contract packaging and gate command definitions.
- Biggest pain point: gate failures that are false negatives due to malformed YAML and brittle shell checks.

## 1) Contract Defects (spec package)

### 1.1 `event-catalog.yaml` parse blocker (duplicate/malformed keys)
- File: `contracts/event-catalog.yaml`
- Impact: all contract-aware verifier logic silently downgrades to "no contracts loaded", causing false orphan warnings and weakened checks.
- Defects found:
  - Malformed `feature_deployed.consumer` list (list item outside list structure).
  - Missing event key before a second block under `scan.requested`:
    - `emitter: runtime`, `consumer: scanner-directories`, etc. were duplicated inside `scan.requested` instead of being keyed as `scanner.directories.scan_assigned`.
- Suggested action:
  - Add CI gate that parses contracts with `gopkg.in/yaml.v3` (strict duplicate-key behavior) and `pyyaml`.
  - Fail package build if either parser fails.

## 2) Gate Manifest Defects (`verification-gates.yaml`)

### 2.1 `wiring-verifier-clean` command does not validate warn count
- Current command:
  - `go test ./internal/runtime -run TestSpecRuntimeWiringVerification -v 2>&1 | tail -1 | grep -q PASS`
- Problem:
  - This only checks the final `go test` status line, not `fail=0 warn=0`.
- Suggested fix:
  - Parse summary line explicitly, e.g.:
  - `go test ... -v 2>&1 | tee /tmp/wiring.out; grep -q 'summary: .*fail=0 warn=0' /tmp/wiring.out`

### 2.2 `wc -l | grep '^0$'` false negatives due to spacing
- Examples:
  - `grep ... | wc -l | grep -q '^0$'`
- Problem:
  - `wc -l` output is left-padded, often `'       0'`.
- Suggested fix:
  - Use `grep -q '^[[:space:]]*0$'` or `tr -d '[:space:]'`.

### 2.3 `ddl-schema-diff` handling of missing CLI command
- Current behavior observed:
  - `/bin/sh: empire: command not found` marked as FAIL.
- Spec intent:
  - mark infra/tooling unavailable as `UNVERIFIED`, not `FAIL`.
- Suggested fix:
  - Wrapper should classify command-not-found (`127`) as `UNVERIFIED` when gate is marked optional/unverified-eligible.

### 2.4 Integration gates that pass with `[no tests to run]`
- Observed for several integration gates.
- Problem:
  - command exits 0 even when no matching tests exist, creating false confidence.
- Suggested fix:
  - enforce test existence:
  - `go test ... -run TestX -count=1 -v | tee /tmp/t.out && ! grep -q '\\[no tests to run\\]' /tmp/t.out`

## 3) Data Request / Diagnostic Command Template Improvements

### 3.1 Event schema source paths
- Some templates point to `internal/events/*.go`, but runtime schemas are currently defined in runtime/tooling paths.
- Suggested fix:
  - include repo-specific source map in the request template:
  - event schemas: `internal/runtime/event_emit_tools.go`
  - commgraph producers: `internal/commgraph/registry.go`
  - interceptor logic: `internal/runtime/pipeline_coordinator.go`

### 3.2 Routes structure key assumptions
- Some parser snippets expect `routes` / `bootstrap`.
- Actual file uses:
  - `bootstrap_routes`
  - `seeded_routes`
- Suggested fix:
  - update parser snippets to check these keys first.

## 4) Runtime/Spec Alignment Notes (now fixed in implementation)
- `pipeline_receipts` moved to `status/error` schema: runtime store now writes new columns with legacy fallback.
- Pipeline coordinator persistence/state load aligned with v2.0.35 canonical runtime table columns:
  - `scan_accumulators`: `expected/complete/discovered/skipped`
  - `pending_dedup_candidates`: `dedup_event_id/name/payload`
  - `validation_pipelines`: `g2_spec/g3_cto/g4_brand`, `scoring_payload`, `packaging_requested`, `packaging_retries`

## 5) Suggested Additions for v2.0.36+
- Add `contracts/schema-parse-gate.sh`:
  - validates all YAML contracts with both Go YAML v3 and PyYAML.
- Add `contracts/commands-smoke.yaml`:
  - lists every referenced CLI command used by gates, with expected availability and fallback policy.
- Add gate-runner policy table:
  - `FAIL`: semantic mismatch
  - `UNVERIFIED`: missing infra/tooling where allowed
  - `PASS`: strict criteria met

## 6) Quick Message You Can Send
Use this summary verbatim if useful:

> We fixed runtime-side v2.0.35 blockers, but found contract/gate packaging defects causing false non-compliance. `event-catalog.yaml` had malformed/duplicate key sections that prevented contract loading; gate commands also include brittle checks (`tail -1 PASS`, `wc -l | grep '^0$'`) that generate false failures. Please add strict YAML parse gates (Go+PyYAML), correct event-catalog keying, and harden verification commands to check actual summary criteria (`fail=0 warn=0`) and treat missing tooling as `UNVERIFIED` where intended.

