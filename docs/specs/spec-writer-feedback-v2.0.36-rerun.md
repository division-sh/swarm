# Spec Writer Feedback — v2.0.36 Re-run

## Re-run Summary
- Process repeated end-to-end:
  1. Extracted updated `docs/specs/empireai-v2.0.36.tar`
  2. Synced `contracts/*` to repo root
  3. Ran full 18-gate pass on fresh ephemeral Postgres
- Reports:
  - `docs/specs/v2.0.36-gate-report.txt`
  - `docs/specs/v2.0.36-gate-report.json`

Current outcome:
- `total=18`
- `pass=12`
- `fail=3`
- `unverified=3`
- `must_pass_fail=2`

## Remaining Must-pass Failures

### 1) `xval-emit-events-in-catalog` fails on scanner template placeholder
- Error:
  - `Emit events missing from catalog: ['scanner-agent: scanner.{type}.scan_complete']`
- Cause:
  - `contracts/agent-tools.yaml` uses template placeholder events for `scanner-agent`:
    - `scanner.{type}.scan_complete`
  - `contracts/event-catalog.yaml` enumerates concrete event names only:
    - `scanner.google_maps.scan_complete`
    - `scanner.instagram.scan_complete`
    - `scanner.reviews.scan_complete`
    - `scanner.directories.scan_complete`
    - `scanner.yelp.scan_complete`

### 2) `xval-subscriptions-in-catalog` fails on scanner template placeholder
- Error:
  - `Subscriptions missing from catalog: ['scanner-agent: scanner.{type}.scan_assigned']`
- Cause:
  - Same placeholder-vs-concrete mismatch.

## Recommended Contract Fix
Choose one and keep it consistent across both contracts and gates:

Option A (recommended): keep concrete-only event names
- In `agent-tools.yaml`, replace scanner placeholders with concrete entries:
  - `scanner.google_maps.scan_assigned`
  - `scanner.instagram.scan_assigned`
  - `scanner.reviews.scan_assigned`
  - `scanner.directories.scan_assigned`
  - `scanner.yelp.scan_assigned`
  - and corresponding `*.scan_complete`

Option B: keep placeholders
- Extend xval gate logic to treat `scanner.{type}.X` as wildcard pattern matching concrete catalog events.

## Non-blocking Observations
- `ddl-runtime-tables-match-structs` now passes.
- `wiring-delivery-channel-complete` now correctly reports non-zero event count (`166 events`).
- Integration gates still `UNVERIFIED` due `[no tests to run]` (expected by current gate policy for infra/test availability).
- `ddl-schema-diff` still fails because `empire` CLI command is unavailable in this environment (`command not found`).

