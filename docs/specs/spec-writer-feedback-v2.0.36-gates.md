# Spec Writer Feedback — v2.0.36 Gate Integrity

## Context
I extracted `empireai-v2.0.36.tar`, synced contracts, and ran all 18 gates.

Generated artifacts:
- `docs/specs/v2.0.36-gate-report.json`
- `docs/specs/v2.0.36-gate-report.txt`

Current summary from gate run:
- `total=18`
- `pass=13`
- `fail=2`
- `unverified=3`
- `must_pass_fail=1` (reported as non-compliant)

## Key Findings

### 1) `ddl-runtime-tables-match-structs` false FAIL (gate command bug)
- Gate fails on:
  - `psql -c "\di idx_sessions_active" | grep -q scope_key`
- Problem:
  - `\di` output does not include index column expression text.
  - In v2.0.36 canonical DDL, index is expression-based:
    - `COALESCE(scope_key, ''::text)` (not literal `scope_key` in `\di` listing)
- Verified actual index definition is correct:
  - `SELECT indexdef FROM pg_indexes WHERE indexname='idx_sessions_active'`
  - shows `... (agent_id, runtime_mode, COALESCE(scope_key, ''::text)) ...`

Suggested fix:
- Replace `\di ... | grep ...` checks with `pg_indexes.indexdef` checks.

---

### 2) Several gates are currently no-op due contract structure mismatch

The new contract files are **map-shaped** (top-level keys), but several gate commands treat them as **list-shaped** (`events: []`, `agents: []`).

#### 2.1 `wiring-delivery-channel-complete` (must_pass)
- Current command does:
  - `catalog = yaml.safe_load(...); events = catalog.get('events', [])`
- Because `event-catalog.yaml` is map-shaped, `events` becomes `[]`.
- Output becomes: `0 events, all have delivery_channel` (false positive).

#### 2.2 `xval-emit-events-in-catalog` (must_pass)
- Same `catalog.get('events', [])` list assumption -> false positive risk.

#### 2.3 `xval-subscriptions-in-catalog` (must_pass)
- Same list assumption -> false positive risk.

#### 2.4 `xval-catalog-emitters-exist` (should_pass)
- Same list assumption -> false positive risk.

#### 2.5 `agents-naming-convention` (should_pass)
- Current command expects:
  - `agents = yaml.safe_load(...); agents.get('agents', [])`
  - each item with `.get('type')`
- But `agent-tools.yaml` is map-shaped by agent id, so it prints:
  - `0 agent configs found` (false positive).

Suggested fix:
- Update all YAML gate scripts to support map-shaped contracts directly:
  - iterate `catalog.items()` for events
  - iterate `agents.items()` for agent entries
  - only skip metadata keys starting with `_` if needed

---

### 3) Upgrade action verify commands still have brittle count checks
In `upgrade-actions.yaml`, some verify commands use:
- `... | wc -l | grep -q '^0$'`

`wc -l` output is padded (`"       0"`), causing false failures.

Suggested fix:
- Use:
  - `... | wc -l | tr -d '[:space:]' | grep -q '^0$'`
or:
  - `... | grep -q '^[[:space:]]*0$'`

---

### 4) Integration gates are now correctly hardened
- Good improvement: `no tests to run` is now detected.
- Current run status:
  - `integration-event-roundtrip`: `UNVERIFIED`
  - `integration-crash-recovery`: `UNVERIFIED`
  - `integration-dual-assessment`: `UNVERIFIED`
- This matches expected behavior when named tests are not present.

## Implementation Status vs v2.0.36

Runtime behavior checks are healthy:
- `go test ./internal/runtime -run TestSpecRuntimeWiringVerification -v` -> `fail=0 warn=0`
- Runtime package tests pass.
- Schema alignment issues fixed in runtime/store code paths (v2.0.35 carryover).

Remaining blocker is gate-definition integrity, not runtime functionality.

## Quick Message to Send

> v2.0.36 is much better, but one must-pass gate is still a false FAIL (`ddl-runtime-tables-match-structs`) because it checks `\di` output for `scope_key`; the index is expression-based and valid in `pg_indexes.indexdef`. Also, multiple gates are currently no-op due map-vs-list contract parsing (`event-catalog.yaml` and `agent-tools.yaml` are top-level maps, but gates still do `.get('events', [])` / `.get('agents', [])`), producing false PASS like `0 events, all have delivery_channel` and `0 agent configs found`. Please update gate commands to iterate map-shaped contracts and replace `wc -l | grep '^0$'` patterns in upgrade-actions with whitespace-safe checks.

