# Catalog E2E — Runtime Validation Gaps (Implementer)

## Context

After Phase A.1 triage, 47 fixtures are supported and green. Of the 71 excluded, 15 are fixture-issues (spec writer track). The remaining **~56 are validation-gaps** — the fixtures load and the runtime processes them, but the outcome doesn't match expected.yaml.

There are **3 systemic patterns** covering most of the gaps. Fixing these patterns unlocks large batches of fixtures at once.

## Gap 1: Accumulation completion never fires (8+ fixtures)

**Symptom**: Entity stays in `collecting` instead of advancing to `complete`/`partial`/target state when accumulation thresholds are met.

**Affected fixtures** (all Tier 2):
- `test-accumulate-all` (completion: all)
- `test-accumulate-crash-recovery`
- `test-accumulate-expected-from-entity`
- `test-accumulate-from-filter`
- `test-accumulate-on-timeout`
- `test-accumulate-threshold`
- `test-accumulate-timeout`
- `test-accumulate-with-compute`

**Where to look**: The accumulation pipeline is in `internal/runtime/engine/executor.go`:

1. `stepAccumulate()` (~line 400) — runs the accumulation step
2. When incomplete, sets `OutcomeWaiting` and returns `(true, nil)`, which stops the step pipeline
3. When complete, returns `(false, nil)` to continue to `stepAdvancesTo`
4. `accumulatorComplete()` in `engine/helpers.go` (~line 405) determines if completion criteria are met

**Investigation approach**:
1. Add debug logging to `accumulatorComplete()` to see what `acc.Expected`, `acc.ExpectedCount`, and `acc.Received` look like when the final event arrives
2. Check if `expectedAccumulatorTargets()` correctly resolves `expected_from: entity.expected_count` — does it read the entity field that was seeded via `entity_fields_before`?
3. Check if `dedupIdentifier()` generates the right keys — if all events get the same dedup key, only one is counted as received
4. Check if the accumulator state persists across events — each event should see the accumulated state from previous events

**Expected payoff**: Fixing the completion check should promote all 8 Tier 2 fixtures in one shot.

## Gap 2: Fan-out doesn't execute (3 fixtures)

**Symptom**: Entity stays in `pending` instead of advancing. Fan-out events are not emitted.

**Affected fixtures** (Tier 3):
- `test-fan-out-basic` (expects 3x `scan.assigned`, state → `scanning`)
- `test-fan-out-count` (expects state → `processing`)
- `test-fan-out-empty` (expects `fan_out_count: 0` in entity fields)

**Where to look**: `stepFanOut()` in `engine/executor.go` (~line 542):

1. Check if `resolveContractPath()` correctly resolves `items_from: payload.items`
2. Check if `sliceFromAny()` converts the payload items correctly
3. If fan-out runs, it sets `OutcomeFannedOut` and stops execution — check if `stepAdvancesTo` gets skipped

**Expected payoff**: 3 Tier 3 fixtures.

## Gap 3: Rules / on_complete / compound guards stay in pending (5+ fixtures)

**Symptom**: Entity stays in `pending` instead of advancing via rules-based or on_complete-based transitions.

**Affected fixtures** (Tier 1):
- `test-guard-compound-condition` — compound guard doesn't evaluate
- `test-on-complete-first-match` — on_complete doesn't fire
- `test-on-complete-second-match` — on_complete doesn't fire
- `test-on-complete-with-state` — on_complete doesn't fire
- `test-rules-advances-to` — rules-based advances_to doesn't fire

**Where to look**:
- `stepOnComplete()` in executor.go (~line 592) — only reached if accumulation passes
- `stepRules()` — rule condition evaluation
- `stepGuard()` — compound condition evaluation

These are likely individual feature gaps in the step execution pipeline. Debug each one separately.

## Gap 4: Reduce value computation (3 fixtures)

**Symptom**: Entity field `result` or `composite` stays at zero instead of computing the expected value.

**Affected fixtures** (Tier 3):
- `test-reduce-pick-or-average`
- `test-reduce-sum`
- `test-reduce-weighted-average`

**Where to look**: `stepReduce()` or `stepCompute()` in the executor. The reduce operation should compute a value from accumulated items and store it in an entity field.

## Gap 5: Guard outcome escalate (1 fixture)

**Symptom**: `handler_outcome=escalate` not asserted by the E2E harness.

**Affected fixture**: `test-guard-escalate`

This may be a harness gap (assertions don't support escalate outcome) rather than a runtime gap. Check if the runtime produces the correct receipt outcome for escalation, then add assertion support if needed.

## Remaining gaps (Tiers 5–7)

These are smaller individual gaps, not systemic patterns:

**Tier 5** (11 excluded):
- Timer fixtures (`test-timer-*`) — need async time advancement
- `test-create-flow-instance` — UNDEFINED-FIELD on `action_params` (fixture-issue, spec writer track)
- `test-wildcard-subscription` — wildcard matching not implemented

**Tier 6** (9 excluded):
- Atomicity fixtures (`test-atomicity-*`) — need transaction-level assertions
- `test-dead-letter` — event doesn't dead-letter in live runtime
- `test-entity-serialization` — serialization assertions not wired
- `test-chain-depth-limit` — depth limit not enforced

**Tier 7** (6 excluded):
- Agent fixtures need `start=true` runtime stabilization
- `test-agent-emits-to-node` — agents.yaml not boot-valid for real loader

## Recommended execution order

1. **Gap 1 (accumulation)** — highest fixture count (8), likely a single root cause
2. **Gap 2 (fan-out)** — 3 fixtures, likely simple resolution path issue
3. **Gap 3 (rules/on_complete)** — 5 fixtures, may be individual feature gaps
4. **Gap 4 (reduce)** — 3 fixtures, depends on accumulation working first
5. **Gap 5 + remainder** — individual items

After each fix, move the fixture from the excluded map to the supported list and verify green.

## Verification

```bash
go test ./internal/runtime/cataloge2e/... -count=1 -v
go test ./... -count=1
```
