# Catalog E2E — Runtime Gaps (Implementer)

## Context

53 of 118 fixtures are supported and green (45%). The remaining 65 are excluded with individually verified reasons. This handoff covers all runtime-side work needed to close the gaps and maximize fixture coverage.

Current state by tier:

| Tier | Supported | Excluded | Total |
|------|-----------|----------|-------|
| 1 | 20 | 12 | 32 |
| 2 | 2 | 8 | 10 |
| 3 | 6 | 8 | 14 |
| 4 | 4 | 1 | 5 |
| 5 | 1 | 11 | 12 |
| 6 | 1 | 9 | 10 |
| 7 | 1 | 6 | 7 |
| 8 | 18 | 10 | 28 |
| 9/10/11 | — | — | 34 (no runners yet) |

Work is organized into 8 work items, ordered by fixture payoff.

---

## Work Item 1: Accumulation completion (8 Tier 2 fixtures)

**Symptom**: Entity stays in `collecting` instead of advancing when accumulation thresholds are met.

**Affected fixtures**:
- `test-accumulate-all` — completion: all, expects state → `complete`
- `test-accumulate-crash-recovery` — expects state → `complete`
- `test-accumulate-expected-from-entity` — expected_from reads entity field, expects state → `complete`
- `test-accumulate-from-filter` — accumulate with from_filter, expects state → `complete`
- `test-accumulate-on-timeout` — expects state → `partial`
- `test-accumulate-threshold` — threshold-based completion, expects state → `complete`
- `test-accumulate-timeout` — timeout completion, expects state → `complete`
- `test-accumulate-with-compute` — accumulate + compute combo, expects state → `complete`

**Where to look**: `internal/runtime/engine/executor.go` and `engine/helpers.go`:

1. `stepAccumulate()` — when incomplete, sets `OutcomeWaiting` and returns `(true, nil)` which halts the step pipeline. When complete, returns `(false, nil)` to continue to `stepAdvancesTo`.
2. `accumulatorComplete()` in helpers.go — determines if completion criteria are met by checking `acc.Expected` list, `acc.ExpectedCount`, or `acc.Received` map.
3. `expectedAccumulatorTargets()` — resolves the expected count. For `expected_from: entity.expected_count`, this must read the entity field seeded via `entity_fields_before`.
4. `dedupIdentifier()` — generates arrival keys. If all events get the same dedup key, only one counts.

**Investigation approach**:
1. Add debug logging to `accumulatorComplete()` — print `acc.Expected`, `acc.ExpectedCount`, `len(acc.Received)`, and the dedup keys
2. Check if accumulator state persists across events — each event must see the accumulated state from previous events, not a fresh accumulator
3. Check if `expectedAccumulatorTargets()` correctly reads entity fields seeded by `entity_fields_before`
4. For timeout fixtures: check if the timeout path triggers completion or if it requires async timer support

**Payoff**: 8 fixtures. Likely a single root cause.

---

## Work Item 2: Rules / on_complete / compound guard evaluation (5 Tier 1 fixtures)

**Symptom**: Entity stays in `pending` instead of advancing via rules-based or on_complete-based transitions.

**Affected fixtures**:
- `test-guard-compound-condition` — compound guard, expects state → `done`
- `test-on-complete-first-match` — on_complete rule, expects state → `passed`
- `test-on-complete-second-match` — on_complete rule, expects state → `failed`
- `test-on-complete-with-state` — on_complete with state check, expects state → `done`
- `test-rules-advances-to` — rules block with advances_to, expects state → `approved`

**Where to look**: Step execution pipeline in `engine/executor.go`:
- `stepRules()` — evaluates `rules:` block conditions and applies matching rule's `advances_to`
- `stepOnComplete()` — evaluates `on_complete:` rules after accumulation completes
- `stepGuard()` — evaluates compound `check:` conditions with `&&`/`||`

These are likely 2-3 separate issues:
1. Compound guard condition parsing (`&&` / `||` in CEL expressions)
2. `on_complete` rule matching — may not fire because it depends on accumulation (which is broken per Work Item 1)
3. `rules` block condition evaluation and advances_to application

Debug each fixture individually. The on_complete fixtures may unblock automatically once accumulation (Work Item 1) is fixed.

**Payoff**: 5 fixtures.

---

## Work Item 3: Guard outcome recording (4 Tier 1 fixtures)

**Symptom**: Guard evaluates correctly (state doesn't advance) but the handler_outcome is recorded as `success` instead of the expected guard outcome.

**Affected fixtures**:
- `test-guard-discard` — expects handler_outcome `discard`, runtime records `success`
- `test-guard-kill` — expects handler_outcome `dead_letter`, runtime records `success`
- `test-guard-multi-fail` — expects handler_outcome `reject` (harness doesn't assert this yet)
- `test-guard-reject` — expects handler_outcome `reject` (harness doesn't assert this yet)

**Two sub-tasks**:

1. **Runtime fix** (discard/kill): When a guard fires with `on_fail: discard` or `on_fail: kill`, the receipt outcome should be `discard` or `dead_letter` respectively, not `success`. Check `stepGuard()` — when a guard fails, does it set the correct `frame.result.Status`? And does the receipt writer translate that status to the correct outcome string?

2. **Harness fix** (reject): `assertHandlerOutcome` doesn't support `reject` as a valid outcome. Add `reject` to the assertion's accepted outcomes, mapping it to whatever the runtime actually records for `on_fail: reject`.

**Payoff**: 4 fixtures.

---

## Work Item 4: Fan-out execution (3 Tier 3 fixtures)

**Symptom**: Entity stays in `pending`. Fan-out events are not emitted.

**Affected fixtures**:
- `test-fan-out-basic` — expects 3x `scan.assigned`, state → `scanning`
- `test-fan-out-count` — expects state → `processing`
- `test-fan-out-empty` — expects `fan_out_count: 0` in entity fields

**Where to look**: `stepFanOut()` in `engine/executor.go`:
1. Check if `resolveContractPath()` correctly resolves `items_from: payload.items`
2. Check if `sliceFromAny()` converts the JSON payload items correctly
3. Fan-out sets `OutcomeFannedOut` and returns `(true, nil)` — check if `stepAdvancesTo` still runs (it shouldn't be skipped if fan-out also needs to advance state)
4. For `test-fan-out-empty`: check if the empty-items path still persists `fan_out_count: 0`

**Payoff**: 3 fixtures.

---

## Work Item 5: Reduce value computation (3 Tier 3 fixtures)

**Symptom**: Entity fields (`result`, `composite`) stay at zero instead of computing the expected value.

**Affected fixtures**:
- `test-reduce-pick-or-average` — expects computed `result` value
- `test-reduce-sum` — expects sum in entity field
- `test-reduce-weighted-average` — expects weighted average in `composite`

**Where to look**: `stepReduce()` or `stepCompute()` in the executor. The reduce operation should:
1. Read accumulated items
2. Apply the reduce operation (sum, average, weighted average, pick)
3. Store the result in the entity field specified by the fixture

**Note**: These may depend on accumulation (Work Item 1) working correctly to have items to reduce.

**Payoff**: 3 fixtures.

---

## Work Item 6: Data accumulation literal persistence (1 Tier 1 fixture)

**Symptom**: Runtime records bookkeeping for data_accumulation but doesn't persist the literal value into the entity field.

**Affected fixture**:
- `test-data-accumulation-literal` — expects `category` field to be set to `"premium"` via literal write

**Where to look**: `stepDataAccumulation()` or equivalent — the `writes:` block with a literal `value:` should persist the value directly to the entity field, not just record it as bookkeeping.

**Payoff**: 1 fixture.

---

## Work Item 7: Harness and assertion extensions (3 Tier 1 + various)

These are harness-side gaps, not runtime bugs:

**Guard escalate assertion**:
- `test-guard-escalate` — harness doesn't assert `handler_outcome=escalate`
- Add `escalate` to `assertHandlerOutcome`'s accepted outcomes

**Guard reject assertion** (if not covered by Work Item 3):
- `test-guard-multi-fail`, `test-guard-reject` — same pattern
- Add `reject` to `assertHandlerOutcome`

**Payoff**: Up to 3 fixtures after harness + runtime fixes.

---

## Work Item 8: Tier 9/10/11 test runners + Tier 5/6/7 remaining

### Tier 9 (composition patterns) — 12 fixtures, no runner yet

Create `tier9_composition_e2e_test.go`. These are multi-primitive compositions (accumulate+compute+branch, gate chains, guard+query, multi-emit cross-flow). They should work once Work Items 1-5 are fixed since they compose the same primitives.

Start with a whitelist of 0 supported, all 12 excluded. Triage each one after the primitive gaps are closed.

### Tier 10 (policy patterns) — 6 fixtures, no runner yet

Create `tier10_policy_e2e_test.go`. These test policy evaluation (capacity queries, counter escalation, threshold, timeout, hard gate override, multi-guard partial).

Start with 0 supported, all excluded. Triage after primitives are stable.

### Tier 11 (flow composition) — 16 fixtures, no runner yet

Create `tier11_flow_composition_e2e_test.go`. These test child flows, pin wiring, nested 3-level flows, policy inheritance, tool inheritance, and sibling isolation.

**Important**: These likely need multi-flow loading — `loadFixtureBundle` may need to handle the `flows/` subdirectory in the fixture root. Check if the existing bundle loader already handles this.

Start with 0 supported, all excluded. Triage incrementally.

### Tier 5 remaining (11 excluded)

5 are fixture-issues (spec writer track: missing produces, action_params). The rest:
- `test-timer-fire` — timer fires async, not in quiescence window
- `test-timer-recurring` — scheduler-driven, needs async support
- `test-timer-start-on` — fixture-issue (incomplete timer fire event)
- `test-wildcard-subscription` — wildcard handler matching not implemented

### Tier 6 remaining (9 excluded)

All validation-gaps:
- 3 atomicity fixtures — need transaction-level assertions
- `test-chain-depth-limit` — depth limit not enforced
- `test-cross-entity-concurrent` — concurrency assertions needed
- `test-dead-letter` — emits contradiction diagnostics instead of dead_letter
- `test-entity-serialization` — serialization assertions needed
- `test-event-validation` — reject + dead-letter not produced
- `test-guards-pre-handler-state` — pre-handler guard state ordering

### Tier 7 remaining (6 excluded)

- `test-agent-emits-to-node` — fixture-issue (agents.yaml missing required fields)
- 3 untriaged — need `start=true` runtime stabilization first
- `test-dual-delivery` — needs `agent_received` assertion
- `test-multi-gate-pipeline` — needs gate assertions

**Note on start=true**: Tiers 7 and 11 involve agent execution. The current harness uses `start=false`. Agent fixtures likely need the full runtime loop running. Stabilize the started-runtime path before triaging agent fixtures — expect extra processing noise and adjust quiescence detection or assertions accordingly.

---

## Execution order

| Priority | Work Item | Fixtures unlocked | Dependencies |
|----------|-----------|-------------------|--------------|
| 1 | Accumulation completion | 8 | None |
| 2 | Rules/on_complete/compound guard | 5 | May partly depend on #1 |
| 3 | Guard outcome recording + harness | 4+3 | None |
| 4 | Fan-out execution | 3 | None |
| 5 | Reduce computation | 3 | Depends on #1 |
| 6 | Data accumulation literal | 1 | None |
| 7 | Tier 9/10/11 runners | 0 initially | Create runners, triage after #1-6 |
| 8 | Tier 5/6/7 remaining | Individual | Mixed dependencies |

After each work item, promote passing fixtures from excluded to supported and verify:

```bash
go test ./internal/runtime/cataloge2e/... -count=1
go test ./... -count=1
```

## Definition of done

- [ ] Work Items 1-6 fixed and passing fixtures promoted
- [ ] Tier 9/10/11 test runners created with classification gates
- [ ] All fixtures explicitly classified (supported, validation-gap, fixture-issue, or harness-gap)
- [ ] `go test ./internal/runtime/cataloge2e/... -count=1` green
- [ ] `go test ./... -count=1` green
