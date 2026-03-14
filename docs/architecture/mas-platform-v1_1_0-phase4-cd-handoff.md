# Phase 4 Tranche C+D: Runtime Hardening & Flow Instance Lifecycle

**Date:** 2026-03-13
**Prerequisite:** Tranche B (all sub-tranches) committed and green
**Risk level:** HIGH — touches live event processing and agent lifecycle
**Scope:** 6 focused changes across runtime enforcement (C) and flow instance lifecycle (D)

---

## Context

The engine (`internal/runtime/engine/executor.go`) already implements:
- Full 13-step handler execution (clear_gates → guard → accumulate → ... → action)
- Guard evaluation (CEL expressions + 6 built-in guards)
- Guard failure handling (reject, blocked, discard, kill, escalate)
- Action execution (increment_revision_count, record_evidence, create_flow_instance, state change)
- Entity locking + transaction wrapper
- Accumulation with idempotency
- Timer intents

Flow activation (`manager/flow_activation.go`) already implements:
- Agent spawning from flow templates with variable substitution
- Subscription namespacing for local events
- Route installation via `bus.AddFlowInstance()`
- Instance persistence via `WorkflowInstanceStore`

This tranche closes the remaining gaps to make the system safe for a real run.

---

## C.1: Fix `execSaveEntityField` upsert race

**File:** `internal/runtime/tools/executor_entity.go`
**Risk:** HIGH — concurrent agents hitting the same entity will get duplicate key errors

**Problem:** Lines 61-86 do UPDATE, check rows affected, then INSERT if zero. Two concurrent agents can both see 0 rows, both try INSERT, one gets a duplicate key error.

**Fix:** Replace the UPDATE-then-INSERT with a single `INSERT ... ON CONFLICT` statement:

```go
query := fmt.Sprintf(
    `INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, NOW())
     ON CONFLICT (%s) DO UPDATE SET %s = $2, %s = NOW()`,
    entityQuoteIdent(schema.EntityType),
    entityQuoteIdent("entity_id"),
    entityQuoteIdent(strings.TrimSpace(field.Name)),
    entityQuoteIdent("updated_at"),
    entityQuoteIdent("entity_id"),
    entityQuoteIdent(strings.TrimSpace(field.Name)),
    entityQuoteIdent("updated_at"),
)
_, err = db.ExecContext(ctx, query, entityID, value)
```

Remove the existing UPDATE + rowsAffected + conditional INSERT block entirely.

**Pattern reference:** `internal/store/event_receipt_store.go:25-27` uses `ON CONFLICT` already.

**Test:** Existing `TestEntityTools_HappyPath` covers the save path. Add a concurrent test if practical.

---

## C.2: Advances_to runtime reachability check

**File:** `internal/runtime/engine/executor.go`, function `stepAdvancesTo` (~line 469)

**Problem:** The engine advances to whatever `handler.AdvancesTo` says without checking if it's a valid transition from the current state. Boot validation checks `advances_to` is a declared state, but not that it's reachable from the current state.

**Fix:** Before applying the state change, call `WorkflowDefinition.CanTransition()` to verify the target is reachable:

```go
func (e *Executor) stepAdvancesTo(frame *executionFrame) error {
    next := strings.TrimSpace(frame.req.Handler.AdvancesTo)
    if frame.rule != nil && strings.TrimSpace(frame.rule.AdvancesTo) != "" {
        next = strings.TrimSpace(frame.rule.AdvancesTo)
    }
    if next == "" || next == frame.result.CurrentState {
        return nil
    }
    // NEW: validate transition is declared reachable
    if e.deps.TransitionValidator != nil {
        if err := e.deps.TransitionValidator.ValidateTransition(frame.result.CurrentState, next); err != nil {
            return err
        }
    }
    // ... rest unchanged
}
```

**Implementation guidance:**
- Add a `TransitionValidator` interface to `RuntimeDependencies` (optional, nil = skip check)
- Implement in `engine_adapter.go` using `pc.WorkflowDefinition().CanTransition()`
- On failure: return error, which makes the engine reject the event
- Allow transitions from empty state (initial entity creation) — `CanTransition` already handles this via synthetic seed transitions
- This is a soft enforcement: if `TransitionValidator` is nil, skip the check (backward compatible)

**Test:** Add a test case in `engine/executor_test.go` that verifies a handler with `advances_to: "unreachable_state"` is rejected.

---

## C.3: Engine enforcement integration test

**File:** New test file `internal/runtime/engine/executor_enforcement_test.go` or extend `executor_test.go`

Write 3 focused tests proving the enforcement chain works end-to-end:

1. **Guard blocks transition** — Handler with guard `not_in_terminal_state`, entity in terminal state → outcome is `rejected`, no state change
2. **CEL guard evaluates** — Handler with `check: "entity.score >= 75"`, entity score = 50 → blocked; entity score = 80 → passes
3. **Accumulation deduplicates** — Same event delivered twice → second delivery is idempotent, accumulator count stays at 1

Use the existing test patterns in `executor_test.go`. These tests run against the engine directly (not through the pipeline coordinator), so they're fast and isolated.

---

## D.1: Flow instance teardown

**File:** `internal/runtime/manager/flow_activation.go` (add new function)

**Problem:** `ActivateFlowInstance` creates agents and installs routes, but there's no reverse operation. When a flow instance completes (entity reaches terminal state in its sub-flow), the agents and routes remain active forever.

**Action:** Add `DeactivateFlowInstance`:

```go
func (am *AgentManager) DeactivateFlowInstance(ctx context.Context, templateID, instanceID, entityID string) error
```

This must:
1. Find all agents spawned for this flow instance (match by `Mode == templateID` and `EntityID == entityID`)
2. Call `TeardownAgent` for each
3. Remove routes from the event bus (add `RemoveFlowInstance` to the bus — reverse of `AddFlowInstance`)

**Wiring:** Add a `FlowInstanceDeactivator` to the coordinator (same pattern as `FlowInstanceActivator`). Call it when an entity reaches a terminal state in the flow's workflow definition.

**Where terminal state is detected:** `engine/executor.go` `stepAdvancesTo` — when `next` is a terminal state, emit a deactivation signal. OR: add to `engine_adapter.go` `SaveState` — when mutation.NextState is terminal, call deactivator.

**Implementation guidance:**
- `RemoveFlowInstance` on the route table is the reverse of `AddFlowInstance` in `bus/routing_derivation.go:89-150` — remove the instance from `rt.instances` map and clean up derived routes
- Agent lookup by templateID+entityID: iterate `am.agents` and match `cfg.Mode == templateID && cfg.EntityID == entityID`
- This is the highest-risk item in this tranche — test carefully

**Test:** Add test in `manager/flow_activation_test.go`:
1. Activate a flow instance (agents spawned, routes installed)
2. Deactivate it (agents terminated, routes removed)
3. Verify: agents no longer in manager, events to flow-local topics are not delivered

---

## D.2: Auto-emit on flow instance creation

**File:** `internal/runtime/manager/flow_activation.go`, at the end of `ActivateFlowInstance`

**Problem:** The contract supports `auto_emit_on_create` in flow schemas (e.g., create a validation flow instance → automatically emit `validation.started`). The field is parsed and used for routing, but the event is never actually emitted.

**Action:** After agents are spawned and routes installed, check if `schema.AutoEmitOnCreate.Event` is non-empty. If so, publish that event:

```go
if autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event); autoEmit != "" {
    payload := map[string]any{
        "entity_id":   entityID,
        "instance_id": instanceID,
        "template_id": templateID,
        "flow_path":   req.FlowPath,
    }
    // merge any config values into payload
    for k, v := range req.Config {
        if _, exists := payload[k]; !exists {
            payload[k] = v
        }
    }
    encoded, _ := json.Marshal(payload)
    if err := am.bus.Publish(ctx, events.Event{
        ID:          uuid.NewString(),
        Type:        events.EventType(autoEmit),
        SourceAgent: "flow-instance-activator",
        Payload:     encoded,
        CreatedAt:   time.Now(),
    }.WithEntityID(entityID)); err != nil {
        return fmt.Errorf("auto-emit %s: %w", autoEmit, err)
    }
}
```

**Dependencies:** The `am.bus` field must be accessible. Check if `AgentManager` has a bus reference — it does (`am.bus` is set in constructor). Verify `am.bus` implements `Publish`.

**Test:** Extend `flow_activation_test.go`: activate with a schema that has `auto_emit_on_create`, verify the event is published.

---

## D.3: RemoveFlowInstance on event bus route table

**File:** `internal/runtime/bus/routing_derivation.go`

**Action:** Add `RemoveFlowInstance(templateID, instanceID string)` to the route table:

```go
func (rt *RouteTable) RemoveFlowInstance(templateID, instanceID string) {
    rt.mu.Lock()
    defer rt.mu.Unlock()
    key := templateID + "/" + instanceID
    delete(rt.instances, key)
    // Remove derived routes that match this instance
    // (iterate rt.routes, remove entries where instancePath matches key)
}
```

Also add `RemoveFlowInstance` to the `EventBus` wrapper (same pattern as `AddFlowInstance` at `eventbus.go:113-124`).

**Test:** Unit test in `bus/` — add instance, verify routes, remove instance, verify routes gone.

---

## What This Tranche Must NOT Do

- Do NOT rewrite the engine execution order — it's correct
- Do NOT change guard/action registry interfaces — they're stable
- Do NOT add post-turn agent emit enforcement — that's a separate concern about LLM behavior quality, not safety
- Do NOT add entity-type-level authorization — future work
- Do NOT change how workflow instances are persisted — the store is solid

## Testing Checklist

- [ ] `go build ./...` passes
- [ ] `go test ./internal/runtime/engine/... -count=1` passes (C.2, C.3)
- [ ] `go test ./internal/runtime/tools/... -count=1` passes (C.1)
- [ ] `go test ./internal/runtime/manager/... -count=1` passes (D.1, D.2)
- [ ] `go test ./internal/runtime/bus/... -count=1` passes (D.3)
- [ ] Existing masflowtest catalog still passes

## Estimated Scope

| Item | Lines | Risk |
|------|-------|------|
| C.1 Upsert race | ~10 changed | LOW |
| C.2 Advances_to validation | ~30 new | MEDIUM |
| C.3 Engine enforcement tests | ~80-120 new | LOW |
| D.1 Flow teardown | ~60-80 new | HIGH |
| D.2 Auto-emit on create | ~20 new | LOW |
| D.3 RemoveFlowInstance | ~30-40 new | MEDIUM |
| **Total** | **~230-300 lines** | |

## Execution Order

Do them in this order — each builds on the previous:
1. **C.1** (upsert fix) — standalone, no dependencies
2. **C.2** (advances_to validation) — needs engine interface change
3. **D.3** (RemoveFlowInstance) — bus change needed before D.1
4. **D.2** (auto-emit) — standalone, just manager change
5. **D.1** (flow teardown) — depends on D.3 for route removal
6. **C.3** (integration tests) — do last, tests everything above
