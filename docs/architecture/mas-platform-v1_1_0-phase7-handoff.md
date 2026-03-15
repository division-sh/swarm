# Phase 7: Boot Integrity + Runtime Completeness

**Date:** 2026-03-14 (updated 2026-03-14 — API corrections applied)
**Prerequisite:** Phase 6 (G-04) for G-17 only. All others are independent.
**Risk level:** MEDIUM — adds boot checks and new engine steps
**Scope:** 6 gaps (G-15, G-16, G-17, G-18a, G-19, G-06), ~410 lines
**Note:** G-06 spec writer has delivered execution order. All items are unblocked.

---

## G-15: Event chain integrity boot check

**Files:** `internal/runtime/pipeline/workflow_contract_validation.go`
**Scope:** ~60 lines

### What's wrong

`workflowEventCatalogWarnings()` (~line 1125) detects orphaned events (EVENT-NO-CONSUMER, EVENT-NO-PRODUCER) but does NOT detect circular event chains. The cycle detection algorithm exists in the test harness (`catalog_runner_test.go:1710-1736`, function `catalogFindEventCycles`) but is not in the production validation pipeline.

### What to do

**Step 1:** Port `catalogFindEventCycles` logic from `catalog_runner_test.go:1710-1736` into `workflow_contract_validation.go`.

Add a function:
```go
func detectEventCycles(source semanticview.Source) error {
    // Build event graph: for each node handler, map subscribes_to → emits
    // DFS cycle detection
    // Return error with cycle path if found, nil if clean
}
```

The test harness implementation builds an `eventGraph map[string]map[string]struct{}` mapping each subscribed event to all emitted events for that handler. Then walks the graph with DFS to find cycles.

**Step 2 — CRITICAL:** EVENT-CYCLE must be a **boot error, not a warning**. The tier 8 test vector (`tests/tier8-boot-verification/test-boot-event-cycle/`) expects `boot_result: error` with `error_category: EVENT-CYCLE`.

In the live boot path (`cmd/mas/main.go:98-101`), `ValidateWorkflowContractsDetailed` returns `(warnings, err)`. Warnings are logged at line 141 but **do not abort boot**. Only the `err` return causes `os.Exit(1)`.

Therefore: cycle detection must cause `ValidateWorkflowContractsDetailed` to return a non-nil `error`, NOT append to the warnings slice. Options:
1. Call `detectEventCycles(source)` inside `ValidateWorkflowContractsDetailed` and return its error directly
2. Or call it separately in `cmd/mas/main.go` between lines 98-101, same pattern as prompt schema guard validation

Do NOT append EVENT-CYCLE to `workflowEventCatalogWarnings()` — that path only produces warnings which are logged and ignored.

**Step 3:** Verify against existing test vector: `tests/tier8-boot-verification/test-boot-event-cycle/`. This test expects `boot_result: error` with `error_category: EVENT-CYCLE`.

### Verification

```bash
go test ./internal/runtime/pipeline/... -count=1 -timeout 60s -run EventCycle
go test ./internal/runtime/pipeline/... -count=1 -timeout 60s -run ContractValidation
```

---

## G-16: Bus-level payload validation

**Files:** `internal/runtime/bus/eventbus.go`, `internal/runtime/bus/eventbus_publish.go`
**Scope:** ~40 lines

### What's wrong

`EventBus.Publish()` (~line 16 of `eventbus_publish.go`) validates event type name format but not payload content. Events published directly (from engine actions, system events) bypass schema validation. Only tool-emitted events go through the emit tool's validation.

### What to do

**Step 1:** Add an optional payload validation callback to EventBus. The callback type lives in the bus package but knows nothing about tools or schemas — it's a pure function signature:

```go
type PayloadValidator func(eventType string, payload []byte) error
```

Add field to the `EventBus` struct (in `eventbus.go`, not a separate options file — `EventBusOptions` already lives at `eventbus.go:31`):
```go
payloadValidator PayloadValidator
```

Add to `EventBusOptions`:
```go
type EventBusOptions struct {
    // ... existing fields ...
    PayloadValidator PayloadValidator
}
```

**Step 2:** In `Publish()` (in `eventbus_publish.go`), after the event type name check, if `payloadValidator` is set:

```go
if eb.payloadValidator != nil {
    if err := eb.payloadValidator(evt.Type, evt.Payload); err != nil {
        return fmt.Errorf("payload validation for %s: %w", evt.Type, err)
    }
}
```

**Step 3:** Wire the validator during boot in `runtime.go`. The validator closure is created in runtime.go where schema access is available, then passed to EventBus via options. The bus package must NOT import tools or contracts — the dependency direction is: `runtime.go` creates the closure, passes it down. Example:

```go
payloadValidator := func(eventType string, payload []byte) error {
    // Look up event type in active schema registry
    // Validate required fields are present
    // Return error if missing
    return nil
}
eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
    // ... existing options ...
    PayloadValidator: payloadValidator,
})
```

**Step 4:** Make validation a warning (log + continue) rather than a hard error during initial rollout, to avoid breaking existing flows. Add a `StrictPayloadValidation` boot flag to make it a hard error.

### Verification

```bash
go test ./internal/runtime/bus/... -count=1 -timeout 60s
```

---

## G-17: create_flow_instance as agent-callable tool

**Files:** `internal/runtime/tools/handler_registry.go`, `internal/runtime/tools/executor_flow.go` (new), `internal/runtime/tools/deps.go`, `internal/runtime/tools/permissions.go`
**Scope:** ~80 lines
**Depends on:** G-04 (permission enforcement — done in Phase 6)

### What's wrong

`create_flow_instance` is only available as an engine action (handler `action` field). Agents cannot programmatically create flow instances via a tool call.

### Current APIs

**Tool handler signature** (`internal/runtime/tools/dispatcher.go:11`):
```go
type ToolHandler func(ctx context.Context, actor models.AgentConfig, input any) (any, error)
```

**Tool registration pattern** (`internal/runtime/tools/handler_registry.go:9-30`):
```go
func (e *Executor) buildToolHandlers() map[string]ToolHandler {
    handlers := map[string]ToolHandler{}
    e.registerAgentHandlers(handlers)
    // ... direct map assignment in each register function:
    // handlers["agent_fire"] = e.execAgentFire
    return handlers
}
```

**FlowInstanceActivator** (already exists at `internal/runtime/pipeline/workflow_instance_activation.go:24`):
```go
type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error
```

**FlowInstanceActivationRequest** (same file, lines 13-22):
```go
type FlowInstanceActivationRequest struct {
    ContractBundle semanticview.Source
    TemplateID     string
    InstanceID     string
    EntityID       string
    FlowPath       string
    InitialState   string
    Config         map[string]any
    TriggerEvent   events.Event
}
```

**ExecutorOptions** (`internal/runtime/tools/deps.go:37-44`) — does NOT yet have a flow activation dependency.

### What to do

**Step 1:** Add `FlowInstanceActivator` to `ExecutorOptions` (`deps.go`):

```go
type ExecutorOptions struct {
    Manager         Manager
    ManagerProvider ManagerProvider
    Config          *config.Config
    MailboxStore    MailboxPersistence
    SQLDB           *sql.DB
    WorkflowSource  semanticview.Source
    FlowActivator   runtimepipeline.FlowInstanceActivator  // NEW
}
```

Store it on the `Executor` struct and wire it from `runtime.go` where the activator is already available.

**Step 2:** Create `internal/runtime/tools/executor_flow.go`:

```go
func (e *Executor) execCreateFlowInstance(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
    params, ok := input.(map[string]any)
    if !ok {
        return nil, fmt.Errorf("create_flow_instance: invalid input")
    }
    templateID, _ := params["template"].(string)
    if templateID == "" {
        return nil, fmt.Errorf("create_flow_instance: template is required")
    }
    instanceID, _ := params["instance_id"].(string)
    // ... extract other fields per platform-spec.yaml tool_model.create_flow_instance.input_schema

    if e.flowActivator == nil {
        return nil, fmt.Errorf("create_flow_instance: flow activation not available")
    }

    err := e.flowActivator(ctx, runtimepipeline.FlowInstanceActivationRequest{
        ContractBundle: e.workflowSource,
        TemplateID:     templateID,
        InstanceID:     instanceID,
        // ... populate from params
    })
    if err != nil {
        return nil, fmt.Errorf("create_flow_instance: %w", err)
    }
    return map[string]any{"status": "created", "template": templateID, "instance_id": instanceID}, nil
}
```

**Step 3:** Register in `handler_registry.go`. Add a new registration function following the existing pattern:

```go
func (e *Executor) registerFlowHandlers(handlers map[string]ToolHandler) {
    handlers["create_flow_instance"] = e.execCreateFlowInstance
}
```

Call it from `buildToolHandlers()`.

**Step 4:** Add `create_flow_instance` to `toolPermissionRequirements` in `permissions.go:31-42`. It is NOT there yet — it must be added as part of this tranche:

```go
"create_flow_instance": "create_flow_instance",
```

### Verification

```bash
go test ./internal/runtime/tools/... -count=1 -timeout 60s -run CreateFlowInstance
```

---

## G-18a: Auto-derive prompt schema guard cases from contracts

**Files:** `internal/runtime/contracts/prompt_schema_guard_cases.go`, `internal/runtime/contracts/prompt_schema_guard.go`
**Scope:** ~30 lines

### What's wrong

`PromptSchemaGuards()` returns `nil`. The guard infrastructure is wired into boot but has no guard cases. Guard cases should be auto-derived from the contract bundle — for every agent that emits events and has a discoverable prompt, generate a guard case from the event schema's required fields.

### Current APIs

**AgentRegistryEntry** (`workflow_contracts.go:1905-1923`): Has `EmitEvents []string` but does NOT have a `PromptFile` field. Prompt discovery is done separately via `prompts.go:47` (`promptLookupPlan`) which searches bundle directories for prompt files matching agent ID candidates.

**AgentContractSource** (`workflow_contracts.go:1083`): `bundle.AgentContractSource(agentID)` returns `ContractItemSource` with `PackageKey`, `FlowID`, `Layer`, `File` — tells you where the agent's contract came from.

**EventCatalogEntry** (`workflow_contracts.go:1888-1903`): Has `Payload EventPayloadSpec` field.

**EventPayloadSpec** (`workflow_contracts.go:614-623`): Has `Required []string` (list of required field names) and `Properties map[string]EventFieldSpec`.

**Event catalog access**: `bundle.EventEntries()` returns `map[string]EventCatalogEntry`. `bundle.EventEntry(eventType)` returns a single entry.

### What to do

**Step 1:** Replace the static list in `prompt_schema_guard_cases.go` with a function that derives cases from a bundle:

```go
func DerivePromptSchemaGuards(bundle *WorkflowContractBundle) []PromptSchemaGuardCase {
    var cases []PromptSchemaGuardCase
    for agentID, entry := range bundle.AgentEntries() {
        if len(entry.EmitEvents) == 0 {
            continue
        }
        // Discover prompt file for this agent via existing prompt resolution
        promptFile := resolvePromptFileForAgent(bundle, agentID)
        if promptFile == "" {
            continue
        }
        for _, emitEvent := range entry.EmitEvents {
            emitTool := "emit_" + strings.ReplaceAll(emitEvent, ".", "_")
            eventEntry, ok := bundle.EventEntry(emitEvent)
            if !ok {
                continue
            }
            requiredFields := eventEntry.Payload.Required
            if len(requiredFields) == 0 {
                continue
            }
            cases = append(cases, PromptSchemaGuardCase{
                PromptFile:       promptFile,
                EmitTool:         emitTool,
                RequiredTopLevel: requiredFields,
            })
        }
    }
    return cases
}
```

For `resolvePromptFileForAgent`: use the existing prompt discovery in `prompts.go`. The function `promptLookupPlan` takes an `AgentConfig` and searches bundle directories. You may need a simpler variant that takes an agent ID and bundle, or construct a minimal AgentConfig just for lookup. Check if there's a simpler path — `bundle.AgentContractSource(agentID)` gives the flow/package context, and prompt files typically live in `prompts/<agentID>.md` within the flow directory.

**Step 2:** Update `ValidatePromptSchemaGuardsForBundle()` in `prompt_schema_guard.go` to call `DerivePromptSchemaGuards(bundle)` instead of `PromptSchemaGuards()`.

**Step 3:** Keep `PromptSchemaGuards()` for backwards compatibility but have it return nil (it's only used by the old repo-aware path which falls back correctly).

### Verification

```bash
go test ./internal/runtime/contracts/... -count=1 -timeout 60s -run PromptSchemaGuard
```

---

## G-19: Accumulator timeout completion

**Files:** `internal/runtime/engine/helpers.go`
**Scope:** ~30 lines

### What's wrong

`AccumulatorComplete()` at line ~434 returns `false` unconditionally for timeout mode:

```go
if completionSpec.Mode == runtimecontracts.AccumulateModeTimeout {
    return false, nil  // STUB
}
```

An accumulator with `completion: timeout(5m)` never completes on timeout.

### What to do

**Step 1:** Store accumulator start time. When an accumulator is first created (first event received), record `started_at` in the accumulator state. Check the `AccumulatorState` struct — if it doesn't have a `StartedAt` field, add one:

```go
type AccumulatorState struct {
    Received  []string       // event IDs
    Items     []any          // accumulated payloads
    StartedAt time.Time      // when first event was received
}
```

**Step 2:** In the accumulate step, when creating a new accumulator (first event), set `StartedAt: time.Now().UTC()`.

**Step 3:** In `AccumulatorComplete()`, replace the stub:

```go
if completionSpec.Mode == runtimecontracts.AccumulateModeTimeout {
    if acc.StartedAt.IsZero() {
        return false, nil
    }
    deadline := acc.StartedAt.Add(completionSpec.Timeout)
    return time.Now().UTC().After(deadline), nil
}
```

Check `AccumulateCompletion` for how the timeout duration is stored — it may be a `Timeout time.Duration` field or parsed from a string like `"5m"`.

**Step 4:** The timeout check only triggers when an event arrives. For proactive timeout expiry (no event needed), a timer would need to re-dispatch. Check if the timer infrastructure (`workflow_timer_lifecycle.go`) can schedule a re-evaluation. If not, document this limitation — timeout only completes when the next event arrives.

### Verification

```bash
go test ./internal/runtime/engine/... -count=1 -timeout 60s -run Accumulator
```

---

## G-06: Five handler primitives

**Files:** `internal/runtime/engine/executor.go`, `internal/runtime/engine/helpers.go`
**Scope:** ~200 lines
**Spec reference:** platform-spec.yaml:733-756 (execution order), 1420-1471 (field definitions)

### What's wrong

Contract schema defines Query, Filter, Reduce, Count, Clear as handler primitives. The engine has 13 execution steps but none for these 5. They are parsed into the handler struct and silently ignored.

The test harness implements all 5 in `catalog_runner_test.go:2024-2139`.

### Confirmed execution order (from spec writer)

The spec now defines (platform-spec.yaml:733-756):

```
query → clear_gates → guard → accumulate → filter → reduce → count → compute → on_complete/rules → {advances_to, sets_gate, data_accumulation} → payload_transform → emits → action → clear
```

Dependency rules:
- `query → clear_gates`: cross-entity data fetched before state evaluation
- `accumulate → filter`: accumulated items available before pruning
- `filter → reduce`: pruned items available before aggregation
- `reduce → count`: aggregated result available before counting
- `count → compute`: all list processing complete before computation
- `action → clear`: state cleanup runs last, after all side effects committed

### What to do

**Step 1:** Add 5 new step constants to `executor.go` (~line 19):

```go
const (
    stepQuery  = "query"
    stepFilter = "filter"
    stepReduce = "reduce"
    stepCount  = "count"
    stepClear  = "clear"
)
```

**Step 2:** Insert into `OrderedSteps` at the confirmed positions:

```go
var OrderedSteps = []string{
    stepQuery,        // NEW — before clear_gates
    stepClearGates,
    stepGuard,
    stepAccumulate,
    stepFilter,       // NEW — after accumulate
    stepReduce,       // NEW — after filter
    stepCount,        // NEW — after reduce
    stepCompute,
    stepFanOut,
    stepOnComplete,
    stepRules,
    stepAdvancesTo,
    stepSetsGate,
    stepDataWrites,
    stepTransform,
    stepEmits,
    stepAction,
    stepClear,        // NEW — after action
}
```

**Step 3:** Add execution plan fields. In the `ExecutionPlan` struct (or equivalent), add:

```go
Query  *runtimecontracts.QuerySpec
Filter *runtimecontracts.FilterSpec
Reduce *runtimecontracts.ReduceSpec
Count  *runtimecontracts.CountSpec
Clear  *runtimecontracts.ClearSpec
```

**Step 4:** Implement 5 step functions. Port the logic from the test harness (`catalog_runner_test.go`):

- `stepQuery`: Cross-entity read. Needs access to entity store. Resolves query against entity state. Spec: "Pre-fetches cross-entity data into handler context for use by guard, compute, or on_complete."
- `stepFilter`: Predicate on accumulated items. Filters `frame.result.AccumulatedItems` using CEL condition. Spec: "Only matching items pass through."
- `stepReduce`: Aggregate filtered items. Applies reduce operation (weighted_average, sum, min, max, pick_or_average). Spec: "Aggregates filtered items into a single value."
- `stepCount`: Count items matching condition. Stores result in entity field. Spec: "Counts items (optionally matching a condition)."
- `stepClear`: Reset accumulator state or entity field buckets. Spec: "Distinct from clear_gates which runs before guard."

**Step 5:** Add cases to `runStep()` switch.

**Step 6:** Wire handler fields from `SystemNodeEventHandler` into execution plan.

### Verification

```bash
# Tier 3 tests cover filter, reduce, count
go test ./internal/runtime/engine/... -count=1 -timeout 60s

# Catalog runner tests (already pass against test harness)
go test ./internal/runtime/masflowtest/... -count=1 -timeout 120s -run 'filter|reduce|count|query|clear'
```

---

## Delivery checklist

- [ ] G-15: Event cycle detection causes **boot error** (not warning)
- [ ] G-16: Optional payload validator callback on EventBus — injected from runtime.go, bus has no tools/contracts dependency
- [ ] G-17: `create_flow_instance` registered in `handler_registry.go` via direct map assignment
- [ ] G-17: Handler signature is `func(ctx context.Context, actor models.AgentConfig, input any) (any, error)`
- [ ] G-17: `FlowInstanceActivator` added to `ExecutorOptions` in `deps.go`
- [ ] G-17: `create_flow_instance` added to `toolPermissionRequirements` in `permissions.go`
- [ ] G-18a: Guard cases derived from bundle using `EventEntry().Payload.Required` and prompt discovery
- [ ] G-19: Accumulator timeout checks deadline instead of returning false
- [ ] G-06: 5 engine steps in confirmed execution order positions
- [ ] All existing tests pass: `go test ./... -count=1 -timeout 120s`

---

## What NOT to do

- Do NOT append EVENT-CYCLE to warning collection — it must be an error that aborts boot
- Do NOT import tools or contracts from the bus package — the payload validator is a callback injected from runtime.go
- Do NOT use `ActorContext` — tool handlers receive `models.AgentConfig`
- Do NOT use a `registerTool()` helper — tool registration is direct map assignment: `handlers["name"] = e.method`
- Do NOT assume `create_flow_instance` is already in the permission map — add it in this tranche
- Do NOT reference `agent.PromptFile` — it doesn't exist; use prompt discovery via `prompts.go`
- Do NOT reference `bundle.EventCatalog` directly — use `bundle.EventEntry(eventType)` or `bundle.EventEntries()`
- Do NOT reference `fieldSpec.Required` on individual fields — required fields are on `EventPayloadSpec.Required []string`
- Do NOT change the existing 13 engine steps — only add new ones
- Do NOT use `log.Printf` — use `slog` for all new code
