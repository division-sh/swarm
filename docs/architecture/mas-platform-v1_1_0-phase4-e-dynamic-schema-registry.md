# Phase 4 Tranche E: Dynamic Event Schema Registry (Platformization)

**Date:** 2026-03-13 (updated 2026-03-13)
**Prerequisite:** None — can run in parallel with C+D
**Risk level:** HIGH — changes emit tool schema pipeline used by all agents
**Scope:** Delete 3779-line generated file, replace with ~100 lines of dynamic loading

---

## Context

`internal/runtime/contracts/schema_registry_generated.go` is a 3779-line generated Go file containing 176 hardcoded Empire event schemas (opco.*, vertical.*, holding.*, etc.) as a JSON blob. It sits in the generic `internal/runtime/contracts/` package — the single biggest platformization blocker remaining.

The data it provides is **already available at runtime** from the contract bundle. `semanticview.Source.EventEntries()` returns `map[string]EventCatalogEntry`, where each entry has `Payload EventPayloadSpec` with `Properties map[string]EventFieldSpec` — the exact same payload shape information.

This tranche replaces the static generated registry with dynamic loading from the contract bundle at boot time.

---

## Current Architecture (what to delete)

```
contracts/event-catalog.yaml (Empire YAML)
    ↓ scripts/generate_event_schema_registry/main.go
contracts/schema_registry_generated.go (3779 lines, 176 schemas)
    ↓ package-level var generatedContractEventSchemaRegistry
schema_registry.go: EventSchemaRegistry() → returns the var
payload_fields.go: EventPayloadFields() → derives field names from the var
phase1_foundation_test.go: TestPhase1SchemaRegistryUsesMASContractsSource → asserts against the var
    ↓
tools/emit_runtime.go: ensureEventSchemaRegistry() → sync.Once loads from EventSchemaRegistry()
    ↓
activeSchemas, emitToolToEvent (package-level vars in tools/)
```

**Problems:**
1. Empire domain vocabulary in generic package (155 references to opco/vertical/holding)
2. Compile-time snapshot of data that's already loaded from YAML at boot
3. Generated file must be regenerated whenever event catalog changes — fragile
4. `sync.Once` in `emit_runtime.go` loads schemas BEFORE `WorkflowSource` is set (line 125 of runtime.go runs before line 214)

**Direct dependents on `generatedContractEventSchemaRegistry` (all must be updated before deletion):**
- `schema_registry.go` — `EventSchemaRegistry()` iterates it
- `payload_fields.go` — `EventPayloadFields()` derives from it via `sync.Once`
- `phase1_foundation_test.go` — `TestPhase1SchemaRegistryUsesMASContractsSource` asserts against it directly

---

## Target Architecture (what to build)

```
contracts/*.yaml (any product's YAML)
    ↓ boot: LoadWorkflowContractBundle()
semanticview.Source.EventEntries() → map[string]EventCatalogEntry
    ↓ new: EventSchemaRegistryFromCatalog()
map[string]EventSchema (same type as before)
    ↓
tools/emit_runtime.go: InitEventSchemaRegistry(source) — called once during NewRuntime
    ↓ calls contracts.SetActiveEventSchemaRegistry(registry) for payload_fields
activeSchemas, emitToolToEvent (same consumers, same behavior)
```

---

## E.1: Add `EventSchemaRegistryFromCatalog` converter

**File:** `internal/runtime/contracts/schema_registry.go`

Add the converter function. Keep `EventSchemaRegistry()` temporarily as a wrapper around a package-level snapshot (see E.5 for the setter pattern):

```go
// EventSchemaRegistryFromCatalog converts contract-loaded event catalog entries
// into the EventSchema format used by emit tools and payload validation.
func EventSchemaRegistryFromCatalog(entries map[string]EventCatalogEntry) map[string]EventSchema {
    out := make(map[string]EventSchema, len(entries))
    for eventType, entry := range entries {
        eventType = strings.TrimSpace(eventType)
        if eventType == "" {
            continue
        }
        out[eventType] = eventSchemaFromCatalogEntry(eventType, entry)
    }
    return out
}

func eventSchemaFromCatalogEntry(eventType string, entry EventCatalogEntry) EventSchema {
    properties := make(map[string]any, len(entry.Payload.Properties))
    for fieldName, field := range entry.Payload.Properties {
        fieldName = strings.TrimSpace(fieldName)
        if fieldName == "" {
            continue
        }
        prop := map[string]any{}
        if t := strings.TrimSpace(field.Type); t != "" {
            prop["type"] = t
        }
        if d := strings.TrimSpace(field.Description); d != "" {
            prop["description"] = d
        }
        properties[fieldName] = prop
    }
    schema := map[string]any{
        "type":                 "object",
        "properties":           properties,
        "additionalProperties": false,
    }
    if len(entry.Required) > 0 {
        schema["required"] = entry.Required
    }
    return EventSchema{
        Description: fmt.Sprintf("Emit %s event", eventType),
        Schema:      schema,
    }
}
```

---

## E.2: Bridge test (write BEFORE deleting generated file)

**File:** New test `internal/runtime/contracts/schema_registry_dynamic_test.go`

This test proves the dynamic loader produces equivalent output to the generated file. Run it while the generated file still exists:

```go
func TestDynamicRegistryMatchesGenerated(t *testing.T) {
    bundle, err := LoadWorkflowContractBundle("../../..")
    if err != nil {
        t.Skipf("no contract bundle: %v", err)
    }
    source := semanticview.Wrap(bundle)
    dynamic := EventSchemaRegistryFromCatalog(source.EventEntries())

    // Every generated schema must have a dynamic equivalent
    for eventType, genSchema := range generatedContractEventSchemaRegistry {
        dynSchema, ok := dynamic[eventType]
        if !ok {
            t.Errorf("generated schema %s missing from dynamic registry", eventType)
            continue
        }
        // Compare property names (field set must match)
        genFields := payloadFieldNamesForSchema(genSchema.Schema)
        dynFields := payloadFieldNamesForSchema(dynSchema.Schema)
        sort.Strings(genFields)
        sort.Strings(dynFields)
        if !reflect.DeepEqual(genFields, dynFields) {
            t.Errorf("schema %s field mismatch: generated=%v dynamic=%v", eventType, genFields, dynFields)
        }
    }
}
```

**Note:** This test references `generatedContractEventSchemaRegistry` directly (package-internal var). It will be deleted alongside the generated file in step 7.

---

## E.3: Replace `sync.Once` with explicit initialization in emit_runtime.go

**File:** `internal/runtime/tools/emit_runtime.go`

The current `ensureEventSchemaRegistry()` uses `sync.Once` to lazily load from the generated file. Replace with explicit initialization:

```go
var (
    emitRegistryMu    sync.RWMutex
    emitToolToEvent   map[string]string
    activeSchemas     map[string]EmitSchema
    generatedSchemas  map[string]struct{}
    emitRegistryReady bool
)

// InitEventSchemaRegistry initializes the emit schema registry from a semantic source.
// Must be called once during runtime startup, after the WorkflowSource is available.
func InitEventSchemaRegistry(source semanticview.Source) {
    emitRegistryMu.Lock()
    defer emitRegistryMu.Unlock()

    var catalog map[string]runtimecontracts.EventCatalogEntry
    if source != nil {
        catalog = source.EventEntries()
    }
    activeSchemas = runtimecontracts.EventSchemaRegistryFromCatalog(catalog)

    // Push the active registry back to contracts/ for EventPayloadFields() callers
    runtimecontracts.SetActiveEventSchemaRegistry(activeSchemas)

    generatedSchemas = make(map[string]struct{})
    missing := missingProducerEventSchemas(commgraph.ProducerRoles, commgraph.ProducerEventsForRole, activeSchemas)
    for _, eventType := range missing {
        generatedSchemas[eventType] = struct{}{}
    }
    emitToolToEvent = make(map[string]string, len(activeSchemas))
    for eventType := range activeSchemas {
        emitToolToEvent[EmitToolName(eventType)] = eventType
    }
    emitRegistryReady = true
}

func ensureEventSchemaRegistry() {
    emitRegistryMu.RLock()
    ready := emitRegistryReady
    emitRegistryMu.RUnlock()
    if !ready {
        // Fallback: if not explicitly initialized, load empty registry.
        // This supports tests that don't call InitEventSchemaRegistry.
        InitEventSchemaRegistry(nil)
    }
}
```

**Important:** All existing callers of `ensureEventSchemaRegistry()` remain unchanged — they still call it, and it still works. The difference is that the data now comes from the contract bundle instead of a generated file.

---

## E.4: Wire `InitEventSchemaRegistry` into runtime boot

**File:** `internal/runtime/runtime.go`

Move the emit schema check from line 125 (before source is available) to AFTER the ToolExecutor is created (after line 218):

**Remove** (lines 125-134):
```go
if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
    // ... strict mode check ...
}
```

**Add** after line 218 (after ToolExecutor creation, where WorkflowSource is available):
```go
// Initialize emit schema registry from contract bundle (dynamic, not generated)
runtimetools.InitEventSchemaRegistry(opts.WorkflowModule.SemanticSource())

if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
    if runtimeEnvBool("MAS_EMIT_SCHEMA_STRICT", true) {
        return nil, fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries", len(generated))
    }
    sample := generated
    if len(sample) > 10 {
        sample = sample[:10]
    }
    slog.Warn("emit schema hardening: agent-emitted event schemas missing explicit definitions",
        "count", len(generated),
        "sample", strings.Join(sample, ", "),
    )
}
```

**Note on logging:** New code uses `log/slog` per repo convention. The existing `log.Printf` calls elsewhere in `runtime.go` are pre-existing; do not touch them in this tranche. Add `"log/slog"` to the import block if not already present.

---

## E.5: Replace `EventPayloadFields` dependency on generated var

**File:** `internal/runtime/contracts/payload_fields.go` and `internal/runtime/contracts/schema_registry.go`

**Problem:** `payload_fields.go` calls `deriveEventPayloadFields(generatedContractEventSchemaRegistry)` directly. The `contracts/` package cannot import `tools/` (import cycle). The callers of `EventPayloadFields()` are inside `contracts/` itself.

**Solution: package-level setter/snapshot in contracts.**

Add to `schema_registry.go`:

```go
var (
    activeRegistryMu sync.RWMutex
    activeRegistry   map[string]EventSchema
)

// SetActiveEventSchemaRegistry stores the dynamically-built schema registry
// for use by EventSchemaRegistry() and EventPayloadFields().
// Called by tools.InitEventSchemaRegistry after building from contract catalog.
func SetActiveEventSchemaRegistry(registry map[string]EventSchema) {
    activeRegistryMu.Lock()
    defer activeRegistryMu.Unlock()
    activeRegistry = registry
}

// EventSchemaRegistry returns the active schema registry.
// Before SetActiveEventSchemaRegistry is called, returns an empty map.
func EventSchemaRegistry() map[string]EventSchema {
    activeRegistryMu.RLock()
    defer activeRegistryMu.RUnlock()
    out := make(map[string]EventSchema, len(activeRegistry))
    for k, v := range activeRegistry {
        out[k] = v
    }
    return out
}
```

Update `payload_fields.go` to use `EventSchemaRegistry()` instead of the raw generated var:

```go
func EventPayloadFields() map[string][]string {
    eventPayloadFieldsOnce.Do(func() {
        eventPayloadFields = deriveEventPayloadFields(EventSchemaRegistry())
    })
    return cloneEventPayloadFields(eventPayloadFields)
}
```

**IMPORTANT:** The `sync.Once` in `EventPayloadFields` means it captures whatever is in the registry at first call. Since `InitEventSchemaRegistry` → `SetActiveEventSchemaRegistry` runs during boot before any agent calls `EventPayloadFields`, the ordering is safe.

---

## E.6: Update `phase1_foundation_test.go`

**File:** `internal/runtime/contracts/phase1_foundation_test.go`

`TestPhase1SchemaRegistryUsesMASContractsSource` (lines 51-65) directly references `generatedContractEventSchemaRegistry`. After deletion this will fail to compile.

**Replace** the test to assert against the dynamic registry:

```go
func TestPhase1SchemaRegistryUsesMASContractsSource(t *testing.T) {
    t.Parallel()

    bundle, err := LoadWorkflowContractBundle(repoRootForContractsTest(t))
    if err != nil {
        t.Skipf("no contract bundle: %v", err)
    }
    source := semanticview.Wrap(bundle)
    registry := EventSchemaRegistryFromCatalog(source.EventEntries())
    if len(registry) == 0 {
        t.Fatal("expected dynamic event schema registry entries from contract bundle")
    }
    for eventType, schema := range registry {
        if schema.Schema == nil {
            t.Fatalf("schema for %s has nil Schema map", eventType)
        }
        props, _ := schema.Schema["properties"].(map[string]any)
        if props == nil {
            t.Fatalf("schema for %s has no properties", eventType)
        }
    }
}
```

This test now proves:
1. The contract bundle loads successfully
2. `EventSchemaRegistryFromCatalog` produces a non-empty registry from it
3. Each schema has valid properties

The old assertion about description containing "resolved MAS contract bundle" was testing the generator's output format, which is no longer relevant.

---

## E.7: Delete generated file and generator

1. **Delete:** `internal/runtime/contracts/schema_registry_generated.go` (3779 lines)
2. **Delete:** `scripts/generate_event_schema_registry/main.go` (the generator)
3. **Delete** the bridge test from E.2 (`schema_registry_dynamic_test.go`) — it references the now-deleted var
4. **Remove** the `//go:generate` directive that was in the generated file

---

## E.8: Confirm `prompt_schema_guard.go` is safe (no-op)

**File:** `internal/runtime/contracts/prompt_schema_guard.go`

Line 17 calls `EventSchemaRegistry()`. After E.5, this returns whatever was set via `SetActiveEventSchemaRegistry`. But `PromptSchemaGuards()` returns nil (in `prompt_schema_guard_cases.go`), so the loop body never executes and the registry value is irrelevant. **No change needed.**

---

## What This Tranche Must NOT Do

- Do NOT change the `EventSchema` or `EmitSchema` types — they're stable
- Do NOT change how emit tools are generated from schemas (`GenerateEmitTools` is fine)
- Do NOT change payload validation logic (`ValidatePayloadAgainstSchema` is fine)
- Do NOT change the dispatcher or handler registry
- Do NOT rewrite the `emit_runtime.go` consumer functions — only change initialization
- Do NOT change `EventCatalogEntry` or `EventPayloadSpec` types — they're the source of truth
- Do NOT touch existing `log.Printf` calls in `runtime.go` — migrating old calls to `slog` is a separate concern

## Testing Checklist

- [ ] `go build ./...` passes
- [ ] Bridge test (E.2) passes with generated file still present
- [ ] `go test ./internal/runtime/contracts/... -count=1` passes (E.1, E.5, E.6)
- [ ] `go test ./internal/runtime/tools/... -count=1` passes (E.3)
- [ ] `go test ./internal/runtime/tools/... -count=1 -run TestEmit` passes (emit tool generation works with dynamic registry)
- [ ] `go test ./internal/runtime/pipeline/... -count=1` passes (workflow contract validation uses payload fields)
- [ ] Delete generated file + bridge test, rebuild, all tests still pass
- [ ] No references to `generatedContractEventSchemaRegistry` remain (grep to verify)
- [ ] No references to `opco.` or `vertical_id` in `internal/runtime/contracts/` (grep to verify)

## Estimated Scope

| Item | Lines | Risk |
|------|-------|------|
| E.1 Converter function | ~40 new | LOW |
| E.2 Bridge test | ~30 new (temporary) | LOW |
| E.3 Init replacement in emit_runtime.go | ~30 changed | MEDIUM |
| E.4 Boot wiring in runtime.go | ~10 moved | LOW |
| E.5 Setter + EventPayloadFields fix | ~25 changed | MEDIUM — highest risk |
| E.6 phase1_foundation_test.go rewrite | ~15 changed | LOW |
| E.7 Delete generated + generator + bridge test | -3779 deleted | LOW |
| E.8 prompt_schema_guard | 0 (verify only) | NONE |
| **Total** | **~150 new, -3779 deleted** | |

**Net: -3630 lines.** The codebase gets smaller AND more correct.

## Execution Order

This order is critical — each step must compile and test green before the next:

1. **E.1** (converter) — pure addition, no existing code changes
2. **E.2** (bridge test) — proves equivalence while generated file still exists
3. **E.3** (init replacement) — changes `emit_runtime.go`; includes calling `SetActiveEventSchemaRegistry`
4. **E.4** (boot wiring) — moves strict check after ToolExecutor setup in `runtime.go`
5. **E.5** (payload fields + setter) — `EventPayloadFields()` stops depending on generated var; `EventSchemaRegistry()` reads from setter instead
6. **E.6** (phase1_foundation_test.go) — test stops referencing generated var
7. **E.7** (delete) — remove generated file, generator, and bridge test
8. **E.8** (verify) — confirm prompt_schema_guard.go is safe

**Key risk:** Steps 5 and 6 must both land before step 7, or `contracts/` tests will fail to compile. Do NOT delete the generated file until all three direct dependents (`schema_registry.go`, `payload_fields.go`, `phase1_foundation_test.go`) are decoupled.

## Verification After Completion

```bash
# No Empire vocabulary in generic contracts package
grep -rn "opco\.\|vertical_id\|vertical\.\|holding\." internal/runtime/contracts/ --include="*.go"
# Should return 0 results

# No references to deleted var
grep -rn "generatedContractEventSchemaRegistry" internal/runtime/ --include="*.go"
# Should return 0 results

# Build clean
go build ./...
go test ./internal/runtime/... -count=1
```
