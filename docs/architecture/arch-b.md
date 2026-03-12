# Architectural Plan: MAS Platform Extraction

**Date:** 2026-03-11
**Spec authority:** `docs/specs/mas-platform/platform/contracts/platform-spec.yaml` (v1.1.0)
**Litmus test:** Can a second product boot by writing only YAML contracts, a `WorkflowModule`, and a `main.go` — without editing any file under `internal/runtime/`?

---

## Dependency Graph

```
P1 Entity Model ─────┐
                      ├──→ P3 Coordinator Decomposition ──→ P5 Persistence Model
P2 Declarative Nodes ─┘                                  ──→ P6 Flow Instances
                                                          ──→ P7 Boot Validation
P4 CommGraph (independent)
P8 Version Check (independent)
```

Critical path: **P1 → P2 → P3**. Everything else is either new capability, safety net, or cleanup.

---

## P1: Entity Model Abstraction

**Priority:** Foundation — every other item depends on this.

### Current State

The platform hardcodes `vertical_id` as the entity key throughout generic code:

- **`internal/events/types.go:29-41`** — `Event.EntityID()` tries `entity_id` then falls back to `vertical_id`. `withEntityIDPayload()` writes `vertical_id` into payloads when `entity_id` is absent.
- **`internal/runtime/pipeline/coordinator.go:125`** — `FlowInstanceActivationRequest` has a `VerticalID` field.
- **`internal/runtime/budget.go`** — Budget scopes are `factory`, `vertical`, `portfolio` — Empire's organizational hierarchy.
- **`internal/runtime/pipeline/lifecycle_orchestrator.go`** — `workflowEventEntityID()` threads vertical_id through all lifecycle transitions.
- **`internal/runtime/pipeline/coordinator.go:15-30`** — Constants like `maxVerticalNameLen`, `maxVerticalSlugLen` are Empire-specific.

### Spec Requires

- The `workflow_state` DDL uses `instance_id` as the generic entity key.
- `entity_schema` in `package.yaml` defines entity shape per product.
- The platform is entity-type agnostic. A second product tracks "tickets," "documents," or "campaigns" — not "verticals."
- The `has_entity_id` builtin guard checks `event.payload.entity_id`, not `vertical_id`.

### What Needs to Change

1. **`Event.EntityID()`** — Look up `entity_id` only. Remove `vertical_id` fallback from generic code. Empire's product module maps `vertical_id` → `entity_id` at its boundary (in payload factories, emit tools, or a bus interceptor).
2. **`withEntityIDPayload()`** — Write `entity_id`, not `vertical_id`.
3. **`FlowInstanceActivationRequest.VerticalID`** → `EntityID`.
4. **Budget scopes** — Replace hardcoded `factory`/`vertical`/`portfolio` with product-configurable scope names read from `policy.yaml`.
5. **Empire-specific constants** — Move `maxVerticalNameLen`, `maxVerticalSlugLen`, `localServicesScannerExpected`, `corpusBatchSize` to Empire's `policy.yaml`. Generic coordinator reads them via `PolicyReader`.

### Why P1

This is the data threading primitive. Every event, every handler execution, every workflow instance lookup passes through entity ID resolution. If a second product can't define its own entity key without editing `internal/events/types.go`, platformization has failed at the most basic level.

---

## P2: Eliminate Hardcoded System Nodes

**Priority:** Highest structural change — removes the largest block of product-specific Go from generic packages.

### Current State

Two hand-coded Go orchestrators live in the generic pipeline package:

**`LifecycleOrchestrator`** (`internal/runtime/pipeline/lifecycle_orchestrator.go:46-92`) — A `switch` statement dispatching 20+ Empire-specific events:
```go
case "vertical.approved":     n.handleVerticalApproved(ctx, evt)
case "vertical.killed":       n.handleVerticalKilled(ctx, evt)
case "opco.ceo_ready":        n.handleOpCoCEOReady(ctx, evt)
case "opco.steady_state_reached": ...
case "opco.growth_triggered":     ...
case "opco.teardown_requested":   ...
case "build_complete":            ...
case "launch_ready":              ...
```

**`ValidationOrchestrator`** (`internal/runtime/pipeline/validation_orchestrator.go`) — Similar pattern for `vertical.shortlisted`, `research.completed`, `spec.approved`, `cto.spec_approved`, `brand.candidates_ready`.

Both are registered as `BackgroundNode` instances on `Runtime.SystemNodes`.

### Spec Requires

System nodes are defined in `nodes.yaml` with declarative `event_handlers`. The handler engine (`handler_engine_exec.go`) already implements the full execution primitive set: `advances_to`, `emits`, `sets_gate`, `guard`, `data_accumulation`, `on_complete`, `fan_out`, `rules`, `accumulate`, `record_evidence`, `from`. The spec's execution primitives are explicitly designed to make product-specific Go code unnecessary:

> "These patterns eliminate the need for product-specific code hooks. The platform derives all behavior from contracts."

### What Needs to Change

1. **For each `case` branch** in `LifecycleOrchestrator.Handle()` and `ValidationOrchestrator.Handle()`, write an equivalent declarative handler in Empire's `nodes.yaml`. The handler engine already supports the required primitives (guards, gate-setting, state advances, event emission, accumulation, fan-out).

2. **Replace both orchestrators** with instances of `DeclarativeWorkflowNode` (or the existing `DeclarativeDefaultNode`) — generic system node implementations that load behavior entirely from contracts.

3. **The `SystemNodeRunner`** (`system_node_runner.go`) already wraps system nodes generically with retry, dead-letter, dedup. Keep it — the issue is the nodes it wraps, not the runner.

4. **Delete** `lifecycle_orchestrator.go`, `lifecycle_orchestrator_runtime.go`, `validation_orchestrator.go`, `validation_orchestrator_runtime.go` from the generic pipeline package. Move any Empire-specific helper logic into `internal/empire/` or express it as guard/action implementations in Empire's module.

5. **Generic pipeline retains** only: `DeclarativeWorkflowNode`, `DeclarativeDefaultNode`, `SystemNodeRunner`, the handler engine. No product-named types.

### Verification

After this change, `grep -r "vertical\|opco\|scoring\|validation" internal/runtime/pipeline/*.go` should return zero matches in orchestrator files. All those concepts exist only in Empire's `nodes.yaml` and Empire's guard/action registry.

### Why P2

These two files are the single largest concentration of Empire logic in generic code (~800 lines). A second product would need to either: (a) write its own Go orchestrators and register them — which means understanding the pipeline internals, or (b) fit its domain into Empire's lifecycle/validation shape. Neither is acceptable. The handler engine already exists and is capable; the work is migrating imperative Go into declarative YAML.

---

## P3: Coordinator Decomposition

**Priority:** Execution engine generification. Depends on P1 + P2.

### Current State

`FactoryPipelineCoordinator` (`internal/runtime/pipeline/coordinator.go:77-113`) is a monolith bundling:

- **Scan coordination** — `ScanCoordinator` with discovery dedup, accumulation, batch assignment
- **Scoring state** — `ScoringState` with dimension tracking, composite computation, digest buffering
- **Validation gating** — `ValidationGate` with four-gate progression (G1–G4), revision cycles
- **Workflow engine** — Instance store, expression evaluator, handler execution, state persistence
- **Product policy dispatch** — `DiscoveryPolicy`, `ScoringPolicy`, `PayloadFactory` interfaces
- **Infrastructure** — Shard planning, timer scheduling, flow instance activation

Empire-specific constants baked into the coordinator:
```go
maxRevisionCycles              = 3
maxInnerRevisions              = 5
packagingTimeout               = 30 * time.Minute
scanTimeout                    = 90 * time.Minute
scoringTimeout                 = 60 * time.Minute
localServicesScannerExpected   = 5
corpusBatchSize                = 25
```

### Spec Requires

The coordinator is the **declarative workflow engine**. It:
1. Loads workflow instances from the `workflow_instances` table.
2. Matches incoming events to handlers via the `NodeHandlers` semantic index.
3. Executes handler steps (guards → actions → state advance → emit).
4. Persists state transitions.

It should not know about "scans," "scoring," or "validation." Those are flow-level concerns expressed entirely in contracts.

### What Needs to Change

1. **Extract `ScanCoordinator`** into `internal/empire/pipeline/` or express as contract-driven handlers (preferred, per P2).
2. **Extract `ScoringState`** — same treatment. Scoring dimensions, composite computation, and rubric selection are Empire product logic.
3. **Extract `ValidationGate`** — four-gate progression is an Empire workflow pattern, not a platform concept.
4. **Rename** the remaining coordinator to `WorkflowEngine` — it processes events through contract-defined handlers and manages workflow instance state. Period.
5. **`WorkflowModule` interface slimming** — `DiscoveryPolicy()`, `ScoringPolicy()`, `PayloadFactory()` become unnecessary on the generic interface. The generic engine uses `ContractBundle()`, `GuardRegistry()`, `ActionRegistry()`. Empire's module can still implement these internally, but the platform doesn't call them.
6. **Move constants** to `policy.yaml` — the engine reads timeouts, limits, and batch sizes via `PolicyReader`.

### Target Interface

```go
type WorkflowModule interface {
    ContractBundle() *runtimecontracts.WorkflowContractBundle
    WorkflowNodes() []WorkflowNode
    GuardRegistry() GuardRegistry
    ActionRegistry() ActionRegistry
}
```

The removed methods (`ScanPolicy`, `DiscoveryPolicy`, `ScoringPolicy`, `PayloadFactory`) move to an Empire-internal interface if still needed, or are replaced by contract-driven execution.

### Why P3

The coordinator is the execution engine. As long as it hardcodes Empire's three-phase pipeline (discovery → scoring → validation), a second product must either: fit into that shape, or fork the coordinator. The handler engine already supports arbitrary state machines — the coordinator just needs to get out of the way and let the engine do its job.

---

## P4: CommGraph Generification

**Priority:** Independent. Can be done in parallel with P1–P3.

### Current State

- **`internal/commgraph/registry.go:63-80`** — `roleAliases` maps Empire agent names to canonical roles:
  ```go
  "opco-ceo": "ceo", "head-of-product": "product-lead",
  "vp-product": "product-lead", "vp-growth": "growth-lead", ...
  ```
- **`MailboxRoundTrips()`** in the Empire policy assumes holding + opco organizational model.
- **`internal/runtime/manager/runtime.go:142-159`** — `defaultManagerAgentID()` hardcodes `empire-coordinator` as fallback.

### Spec Requires

The `permissions_model` defines permissions generically. Message authorities, routing authorities, and mailbox workflows are loaded from the product's contracts via the `CommGraph.Policy` interface.

### What Needs to Change

1. **`roleAliases`** — Load from contracts (agents.yaml can declare aliases) or from the product's `CommGraph.Policy`. Delete the hardcoded map.
2. **`defaultManagerAgentID()`** — Read `control_plane_agent_id` from `ProductPolicy` everywhere. The key already exists in Empire's static policy (`control_plane_agent_id: "empire-coordinator"`); the generic code just doesn't use it consistently.
3. **Audit `registry.go`** for any remaining Empire assumptions. The `Policy` interface is correctly abstracted — the issue is fallback paths that bypass the interface.

### Why P4

A second product with different agent roles would hit `roleAliases` immediately. The fix is mechanical — the abstraction boundary already exists, the implementation just leaks.

---

## P5: Persistence Model (Entity Schema → Auto-Generated Tools)

**Priority:** New capability. Depends on P3 (clean engine).

### Current State

**Not implemented.** No code exists for:
- Parsing `entity_schema` from `package.yaml`
- Generating backing store from schema
- `create_entity`, `get_entity`, `save_entity_field`, `search_entities`, `query_metrics` tools
- `query` and `clear` execution primitives for system node handlers

Entity state is currently scattered across:
- `accumulator_state` JSONB in `workflow_instances`
- Ad-hoc pipeline state in `FactoryPipelineCoordinator`
- Event payloads (entity data passed through events, not stored centrally)

### Spec Requires

```yaml
persistence_model:
  auto_generated_tools:
    get_entity: Read entity by ID
    save_entity_field: Write a specific field on an entity
    search_entities: Query entities by stage, field values, or metadata
    query_metrics: Read aggregated metrics across entities
  schema_source: package.yaml → entity_schema
```

The platform reads `entity_schema`, creates a backing store, and generates typed CRUD tools. All data access is permissioned, auditable, and storage-backend agnostic.

### What Needs to Change

1. **Schema parser** — Read `entity_schema` from `package.yaml` during contract loading. The field already appears in some contract files.
2. **Storage backend** — Generate PostgreSQL table from schema, or use a typed JSONB approach with schema validation.
3. **Tool implementations** — `create_entity`, `get_entity`, `save_entity_field`, `search_entities`, `query_metrics` as platform builtins in `internal/runtime/tools/executor.go`.
4. **Handler primitives** — Implement `query` and `clear` execution primitives in the handler engine for system node use.
5. **Permission integration** — Tools check agent permissions before executing.

### Why P5

Without entity persistence, flows share data only through events and accumulator JSONB. This works but makes cross-flow data access fragile and product-specific. A second product would need to build its own persistence hacks.

---

## P6: Flow Instance Lifecycle (`create_flow_instance` Tool)

**Priority:** New capability. Depends on P3.

### Current State

Partial implementation:
- `FlowInstanceActivator` callback defined in `coordinator.go:132`
- `internal/runtime/manager/flow_activation.go` exists with activation logic
- `WorkflowInstanceStore` matches the spec's DDL closely (`workflow_instance_store.go:17-30`)
- `internal/runtime/masflowtest/flow_activation.go` has test helpers

**Missing:** The `create_flow_instance` builtin tool is not wired into the tool executor. Agents cannot trigger flow instantiation through the tool system.

### Spec Requires

```yaml
create_flow_instance:
  input_schema:
    template: string — flow template ID (must be mode: template)
    instance_id: string — unique ID for this instance
    config: object — optional instance-specific configuration
  behavior:
    - Validate instance_id uniqueness
    - Load flow template contracts
    - Construct paths: {template_id}/{instance_id}/{local_name}
    - Register nodes, agents, events in runtime registry
    - Expand wildcard subscriptions to include new instance
    - Resolve local subscriptions for new instance
    - Create entity record at initial_state
    - Start nodes and agents
```

### What Needs to Change

1. **Register** `create_flow_instance` in the tool executor's builtin handler map alongside `agent_message`, `mailbox_send`, etc.
2. **Wire** the existing `FlowInstanceActivator` as the tool's handler.
3. **Ensure** `mode: template` flows in `package.yaml` are not auto-started at boot but are discoverable for runtime instantiation.
4. **Namespace isolation** — Instance-scoped event names, agent IDs, and node IDs must not collide with other instances.

### Why P6

Template flows are how Empire's operating companies are dynamically spun up. Without this tool, the only way to create instances is through hardcoded Go code in the lifecycle orchestrator — which P2 is trying to eliminate. A second product would face the same problem with any dynamic entity creation pattern.

---

## P7: Boot Validation (Compliance Rules)

**Priority:** Safety net. Benefits from P1–P6 being in place.

### Current State

`internal/runtime/pipeline/workflow_contract_validation.go` exists and performs some validation. Coverage against the spec's compliance rules is incomplete.

### Spec Requires

Four categories of boot-time validation:

**Flow coherence:**
- Every `advances_to` references a declared/reachable state
- No two flows write the same data pin (boot error)
- Every required input pin is wired
- Every required agent role is fulfilled

**Node coherence:**
- Every `event_handlers` event appears in `subscribes_to`
- Every `produces` event has a payload schema
- Guards reference existing entity/policy fields
- `advances_to` states are reachable

**Agent coherence:**
- Every `tools_tier2` tool exists in `tools.yaml`
- Every `emit_events` event has a payload schema
- Required permissions present for declared tools
- Every subscription event has an emitter

**Project coherence:**
- Every flow in `package.yaml` has a directory
- No node ID collisions across project/flow levels
- All handoff events have both emitter and subscriber

### What Needs to Change

1. **Audit** existing validation against all four spec categories.
2. **Add missing checks** — especially write-pin conflict detection, cross-flow event wiring, and agent permission validation.
3. **Make failures block boot** — not just log warnings. A misconfigured product must fail fast.
4. **Surface violations clearly** — each failure should reference the specific contract file and line.

### Why P7

Without boot validation, a misconfigured second product silently misbehaves at runtime. This is the platform's type system — it catches wiring errors at deploy time instead of at 3am.

---

## P8: Platform Version Compatibility Check

**Priority:** Lowest. Simple to implement.

### Current State

Not implemented. Products declare `platform_version` in `package.yaml` but the runtime ignores it.

### Spec Requires

```yaml
compatibility:
  declaration: Products declare platform_version in package.yaml using semver range
  enforcement: Platform checks at boot. Rejects if outside range.
  forward_compatible: Minor/patch releases don't break existing products
```

### What Needs to Change

1. Read `platform_version` from the product's `package.yaml` during contract loading.
2. Parse as semver range.
3. Check current platform version (from `platform-spec.yaml`) against the range.
4. Reject boot if incompatible, with a clear error message.

Add this check at the start of `ensureWorkflowBootWiring()` in `internal/runtime/runtime.go`.

### Why P8

Simple guard that becomes important when the platform starts evolving independently of products. No structural dependencies.

---

## Execution Notes

### What's Already Right

The codebase has correct abstractions in several places:
- **`WorkflowModule` interface** — Clean product injection point (needs slimming per P3, but the pattern is right).
- **`ProductPolicy` / `PolicyReader`** — Generic config access pattern.
- **`CommGraph.Policy` interface** — Correct abstraction boundary (implementation leaks per P4).
- **Handler engine** (`handler_engine_exec.go`) — Already implements the spec's execution primitives. This is the most important piece of existing infrastructure.
- **`WorkflowInstanceStore`** — Matches the spec's DDL.
- **`SystemNodeRunner`** — Generic node execution wrapper with retry/dedup.
- **Contract loading** (`workflow_contracts.go`) — Hierarchical, scope-aware resolution with source tracking.

### The One Sentence Summary

The handler engine can already execute arbitrary declarative workflows; the remaining work is removing the hardcoded Empire logic that bypasses it.
