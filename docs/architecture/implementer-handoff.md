# New Implementer Handoff

Date: 2026-03-11
Repo: `/Users/youmew/dev/empireai`
Authoritative spec: `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

## Context

You are building a **generic MAS (Multi-Agent System) orchestration platform**. The first product ("Empire") was built simultaneously with the platform, and ~6,800 Empire-specific references leaked into generic packages. An adversarial audit on 2026-03-11 found:

- ~6,800 Empire references in generic packages
- 112 tests protecting Empire behavior in generic code
- 224 tests with valid intent but Empire coupling
- 4 CRITICAL untyped handler fields blocking the execution engine
- Schema registry generated from stale 176-event legacy catalog (MAS has 195)
- 30 hardcoded guard/action IDs in generic switch statements
- `config.go` entirely Empire-shaped (Hetzner, WhatsApp, FounderMode)
- `internal/factory/` — 13-file Empire pipeline in generic tree
- `internal/store/` — mailbox.go (9K lines) and scan_campaigns.go (11K lines) are Empire domain
- `contracts/` root — 30 files of Empire domain model, not MAS generic
- Agents have raw SQL access (`executor_sql.go`) instead of spec-required entity CRUD
- Zero of the spec's 4 auto-generated entity persistence tools exist
- Zero DDL generation from YAML entity_schema
- 87 of 147 spec requirements (59%) completely MISSING from implementation

The old implementer's phased plan is archived at `docs/architecture/archive/`. Do not follow it.

## Your Mission

Four phases, strictly sequential:

1. **Foundation** — type the semantic model, unblock the codebase, fix source-of-truth
2. **Declarative engine** — build the 12-step handler engine, DeclarativeNode, contract-derived routing
3. **Empire extraction** — rewrite tests, delete VerticalID, extract Empire packages, remove vocabulary
4. **Platform completion** — implement the 59% of spec requirements that don't exist yet

The exit criterion: a second product with different flows, agents, events, and entities can run on this platform without modifying any generic package.

---

## Execution Order

| Phase | Gate |
|-------|------|
| **1. Foundation** | `go build ./...` |
| **2. Declarative Engine** | `go build ./...` + `go test ./... -short` |
| **3. Empire Extraction** | `go test ./... -count=1` + zero Empire vocabulary in generic packages |
| **4. Platform Completion** | Full test suite + all 18 boot checks + all 11 enforcement rules |

---

## Phase 1: Foundation

Unblock the codebase: delete tests that protect Empire code, type the semantic model that everything else depends on, and fix the source-of-truth pipeline.

### Step 1.1: Triage the 112 tests (three-way classification)

An audit of every test revealed many "DELETE" tests contain valuable **platform patterns** (accumulation, fan-out, gate coordination, dedup, timer enforcement, dead letter). Blindly deleting them loses coverage intent. The correct classification is:

#### 1.1a: DELETE outright (~40 tests) — zero platform value

| File | Tests | Why DELETE |
|------|-------|-----------|
| `pipeline/holding_flow_strategy_a_to_c_test.go` | A1, B1, B3 | Empire directive/campaign, stage routing |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | C6, C7, C10, GoldenPath | Empire gates (g3/g4), OpCo spinup, golden path |
| `pipeline/workflow_transition_engine_test.go` | ~15 | Legacy flat-transition parity (already skipped) |
| `pipeline/coordinator_legacy_wrappers_test.go` | file | Dead legacy adapters, no actual tests |
| `pipeline/coordinator_projection_test.go` | 1 | Empire scan mode agent counts |
| `pipeline/pipeline_coordinator_stage_projection_test.go` | 1 | Empire validation stages |
| `runtime/commgraph_policy_default_test.go` | init() | Forces Empire commgraph default |
| `agents/agent_llm_test.go` | 1 | Empire scan mode aliases |
| `runtime/canned_llm_additional_scenarios_e2e_test.go` | Scenarios 2, 4, 7 | Empire pre-filter, derivation, campaign modes |

#### 1.1b: EXTRACT intent → REWRITE as generic (~55 tests) — valuable platform patterns

These tests validate real platform mechanics but are welded to Empire constants. For each: document the **test intent**, delete the Empire implementation, and rewrite in the generic test bundle (Step 3.1).

| File | Tests | Platform pattern to preserve |
|------|-------|------------------------------|
| `pipeline/holding_flow_strategy_a_to_c_test.go` | A2, A3, A4, A5 | **Accumulation + fan-out:** dispatch to N agents, count completions, threshold trigger, cleanup on complete |
| `pipeline/holding_flow_strategy_a_to_c_test.go` | B2 | **Numeric threshold gates:** score above X → state transition |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | C8 | **All-gates-met coordination:** composite event with merged payloads |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | C9 | **Human-in-loop escalation:** event → mailbox creation → decision gate |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | D1, D2, D3 | **Feedback loops:** gate reset, max-revision limit with escalation, nested revision counters |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | D4 | **Early termination:** rejection event → pipeline killed |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | E1, E2 | **Idempotency guards:** stale event dropped post-rejection, dedup guard on creation |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | E3, E4 | **Gate reset+reseal:** revision resets gate, follow-up re-sets it |
| `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` | E5 | **Timer forced completion:** timeout → completion with timed_out flag |
| `pipeline/pipeline_supplemental_test.go` | ValidationLifecycle, RevisionResume, ScanDedup | **State machine coverage:** happy path gate sequence, error recovery, dedup + cleanup |
| `pipeline/pipeline_coordinator_scoring_fanout_test.go` | all 6 | **Scoring outcome branching:** dual-publish (audit always, conditional external), per-dimension gates, buffer-to-digest |
| `pipeline/scoring_node_test.go` | all 6 | **System node reliability:** ledger idempotency, dead-letter on retry exhaustion, contract-default fallback, derived-entity handling |
| `pipeline/scan_orchestrator_test.go` | 3 of 4 | **Mode-based dispatch:** contract default fallback, N-way fan-out, timer expiry enforcement |
| `pipeline/validation_orchestrator_test.go` | 1 | **Orchestrator enrichment:** pull stored payloads, merge with trigger event |
| `runtime/canned_llm_e2e_test.go` | 1 | **E2E test framework:** canned LLM fixture setup, publish-and-wait, state validation structure |
| `runtime/canned_llm_full_pipeline_e2e_test.go` | 1 | **Full pipeline E2E framework:** fixture-based agent responses, prompt selection, multi-stage validation |
| `runtime/canned_llm_additional_scenarios_e2e_test.go` | Scenarios 3, 5, 6, 8, 9, 10 | **Workflow patterns:** timer-triggered promotion, failure→revision cycle, human rejection+re-gate, budget state machine, async dedup resolution, teardown cleanup |
| `commgraph/registry_test.go` | 6 | **Contract registry:** role aliasing, wildcard pattern matching, event classification, producer completeness |
| `commgraph/authority_test.go` | 4 | **Authorization matrix:** routing auth by role, cross-scope management, mailbox permissions, scope-aware message authority |
| `runtime/semantic_integration_matrix_test.go` | 4 checks | **Cross-layer contract validation:** org creation, route bootstrap, cycle counter circuit breaker, budget/mailbox contracts |
| `manager/manager_supplemental_test.go` | PanicBackoff, PanicEscalation, TemplateHelpers, Heartbeats | **Runtime lifecycle:** exponential backoff, panic boundary, template expansion, heartbeat installation |
| `manager/manager_lifecycle_test.go` | most tests | **Agent lifecycle:** reconfigure, teardown with typed payload, workspace hooks, budget suppression, directive classification |

#### 1.1c: MOVE to product-owned tests (~17 tests) — valid Empire coverage

| File | Destination | What it covers |
|------|-------------|---------------|
| `pipeline/holding_flow_strategy_*.go` (Empire-only tests) | `pipeline/empire/holding_flow_test.go` | Empire golden path, stage routing |
| `runtime/canned_llm_*.go` (Empire scenarios) | `internal/empire/e2e_test.go` | Empire E2E scenarios |
| `manager/bootstrap_test.go` (2 tests) | `manager/empire/bootstrap_test.go` | DefaultOpCoRoster/Routes |
| `manager/template_spawn_test.go` | `manager/empire/spawn_test.go` | SpawnOpCo template expansion |

After all three sub-steps, run `go build ./...` to find orphaned test helpers. Delete those too.

### Step 1.2: Create a generic test module + policy

12 files have `init()` functions that wire `empirepipeline.NewModule()` and `empireproductpolicy.New()` as package-level defaults. Replace all of them.

**Action:** Create a minimal generic test module that loads a synthetic MAS contract bundle. The `masflowtest/` package (12/12 clean tests, zero Empire vocabulary) is the model.

### Step 1.3: Type the full semantic model (4 layers)

The current codebase has **30+ `map[string]any` fields** across four layers. All must become typed structs. This is the foundation — the handler engine (Phase 2), DDL generation, and pin validation (Phase 4) all depend on typed contracts.

#### Layer 1: Handler fields on `SystemNodeEventHandler` and `HandlerTransitionSemantic`

The handler-first path (`directHandlerExecutionPlanSupported()`) bails out on `on_complete` and `rules` because they're `map[string]any`. But the problem is much bigger — **14 handler fields are untyped**, and 1 is Empire-specific:

| Field | Current type | Typed replacement | Notes |
|-------|-------------|-------------------|-------|
| `guard` | `any` | `GuardSpec` | Inline string OR compound checks |
| `accumulate` | `map[string]any` | `AccumulateSpec` | expected_from, completion, on_complete |
| `compute` | `map[string]any` | `ComputeSpec` | operation, tiers, store_as |
| `fan_out` | `map[string]any` | `FanOutSpec` | items_from, target, emit_per_item |
| `on_complete` | `map[string]any` | `*HandlerRuleEntry` | CRITICAL — causes bail-out |
| `rules` | `map[string]any` | `[]HandlerRuleEntry` | CRITICAL — causes bail-out |
| `filter` | `map[string]any` | `FilterSpec` | CEL predicate over collection |
| `reduce` | `map[string]any` | `ReduceSpec` | Aggregate accumulated values |
| `count` | `map[string]any` | `CountSpec` | Count accumulated items |
| `clear` | `map[string]any` | `ClearSpec` | Reset accumulator state |
| `payload_transform` | `map[string]any` | `PayloadTransformSpec` | Transform before emit |
| `config_from` | `map[string]any` | `ConfigFromSpec` | Read config values into handler context |
| `branch` | `[]any` | `[]BranchSpec` | Conditional handler paths |
| `sets_gate` | `any` | `GateSpec` | Gate name + optional value (string or struct) |
| `clear_gates` | `any` | `[]string` | Gate names to clear (runs BEFORE guard in engine) |
| `query` | `any` | `QuerySpec` | Query expression (CEL or entity reference) |
| `action` | `any` | `ActionSpec` | Platform action (create_flow_instance, record_evidence) or product hook ID |
| `mode_to_scanners` | `map[string]any` | **DELETE** | Empire-specific, not in MAS model |

**Add these typed structs to `workflow_contracts.go`:**

```go
type HandlerRuleEntry struct {
    Condition        string                   `yaml:"condition"`
    AdvancesTo       string                   `yaml:"advances_to"`
    Emits            EventEmission            `yaml:"emits"`
    DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
}

type GuardSpec struct {
    ID        string       `yaml:"id"`
    Check     string       `yaml:"check"`      // CEL expression
    PolicyRef string       `yaml:"policy_ref"` // Informational only — NOT executed. Documents which policy key the check references.
    OnFail    string       `yaml:"on_fail"`
    Checks    []GuardCheck `yaml:"checks"`     // Compound guard
}

type GuardCheck struct {
    ID    string `yaml:"id"`
    Check string `yaml:"check"`
}

type AccumulateSpec struct {
    ExpectedFrom string            `yaml:"expected_from"`
    Completion   string            `yaml:"completion"`   // CEL: "all_received", "count >= N"
    OnComplete   *HandlerRuleEntry `yaml:"on_complete"`
    OnTimeout    *HandlerRuleEntry `yaml:"on_timeout"`
}

type ComputeSpec struct {
    Operation string        `yaml:"operation"`
    Tiers     []ComputeTier `yaml:"tiers"`
    StoreAs   string        `yaml:"store_as"`
}

type ComputeTier struct {
    Dimensions []string `yaml:"dimensions"`
    Weight     float64  `yaml:"weight"`
}

type FanOutSpec struct {
    ItemsFrom   string `yaml:"items_from"`
    Target      string `yaml:"target"`
    EmitPerItem string `yaml:"emit_per_item"`
}

type FilterSpec struct {
    Predicate string `yaml:"predicate"` // CEL expression
    Source    string `yaml:"source"`
}

type ReduceSpec struct {
    Operation string `yaml:"operation"` // sum, avg, min, max, concat
    Source    string `yaml:"source"`
    StoreAs   string `yaml:"store_as"`
}

type CountSpec struct {
    Source  string `yaml:"source"`
    StoreAs string `yaml:"store_as"`
}

type ClearSpec struct {
    Target string `yaml:"target"` // accumulator key to reset
}

type PayloadTransformSpec struct {
    Mappings map[string]string `yaml:"mappings"` // dest_field: CEL expression
}

type ConfigFromSpec struct {
    PolicyKeys []string `yaml:"policy_keys"` // policy values to bind
}

type BranchSpec struct {
    Condition string            `yaml:"condition"` // CEL expression
    Then      *HandlerRuleEntry `yaml:"then"`
    Else      *HandlerRuleEntry `yaml:"else"`
}

type GateSpec struct {
    Name  string `yaml:"name"`
    Value any    `yaml:"value"` // true/false or string
}

type QuerySpec struct {
    Operation string `yaml:"operation"` // CEL expression or entity reference
    Source    string `yaml:"source"`
    StoreAs   string `yaml:"store_as"`
}

type ActionSpec struct {
    ID             string `yaml:"id"`              // Platform action or product hook ID
    Template       string `yaml:"template"`        // For create_flow_instance: template flow ID
    InstanceIDFrom string `yaml:"instance_id_from"` // CEL expression for instance ID
    ConfigFrom     string `yaml:"config_from"`     // CEL expression for config
}
```

Add `UnmarshalYAML` methods for backward compatibility where YAML uses string shorthand (especially `Guard`, `SetsGate` which can be a plain string).

After typing: remove `directHandlerExecutionPlanSupported()` bail-out. Delete the 7+ manual map-walking functions (`matchWorkflowRulesWithVars`, `scanDispatchKeysFromRules`, `decodeWorkflowDataAccumulation`, etc.).

#### Layer 2: Entity schema and node state schema

`EntitySchema` is `map[string]any` in **4 locations**. It must become typed for DDL generation (Phase 4, Step 4.3) and pin validation (boot check #10):

```go
type EntitySchema struct {
    Groups []EntitySchemaGroup `yaml:"groups"`
}

type EntitySchemaGroup struct {
    Name   string              `yaml:"name"`   // e.g. "identity", "scoring_phase"
    Fields []EntitySchemaField `yaml:"fields"`
}

type EntitySchemaField struct {
    Name     string `yaml:"name"`
    Type     string `yaml:"type"`      // text, integer, numeric(p,s), boolean, jsonb, timestamp, uuid
    Primary  bool   `yaml:"primary"`
    Indexed  bool   `yaml:"indexed"`
    Nullable bool   `yaml:"nullable"`
}
```

Replace `map[string]any` → `EntitySchema` in:
- `WorkflowContractBundle.Semantics.EntitySchema` (line 87)
- `ProjectPackageDocument.EntitySchema` (line 495)
- `WorkflowSchemaDocument.Workflow.EntitySchema` (line 634)
- `WorkflowContractBundle.WorkflowEntitySchema()` return type (line 158)

Similarly, `StateSchema map[string]any` on `SystemNodeContract` (line 863) → typed `NodeStateSchema`:

```go
type NodeStateSchema struct {
    Fields []NodeStateField `yaml:"fields"`
}

type NodeStateField struct {
    Name    string `yaml:"name"`
    Type    string `yaml:"type"`
    Default any    `yaml:"default"`
}
```

#### Layer 3: Flow composition — typed recursive tree (ARCHITECTURAL FOUNDATION)

**THIS IS THE MOST IMPORTANT DATA STRUCTURE IN THE PLATFORM.** Everything else — routing derivation, policy resolution, URI addressing, pin validation, namespace substitution, dynamic flow instances — is a projection of this tree. Build it first. If the tree is empty, everything downstream is built on flat workarounds that will need to be rewritten.

The loader discovers the recursive flow tree (`LoadedProjectPackage` with `Depth`, `ParentKey`) but the contract views flatten it. `FlowContractView` and `ProjectContractView` have no parent→child structure. Policy is `map[string]any` with no typed hierarchy.

**The spec says:**
- Flows nest recursively (max depth 99)
- Policy is hierarchical: child flow overrides parent flow's policy values
- URI addressing depends on the flow path hierarchy (`{flow_id}/{flow_id}/.../{name}`)
- Pin validation requires traversing the tree (wiring checks, write conflict detection)

**Action:**

1. Add `Children` to `FlowContractView`:
```go
type FlowContractView struct {
    Paths    FlowContractPaths
    Schema   FlowSchemaDocument
    Nodes    map[string]SystemNodeContract
    Events   map[string]EventCatalogEntry
    Agents   map[string]AgentRegistryEntry
    Tools    map[string]ToolSchemaEntry
    Policy   PolicyDocument                  // typed, not map[string]any
    Children []FlowContractView              // recursive composition
    Parent   *FlowContractView               // back-pointer for policy resolution
}
```

2. Type the policy model:
```go
type PolicyDocument struct {
    Values map[string]PolicyValue `yaml:",inline"`
}

type PolicyValue struct {
    Value       any    `yaml:"value"`
    Description string `yaml:"description"`
    Override    bool   `yaml:"override"` // child can override parent
}
```

Policy resolution: walk up the tree from the current flow to root, child values shadow parent values. This replaces the current `map[string]any` merge.

3. Add a `FlowTree` type for the resolved bundle:
```go
type FlowTree struct {
    Root     *FlowContractView
    ByPath   map[string]*FlowContractView   // URI path → view
    ByID     map[string]*FlowContractView   // flow ID → view
}
```

This is the structure that boot steps 2-6 build and steps 7-11 validate against.

4. **Wire the loader.** After loading flows into the flat `FlowContracts` map, the loader MUST build the tree:
   - Set `FlowTree.Root` to the top-level package's view
   - For each flow, assign it as a child of its parent flow (using `LoadedProjectPackage.ParentKey`)
   - Set `Parent` back-pointers on each child
   - Populate `ByPath` with hierarchical paths: `{parent_flow}/{child_flow}/...`
   - Populate `ByID` with flow ID lookups
   - If `FlowTree.ByPath` is empty after loading, the loader is broken

5. **Build the URI registry as part of tree construction.** URI addressing (local, absolute, full) is not a Phase 4 feature — it is inseparable from the tree. When the tree is built, every node, agent, and event gets a hierarchical URI assigned. This replaces Step 4.7 as a separate phase — URI resolution is a property of the tree, not an add-on.

   URI formats (resolve during tree construction):
   - Local (no `/`): `scoring.requested` → entity in current flow instance
   - Absolute (with `/`): `scoring/entity.shortlisted` → entity in specific flow instance
   - Full URI: `empire://scoring/entity.shortlisted` → multi-root-flow scenario
   - Wildcards: `*/entity.shortlisted` (direct children), `**/entity.completed` (any depth)

**Why this is foundational:** Without the populated tree, the implementer will build flat workarounds for routing (lookup from flat map instead of tree walk), policy (flat merged map instead of hierarchical resolution), and addressing (bare flow ID instead of hierarchical path). These workarounds compile and pass tests but produce the wrong architecture — a second product with nested flows will fail silently.

#### Layer 4: Event catalog and transition data

`EventCatalogEntry` has 7 `any` fields (lines 898-908) — the event routing model is entirely untyped:

```go
// Replace EventCatalogEntry untyped fields:
type EventCatalogEntry struct {
    Emitter           EventEmitterRef   `yaml:"emitter"`           // was `any`
    EmitterType       string            `yaml:"emitter_type"`
    AlternateEmitters []string          `yaml:"alternate_emitters"`
    Consumer          []string          `yaml:"consumer"`          // was `any`
    ConsumerType      []string          `yaml:"consumer_type"`     // was `any`
    Intercepted       bool              `yaml:"intercepted"`       // was `any`
    Passthrough       bool              `yaml:"passthrough"`       // was `any`
    RuntimeHandling   string            `yaml:"runtime_handling"`
    OwningNode        string            `yaml:"owning_node"`
    DeliveryChannel   string            `yaml:"delivery_channel"`  // was `any`
    Payload           EventPayloadSpec  `yaml:"payload"`           // was `any`
    Required          []string          `yaml:"required"`
}

type EventEmitterRef struct {
    AgentID string `yaml:"agent_id"`
    NodeID  string `yaml:"node_id"`
}

type EventPayloadSpec struct {
    Properties map[string]EventFieldSpec `yaml:"properties"`
    Required   []string                  `yaml:"required"`
}

type EventFieldSpec struct {
    Type        string `yaml:"type"`
    Description string `yaml:"description"`
}
```

`WorkflowTransitionContract.From` (line 646) is `any` — should be `[]string` (state names, can be single or array).

`WorkflowDataAccumulation.Value` and `WorkflowDataWrite.Value` (lines 659, 666) are `any` — should be `ExpressionValue` (CEL string or literal):

```go
type ExpressionValue struct {
    Literal any    `yaml:"literal,omitempty"`
    CEL     string `yaml:"cel,omitempty"`
}
```

`ToolSchemaEntry.InputSchema` (line 933) is `map[string]any` — should be a JSON Schema struct or use a validator library.

`FlowInstanceVariables.Variables` (line 592) is `map[string]any` — should be `map[string]FlowVariable`:

```go
type FlowVariable struct {
    Type        string `yaml:"type"`
    Default     any    `yaml:"default"`
    Description string `yaml:"description"`
}
```

#### Summary: `map[string]any` elimination

| Location | Count | Layer |
|----------|-------|-------|
| Handler fields on `SystemNodeEventHandler` | 17 fields | 1 |
| Handler fields on `HandlerTransitionSemantic` | 17 fields | 1 |
| `EntitySchema` on 4 structs | 4 fields | 2 |
| `StateSchema` on `SystemNodeContract` | 1 field | 2 |
| `Policy` on 4 structs | 4 fields | 3 |
| `EventCatalogEntry` fields | 7 fields | 4 |
| Transition data (From, Value) | 3 fields | 4 |
| `FlowInstanceVariables.Variables` | 1 field | 4 |
| `ToolSchemaEntry.InputSchema` | 1 field | 4 |
| **Total** | **~58 fields** | |

All 58 `map[string]any` / `any` fields must become typed. This is not incremental — the typed model is the foundation for the handler engine, DDL generation, pin validation, policy resolution, and URI addressing.

### Step 1.4: Fix source-of-truth pipeline

**Re-point schema registry generator** at MAS resolved bundle:
```go
// FROM:
catalogPath := filepath.Join(repoRoot, "contracts", "event-catalog.yaml")
// TO:
catalogPath := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts", "runtime", "events.yaml")
```

Regenerate. This fixes: 22 missing events, 3 ghost events, transitive staleness.

**Remove self-authority claim** from `contracts/ddl-canonical.sql` header.

### Exit criteria for Phase 1

- `go build ./...` passes
- All 58 `map[string]any` / `any` fields replaced with typed structs
- `FlowTree.Root`, `ByPath`, `ByID` populated by the loader
- Schema registry generated from MAS contracts (not legacy catalog)
- Zero `init()` wiring Empire module/policy as defaults

---

## Phase 2: Declarative Engine

Build the spec's execution engine: typed guards, the 12-step handler, DeclarativeNode, and contract-derived routing. Delete the concepts these replace.

### Step 2.1: Delete hardcoded guard/action switch logic

**Delete `workflow_hooks.go`** — the 19 guard IDs and 11 action IDs must come from contracts, not Go switch statements.

**Delete the guard evaluation switch** in `workflow_transition_engine.go` (~lines 1440-1520) and action execution switch (~lines 1828-1904). Replace with CEL-backed evaluation from typed `GuardSpec.Check`.

**Implement the 5 platform builtin guards** as generic code (not switch cases):

| Guard | Check |
|-------|-------|
| `has_entity_id` | `event.payload.entity_id` is non-empty UUID |
| `not_in_terminal_state` | Entity not in terminal state |
| `revision_count_below_limit` | `revision_count < policy.max_revisions` (default 3) |
| `has_human_decision` | Event originated from mailbox decision path |
| `state_in_phase` | Entity's current state belongs to `config.required_phase` |

**Implement the 5 platform builtin actions:**

| Action | Behavior | Registration |
|--------|----------|-------------|
| `record_state_change` | Append to state_change_history | Implicit (always runs) |
| `update_stage` | Set current_state to transition.to | Implicit (always runs) |
| `cancel_stage_timers` | Cancel timers scoped to departed state | Implicit (always runs) |
| `start_stage_timers` | Start timers scoped to entered state | Implicit (always runs) |
| `increment_revision_count` | `metadata.revision_count += 1` | Explicit (register in hook) |

### Step 2.2: Build the handler execution engine + DeclarativeNode

**THIS IS THE ARCHITECTURAL BACKBONE. Everything depends on it.**

Neither the handler execution engine nor `DeclarativeNode` exist in the current codebase. The current `resolveContractHandlerFirstTransition()` (line 700 of `workflow_transition_engine.go`) bails out on complex handlers. The entire transition engine is a mixture of typed paths and manual `map[string]any` walking with hardcoded Empire switch statements.

#### 2.2a: Implement the handler execution engine

The spec defines a strict dependency graph for handler execution (spec lines 725-761). When a node receives an event, its handler executes these steps **in order**, short-circuiting where appropriate:

```
Step 1:  clear_gates       → reset gate flags listed in handler's clear_gates field. Runs BEFORE guard so re-validation cycles start clean.
Step 2:  guard             → evaluate CEL expression or compound checks. If false → STOP (on_fail: reject|kill|discard|escalate)
Step 3:  accumulate        → track incoming event against expected_from set. Received list is a SET (duplicates ignored). If incomplete → STOP.
Step 4:  compute           → run operation (weighted_average, sum, etc.) over accumulated data
Step 5:  fan_out           → iterate items_from, emit emit_per_item for each. Writes fan_out.count to entity state. STOP after fan_out (async).
Step 6:  on_complete/rules → MUTUALLY EXCLUSIVE. Handler uses ONE, never both. Boot must reject handlers declaring both.
                             on_complete: ordered list [{condition, advances_to, emits, data_accumulation}] — first match wins
                             rules: map {condition → {emits, advances_to, data_accumulation}} — payload-based dispatch, first match wins
Step 7:  advances_to       → set entity state to target. Triggers implicit actions (record_state_change, update_stage, cancel/start timers)
Step 8:  sets_gate         → set gate flag on entity (default: true)
Step 9:  data_accumulation → write entity fields from event payload. Supports direct, mapped, and literal writes.
Step 10: payload_transform → construct output event payload from entity + payload + computed values via CEL field mappings
Step 11: emits             → persist event(s) to event store (within transaction). Delivery happens AFTER commit.
Step 12: action            → execute platform actions (create_flow_instance, record_evidence) or product hook actions
```

**Steps 7-9 (`advances_to`, `sets_gate`, `data_accumulation`) are INDEPENDENT** — no causal dependency between them, any execution order is valid within this group.

**Short-circuit rules:**
- `clear_gates` → always runs if present, no short-circuit
- Guard false → stop at step 2. Execute `on_fail` action.
- Accumulate incomplete → stop at step 3 (wait for more events)
- Fan-out → stop at step 5 (async, each fan-out item re-enters at step 2)
- on_complete/rules → first match wins, stop evaluating

#### Handler atomicity boundary (spec lines 746-753)

**ALL side effects from one handler execution commit in a single database transaction.** This is a load-bearing runtime invariant:

- State change, gate update, data writes, event persistence — all in one transaction
- No external observer sees intermediate state
- **Guards evaluate against entity state BEFORE any handler writes** — `data_accumulation` from the current handler does NOT affect the current handler's guard (it affects the NEXT handler's guard after commit)
- **Emitted events are persisted within the transaction but DELIVERED after commit** — this prevents recursive handler chains and deadlocks
- Agent inbox delivery is async (outside atomic boundary)

#### Entity concurrency model (spec lines 1003-1013)

- **Per-entity serial:** All events for the same entity processed one at a time. Entity-level lock acquired before handler execution, released after commit or rollback.
- **Cross-entity concurrent:** Events for different entities fully concurrent. Handling entity X does not block entity Y.
- **Agent concurrency:** Agent deliveries asynchronous. Events placed in agent inbox in order. Agent processes sequentially. Multiple agents run concurrently.

**CEL context variables** available in all expressions (from spec `context_variables`):
```
entity.{field}           — entity fields from entity_schema
entity.current_state     — current workflow state
entity.gates.{gate_id}   — gate flags
payload.{field}          — trigger event payload
policy.{key}             — policy values (hierarchical, child shadows parent)
accumulated.{node_id}    — accumulated data from node's state (list of received items)
fan_out.item             — current item in fan_out iteration
fan_out.count            — number of items fanned out (written to entity state by fan_out step)
metadata.revision_count  — revision counter
```

**Implementation: Generic CEL Context Builder.** Build a single `BuildCELContext(entity, payload, policy, executionState) → CELEnv` function that auto-populates all context variables based on the current handler execution state. This replaces the current manual variable mapping in `workflow_transition_engine.go:1534-1690`. The builder must:
- Populate `fan_out.*` only when executing inside a fan_out step
- Populate `accumulated.*` only inside `on_complete` after accumulation completes
- Resolve `policy.*` by walking up `FlowTree` (child shadows parent) — not from a flat merged map
- Be called once per handler step evaluation, not once per handler

Replace `resolveContractHandlerFirstTransition()` with a new `executeHandlerSteps()` function that:
1. Takes typed `SystemNodeEventHandler` (after Step 1.3 typing)
2. Acquires entity lock, loads pre-handler entity state
3. Walks steps 1-12 in order using typed structs (not map[string]any)
4. Calls `BuildCELContext()` at each evaluation point with current execution state
5. Commits all side effects in a single transaction
6. Delivers emitted events AFTER commit (not during)
7. Returns the outcome: state change, emitted events, entity writes, or blocked

**Files to modify:**
- `workflow_transition_engine.go:700-750` — replace bail-out with full execution
- `workflow_transition_engine.go:1534-1690` — replace manual variable mapping with CEL context builder
- `workflow_transition_engine.go:1718-1770` — delete `matchWorkflowRulesWithVars()`, replace with typed rule matching
- `workflow_transition_engine.go:1772-1800` — delete `decodeWorkflowDataAccumulation()`, use typed structs
- `workflow_transition_engine.go:1436-1520` — delete guard switch, use CEL evaluation
- `workflow_transition_engine.go:1817-1964` — delete action switch, use platform builtins + product hooks

#### 2.2b: Implement DeclarativeNode

`DeclarativeNode` is the **default** node executor. Every system node ID maps to `DeclarativeNode` unless a product hook overrides it.

```go
type DeclarativeNode struct {
    contract    SystemNodeContract       // typed YAML definition
    engine      *HandlerExecutionEngine  // the 12-step engine from 2.2a
    hookRegistry *ProductHookRegistry    // optional product hooks
}

func (n *DeclarativeNode) HandleEvent(ctx context.Context, evt Event) error {
    handler, ok := n.contract.EventHandlers[evt.Type]
    if !ok {
        return nil // not subscribed
    }
    return n.engine.ExecuteSteps(ctx, handler, evt)
}
```

**Product hook registry** (for the <10% that need custom Go):
```go
type ProductHookRegistry struct {
    actions map[string]ActionHandler  // action ID → Go function
}

type ActionHandler func(ctx context.Context, evt Event, entity *Entity) error

// Registered by product code at boot:
// registry.RegisterAction("spinup_opco_org", empire.SpinupOpCo)
```

The hook is invoked at step 12 of the handler execution engine when the handler's `action` field matches a registered hook. All other steps are generic.

**Replace `workflow_nodes_runtime.go` switch:**
```go
// BEFORE: switch nodeID { case "scoring-node": return NewScoringNode()... }
// AFTER:
func NewNode(contract SystemNodeContract, engine *HandlerExecutionEngine, hooks *ProductHookRegistry) NodeExecutor {
    return &DeclarativeNode{contract: contract, engine: engine, hookRegistry: hooks}
}
```

#### 2.2c: Derive routing from contracts (replace mutable routing tables)

Current code uses `SetRoutingTable(verticalID, routes)` to mutate routing at runtime. The MAS spec says routing is **derived**, not mutated.

**Boot-time routing derivation:**
1. For each flow's `agents.yaml`: extract `subscriptions` → build agent→event map
2. For each flow's `nodes.yaml`: extract `subscribes_to` → build node→event map
3. Merge into a `RouteTable` keyed by event type → list of subscribers (agents + nodes)
4. Resolve wildcards (`*/event`, `**/event`) against the flow tree

**Runtime routing for dynamic instances:**
1. On `create_flow_instance(template_id, instance_id)`:
2. Load template's agents.yaml and nodes.yaml
3. Construct instance-scoped paths: `{template_id}/{instance_id}/{subscriber_name}`
4. Add to route table: instance-scoped event types → instance-scoped subscribers
5. Re-expand wildcard subscriptions that match new instance events

**No `SetRoutingTable()` — routing is a derivation from contracts + active instances.** The EventBus holds a read-only snapshot built at boot, extended on instance creation.

**Files to replace:**
- `bus/eventbus.go:181-196` — delete `SetRoutingTable`/`GetRoutingTable`
- `bus/eventbus_routing.go:51-64` — delete `isFactoryEvent()`, replace with contract-derived routing
- `bus/eventbus_routing.go:71-92` — delete `resolveOpCoRecipients()`, replace with instance-scoped lookup
- `bus/routing.go:27-65` — delete `FactoryEventPrefixes`

### Step 2.3: Delete concepts that don't exist in MAS

**a) Mutable routing tables.** `SetRoutingTable`/`GetRoutingTable` → delete. MAS derives routing at boot + flow activation. Routing is a projection of `FlowTree.ByPath` — walk the tree, collect `subscribes_to` declarations, build a read-only route table.

**b) `accumulator_state` JSON bucket.** The untyped JSON grab-bag → replace with typed entity fields (from `entity_schema`) or node-scoped state (from `state_schema`).

**c) `PipelineStage` / `current_stage`.** MAS has per-flow-instance states, not global stage. Delete `current_stage` from entity table. Per-instance state lives in `workflow_instances.current_state`.

**d) `productpolicy.Policy` interface.** The 30-method interface is wrong. MAS reads policy from `policy.yaml` per flow, resolved via `{{policy.X}}` in CEL. Delete the interface. Policy resolution walks up the `FlowTree` from the current flow's `FlowContractView.Parent` to root — child values shadow parent values. Do NOT replace the interface with flat helper functions or a merged map. The replacement is the tree walk.

**e) Custom orchestrator Go structs.** `LifecycleOrchestrator`, `ValidationOrchestrator`, `DiscoveryAggregator`, `ScanOrchestrator`, `ScoringNode` — after Steps 2.1+2.5, most execute declaratively. Delete the structs; node IDs remain in `nodes.yaml`, execute through `DeclarativeNode`.

**f) `ScanCampaignManager` background loop.** Sharding and parallelism in MAS are not a background loop — they are the `fan_out` + `accumulate` + `agent_hire` handler primitives working together. The handler engine already supports `fan_out` (iterate items, emit per item) and `accumulate` (track completions, fire on threshold). The scan campaign manager is Empire product logic expressed as infrastructure. Delete it from generic code; Empire can express the same behavior declaratively in its `nodes.yaml` handlers.

**g) Empire-specific parallel state stores.** `workflow_instances` must be the sole runtime state authority. The generic runtime must NOT restore state from Empire-specific side tables. Delete generic dependencies on:
- `scan_accumulators`
- `pending_dedup_candidates`
- `validation_pipelines`
- `pipeline_processed_events`

These are Empire product state. If Empire needs them, they belong behind a product-owned adapter, not in generic `state_store.go` recovery paths.

**Target `WorkflowModule` interface after Step 2.3:**

```go
type WorkflowModule interface {
    ContractBundle() *runtimecontracts.WorkflowContractBundle
    WorkflowNodes() []WorkflowNode
    GuardRegistry() GuardRegistry
    ActionRegistry() ActionRegistry
}
```

The following methods are REMOVED from the generic interface (move to Empire-internal if still needed):
- `ScanPolicy()` — Empire scanning logic
- `DiscoveryPolicy()` — Empire discovery logic
- `ScoringPolicy()` — Empire scoring logic
- `PayloadFactory()` — Empire payload shaping

### Exit criteria for Phase 2

- `go build ./...` + `go test ./... -short` passes
- Handler-first execution handles `on_complete` and `rules` without bailing out
- All system nodes execute through `DeclarativeNode`
- No `productpolicy.Policy` interface
- No mutable routing tables
- No `accumulator_state` JSON bucket
- No `current_stage` on entity table

---

## Phase 3: Empire Extraction

Remove all Empire logic from generic packages: rewrite tests, delete VerticalID, extract Empire-specific packages, scrub vocabulary.

### Step 3.1: Build generic test bundle + rewrite extracted intents

Create a minimal MAS YAML package under a test fixtures directory:
- 2-3 synthetic flows (`intake`, `processing`, `delivery`)
- Generic agents (`coordinator`, `worker-a`, `worker-b`)
- Generic events (`item.created`, `item.processed`, `item.completed`)
- Generic entity fields (`status`, `priority`, `score`)
- Handlers exercising guard, accumulate, on_complete, rules, compute, emits

**Then rewrite the ~55 extracted intents from Step 1.1b as generic tests.** Organize by platform pattern:

| Test file (new) | Patterns covered | Source intents |
|-----------------|-----------------|----------------|
| `testcases/accumulation_fanout_test.go` | Fan-out to N agents, count completions, threshold trigger, cleanup | A2, A3, A4, A5, scan_orchestrator |
| `testcases/multigate_state_machine_test.go` | Gate coordination, reset-on-feedback, max-revision, stale-event guard, dedup | C8, D1-D4, E1-E4, pipeline_supplemental |
| `testcases/scoring_outcome_test.go` | Dual-publish, per-dimension gates, buffer-to-digest | scoring_fanout tests |
| `testcases/system_node_reliability_test.go` | Ledger idempotency, dead-letter, contract defaults, derived-entity | scoring_node tests |
| `testcases/timer_lifecycle_test.go` | Timer forced completion, timer expiry enforcement | E5, scan_orchestrator timer |
| `testcases/budget_suppression_test.go` | Budget state machine, event suppression by policy | Scenario 8, manager budget |
| `testcases/agent_lifecycle_test.go` | Panic backoff, heartbeat, reconfigure, teardown | manager_supplemental, manager_lifecycle |
| `testcases/e2e_framework_test.go` | Canned LLM fixtures, publish-and-wait, state validation | canned_llm structure |
| `testcases/authorization_matrix_test.go` | Routing auth, message authority, mailbox permissions | commgraph/authority, commgraph/registry |

### Step 3.2: Rewrite the 224 REWRITE tests in batches

| Priority | Package | Count | Typical fix |
|----------|---------|-------|-------------|
| 1 | `store/` | 6 | Replace Empire agent IDs |
| 2 | `workspace/` | 3 | Replace Empire role names |
| 3 | `bus/` | 22 | Replace Empire event names |
| 4 | `tools/` | 20 | Replace Empire agent/event IDs |
| 5 | `agents/` | 15 | Replace Empire agent/scan modes |
| 6 | `manager/` | 26 | Replace OpCo vocabulary |
| 7 | `runtime/` root | 47 | Replace Empire module with generic |
| 8 | `pipeline/` | 81 | Replace Empire nodes/events/stages |
| 9 | `contracts/` | 20 | Replace Empire paths |
| 10 | `commgraph/` | 4 | Replace Empire role names |

### Step 3.3: Move product E2E coverage to product-owned packages

- `holding_flow_strategy_*.go` → `pipeline/empire/`
- `canned_llm_*.go` → `internal/runtime/empire_e2e_test.go`
- `commgraph/authority_test.go` DELETE tests → `commgraph/empire/`

### Step 3.4: Delete VerticalID from event envelope

**Delete `VerticalID string` from Event struct** (`internal/events/types.go:15`). Do NOT rename — there is no concept of entity scope field on the event envelope in MAS.

Routing comes from subscriptions + `subscribes_to`, not an entity ID on the envelope. Entity identity is a payload field.

| Current usage | MAS replacement |
|---------------|----------------|
| Bus routing (factory vs OpCo) | Derive from event type + flow instance subscriptions |
| Workspace addressing | `workspace_class` from agents.yaml + flow instance path |
| Persistence scoping | `instance_id` (flow instance concept) |
| Agent session scoping | Flow instance path for `session_per_entity` |
| Store queries | Payload `entity_id` field |

~300 callsites. Do it as one coordinated pass: break everything, fix everything, green. No bridge.

### Step 3.5: Extract Empire from config, factory, store, tools

**a) `internal/config/config.go`** (235 lines, CRITICAL)

Current: 8 sections (Hetzner, WhatsApp, Registrar, FounderMode, Mailbox, Budget, Sharding, LLM). Generic MAS needs only: Runtime, Database, LLM.

Action: Keep Runtime + Database + LLM. Move Hetzner, WhatsApp, Registrar, FounderMode, Mailbox, Budget, Sharding to `internal/empire/config/` or delete.

**b) `internal/factory/`** (13 files, ~2000 lines)

Entire package is Empire vertical discovery pipeline (scanners, scoring rubrics, validation). Not generic.

Action: Move to `internal/empire/factory/` or delete entirely (push logic into agent behavior).

**c) `internal/store/mailbox.go`** (9,382 lines) + **`scan_campaigns.go`** (11,597 lines)

Empire domain models. Not generic.

Action: Move to `internal/empire/store/`. Keep generic store interfaces for agents, events, scheduling, LLM.

**d) `internal/runtime/tools/executor_sql.go`** (207 lines)

Agents have raw SQL access. Spec forbids this — agents use typed entity CRUD tools.

Action: Delete. Replace with auto-generated entity persistence tools (Step 4.2).

**e) `contracts/` root directory** (30 files)

100% Empire domain model. The MAS contracts live at `docs/specs/mas-platform/`.

Action: Move to `internal/empire/contracts/` or `docs/specs/mas-platform/empire/contracts/`. The schema registry generator already needs to point at MAS bundle (Step 1.4).

### Step 3.6: Genericize `cmd/empire/main.go`

978 lines with Empire-specific wiring: Hetzner workspace lifecycle, factory scan runner, inbound gateway, mailbox notifier, human task expiry, marginal maintenance, portfolio digest, health monitor, Telegram bot, dashboard.

Action: Extract a generic `cmd/mas/main.go` (~300 lines) that boots only core runtime: LLM runtime, event bus, agent lifecycle, scheduling. `cmd/empire/main.go` extends it with Empire subsystems via a product hook registration pattern.

### Step 3.7: Remove remaining Empire vocabulary

Work package by package:

| Package | Empire refs | Key violations |
|---------|-----------|----------------|
| `pipeline/` | ~4,588 | Orchestrators, scoring, scan campaign, state machine |
| `tools/` | 584 | VerticalID, Empire field names |
| `contracts/` | 538 | Hardcoded "empire" paths |
| `manager/` | 519 | SpawnOpCo, vertical routing |
| `bus/` | 282 | FactoryEventPrefixes, OpCo routing |
| `workspace/` | 152 | EMPIREAI_ env vars |
| `agents/` | 101 | Factory mode, session_per_vertical |
| `mcp/` | 85 | Empire env vars |

Specific deletions:
- `manager/opco.go` — SpawnOpCo/TeardownOpCo
- `manager/bootstrap.go` — DefaultOpCoRoster/DefaultOpCoRoutes
- `pipeline/scan_campaign_manager.go` — Empire scanning
- `pipeline/lifecycle_orchestrator.go` — Empire event switch
- `pipeline/validation_orchestrator.go` — Empire validation lifecycle
- `pipeline/discovery_aggregator_runtime.go` — Empire discovery events
- `pipeline/workflow_node_scoring.go` — Empire scoring
- `bus/routing.go` — `FactoryEventPrefixes`
- `productpolicy/policy.go` — 30-method Empire interface
- `pipeline/state_machine.go` — 12 hardcoded Empire stages

Rename:
- `FactoryPipelineCoordinator` → `PipelineCoordinator`
- `OpCoCycleTracker` → `CycleTracker`
- `session_per_vertical` → `session_per_entity`
- `EMPIREAI_*` env vars → `MAS_*`
- `verticals` table → product-owned entity table derived from `entity_schema`
- Delete `isFactoryEvent` (routing is derivation-based)

### Exit criteria for Phase 3

- `go test ./... -count=1` green
- Zero imports of `pipeline/empire`, `productpolicy/empire`, `commgraph/empire` from generic packages
- `grep -r "vertical\|opco\|empire\|factory\|holding" internal/runtime/pipeline/*.go` returns zero hits in non-empire/ files
- The 140 KEEP tests untouched
- The ~224 REWRITE tests use generic vocabulary only
- Zero test files in generic packages importing `empirepipeline` or `empireproductpolicy` directly
- No raw SQL tool in generic packages
- Config has only Runtime/Database/LLM sections
- `cmd/mas/main.go` exists and boots without Empire

---

## Phase 4: Platform Completion

The spec defines 147 requirements. After Phases 1-3, roughly 40% are covered. Phase 4 implements the missing 59%.

### Step 4.1: Boot sequence (15 steps)

Implement the full boot sequence from `engine → boot_sequence`:

| Step | Description | Status |
|------|-------------|--------|
| 1. `load_platform_spec` | Read platform-spec.yaml, verify version | Partial |
| 2. `walk_flow_tree` | Recursive from root package.yaml, max depth 99 | Implemented |
| 3. `construct_paths` | Hierarchical paths: `{flow_instance_path}/{local_name}` | Implemented |
| 4. `register_templates` | `mode: template` → register, don't instantiate | Implemented |
| 5. `build_registries` | Nodes, agents, events, tools (with inheritance), policy (hierarchical) | Partial |
| 6. `resolve_subscriptions` | Local (no /) vs absolute (/) vs wildcards | Implemented |
| 7. `validate_pins` | Required input pins wired, no write conflicts | **MISSING** |
| 8. `validate_required_agents` | All flow `required_agents` fulfilled | **MISSING** |
| 9. `validate_tools` | All `tools_tier2` exist in tool registry | **MISSING** |
| 10. `validate_permissions` | Agents have sufficient permissions for tools | **MISSING** |
| 11. `validate_platform_version` | Root `platform_version` includes running version | **MISSING** |
| 12. `initialize_state_stores` | Derive DDL from `entity_schema` + `state_schema` | **MISSING** |
| 13. `start_system_nodes` | Nodes subscribe to declared events | Implemented |
| 14. `start_agents` | Agent subscriptions active | Implemented |
| 15. `ready` | Log boot summary | Partial |

**Failure mode:** Any step fails → abort boot with clear error (step, flow/agent/event, fix). No partial startup.

### Step 4.2: Entity persistence tools (auto-generated)

The spec requires 4 auto-generated persistence tools from `entity_schema` in `package.yaml`:

| Tool | Description |
|------|-------------|
| `get_entity` | Read entity by ID. Returns fields the agent has permission to see |
| `save_entity_field` | Write a specific field. Field must exist in `entity_schema` |
| `search_entities` | Query entities by stage, field values, metadata |
| `query_metrics` | Aggregated metrics (counts, sums, averages) across entities |

These replace `executor_sql.go`. Agents do NOT have raw database access. All access is permissioned, auditable, and storage-backend agnostic.

### Step 4.3: DDL generation from entity_schema

The spec says entity tables are derived from `entity_schema` at boot (step 12).

1. Read `entity_schema` from package.yaml
2. Map field type descriptors to DDL types: `text`→VARCHAR, `integer`→BIGINT, `numeric(p,s)`→NUMERIC, `boolean`→BOOLEAN, `jsonb`→JSONB, `timestamp`→TIMESTAMPTZ, `uuid`→UUID
3. Generate CREATE TABLE statement
4. Merge child flow schemas into parent
5. Verify no write-pin conflicts (two flows writing same field = boot error)
6. Create or verify tables at boot step 12

The `workflow_instances` DDL is platform-defined (from spec):
```sql
CREATE TABLE workflow_instances (
    instance_id       UUID PRIMARY KEY,
    workflow_name     TEXT NOT NULL,
    workflow_version  TEXT NOT NULL,
    current_state     TEXT NOT NULL,
    entered_stage_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state_change_history JSONB NOT NULL DEFAULT '[]',
    accumulator_state JSONB NOT NULL DEFAULT '{}',
    timer_state       JSONB NOT NULL DEFAULT '[]',
    metadata          JSONB NOT NULL DEFAULT '{}',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Note: `accumulator_state` on `workflow_instances` is per-node keyed state (the spec says "per-node state buckets, keyed by node_id") — this is different from the current untyped grab-bag. The platform should enforce that node state matches `state_schema` declarations.

### Step 4.4: Boot validation checks (18 required)

All must pass before step 13 (start_system_nodes):

1. All YAML files parse without errors
2. `package.yaml` declares flows; each flow directory has `nodes.yaml`, `events.yaml`, `schema.yaml`
3. All required input event pins are wired
4. All `required_agents` roles fulfilled
5. No data pin write conflicts
6. Namespace substitution produces no event name collisions
7. All `tools_tier2` tools exist in tools.yaml
8. All agent permissions include required permissions for their tools
9. All `emit_events` have schemas in events.yaml
10. `entity_schema` covers all `data_accumulation` write targets
11. All `required_agents` fulfilled after namespace substitution
12. Cross-flow event schemas: an event schema is defined ONCE in the emitting flow's `events.yaml`. Consuming flows reference it by absolute path — they do NOT redefine the schema. Boot must reject conflicting schema definitions for the same event name across flows.
13. `on_complete` and `rules` are mutually exclusive on a handler. Boot must reject handlers declaring both (spec line 755).
14. Flow ID uniqueness across entire tree. Duplicate flow IDs across different parents is a boot error (spec line 793).
15. All handler fields must be defined in `system_node_specification.handler_fields` — reject unknown handler fields (spec line 1238).
16. All guard `check` expressions, rule `condition` expressions, filter conditions, and `on_complete` conditions must parse as valid CEL. Syntax errors abort boot (spec lines 1254-1257).
17. Every agent must have a prompt file in flow `prompts/` directory (spec line 1244).
18. No deprecated fields used: `subscriptions_bootstrap`, `logic`, `on_below_threshold`, `on_dedup`, `on_pass`, handler-level `condition` (spec lines 1250-1253).

### Step 4.5: Runtime enforcement rules (11 required)

Enforce during execution:

1. Tool calls validated against tool schema before execution
2. Tool calls checked against agent permissions before execution
3. Message delivery checked against agent message scope permission
4. State transitions only follow declared `advances_to` paths
5. Guards evaluated before state advancement — block if false
6. Events validated against payload schema before publish
7. Accumulation is idempotent — duplicate events do not double-count
8. Permission checks read from `agent.permissions`, not hardcoded functions
9. Scan mode behavior read from `policy.scan_modes`, not hardcoded
10. Manager fallback read from `agent.manager_fallback`, not hardcoded
11. Workspace class read from `agent.workspace_class`, not hardcoded

### Step 4.6: Error model

Implement the full error model:

**Handler execution failure:**
- `max_retries`: 3
- `backoff`: exponential (1s, 2s, 4s)
- `retry_on`: transient errors (DB timeout, lock contention)
- `no_retry_on`: guard failures, validation errors, business logic
- After max retries: emit `pipeline.dead_letter` with original event + error details + retry history

**Agent session failure:**
- `max_retries`: 1 (sessions are expensive)
- Then dead-letter

**Chain depth limit:**
- `max_depth`: 50
- Each event carries `chain_depth` counter (starts 0, increments on emit)
- Exceeds max_depth → route to `pipeline.dead_letter`

**Timer failure:** Same retry policy as handler failures.

**Terminal state behavior (spec lines 1055-1062):**
When an entity reaches a terminal state (declared in `schema.yaml terminal_states`):
- Entity stops accepting new events — handlers reject events for terminal entities
- All active timers for the entity are cancelled
- All agent sessions working on the entity are terminated
- **Entity data is NOT cleared.** Terminal entities retain all accumulated state, scores, research, and decision history. This is explicit in the spec — do not implement cleanup-on-terminal.

### Step 4.7: URI addressing model

Implement the three addressing formats:

| Format | Example | Meaning |
|--------|---------|---------|
| Local (no /) | `scoring.requested` | Entity in current flow instance |
| Absolute (with /) | `scoring/entity.shortlisted` | Entity in specific flow instance |
| Full URI | `empire://scoring/entity.shortlisted` | Multi-root-flow scenario |

Resolution rule: slash presence determines scope. No slash = local. Slash = absolute from root.

Wildcards: `*/entity.shortlisted` (direct children), `**/entity.completed` (any depth).

**Note:** URI construction is done during FlowTree build (Step 1.3, Layer 3). This step covers runtime resolution — looking up entities by URI at event delivery time.

### Step 4.8: Permissions model (13 permissions)

Implement and enforce:

| Permission | Description |
|------------|-------------|
| `agent_fire` | Terminate an agent session |
| `agent_hire` | Spawn a new agent session |
| `agent_reconfigure` | Modify agent prompt, tools, subscriptions |
| `approve_spend` | Authorization for expenditure |
| `configure_routing` | Modify event routing at runtime |
| `create_flow_instance` | Create instance of template flow |
| `human_task_decide` | Record human decision |
| `human_task_request` | Create task for human execution |
| `mailbox_send` | Send item to human mailbox |
| `message_all` | Send message to any agent |
| `message_domain` | Send to agents in same domain |
| `message_peers` | Send to agents in adjacent roles |
| `schedule` | Set timers and delayed actions |

**Enforcement:** Before executing any tool call, check agent's permissions. Message scope: `message_all` > `message_domain` > `message_peers`.

**Bundles:** `permissions_bundle` + explicit `permissions` → bundle expands first, explicit extends, deduped.

**Workflow extensions:** Unrecognized permissions pass to workflow-registered handlers.

**Tool inheritance model (spec lines 845-847):**
- Root `tools.yaml` tools are shared with ALL child flows — agents in any child flow can use them
- Child flow `tools.yaml` tools are available ONLY within that child flow
- If a child declares a tool with the same ID as a root tool, the child's version takes precedence within that flow
- Tools are NOT URI-scoped (unlike nodes, agents, events) — they are shared resources
- The tool registry must track per-flow precedence, not just a flat global map

**Agent `model_tier` authority (spec line 714):**
`agents.yaml` `model_tier` is AUTHORITATIVE — it wins over `policy.yaml model_tiers` on conflict. `policy.yaml model_tiers` is a convenience lookup for tooling and dashboards only.

### Step 4.9: Prompt templating

- Agent prompts: markdown in `prompts/{agent-id}.md`
- `{{variable}}` placeholders substituted from `policy.yaml` at session creation
- Simple string replacement, no logic
- Variables not in policy.yaml → left as-is (fail-open)

### Step 4.10: Session model completeness

Verify implementation covers:

**Conversation modes:**
- `task` — new session per event, stateless (default)
- `session` — persists across events, context accumulates
- `session_per_entity` — one session per entity, separate per entity. **Implementation requirement:** the platform must maintain a persistent mapping from `(agent_id, entity_id) → session_id` so that multiple event deliveries for the same entity resume the same conversation. This is NOT a simple cache — it must survive agent restarts and be queryable at session creation time.

**Emit tool auto-generation:**
- From each agent's `emit_events`, generate `emit_{event_name}` tools (dots → underscores)
- Validate payload against events.yaml schema
- Agents do NOT list emit tools in `tools_tier2`

**Universal tools:** `agent_message`, `mailbox_send` auto-granted to all agents.

**Turn budget:** `max_turns_per_task` from agents.yaml. Exceeded → terminate session, emit timeout event.

### Step 4.11: Timer model completeness

Verify implementation covers all timer fields:

| Field | Description |
|-------|-------------|
| `id` | Unique timer name |
| `event` | Event to fire on expiry |
| `delay` | Duration string or `policy.X` reference |
| `recurring` | Boolean — re-fire until cancelled |
| `start_on` | State or event that starts timer |
| `cancel_on` | State or event that cancels timer |

**Lifecycle:** start_on triggers → persist (entity_id, timer_id, fire_at) → fire_at reached → emit event → cancel_on triggers → cancel. Recurring timers restart after firing unless cancelled.

**cancel_on supports both states AND events:** `cancel_on: state:ready_for_review` cancels when entity reaches that state. `cancel_on: event:spec.approved` cancels when that event fires. The scheduler must subscribe to both state transitions and events to handle cancellation — not just state checks.

**Crash recovery:** Timers are persisted. On restart, check for expired timers and fire them.

### Step 4.12: Namespace substitution

Flow schemas declare `namespace_prefix`. Root flows assign namespace per instance. Platform substitutes at boot.

**Recursive nesting:** Substitution must be applied recursively during `walk_flow_tree`. A child flow's namespace prefix prepends its children's events, agents, and node IDs. For a flow at depth 3 (`root/flow-a/flow-b/flow-c`), the final event name is the concatenation of all namespace prefixes from root to leaf. The substitution happens during tree construction (Layer 3 FlowTree build), not as a separate post-processing pass — otherwise cross-flow references resolve against un-substituted names.

Must validate: no event name collisions after substitution (boot check #6). All `required_agents` fulfilled after substitution (boot check #11).

### Step 4.13: Dynamic flow instance lifecycle (11 steps)

Current implementation (`manager/flow_activation.go`) covers ~6 of 11 spec steps. Missing steps are critical.

| Step | Spec requirement | Status |
|------|-----------------|--------|
| 1 | Node handler calls `create_flow_instance(template_id, instance_id)` | Works |
| 2 | **Validate: template exists AND `mode == template`** | **MISSING** — currently creates instance of any flow |
| 3 | **Validate: instance_id unique within template scope** | **MISSING** — no uniqueness check |
| 4 | Load flow template contracts | Works |
| 5 | Construct paths: `{template_id}/{instance_id}/{local_name}` | Works |
| 6 | Register nodes, agents, events in runtime registry | Partial — agents only, **no nodes** |
| 7 | **Expand wildcard subscriptions matching new instance events** | **MISSING** |
| 8 | Resolve local subscriptions for instance | Works |
| 9 | **Create entity record at initial_state** | **MISSING** — delegated to store without explicit state init |
| 10 | **Start nodes AND agents** | Partial — agents started, **nodes not activated** |
| 11 | **Auto-emit on create** | **MISSING** — `auto_emit_on_create` field is read but never emitted |

### Step 4.14: Required field validation at boot

The spec mandates all required fields present on every contract object. Add boot checks for:

| Object type | Required fields |
|-------------|----------------|
| state_change | id, from, to, trigger, node |
| guard | id, category, description |
| action | id, category, description |
| node | id, execution_type, subscribes_to, produces, event_handlers |
| agent | id, model_tier, conversation_mode, subscriptions, emit_events |
| flow manifest | name, version |
| state | id, phase |
| schema | name, namespace, pins |

These are structural integrity checks — fail fast at boot with clear error messages.

### Step 4.15: Emit tool auto-generation

From each agent's `emit_events` list, auto-generate tool schemas:

1. For each event in `emit_events`, create tool named `emit_{event_name}` (dots → underscores)
2. Tool input schema = the event's payload schema from `events.yaml`
3. Tool execution = validate payload → publish event to event loop
4. Agents do NOT list these in `tools_tier2` — they are auto-granted based on `emit_events`
5. Universal tools (`agent_message`, `mailbox_send`) auto-granted to all agents without declaration

### Step 4.16: Database migration strategy

Three-tier schema responsibility:

| Tier | Source | Owner |
|------|--------|-------|
| Platform tables | `workflow_instances`, event store, routing | Platform DDL |
| Entity tables | `entity_schema` in package.yaml | Generated at boot |
| Node state tables | `state_schema` + `state_table` in nodes.yaml | Generated at boot |

**Multi-product:** Each product's entity tables are derived from its `entity_schema`. Platform tables are shared. Event store may partition by product/flow. No multi-tenancy model — each product is a separate deployment in v1.1.0.

**Schema changes:** Detect at boot. The spec does not define automatic migration — implementer designs the upgrade path.

### Exit criteria for Phase 4

- All 15 boot steps implemented
- All 18 boot validation checks pass
- All 11 runtime enforcement rules active
- All required fields validated at boot (Step 4.14)
- 4 entity persistence tools auto-generated from entity_schema
- DDL generated from entity_schema at boot
- Error model with exponential backoff, dead letter, chain depth 50
- URI addressing with local/absolute/full-URI + wildcards
- 13 permissions enforced on tool calls and message delivery
- Prompt templating with `{{variable}}` from policy.yaml
- 3 conversation modes implemented
- Emit tools auto-generated from `emit_events` (Step 4.15)
- Timer model with start_on/cancel_on/recurring/crash recovery
- Namespace substitution at boot with collision detection
- Dynamic instance lifecycle: all 11 steps (Step 4.13), including mode validation, uniqueness, node activation, auto-emit
- `create_flow_instance` rejects non-template flows
- Wildcard subscriptions expanded on instance creation

---

## Reference: What's Already Right — DO NOT REFACTOR

**WARNING:** An implementer who doesn't know what's correct will accidentally refactor working code. The items below are **structurally sound** and match the spec's abstractions. Do not redesign, rewrite, or "improve" them. Build on top of them.

### Correct abstractions (keep as-is, extend only)

- **`SystemNodeRunner`** (`system_node_runner.go`) — Generic node execution wrapper with retry, dead-letter, dedup. This is the spec's system node lifecycle. The problem is the nodes it wraps (Empire orchestrators), not the runner itself.
- **`WorkflowInstanceStore`** (`workflow_instance_store.go`) — CRUD, mutation, template-flow coexistence. Matches the spec's `workflow_instances` DDL closely. Keep.
- **Handler engine** (`handler_engine_exec.go`) — Already implements the spec's execution primitives: `advances_to`, `emits`, `sets_gate`, `guard`, `data_accumulation`, `on_complete`, `fan_out`, `rules`, `accumulate`, `record_evidence`, `from`. This is the most important piece of existing infrastructure. Finish it (12-step engine, DeclarativeNode) — don't replace it.
- **`WorkflowModule` interface** — Clean product injection point. Needs slimming (remove `ScanPolicy`, `DiscoveryPolicy`, `ScoringPolicy`, `PayloadFactory`) but the pattern of products providing a module to the platform is exactly right.
- **`CommGraph.Policy` interface** — Correct abstraction boundary for communication graph policy. The issue is fallback paths that bypass the interface (hardcoded `roleAliases`, `defaultManagerAgentID()`), not the interface itself.
- **`ProductPolicy` / `PolicyReader` pattern** — Generic config access from `policy.yaml`. The abstraction is correct; the issue is Empire-specific keys/modes leaking into the generic reader.
- **Contract loading** (`workflow_contracts.go`) — Hierarchical, scope-aware resolution with source tracking. The loader architecture is sound. The issue is: (a) it doesn't populate `FlowTree`, and (b) it defaults to Empire paths. Fix those two things — don't rewrite the loader.

### Correct implementations (keep, no changes needed)

- **MAS contract loader** — walks package tree, resolves flows, merges bundles, handles namespacing. Solid.
- **Flow activation (partial)** — `create_flow_instance` exists, wildcard subscriptions expand at boot, instance-scoped routing works. Missing: mode validation, node activation, auto-emit (5 of 11 steps missing). Extend, don't replace.
- **Wildcard handler resolution** — closed across all 3 lookup paths. Works.
- **Budget system** — `budget_test.go` 6/6 KEEP, fully generic.
- **CEL expression evaluator** — 7/13 tests KEEP, product-agnostic. But: context variable binding is manual and Empire-specific.
- **masflowtest framework** — 12/12 clean, shows the right pattern for generic tests.

### The one-sentence summary

The handler engine can already execute arbitrary declarative workflows; the remaining work is removing the hardcoded Empire logic that bypasses it and populating the FlowTree that everything else projects from.

## Reference: Key documents

- **Authoritative spec YAML**: `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`
- **Spec prose**: `docs/specs/mas-platform/platform/platform-spec.md`
- **Empire contracts**: `docs/specs/mas-platform/empire/contracts/`
- **MAS test catalog**: `docs/specs/mas-platform/tests/TEST-CATALOG.md` (105 tests, 3 implemented)
- **Clean test model**: `internal/runtime/masflowtest/`
- **Old plan (archived)**: `docs/architecture/archive/mas-platform-v1_1_0-implementation-plan.md`

## Operational Notes

```bash
# Full suite
go test ./... -count=1

# Pipeline package only
go test ./internal/runtime/pipeline -count=1

# Contract compliance
go test ./internal/runtime -run TestContractCompliance -count=1
```

```bash
docker compose up -d postgres
docker compose build workspace-base orchestrator dashboard
docker compose up -d orchestrator dashboard
```

Dashboard: `http://localhost:8070/dashboard/` (excluded from platformization scope)
