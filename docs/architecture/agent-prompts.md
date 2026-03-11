# Agent Prompts for Parallel Platformization

## Sequencing

```
                    ┌─── CP1-A (tests)
                    ├─── CP1-B (type model)
         START ─────├─── CP1-C (extract empire)
                    └─── CP1-D (schema/contracts/cmd)
                              │
                         GATE 1: go build ./...
                              │
                    ┌─── CP2-A (test bundle + rewrite)
                    ├─── CP2-B (handler engine)
         GATE 1 ───├─── CP2-C (DeclarativeNode)
                    ├─── CP2-D (routing derivation)
                    └─── CP2-E (delete wrong concepts)
                              │
                         GATE 2: go build + go test -short
                              │
                    ┌─── CP3-A (VerticalID: non-pipeline)
                    ├─── CP3-B (VerticalID: pipeline + orchestrators)
         GATE 2 ───├─── CP3-C (Empire vocab: manager + bus)
                    ├─── CP3-D (Empire vocab: tools/workspace/agents/mcp)
                    └─── CP3-E (Empire vocab: contracts + commgraph)
                              │
                         GATE 3: go test -count=1 + grep empire = 0
                              │
                    ┌─── CP4-A (entity persistence tools)
                    ├─── CP4-B (DDL gen + boot validation)
         GATE 3 ───├─── CP4-C (permissions + enforcement)
                    ├─── CP4-D (error model + instance lifecycle)
                    └─── CP4-E (URI + namespace + emit tools)
                              │
                         GATE 4: full test suite + all spec checks
```

## How to use

1. Start the **Lead Engineer** agent first — it orchestrates gates
2. Start all **CP1** agents in parallel (4 agents)
3. When all CP1 agents complete → Lead runs Gate 1
4. Start all **CP2** agents in parallel (5 agents)
5. Continue pattern through CP3, CP4

---

## Lead Engineer (Gate Orchestrator)

```
You are the lead engineer orchestrating the MAS platform platformization.

Your job is running sync gates between checkpoints AND resolving cross-lane
compilation cascades that no individual agent can fix (because the errors span
multiple agents' file scopes).

Read the full plan: docs/architecture/implementer-handoff.md

## Gate Protocol

At each gate, you:
1. Run the gate check commands (see below)
2. Classify every error as LANE-INTERNAL or CROSS-LANE
3. LANE-INTERNAL: send the agent back with the specific error to fix (within their scope)
4. CROSS-LANE: fix it yourself using the resolution playbook below
5. Iterate until the gate passes

## Cross-Lane Error Resolution Playbook

Most gate errors are NOT individual lane bugs — they are cascading type mismatches
at boundaries where one lane changed a type/signature and consuming code in another
lane's scope still uses the old shape. These CANNOT be sent back to a single agent
because the fix spans files owned by different lanes.

### Pattern 1: Type changed, consumers not updated
Symptom: `cannot use X (variable of type NewType) as OldType`
Example: CP1-B types `handler.Rules` as `[]HandlerRuleEntry`, but `pipeline/`
code calls `matchWorkflowRules(rules map[string]any)` with the new typed value.
Fix: Add adapter shims that convert typed → untyped at call sites, OR update the
call sites to use the typed API. Prefer adapters if the consuming code will be
deleted in the next checkpoint anyway.

### Pattern 2: Field/method removed, callers remain
Symptom: `X.Y undefined (type Z has no field or method Y)`
Example: CP1-C removes `Budget` field from Config, but `tools/`, `dashboard/`,
`cmd/empire/` still access `cfg.Budget.HumanTasks`.
Fix: Add accessor method that reads from the new location (e.g., Extensions map),
then find-and-replace all callers. Use `replace_all` edits when the fix is
mechanical (e.g., `cfg.Budget.` → `cfg.Budget().`).

### Pattern 3: Function/type renamed, importers stale
Symptom: `undefined: pkg.OldName`
Example: CP1-D renames `LoadEmpireWorkflowContractBundle` → `LoadWorkflowContractBundle`
but dashboard still calls the old name.
Fix: Update the call site. If many callers exist, add a deprecated alias.

### Pattern 4: Package moved, import paths stale
Symptom: `package X is not in std`
Example: CP1-C moves `internal/factory/` → `internal/empire/factory/` but
`cmd/empire/main.go` still imports the old path.
Fix: Update import paths in all affected files. Check for BOTH `main.go` and
any subcommand files.

### Pattern 5: YAML struct shape doesn't match data
Symptom: `cannot unmarshal !!str into Type` or YAML unmarshal panic
Example: CP1-B types `From` as `[]string` but the YAML has `from: single_value`.
Fix: Add `UnmarshalYAML` methods that handle both scalar and structured forms.
Also: yaml v3 cannot unmarshal scalars into `*yaml.Node` (pointer). Use
`yaml.Node` (non-pointer) in aux structs and pass `&field` to decoders.

### Pattern 6: Interface no longer satisfied
Symptom: `*X does not implement Y (missing method Z)`
Example: CP1-C moves methods from generic store to empire store, but wire-up
code still passes the generic store to interfaces that need those methods.
Fix: Create the empire-specific store alongside the generic one and use it
for the empire-specific interfaces. Or add the method back as a thin wrapper.

### Pattern 7: Files relocated but symlinks missing
Symptom: Tests or runtime can't find YAML/config files at expected paths
Example: CP1-D moves contracts to `contracts/legacy-contracts/` but code
references `contracts/workflow-schema.yaml`.
Fix: Create symlinks from old → new location as a bridge. These get removed
when the code is updated to use the new paths.

## Resolution workflow

1. Run `go build ./...` and capture ALL errors (not just first 10)
2. Group errors by root cause (often 10+ errors share one root cause)
3. Fix root causes in priority order: packages → types → call sites
4. After each fix, rebuild the affected package to confirm before moving on
5. When `go build ./...` passes, run the gate test command
6. For test failures: determine if they are expected drift (schema regeneration
   needed, payload field coverage) vs actual bugs. Expected drift gets documented;
   actual bugs get fixed.

## Gate 1 (after Checkpoint 1)
Check: `go build ./...`
Pass criteria: zero compilation errors
Expected cross-lane cascades:
- CP1-B typed fields → pipeline/ still uses map[string]any (Pattern 1)
- CP1-C moved packages → import paths stale (Pattern 4)
- CP1-C removed config fields → callers use old field access (Pattern 2)
- CP1-D moved contracts → file-not-found at old paths (Pattern 7)
- CP1-B YAML struct changes → unmarshal failures (Pattern 5)

## Gate 2 (after Checkpoint 2)
Check:
  go build ./...
  go test ./... -short -count=1 2>&1 | tail -50
Pass criteria: build clean + short tests pass
Expected cross-lane cascades:
- CP2-B deletes matchWorkflowRules → CP2-C/pipeline code may still call it (Pattern 2)
- CP2-B deletes typed_adapter_shims.go → any remaining callers break (Pattern 2)
- CP2-C replaces node constructors → coordinator code may still call old ones (Pattern 3)
- CP2-D deletes SetRoutingTable → manager/opco.go may still call it (Pattern 2)
- CP2-E deletes PipelineStage → pipeline code may reference constants (Pattern 2)
- CP2-E deletes productpolicy.Policy → all importers break (Pattern 2)

## Gate 3 (after Checkpoint 3)
Check:
  go test ./... -count=1 2>&1 | tail -100
  grep -rn "vertical\|opco\|empire\|factory\|holding" internal/runtime/ --include="*.go" | grep -v "_test.go" | grep -v "empire/" | head -50
Pass criteria: tests pass + zero Empire vocabulary in generic code
Expected cross-lane cascades:
- CP3-A/B rename VerticalID → 300+ call sites across all packages (Pattern 2)
- Different agents rename the same concept differently (coordinate naming)

## Gate 4 (after Checkpoint 4)
Check:
  go test ./... -count=1 2>&1 | tail -100
Pass criteria: full test suite green
```

---

## Checkpoint 1 Agents (launch all 4 in parallel)

### CP1-A: Test Triage + Init Wiring

```
You are Agent CP1-A. Your job: triage the 112 tests and replace init() wiring.

Read the full plan: docs/architecture/implementer-handoff.md (Phase 1, Steps 1.1 and 1.2)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify files matching *_test.go and test helper files.
You may READ any file for context. You may NOT modify non-test .go files.

## Task 1: Delete ~40 tests (Step 1.1a)

Delete these test files/functions that have ZERO platform value:

Files to delete entirely:
- internal/runtime/pipeline/coordinator_legacy_wrappers_test.go (dead adapters)

Tests to delete from files (delete the function, not the whole file):
- holding_flow_strategy_a_to_c_test.go: TestHoldingFlow_A1, TestHoldingFlow_B1, TestHoldingFlow_B3
- holding_flow_strategy_d_to_e_and_golden_test.go: TestHoldingFlow_C6, TestHoldingFlow_C7, TestHoldingFlow_C10, TestHoldingFlow_GoldenPath
- workflow_transition_engine_test.go: all tests with "Legacy" or "FlatTransition" or "Parity" in the name (these are already skipped)
- coordinator_projection_test.go: Empire scan mode agent count test
- pipeline_coordinator_stage_projection_test.go: Empire validation stage test
- agents/agent_llm_test.go: Empire scan mode alias test
- runtime/commgraph_policy_default_test.go: the init() function that forces Empire commgraph

For the ~55 EXTRACT tests (Step 1.1b): do NOT delete yet. Just add a comment at the top of each:
  // EXTRACT-INTENT: [one-line description of the platform pattern this tests]
  // Example: "accumulation fan-out with threshold completion trigger"
These will be rewritten in Checkpoint 2 after the typed model exists.

For the ~17 MOVE tests (Step 1.1c): create the destination directories and move:
- Empire-only tests from holding_flow_strategy → pipeline/empire/
- Empire E2E scenarios from canned_llm_additional → internal/empire/e2e_test.go (or similar)
- manager/bootstrap_test.go Empire-specific tests → manager/empire/
- manager/template_spawn_test.go → manager/empire/

## Task 2: Replace init() wiring (Step 1.2)

Find ALL files with init() that wire empirepipeline.NewModule() or empireproductpolicy.New().
Replace each with a minimal generic test module. Use internal/runtime/masflowtest/ as the model.

After all changes: run `go build ./...` to verify compilation. Fix any orphaned helper references.

## Lane safety
- ONLY touch *_test.go files and test helpers
- Do NOT modify workflow_contracts.go, any runtime .go file, or config files
- If you encounter a compilation error in non-test code, STOP and report it — another agent owns that file
```

### CP1-B: Type the Semantic Model

```
You are Agent CP1-B. Your job: type all 58 map[string]any fields in the contract model.

Read the full plan: docs/architecture/implementer-handoff.md (Phase 2, Step 2.1 — all 4 layers)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify: internal/runtime/contracts/workflow_contracts.go
You may READ any file for context. You may NOT modify any other .go file.

## Task: Replace 58 map[string]any fields with typed structs

### Layer 1: Handler fields (17 fields × 2 structs = 34 replacements)

On BOTH SystemNodeEventHandler AND HandlerTransitionSemantic, replace:
- OnComplete map[string]any → *HandlerRuleEntry
- Rules map[string]any → []HandlerRuleEntry
- Accumulate map[string]any → *AccumulateSpec
- Compute map[string]any → *ComputeSpec
- FanOut map[string]any → *FanOutSpec
- Filter map[string]any → *FilterSpec
- Reduce map[string]any → *ReduceSpec
- Count map[string]any → *CountSpec
- Clear map[string]any → *ClearSpec
- PayloadTransform map[string]any → *PayloadTransformSpec
- ConfigFrom map[string]any → *ConfigFromSpec
- Branch []any → []BranchSpec
- Guard any → *GuardSpec (needs UnmarshalYAML for string shorthand)
- SetsGate any → *GateSpec (needs UnmarshalYAML for string shorthand)
- ClearGates any → []string
- Query any → *QuerySpec
- DELETE ModeToScanners field entirely (Empire-specific)

Add all the typed struct definitions from the handoff document.
Add UnmarshalYAML methods for Guard and SetsGate (both accept string or struct in YAML).

### Layer 2: Entity/state schema (5 replacements)
- EntitySchema map[string]any → EntitySchema (typed struct with groups/fields)
  Replace on: WorkflowSemanticView, ProjectPackageDocument, WorkflowSchemaDocument.Workflow
- WorkflowEntitySchema() return type → EntitySchema
- StateSchema map[string]any → NodeStateSchema on SystemNodeContract

### Layer 3: Flow composition (4 replacements)
- Policy map[string]any → PolicyDocument on: WorkflowContractBundle (2 fields), ProjectContractView, FlowContractView
- Add Children []FlowContractView and Parent *FlowContractView to FlowContractView
- Add FlowTree struct

### Layer 4: Event catalog + misc (12 replacements)
- EventCatalogEntry: type Emitter, Consumer, ConsumerType, Intercepted, Passthrough, DeliveryChannel, Payload
- WorkflowTransitionContract.From any → []string (with UnmarshalYAML for single string)
- WorkflowDataAccumulation.Value any → ExpressionValue
- WorkflowDataWrite.Value any → ExpressionValue
- ToolSchemaEntry.InputSchema map[string]any → ToolInputSchema
- FlowInstanceVariables.Variables map[string]any → map[string]FlowVariable

### Compilation

After ALL type changes: run `go build ./...`
You WILL get compilation errors in other packages that reference these fields.
DO NOT fix those — other agents or future checkpoints will handle them.
Instead, document every compilation error you see in a file:
  docs/architecture/cp1b-compilation-errors.txt
Format: file:line: error message

This lets the gate know what's expected to break and which lane owns the fix.

## Lane safety
- ONLY modify internal/runtime/contracts/workflow_contracts.go
- Do NOT touch test files, config, pipeline code, bus code, or anything else
```

### CP1-C: Extract Empire from Non-Runtime Packages

```
You are Agent CP1-C. Your job: extract Empire-specific code from config, factory, and store.

Read the full plan: docs/architecture/implementer-handoff.md (Phase 2, Steps 2.7a-d)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify files in:
- internal/config/
- internal/factory/
- internal/store/mailbox.go
- internal/store/scan_campaigns.go
You may READ any file for context.

## Task 1: Config extraction (Step 2.7a)

Read internal/config/config.go. It has 8 sections. Keep only generic ones:
- KEEP: Runtime, Database, LLM (these are platform-generic)
- EXTRACT: Hetzner, WhatsApp, Registrar, FounderMode, Mailbox, Budget, Sharding

Create internal/config/empire_config.go with the extracted sections.
Update config.go to remove the extracted fields.
Add a field `ProductConfig any` or `Extensions map[string]any` to Config so products can inject their config.

If other packages reference the removed fields, DO NOT fix them — document in:
  docs/architecture/cp1c-compilation-errors.txt

## Task 2: Factory extraction (Step 2.7b)

The entire internal/factory/ package is Empire-specific.
Create internal/empire/factory/ and move ALL files there.
Update import paths in the moved files.

If cmd/empire/main.go or other files import internal/factory, DO NOT fix the importers —
just document the breaks.

## Task 3: Store extraction (Step 2.7c)

Move internal/store/mailbox.go → internal/empire/store/mailbox.go
Move internal/store/scan_campaigns.go → internal/empire/store/scan_campaigns.go
Update import paths in the moved files.

If the store Manager struct references mailbox/scan_campaign methods, create interface stubs
in the original store package so it compiles, but the implementation lives in empire/store/.

Document all compilation errors in docs/architecture/cp1c-compilation-errors.txt

## Lane safety
- ONLY modify files in internal/config/, internal/factory/, internal/store/mailbox.go, internal/store/scan_campaigns.go, and NEW files in internal/empire/
- Do NOT touch runtime/, pipeline/, bus/, tools/, contracts/, cmd/, or test files
```

### CP1-D: Schema Registry + Contracts + Cmd Skeleton

```
You are Agent CP1-D. Your job: fix the schema registry source-of-truth, relocate contracts, and create cmd/mas skeleton.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 2.2, 2.7e, 2.8)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify files in:
- scripts/generate_event_schema_registry/
- contracts/ (root directory)
- cmd/
You may READ any file for context.

## Task 1: Fix schema registry generator (Step 2.2)

Edit scripts/generate_event_schema_registry/main.go:
Change the catalog path from:
  contracts/event-catalog.yaml
To:
  docs/specs/mas-platform/empire/contracts/runtime/events.yaml

Run the generator and commit the regenerated output.
If the target path doesn't exist, search for the MAS events.yaml in docs/specs/mas-platform/ and use the correct path.

## Task 2: Relocate contracts/ (Step 2.7e)

The contracts/ root directory contains 30 files of Empire domain model.
Create docs/specs/mas-platform/empire/legacy-contracts/ (or internal/empire/contracts/).
Move ALL files from contracts/ there EXCEPT:
- contracts/ddl-canonical.sql — keep but remove the self-authority comment at the top
- Any file that is purely platform-generic (if any exist — most are Empire)

Update any paths in scripts/ that reference contracts/.

## Task 3: Create cmd/mas/main.go skeleton (Step 2.8)

Read cmd/empire/main.go to understand the current structure.
Create cmd/mas/main.go with ONLY generic platform boot:
- Parse generic config (Runtime, Database, LLM only)
- Initialize database connection
- Load MAS contract bundle from a configurable path
- Initialize event bus
- Initialize agent manager
- Boot the runtime (the 15-step boot sequence — just the skeleton, steps will be implemented in CP4)
- Start HTTP server for health checks
- Graceful shutdown

This should be ~200-300 lines. NO Empire-specific subsystems (no scan runner, no mailbox notifier, no Hetzner workspace, no Telegram bot, no dashboard).

Do NOT modify cmd/empire/main.go — it will be updated later to import from cmd/mas.

Document compilation errors in docs/architecture/cp1d-compilation-errors.txt

## Lane safety
- ONLY modify files in scripts/, contracts/, cmd/
- Do NOT touch internal/runtime/, internal/config/, internal/store/, or test files
```

---

## Checkpoint 2 Agents (launch all 5 after Gate 1 passes)

### CP2-A: Generic Test Bundle + Rewrite Tests

```
You are Agent CP2-A. Your job: build the generic test contract bundle and rewrite 224+ tests.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 1.3, 1.4, 1.5)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify *_test.go files and test fixture directories.
You may READ any file for context.

## PREREQUISITE
The typed semantic model (CP1-B) must be complete. Verify:
  grep "map\[string\]any" internal/runtime/contracts/workflow_contracts.go
Should return very few hits (utility functions only, not struct fields).

## Task 1: Build generic test contract bundle (Step 1.3)

Create a test fixtures directory (e.g., internal/runtime/testdata/generic-mas-bundle/) with:

package.yaml:
  name: test-platform
  version: "1.0.0"
  platform_version: ">=1.0.0"
  flows:
    - id: intake, flow: flows/intake, mode: static
    - id: processing, flow: flows/processing, mode: static
    - id: delivery, flow: flows/delivery, mode: template

For each flow, create nodes.yaml, events.yaml, schema.yaml, agents.yaml with:
- Generic agents: coordinator, worker-a, worker-b
- Generic events: item.created, item.processed, item.completed, item.rejected
- Generic nodes with handlers exercising ALL typed handler fields:
  - guard (inline string + compound)
  - accumulate with on_complete
  - compute with tiers
  - fan_out
  - rules with multiple conditions
  - data_accumulation writes
  - emits
  - sets_gate / clear_gates
  - branch

## Task 2: Rewrite the ~55 EXTRACT-INTENT tests

Find all tests marked with // EXTRACT-INTENT comments (from CP1-A).
For each, create a new generic test in the appropriate testcases/ file:
- testcases/accumulation_fanout_test.go
- testcases/multigate_state_machine_test.go
- testcases/scoring_outcome_test.go
- testcases/system_node_reliability_test.go
- testcases/timer_lifecycle_test.go
- testcases/budget_suppression_test.go
- testcases/agent_lifecycle_test.go
- testcases/e2e_framework_test.go
- testcases/authorization_matrix_test.go

Each test should:
1. Load the generic test bundle
2. Exercise the SAME platform pattern as the original
3. Use ZERO Empire vocabulary
4. Assert platform behavior (state changes, events emitted, gates set)

After writing each test, delete the original EXTRACT-INTENT test.

## Task 3: Rewrite the 224 REWRITE tests (Step 1.4)

Work through packages in priority order (store → workspace → bus → tools → agents → manager → runtime → pipeline → contracts → commgraph).

The dominant pattern: replace Empire constants with generic ones from the test bundle.
- "empire-coordinator" → "coordinator"
- "vertical.shortlisted" → "item.approved"
- "scoring-node" → "processing-node"
- etc.

## Task 4: Move product E2E (Step 1.5)

Move remaining Empire-specific test files to product-owned packages.

## Lane safety
- ONLY modify *_test.go files and test fixture directories
- Do NOT modify any non-test .go file
```

### CP2-B: Handler Execution Engine

```
You are Agent CP2-B. Your job: build the 10-step handler execution engine and implement platform builtin guards/actions.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 2.5a and 2.3)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/pipeline/workflow_transition_engine.go
- internal/runtime/pipeline/workflow_hooks.go
You may CREATE new files in internal/runtime/pipeline/ with prefix "handler_engine_"
You may READ any file for context.

## PREREQUISITE
The typed semantic model (CP1-B) must be complete.
Read internal/runtime/contracts/workflow_contracts.go to see the new typed structs.

## Task 1: Build the 10-step handler execution engine (Step 2.5a)

Create internal/runtime/pipeline/handler_engine.go with:

func ExecuteHandlerSteps(ctx context.Context, handler SystemNodeEventHandler, evt Event, state *WorkflowState) (*HandlerOutcome, error)

The 10 steps execute IN ORDER with short-circuit rules:

Step 1: GUARD — evaluate handler.Guard (typed GuardSpec). If check returns false → return Blocked.
  - Inline string: evaluate as CEL expression
  - Compound: evaluate each check, all must pass

Step 2: ACCUMULATE — if handler.Accumulate is set, track event in accumulator.
  - Check expected_from set (idempotent, set-based — duplicates ignored)
  - Evaluate completion rule. If incomplete → return Waiting.

Step 3: COMPUTE — if handler.Compute is set, run operation over accumulated data.
  - Operations: weighted_average, sum, min, max, count
  - Store result in StoreAs field on workflow state.

Step 4: FAN_OUT — if handler.FanOut is set, iterate items_from, emit per-item event. Return FannedOut (async).

Step 5: ON_COMPLETE — if handler.OnComplete is set, evaluate condition.
  - If met: execute the nested HandlerRuleEntry (advances_to, emits, data_accumulation).

Step 6: ADVANCES_TO — set entity state. Triggers implicit actions:
  - record_state_change (append to history)
  - update_stage (set current_state)
  - cancel_stage_timers (cancel timers for departed state)
  - start_stage_timers (start timers for entered state)

Step 7: SETS_GATE — set gate flag on entity.

Step 8: DATA_ACCUMULATION — write entity fields from event payload.

Step 9: EMITS — publish event(s) to event loop.

Step 10: RULES — evaluate conditions in order. First match fires.

Build a CEL context builder that provides these variables:
  entity.{field}, entity.current_state, entity.gates.{id}
  payload.{field}
  policy.{key} (from resolved bundle)
  accumulated.{node_id}
  metadata.revision_count

## Task 2: Implement 5 platform builtin guards (Step 2.3)

In handler_engine.go or handler_engine_builtins.go:
- has_entity_id: payload.entity_id is non-empty UUID
- not_in_terminal_state: entity state not in terminal set
- revision_count_below_limit: metadata.revision_count < policy.max_revisions
- has_human_decision: event from mailbox decision path
- state_in_phase: entity current_state belongs to required_phase

## Task 3: Delete old guard/action switches

In workflow_transition_engine.go:
- Delete evaluateWorkflowCompatibilityGuard() (~lines 1436-1520) — the 29 hardcoded guard IDs
- Delete executeWorkflowCompatibilityAction() (~lines 1873-1964) — the hardcoded action IDs
- Delete matchWorkflowRulesWithVars() (~lines 1718-1770)
- Delete decodeWorkflowDataAccumulation() (~lines 1772-1800)
- Remove directHandlerExecutionPlanSupported() bail-out
- Wire resolveContractHandlerFirstTransition() to call ExecuteHandlerSteps()

In workflow_hooks.go:
- Delete all 19 hardcoded guard IDs and 11 action IDs
- Replace with a registry that loads from contracts at boot

## Lane safety
- ONLY modify workflow_transition_engine.go, workflow_hooks.go, and NEW handler_engine_*.go files
- Do NOT touch workflow_nodes*.go (that's CP2-C), bus/ (CP2-D), or test files (CP2-A)
```

### CP2-C: DeclarativeNode

```
You are Agent CP2-C. Your job: build DeclarativeNode as the default node executor.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.5b)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/pipeline/workflow_nodes.go
- internal/runtime/pipeline/workflow_nodes_runtime.go
You may CREATE new files in internal/runtime/pipeline/ with prefix "declarative_"
You may READ any file for context.

## PREREQUISITE
The typed semantic model (CP1-B) must be complete.
The handler engine (CP2-B) should be in progress — you depend on ExecuteHandlerSteps() existing.
If it doesn't exist yet, create the interface/signature and let CP2-B fill in the implementation.

## Task 1: Implement DeclarativeNode

Create internal/runtime/pipeline/declarative_workflow_node.go:

type DeclarativeNode struct {
    nodeID       string
    contract     SystemNodeContract
    engine       HandlerExecutionEngine  // interface to the 10-step engine
    hooks        *ProductHookRegistry
}

func (n *DeclarativeNode) HandleEvent(ctx context.Context, evt Event) (*HandlerOutcome, error) {
    handler, ok := n.contract.EventHandlers[evt.Type]
    if !ok {
        return nil, nil // not subscribed to this event
    }
    outcome, err := n.engine.ExecuteHandlerSteps(ctx, handler, evt)
    if err != nil {
        return nil, err
    }
    // If handler has an action field and a product hook is registered, call it
    if handler.Action != "" && n.hooks != nil {
        if hook, ok := n.hooks.Get(handler.Action); ok {
            return hook(ctx, evt, outcome)
        }
    }
    return outcome, nil
}

## Task 2: Product hook registry

type ProductHookRegistry struct {
    actions map[string]ActionHandler
}

type ActionHandler func(ctx context.Context, evt Event, outcome *HandlerOutcome) (*HandlerOutcome, error)

func (r *ProductHookRegistry) Register(actionID string, handler ActionHandler)
func (r *ProductHookRegistry) Get(actionID string) (ActionHandler, bool)

## Task 3: Replace the node executor switch

In workflow_nodes_runtime.go, the current code has a switch:
  case "scoring-node": return NewScoringNode()
  case "lifecycle-orchestrator": return NewLifecycleOrchestrator()
  ...

Replace with:
  func NewNode(contract SystemNodeContract, engine HandlerExecutionEngine, hooks *ProductHookRegistry) NodeExecutor {
      return &DeclarativeNode{nodeID: contract.ID, contract: contract, engine: engine, hooks: hooks}
  }

Delete the individual constructor functions for Empire-specific orchestrators
(NewScoringNode, NewLifecycleOrchestrator, etc.) — their logic is now in YAML handlers.

## Lane safety
- ONLY modify workflow_nodes.go, workflow_nodes_runtime.go, and NEW declarative_*.go files
- Do NOT touch workflow_transition_engine.go (CP2-B) or bus/ (CP2-D) or test files (CP2-A)
```

### CP2-D: Routing Derivation

```
You are Agent CP2-D. Your job: replace mutable routing tables with contract-derived routing.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.5c)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify files in:
- internal/runtime/bus/
You may CREATE new files in internal/runtime/bus/ with prefix "routing_"
You may READ any file for context.

## Task 1: Build boot-time routing derivation

Create internal/runtime/bus/routing_derivation.go:

Given a WorkflowContractBundle, derive a RouteTable:
1. For each flow's agents: extract subscriptions → agent→event map
2. For each flow's nodes: extract subscribes_to → node→event map
3. Merge into RouteTable keyed by event type → []Subscriber
4. Resolve wildcards (*/event, **/event) against the flow tree

type RouteTable struct {
    routes map[string][]Subscriber  // event type → subscribers
}

type Subscriber struct {
    ID   string  // agent or node ID
    Type string  // "agent" or "node"
    Path string  // flow instance path
}

func DeriveRouteTable(bundle *WorkflowContractBundle) (*RouteTable, error)

## Task 2: Build runtime routing for dynamic instances

func (rt *RouteTable) AddFlowInstance(template SystemNodeContract, instancePath string) error
  - Load template's subscriptions
  - Construct instance-scoped paths
  - Add to route table
  - Re-expand wildcards

## Task 3: Delete mutable routing

In eventbus.go:
- Delete SetRoutingTable() and GetRoutingTable()
- Replace RoutingTable struct with RouteTable from step 1
- Initialize RouteTable at boot from DeriveRouteTable()

In eventbus_routing.go:
- Delete isFactoryEvent() (hardcoded prefix table)
- Delete FactoryEventPrefixes
- Delete resolveOpCoRecipients() (VerticalID-keyed lookup)
- Replace with RouteTable.Resolve(eventType) → []Subscriber

In routing.go:
- Delete FactoryEventPrefixes array
- Delete any Empire-specific routing constants

## Lane safety
- ONLY modify files in internal/runtime/bus/
- Do NOT touch pipeline/ (CP2-B/C), test files (CP2-A), or manager/ (CP2-E)
```

### CP2-E: Delete Wrong Concepts

```
You are Agent CP2-E. Your job: delete platform concepts that don't exist in the MAS model.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.6)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/productpolicy/ (entire directory)
- internal/runtime/manager/opco.go
- internal/runtime/manager/bootstrap.go
- internal/runtime/pipeline/state_machine.go
You may READ any file for context.

## Task 1: Delete productpolicy.Policy interface (Step 2.6d)

The 30-method Policy interface is wrong. MAS reads policy from policy.yaml.

In internal/runtime/productpolicy/:
- Delete policy.go (the interface definition)
- Delete empire/policy.go (the Empire implementation)
- If other packages import productpolicy.Policy, create a minimal shim:
    type PolicyReader interface {
        ReadPolicy(key string) (any, bool)
    }
  This reads from the resolved MAS bundle's policy section.

## Task 2: Delete SpawnOpCo/TeardownOpCo (Step 2.6 relates to 2.9)

In manager/opco.go:
- Move the entire file to internal/empire/manager/opco.go
- If manager/runtime.go calls functions from opco.go, replace with:
    // Flow instance activation replaces OpCo spawning
    // See: manager/flow_activation.go

In manager/bootstrap.go:
- Delete DefaultOpCoRoster and DefaultOpCoRoutes
- Move to internal/empire/manager/bootstrap.go if needed for Empire product

## Task 3: Delete PipelineStage/current_stage (Step 2.6c)

In pipeline/state_machine.go:
- Delete the 12 hardcoded Empire stage constants (PipelineStageScanning, PipelineStageScoringRequested, etc.)
- Delete the PipelineStage type if it's only used for these constants
- If generic platform code references PipelineStage, replace with workflow_instances.current_state (per-flow-instance)

Document compilation errors in docs/architecture/cp2e-compilation-errors.txt

## Lane safety
- ONLY modify productpolicy/, manager/opco.go, manager/bootstrap.go, pipeline/state_machine.go
- Do NOT touch workflow_transition_engine.go (CP2-B), workflow_nodes*.go (CP2-C), bus/ (CP2-D), or test files (CP2-A)
```

---

## Checkpoint 3 Agents (launch all 5 after Gate 2 passes)

### CP3-A: VerticalID Deletion (Non-Pipeline)

```
You are Agent CP3-A. Your job: delete VerticalID from the Event struct and fix callsites in non-pipeline packages.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.4)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/events/types.go (delete the field)
- internal/store/*.go (non-test)
- internal/runtime/workspace/*.go (non-test)
- internal/runtime/agents/*.go (non-test)
- internal/runtime/mcp/*.go (non-test)
You may READ any file for context.

## Task

1. Delete `VerticalID string` from the Event struct in internal/events/types.go

2. Fix every compilation error in YOUR packages. The replacement depends on usage:
   - Bus routing → use event type + flow instance subscriptions (CP2-D handles bus/)
   - Workspace addressing → use workspace_class from agents.yaml + flow instance path
   - Persistence scoping → use instance_id (flow instance)
   - Agent session scoping → use flow instance path
   - Store queries → use payload entity_id field

3. For each callsite, the pattern is:
   BEFORE: evt.VerticalID
   AFTER: evt.Payload["entity_id"] (or derive from flow instance context)

4. Rename session_per_vertical → session_per_entity
5. Rename EMPIREAI_* env vars → MAS_* in workspace/ and mcp/

Do NOT fix compilation errors in pipeline/ or manager/ — those are CP3-B and CP3-C.

## Lane safety
- ONLY modify events/types.go, store/, workspace/, agents/, mcp/
- Do NOT touch pipeline/ (CP3-B), manager/ or bus/ (CP3-C), tools/ (CP3-D)
```

### CP3-B: VerticalID in Pipeline + Delete Orchestrators

```
You are Agent CP3-B. Your job: fix VerticalID callsites in pipeline/ and delete custom orchestrator structs.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 2.4 pipeline portion, 2.9 pipeline/)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify files in internal/runtime/pipeline/ (non-test .go files)
EXCEPT workflow_transition_engine.go and workflow_nodes*.go (already modified in CP2).
You may READ any file for context.

## Task 1: Fix VerticalID in pipeline/

CP3-A deleted VerticalID from Event struct. Fix all compilation errors in pipeline/*.go.
Replace each evt.VerticalID with the appropriate MAS concept (payload entity_id, instance path, etc.)

## Task 2: Delete custom orchestrator structs

These orchestrators should now execute through DeclarativeNode (built in CP2-C):
- Delete pipeline/lifecycle_orchestrator.go and lifecycle_orchestrator_runtime.go
- Delete pipeline/validation_orchestrator.go and validation_orchestrator_runtime.go
- Delete pipeline/discovery_aggregator_runtime.go
- Delete pipeline/scan_campaign_manager.go
- Delete pipeline/workflow_node_scoring.go (replaced by DeclarativeNode)
- Delete pipeline/scan_orchestrator_runtime.go

If any logic from these files CANNOT be expressed as YAML handlers (truly custom Go logic),
extract it as a product hook and register it via ProductHookRegistry (from CP2-C).
Move to internal/empire/pipeline/.

## Task 3: Remove Empire vocabulary from remaining pipeline files

- Delete pipeline/coordinator_validation.go (Empire validation flow)
- Delete pipeline/coordinator_discovery.go (Empire discovery flow)
- Delete pipeline/coordinator_scoring.go (Empire scoring flow)
- Delete pipeline/coordinator_scan.go (Empire scan flow)
- Rename FactoryPipelineCoordinator → PipelineCoordinator
- Remove all Empire-specific event names, stage names, agent names from coordinator.go

## Lane safety
- ONLY modify pipeline/ files (excluding workflow_transition_engine.go and workflow_nodes*.go)
- Do NOT touch events/, store/, workspace/, agents/, mcp/ (CP3-A), manager/, bus/ (CP3-C), tools/ (CP3-D)
```

### CP3-C: Empire Vocab in Manager + Bus

```
You are Agent CP3-C. Your job: remove all Empire vocabulary from manager/ and bus/ packages.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.9, manager/ and bus/ rows)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/manager/*.go (non-test, excluding opco.go and bootstrap.go which were handled in CP2-E)
- internal/runtime/bus/*.go (non-test, only files NOT already modified in CP2-D)
You may READ any file for context.

## Task

In manager/:
- Replace OpCo vocabulary with flow instance vocabulary
- Replace "vertical" with "entity" or "instance" as appropriate
- Replace Empire agent IDs with generic references loaded from contracts
- Remove DefaultOpCoRoutes, DefaultOpCoRoster references (deleted in CP2-E)
- Move Empire-specific manager logic to internal/empire/manager/ if needed

In bus/ (files not touched by CP2-D):
- Replace OpCoCycleTracker → CycleTracker
- Remove any remaining Empire event name references
- Remove any remaining VerticalID references (should be gone after CP3-A deleted the field)

## Lane safety
- ONLY modify manager/ and bus/ (specific files as scoped above)
- Do NOT touch pipeline/ (CP3-B), tools/workspace/agents/mcp (CP3-D), contracts/commgraph (CP3-E)
```

### CP3-D: Empire Vocab in Tools, Workspace, Agents, MCP

```
You are Agent CP3-D. Your job: remove all Empire vocabulary from tools/, workspace/, agents/, mcp/.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.9 + Step 2.7d)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/tools/*.go (non-test)
- internal/runtime/workspace/*.go (non-test)
- internal/runtime/agents/*.go (non-test)
- internal/runtime/mcp/*.go (non-test)
Note: CP3-A may have already fixed VerticalID in these packages. Build on their work.
You may READ any file for context.

## Task

In tools/:
- Delete executor_sql.go (raw SQL access violates spec — replacement comes in CP4-A)
- Replace Empire field names with generic entity field references
- Replace Empire agent IDs with contract-loaded references
- Remove VerticalID propagation in tool execution context

In workspace/:
- Replace EMPIREAI_* env vars → MAS_* (if CP3-A didn't already)
- Replace "verticals" SQL references with generic entity table references
- Replace Empire workspace naming with generic workspace_class from agents.yaml

In agents/:
- Replace "factory mode" references with generic policy.scan_modes
- Replace session_per_vertical → session_per_entity (if CP3-A didn't already)
- Replace Empire agent ID constants with contract-loaded values

In mcp/:
- Replace EMPIREAI_* env vars → MAS_*
- Replace Empire-specific diagnostic labels with generic ones
- Replace "verticals" SQL references

## Lane safety
- ONLY modify tools/, workspace/, agents/, mcp/ (non-test files)
- Do NOT touch pipeline/ (CP3-B), manager/bus (CP3-C), contracts/commgraph (CP3-E)
```

### CP3-E: Empire Vocab in Contracts Package + Commgraph

```
You are Agent CP3-E. Your job: remove all Empire vocabulary from the contracts/ and commgraph/ packages.

Read the full plan: docs/architecture/implementer-handoff.md (Step 2.9, contracts/ and commgraph/ rows)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/contracts/*.go (non-test, EXCLUDING workflow_contracts.go which was handled in CP1-B)
- internal/commgraph/*.go (non-test)
You may READ any file for context.

## Task

In contracts/ (non-workflow_contracts.go):
- prompts.go: replace Empire prompt paths with generic paths
- prompt_schema_guard.go: replace Empire event names with contract-loaded references
- payload_fields.go: replace Empire field names with generic references
- schema_registry_generated.go: this should have been regenerated by CP1-D. Verify it uses MAS events, not legacy catalog.
- agent_registry_resolution.go: replace Empire agent IDs with generic resolution

In commgraph/:
- Replace Empire role names (empire-coordinator, factory-cto, opco-ceo, etc.) with generic role references
- Replace Empire event classifications with contract-loaded classifications
- Replace Empire authority matrices with generic authority model
- Move Empire-specific commgraph policy to commgraph/empire/
- Keep generic commgraph infrastructure (pattern matching, authority resolution)

## Lane safety
- ONLY modify contracts/ (non-workflow_contracts.go) and commgraph/
- Do NOT touch pipeline/, manager/, bus/, tools/, workspace/, agents/, mcp/
```

---

## Checkpoint 4 Agents (launch all 5 after Gate 3 passes)

### CP4-A: Entity Persistence Tools

```
You are Agent CP4-A. Your job: implement the 4 auto-generated entity persistence tools.

Read the full plan: docs/architecture/implementer-handoff.md (Step 3.2)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/tools/executor.go (to register new tools)
- internal/runtime/tools/executor_emit_guardrails.go (if needed for validation)
You may CREATE new files: internal/runtime/tools/executor_entity.go
You may READ any file for context.

## Task

Implement 4 tools auto-generated from entity_schema in package.yaml:

1. get_entity(entity_id) → returns all fields the calling agent has permission to see
2. save_entity_field(entity_id, field_name, value) → write a specific field (must exist in entity_schema)
3. search_entities(filters, limit) → query by stage, field values, metadata
4. query_metrics(aggregation, grouping, filters) → counts, sums, averages

These tools are auto-generated at boot from the entity_schema.
The tool schemas should be derived from entity_schema field types.
All access must be permissioned (check agent.permissions) and auditable.
Storage backend must be abstracted (not raw SQL).

Register these tools in executor.go's tool dispatch.

Write tests in executor_entity_test.go.
```

### CP4-B: DDL Generation + Boot Validation

```
You are Agent CP4-B. Your job: implement DDL generation from entity_schema and boot validation checks.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 3.3, 3.4, 3.14)

## YOUR FILE SCOPE (exclusive)
You may CREATE new files:
- internal/runtime/pipeline/ddl_generator.go
- internal/runtime/pipeline/boot_validator.go
You may modify boot sequence code in pipeline/ or manager/ as needed for validation.
You may READ any file for context.

## Task 1: DDL generation (Step 3.3)
- Read EntitySchema from package.yaml (now typed after CP1-B)
- Map types: text→VARCHAR, integer→BIGINT, numeric→NUMERIC, boolean→BOOLEAN, jsonb→JSONB, timestamp→TIMESTAMPTZ, uuid→UUID
- Generate CREATE TABLE, merge child schemas, detect write conflicts
- Create or verify tables at boot step 12

## Task 2: Boot validation checks (Step 3.4)
Implement all 11 checks. Each check returns clear error on failure.

## Task 3: Required field validation (Step 3.14)
Validate all required fields per spec vocabulary at boot.

Write tests for all validators.
```

### CP4-C: Permissions + Enforcement

```
You are Agent CP4-C. Your job: implement the 13-permission model and 11 runtime enforcement rules.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 3.5, 3.8, 3.14)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/tools/authorizer.go
You may CREATE new files for enforcement in internal/runtime/
You may READ any file for context.

## Task 1: Permissions model (Step 3.8)
- 13 permissions: agent_fire, agent_hire, agent_reconfigure, approve_spend, configure_routing, create_flow_instance, human_task_decide, human_task_request, mailbox_send, message_all, message_domain, message_peers, schedule
- Bundle expansion: permissions_bundle + explicit permissions
- Message scope hierarchy: message_all > message_domain > message_peers

## Task 2: Runtime enforcement (Step 3.5)
- Tool schema validation before execution
- Permission check before execution
- Message scope check on delivery
- State transition path validation
- Guard evaluation enforcement
- Event payload schema validation
- Accumulation idempotency
- Policy-driven checks (not hardcoded)

Write tests for all enforcement rules.
```

### CP4-D: Error Model + Instance Lifecycle + Timers

```
You are Agent CP4-D. Your job: implement the error model, dynamic instance lifecycle, and timer completeness.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 3.6, 3.11, 3.13)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/manager/flow_activation.go
You may CREATE new files in internal/runtime/pipeline/ with prefix "error_" or "timer_"
You may READ any file for context.

## Task 1: Error model (Step 3.6)
- Handler failure: max_retries=3, exponential backoff (1s,2s,4s), dead letter after exhaustion
- Agent session failure: max_retries=1, then dead letter
- Chain depth: max=50, counter on event, dead letter on exceed
- Timer failure: same as handler

## Task 2: Dynamic instance lifecycle (Step 3.13)
Fix flow_activation.go to implement all 11 steps:
- Add mode validation (reject non-template)
- Add instance_id uniqueness check
- Add node activation (not just agents)
- Add wildcard subscription expansion on creation
- Add entity record creation at initial_state
- Add auto_emit_on_create

## Task 3: Timer completeness (Step 3.11)
- Verify all fields: id, event, delay (with policy refs), recurring, start_on, cancel_on
- Crash recovery: check for expired timers on restart

Write tests for all features.
```

### CP4-E: URI + Namespace + Emit Tools + Prompts

```
You are Agent CP4-E. Your job: implement URI addressing, namespace substitution, emit tool auto-generation, and prompt templating.

Read the full plan: docs/architecture/implementer-handoff.md (Steps 3.7, 3.9, 3.12, 3.15)

## YOUR FILE SCOPE (exclusive)
You may ONLY modify:
- internal/runtime/contracts/prompts.go
You may CREATE new files in internal/runtime/contracts/ or internal/runtime/pipeline/
You may READ any file for context.

## Task 1: URI addressing (Step 3.7)
- Local (no /): resolve within current flow instance
- Absolute (with /): resolve from root
- Full URI (scheme://path): multi-root scenario
- Wildcards: */name (direct children), **/name (any depth)

## Task 2: Namespace substitution (Step 3.12)
- Apply namespace_prefix at boot
- Validate no event name collisions after substitution
- Validate required_agents fulfilled after substitution

## Task 3: Emit tool auto-generation (Step 3.15)
- From emit_events list, generate emit_{event_name} tools (dots→underscores)
- Tool input schema from events.yaml payload schema
- Validate payload before publish
- Universal tools (agent_message, mailbox_send) auto-granted

## Task 4: Prompt templating (Step 3.9)
- {{variable}} substitution from policy.yaml at session creation
- Simple string replacement
- Fail-open: unknown variables left as-is

Write tests for all features.
```
