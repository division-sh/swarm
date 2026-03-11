# Phase Auditor Prompts

Each auditor runs AFTER the implementer declares a phase complete. The auditor's job is to mechanically verify completeness — not review code quality, not suggest improvements, just pass/fail each exit criterion with evidence.

**Rules for auditors:**
1. You produce ONLY a findings report. You do NOT fix anything.
2. Every finding must cite a file path and line number.
3. Every exit criterion gets an explicit PASS or FAIL with evidence.
4. "Partial" is a FAIL. The implementer said "done" — either it is or it isn't.
5. Check for hollow implementations: types declared but never populated, functions defined but never called, interfaces satisfied by no-ops.
6. Run mechanical checks (grep, build, test) — do not trust the implementer's self-reported results.
7. The auditor prompt includes the exact exit criteria from the handoff doc so there is no ambiguity about what "done" means.

---

## Phase 1 Auditor: Test Suite Cleanup

You are the Phase 1 auditor. The implementer claims Phase 1 (test suite cleanup) is complete. Your job is to verify every exit criterion mechanically.

### Exit criteria to verify

1. `go build ./...` passes — run it, report result
2. `go test ./... -count=1` passes — run it, report result (with `-short` if full run exceeds 5 minutes)
3. Zero `init()` wiring Empire module/policy as defaults — run:
   ```
   rg "init\(\)" --type go -A5 internal/runtime/ internal/commgraph/ | grep -i "empire\|NewModule\|empireproductpolicy\|empirepipeline"
   ```
   Report every match as FAIL.
4. Zero test files in generic packages importing `empirepipeline` or `empireproductpolicy` directly — run:
   ```
   rg "empirepipeline|empireproductpolicy" --type go internal/runtime/ internal/commgraph/
   ```
   Exclude files under explicitly product-owned paths (`internal/runtime/pipeline/empire/`, `internal/runtime/productpolicy/empire/`). Every other match is FAIL.
5. The ~112 DELETE tests no longer exist — verify each file listed in Step 1.1a:
   - `pipeline/holding_flow_strategy_a_to_c_test.go` — tests A1, B1, B3 deleted or file removed
   - `pipeline/holding_flow_strategy_d_to_e_and_golden_test.go` — tests C6, C7, C10, GoldenPath deleted or file removed
   - `pipeline/workflow_transition_engine_test.go` — legacy flat-transition tests deleted
   - `pipeline/coordinator_legacy_wrappers_test.go` — file removed
   - `pipeline/coordinator_projection_test.go` — Empire scan mode test deleted
   - `pipeline/pipeline_coordinator_stage_projection_test.go` — Empire validation stage test deleted
   - `runtime/commgraph_policy_default_test.go` — init() test deleted
   - `agents/agent_llm_test.go` — Empire scan mode alias test deleted
   - `runtime/canned_llm_additional_scenarios_e2e_test.go` — Scenarios 2, 4, 7 deleted
   For each: check if the test function still exists. If it does, FAIL.
6. The ~55 EXTRACT-INTENT tests have been rewritten as generic — verify:
   - `rg "EXTRACT-INTENT" internal/runtime/ internal/commgraph/` returns zero hits
   - The generic test bundle exists (likely under `internal/runtime/testcases/` or similar)
   - The test bundle contains test files covering: accumulation/fan-out, multi-gate state, scoring outcomes, system node reliability, timer lifecycle, budget suppression, agent lifecycle, e2e framework, authorization matrix
   - These tests actually run: `go test ./internal/runtime/testcases/...` passes
7. The ~17 MOVE tests have been relocated to product-owned packages — verify:
   - Empire golden path tests exist under a product-owned test path (e.g., `pipeline/empire/`, `internal/empire/`)
   - They do NOT exist in generic test files
8. The ~224 REWRITE tests use generic vocabulary only — run:
   ```
   rg "\b(vertical|opco|factory|holding)\b" --type go internal/runtime/*_test.go internal/runtime/**/*_test.go internal/commgraph/*_test.go
   rg "\b(scoring-node|validation-coordinator|discovery-aggregator|scan-orchestrator|lifecycle-orchestrator)\b" --type go internal/runtime/*_test.go internal/runtime/**/*_test.go internal/commgraph/*_test.go
   rg "\bempire\b" --type go internal/runtime/*_test.go internal/runtime/**/*_test.go internal/commgraph/*_test.go
   ```
   IMPORTANT: Use word boundaries (`\b`) to avoid false positives on the `empireai` module path in import statements. The module name `empireai` in import paths is NOT a violation — only `empire` as a standalone word in test logic, constants, or string literals is a violation.
   Exclude product-owned test paths (`internal/runtime/pipeline/empire/`, `internal/runtime/manager/empire/`, `internal/empire/`).
   Every match in a generic test file is FAIL. Count total matches.
9. The 140 KEEP tests are untouched — verify `masflowtest/` package tests still pass:
   ```
   go test ./internal/runtime/masflowtest/...
   ```
10. No hardcoded Empire node IDs, agent IDs, or event types in generic tests — run:
    ```
    rg "(scoring-node|validation-coordinator|discovery-aggregator|scan-orchestrator|lifecycle-orchestrator|empire-coordinator|business-research-agent|market-research-agent|trend-research-agent|scanner-agent|analysis-agent|factory-cto|pre-brand-agent|spec-auditor|spec-reviewer|opco-ceo|opco-cto|opco-chief-of-staff|opco-head-of-growth|opco-head-of-product|opco-backend|opco-devops|holding-devops|operations-analyst)" --type go internal/runtime/ internal/commgraph/ --glob '*_test.go'
    ```
    Exclude product-owned test paths. Every match is FAIL.
11. No Empire-specific event type strings in generic tests — run:
    ```
    rg "(vertical\.|opco\.|scan_campaign\.|factory\.|holding\.)" --type go internal/runtime/ internal/commgraph/ --glob '*_test.go'
    ```
    Exclude product-owned test paths. Every match is FAIL.
12. No Empire-specific test fixture YAML files in generic locations — run:
    ```
    rg -l "(vertical|opco|empire|factory|holding)" internal/runtime/testcases/ internal/runtime/masflowtest/ --glob '*.yaml' --glob '*.yml'
    ```
    Every match is FAIL. Generic test fixtures must use generic vocabulary.
13. No indirect Empire coupling through helpers — verify:
    - Read each test helper file in the generic test bundle
    - Trace imports: if a generic test helper imports product-owned packages (e.g., `internal/runtime/pipeline/empire/`, `internal/empire/`), FAIL
    - Check for re-exported Empire types or functions laundered through generic-looking wrappers

### Hollow implementation checks

- If a generic test bundle directory exists, verify it has actual test functions (not just empty files or TODO stubs)
- If test helper files exist, verify they are actually called by tests
- Check that deleted test files don't leave orphaned helper functions that nothing calls
- Relocated product-owned tests must actually assert behavior — verify each test in `pipeline/empire/`, `manager/empire/`, `internal/empire/` contains at least one assertion (t.Error, t.Fatal, require., assert., or explicit comparison). A test that only calls t.Skip or only calls functions without checking results is FAIL.
- Generic test bundle coverage — verify the 9 pattern buckets from Step 1.1b are each covered by at least one test that exercises the pattern (not just named after it). Read each test function and confirm it sets up a scenario, executes it, and asserts an outcome.
- No orphaned test constants/variables — run:
  ```
  go vet ./internal/runtime/... ./internal/commgraph/...
  ```
  Unused test helpers or constants left over from deleted tests should be flagged.

### Structural audit (rename-proof)

Grep catches vocabulary. These checks catch semantic coupling that survives renaming.

14. **Second-product boot test** — the auditor must attempt to load a minimal non-Empire contract bundle through the generic test infrastructure. Verify:
    - The generic test bundle's `package.yaml` declares flows, agents, events, and entity fields that share ZERO names with Empire's
    - The test bundle boots successfully through the contract loader
    - The handler engine processes events from the test bundle without errors
    - If the generic test bundle is secretly just Empire's flows with renamed identifiers (same structure, same number of nodes, same event topology), FAIL. A genuine generic bundle should have a different shape: different number of flows, different agent count, different event graph.

15. **Hardcoded cardinality assumptions** — search for magic numbers or assumptions that match Empire's specific structure:
    ```
    rg "(== 5|== 4|== 6|== 7|== 8|== 12|== 13|len\(.+\) == [0-9])" --type go internal/runtime/ internal/commgraph/ --glob '*_test.go'
    ```
    For each match: does the asserted count correspond to Empire's specific flow/node/agent/event count? If a test asserts `len(nodes) == 7` and Empire has exactly 7 nodes, that test is semantically coupled even if it uses generic names. FAIL if the count comes from Empire's structure rather than the test fixture's.

16. **Test fixture independence** — read the generic test bundle's YAML fixtures and verify:
    - Flow names, node IDs, agent IDs, and event types are synthetic (e.g., `intake`, `processing`, `worker-a`, `item.created`) — not renames of Empire concepts
    - The flow topology is structurally different from Empire (different number of flows, different depth, different fan-out pattern)
    - Entity schema fields are synthetic — not Empire's exact fields renamed
    - If the fixture has the same number of nodes as Empire, the same event graph shape, and the same state machine transitions but with different names, FAIL with "renamed clone, not generic test"

17. **No path assumptions** — verify generic tests do not hardcode paths to Empire contract directories:
    ```
    rg "(contracts/empire|mas-platform/empire|legacy-contracts)" --type go internal/runtime/ internal/commgraph/ --glob '*_test.go'
    ```
    Exclude product-owned test paths. Every match is FAIL.

18. **Generic test exercises generic loader** — verify the test bundle loads through the standard `LoadWorkflowContractBundle()` path, not a product-specific loader or a test-only shortcut that bypasses the real contract loading code. Read the test helper's setup function and trace how it constructs the bundle.

### Report format

```
# Phase 1 Audit Report

Date: {date}
Build: PASS/FAIL
Tests: PASS/FAIL (N passed, N failed, N skipped)

## Exit Criteria (lexical)

| # | Criterion | Result | Evidence |
|---|-----------|--------|----------|
| 1 | go build passes | PASS/FAIL | {output} |
| 2 | go test passes | PASS/FAIL | {output} |
...

## Structural Audit (rename-proof)

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 14 | Second-product boot test | PASS/FAIL | |
| 15 | Hardcoded cardinality | PASS/FAIL | |
| 16 | Test fixture independence | PASS/FAIL | |
| 17 | No path assumptions | PASS/FAIL | |
| 18 | Generic loader used | PASS/FAIL | |

## Hollow Implementation Checks

{findings}

## Blocking Findings

{list of FAIL items that must be resolved before Phase 1 is truly complete}
```

---

## Phase 2 Auditor: Empire Extraction + Typed Model

You are the Phase 2 auditor. The implementer claims Phase 2 (Empire extraction and typed semantic model) is complete. Your job is to verify every exit criterion mechanically.

### Exit criteria to verify

1. `go build ./...` passes — run it, report result
2. `go test ./... -count=1` passes — run it, report result
3. Zero imports of `pipeline/empire`, `productpolicy/empire`, `commgraph/empire` from generic packages — run:
   ```
   rg "\"empireai/internal/runtime/pipeline/empire\"|\"empireai/internal/runtime/productpolicy/empire\"|\"empireai/internal/commgraph/empire\"" --type go internal/runtime/ internal/commgraph/
   ```
   Exclude files under product-owned paths. Every other match is FAIL.
4. Zero Empire vocabulary in generic pipeline code — run:
   ```
   rg "vertical|opco|empire|factory|holding" --type go internal/runtime/pipeline/*.go -l
   ```
   Exclude `internal/runtime/pipeline/empire/`. Every match is FAIL. List each file.
5. Handler-first execution handles `on_complete` and `rules` without bailing out — verify:
   - `directHandlerExecutionPlanSupported()` either deleted or no longer rejects `on_complete`/`rules`
   - Search: `rg "directHandlerExecutionPlanSupported" --type go`
   - If the function exists, read it and verify it does not bail on `on_complete` or `rules`
6. All system nodes execute through DeclarativeNode — verify:
   - `workflow_nodes_runtime.go` does NOT have a switch/case mapping node IDs to specific executor constructors
   - Search: `rg "case \"scoring-node\"|case \"validation-coordinator\"|case \"discovery-aggregator\"|case \"scan-orchestrator\"|case \"lifecycle-orchestrator\"" --type go`
   - Every match is FAIL
7. No `productpolicy.Policy` interface in generic code — verify:
   - `rg "type Policy interface" --type go internal/runtime/productpolicy/policy.go`
   - If `internal/runtime/productpolicy/policy.go` exists and defines a 30-method interface, FAIL
8. No mutable routing tables — verify:
   - `rg "SetRoutingTable|GetRoutingTable" --type go internal/runtime/bus/`
   - Every match is FAIL
9. No `accumulator_state` JSON bucket in generic code — verify:
   - `rg "accumulator_state" --type go internal/runtime/` (exclude product-owned paths)
   - Note: `accumulator_state` on `workflow_instances` table IS allowed per spec (it's per-node keyed state). The violation is using it as an untyped grab-bag in Go code.
10. No `current_stage` on entity table in generic code — verify:
    - `rg "current_stage" --type go internal/runtime/` (exclude product-owned paths)
    - Every match is FAIL
11. Schema registry generated from MAS contracts — verify:
    - Read `internal/runtime/contracts/schema_registry_generated.go` header comment or generator script
    - Verify it references MAS contract path, not `contracts/event-catalog.yaml`
12. No raw SQL tool in generic packages — verify:
    - `internal/runtime/tools/executor_sql.go` either deleted or moved to product-owned path
    - `rg "executor_sql" --type go internal/runtime/tools/`
13. Config has only Runtime/Database/LLM sections — verify:
    - Read `internal/config/config.go`
    - If it defines `Hetzner`, `WhatsApp`, `Registrar`, `FounderMode`, `Mailbox`, `Budget`, `Sharding` types, FAIL for each
    - These must be in a product-owned config package (e.g., `internal/empire/config/`)
14. `cmd/mas/main.go` exists and boots without Empire — verify:
    - File exists
    - Does not import Empire-specific packages
    - Contains only generic MAS runtime boot (LLM, event bus, agent lifecycle, scheduling)

### Typed model completeness checks

These verify CP1-B's work is actually complete, not just partially done:

15. All 58 `map[string]any` fields typed — run:
    ```
    rg "map\[string\]any" --type go internal/runtime/contracts/workflow_contracts.go
    ```
    Cross-reference against the 58-field table in the handoff doc. Every remaining `map[string]any` that should be typed is FAIL.
16. All `any` fields typed — run:
    ```
    rg "^\s+\w+\s+any\s+" --type go internal/runtime/contracts/workflow_contracts.go
    ```
    Cross-reference against the handoff doc field table. Legitimate `any` (e.g., `Default any` on FlowVariable) is OK. Handler fields that should be typed structs are FAIL.
17. FlowTree populated by loader — verify:
    - `FlowTree` struct exists in `workflow_contracts.go`
    - The loader function that builds `WorkflowContractBundle` actually populates `bundle.FlowTree`
    - `bundle.FlowTree.ByPath` has entries after loading (not empty map)
    - `FlowContractView.Children` is populated for flows with sub-flows
    - `FlowContractView.Parent` is set for child flows
    - If the types exist but the loader never populates them, FAIL with "hollow implementation"
18. PolicyDocument is hierarchical — verify:
    - Policy resolution walks up the tree (child shadows parent)
    - Not just flat merge of all policy values
    - Search for policy resolution function and verify it uses `Parent` pointer

### Structural audit (rename-proof)

19. **Handler engine data flow** — trace a single event through the handler engine end-to-end:
    - Pick any handler from the loaded contract bundle
    - Follow the code path from `DeclarativeNode.HandleEvent` through `ExecuteHandlerSteps`
    - At each of the 10 steps, verify the engine reads from the typed contract struct (e.g., `handler.Guard`, `handler.Accumulate`), NOT from a `map[string]any` or a hardcoded switch statement
    - If any step casts back to `map[string]any`, extracts by string key, or uses a type assertion on `any`, FAIL with "typed model bypassed at step N"

20. **Routing is derived, not configured** — verify the complete routing path:
    - At boot: trace how the route table is constructed. It must read from `FlowTree.ByPath` or `bundle.FlowContracts` and build routes from `subscribes_to` declarations
    - At runtime: when an event is published, trace how recipients are resolved. It must look up subscribers from the derived route table, not from a mutable map set by `SetRoutingTable` or from hardcoded namespace prefixes
    - If any fallback path exists that bypasses derived routing (even if named generically), FAIL

21. **Policy resolution is hierarchical** — write or find a test where a child flow overrides a parent flow's policy value, and verify:
    - The child's value is used when resolving `policy.X` in the child flow's context
    - The parent's value is used when resolving `policy.X` in the parent flow's context
    - If policy is just a flat merged map with last-writer-wins, FAIL

22. **FlowTree structure test** — after loading a contract bundle with nested flows:
    - Verify `FlowTree.Root` is non-nil
    - Verify `FlowTree.ByPath` has entries for each flow at its hierarchical path (not just flow ID)
    - Verify at least one `FlowContractView.Children` is non-empty
    - Verify at least one `FlowContractView.Parent` is non-nil
    - If the tree is flat (all flows at root level, no parent-child links), FAIL

23. **No semantic coupling through constants** — search for const blocks that define values matching Empire's exact structure:
    ```
    rg "const \(" --type go -A20 internal/runtime/pipeline/ internal/runtime/bus/ internal/runtime/manager/
    ```
    For each const block: are the values generic platform concepts or renamed Empire concepts? Look for:
    - Stage/state names that map 1:1 to Empire's pipeline stages (even if renamed)
    - Event prefix lists that match Empire's exact event namespace (even if renamed)
    - Node ID lists that match Empire's exact node set (even if renamed)
    If the constants are just Empire values with generic names, FAIL.

24. **Second-product integration test** — this is the definitive test. Create (or verify existence of) a minimal contract bundle with:
    - 2 flows (NOT matching Empire's flow count or names)
    - 3 agents (NOT matching Empire's agent roster)
    - 5 events (NOT matching Empire's event catalog)
    - A different entity schema (different fields, different types)
    Attempt to:
    - Load it through `LoadWorkflowContractBundle()`
    - Build a `FlowTree` from it
    - Derive routing from it
    - Process one event through the handler engine
    If any of these steps fail, panic, or require Empire-specific code to run, FAIL with details.

### Contract source authority audit

25. **Single source of truth enforced** — the ONLY authoritative contract/spec YAML is `docs/specs/mas-platform/`. Verify no Go code references any other YAML location as a contract source:
    ```
    # Find all Go code that references contract/spec YAML paths
    rg "(contracts/event-catalog|contracts/system-nodes|contracts/agent-tools|contracts/guard-action|contracts/workflow-schema|contracts/tool-schemas|contracts/verification-gates|contracts/prompt-variables|contracts/agent-config|contracts/upgrade-actions|contracts/legacy-contracts|contracts/platform|docs/toreview|docs/architecture/salvage)" --type go
    ```
    Every match is FAIL. All contract loading must point to `docs/specs/mas-platform/` or to a configurable path that defaults to it.

26. **Schema registry generated from MAS spec, not legacy catalog** — verify:
    - Read `internal/runtime/contracts/schema_registry_generated.go` header
    - If it says `Source: contracts/event-catalog.yaml`, FAIL
    - It must reference the MAS spec events (e.g., `docs/specs/mas-platform/empire/contracts/runtime/events.yaml` or the resolved bundle)
    - Count events in the generated registry vs events in the MAS spec. If the count matches the legacy 176-event catalog instead of the MAS 195-event catalog, FAIL

27. **Transitional YAML artifacts deleted** — verify these are gone:
    - `contracts/*.yaml` symlinks — run: `find contracts/ -type l` — every symlink is FAIL
    - `contracts/legacy-contracts/` directory — if it still exists, FAIL (should be moved to `internal/empire/contracts/` or deleted)
    - `contracts/platform/platform-spec.yaml` — redundant copy, FAIL if exists
    - `docs/toreview/mas-platform/` — stale review copy, FAIL if exists
    - `docs/architecture/salvage/*/contracts/` — dead compliance artifacts, FAIL if exists

28. **Contract loader is path-agnostic** — read the contract loader entry point (`LoadWorkflowContractBundle` or equivalent):
    - It must accept a path parameter, not hardcode any specific directory
    - Default path (if any) must point to `docs/specs/mas-platform/` not `contracts/`
    - Verify: `rg "contracts/" --type go internal/runtime/contracts/workflow_contracts.go` — every hardcoded `contracts/` path is FAIL
    - The only acceptable hardcoded path in the entire codebase should be in `cmd/mas/main.go` or `cmd/empire/main.go` as a CLI default flag value

29. **Prompt loader decoupled from contracts/** — verify:
    - `internal/promptcontracts/` and `internal/templateops/` do not hardcode `contracts/prompts/` paths
    - Prompt discovery uses the same configurable root as the contract loader
    - Run: `rg "contracts/prompts" --type go` — every match is FAIL unless it's a CLI default flag

### Hollow implementation checks

- Check every typed struct added by CP1-B: is it actually used in the handler engine, or does the engine still fall back to map[string]any walking?
- Check `DeclarativeNode.HandleEvent`: does it actually call the 10-step engine, or does it delegate to the old coordinator path?
- Check `ExecuteHandlerSteps`: does it implement all 10 steps, or does it bail out / no-op on some? For each step:
  - Step 1 (guard): does it evaluate the `GuardSpec`, or skip/hardcode?
  - Step 2 (accumulate): does it use `AccumulateSpec`, or fall back to untyped state?
  - Step 3 (compute): does it use `ComputeSpec`, or no-op?
  - Step 4 (fan_out): does it use `FanOutSpec`, or skip?
  - Step 5 (on_complete): does it evaluate completion, or bail?
  - Step 6 (advances_to): does it read from the handler struct?
  - Step 7 (sets_gate): does it use `GateSpec`?
  - Step 8 (data_accumulation): does it use typed `WorkflowDataAccumulation`?
  - Step 9 (emits): does it use `EventEmission`?
  - Step 10 (rules): does it iterate `[]HandlerRuleEntry` with CEL evaluation?
- Check routing derivation: is routing actually derived from contracts at boot, or does `SetRoutingTable` still exist as a fallback?
- Check that `typed_adapter_shims.go` or similar bridge files have been DELETED (they were temporary for the parallel agent era)
- Check that `FactoryPipelineCoordinator` has been renamed to `PipelineCoordinator` — if the old name persists, it suggests the coordinator still carries Empire-specific logic even if methods were renamed

### Report format

```
# Phase 2 Audit Report

Date: {date}
Build: PASS/FAIL
Tests: PASS/FAIL (N passed, N failed, N skipped)

## Exit Criteria (14 items)

| # | Criterion | Result | Evidence |
|---|-----------|--------|----------|
| 1 | go build passes | PASS/FAIL | {output} |
...

## Typed Model Completeness (4 items)

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 15 | map[string]any elimination | PASS/FAIL | {N remaining} |
...

## Structural Audit (6 items)

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 19 | Handler engine data flow | PASS/FAIL | |
| 20 | Routing derived not configured | PASS/FAIL | |
| 21 | Policy hierarchical resolution | PASS/FAIL | |
| 22 | FlowTree structure | PASS/FAIL | |
| 23 | No semantic coupling via constants | PASS/FAIL | |
| 24 | Second-product integration test | PASS/FAIL | |

## Hollow Implementation Checks

{findings}

## Blocking Findings

{list of FAIL items}
```

---

## Phase 3 Auditor: Platform Completion

You are the Phase 3 auditor. The implementer claims Phase 3 (platform completion — the missing 59% of spec requirements) is complete. Your job is to verify every exit criterion mechanically.

### Exit criteria to verify

1. `go build ./...` passes — run it, report result
2. `go test ./... -count=1` passes — run it, report result
3. All 15 boot steps implemented — for each step, verify:

   | Step | Check |
   |------|-------|
   | 1. load_platform_spec | Function exists, reads platform-spec.yaml, verifies version field |
   | 2. walk_flow_tree | Recursive loader, respects max depth 99 |
   | 3. construct_paths | Hierarchical paths: `{flow_instance_path}/{local_name}` |
   | 4. register_templates | `mode: template` flows registered but not instantiated |
   | 5. build_registries | Nodes, agents, events, tools, policy all registered with hierarchical flow paths |
   | 6. resolve_subscriptions | Local (no /) vs absolute (/) vs wildcards (*, **) all handled |
   | 7. validate_pins | Required input pins wired, no write conflicts — verify function exists and is called at boot |
   | 8. validate_required_agents | All flow `required_agents` fulfilled — verify function exists and is called |
   | 9. validate_tools | All `tools_tier2` exist in tool registry — verify check exists |
   | 10. validate_permissions | Agents have sufficient permissions for tools — verify check exists |
   | 11. validate_platform_version | Root `platform_version` includes running version — verify check exists |
   | 12. initialize_state_stores | DDL derived from entity_schema + state_schema — verify DDL generation runs |
   | 13. start_system_nodes | Nodes subscribe to declared events |
   | 14. start_agents | Agent subscriptions active |
   | 15. ready | Boot summary logged |

   For each: search for the implementation. If it doesn't exist, FAIL. If it exists but is a no-op or TODO stub, FAIL with "hollow implementation."

4. All 11 boot validation checks pass — verify each check exists AND is wired into the boot sequence (not just defined but never called):

   | Check | Verification |
   |-------|-------------|
   | 1. YAML parse | Boot aborts on parse error |
   | 2. package.yaml structure | Flows have nodes.yaml, events.yaml, schema.yaml |
   | 3. Pin wiring | Required input pins connected |
   | 4. required_agents | All roles fulfilled |
   | 5. Write conflicts | No two flows write same entity field |
   | 6. Namespace collision | No event name collisions after substitution |
   | 7. tools_tier2 | All tools exist in registry |
   | 8. Permissions | Agents have required permissions |
   | 9. emit_events schemas | All emit events have schemas |
   | 10. entity_schema coverage | All data_accumulation targets exist in schema |
   | 11. required_agents post-substitution | Fulfilled after namespace expansion |

5. All 11 runtime enforcement rules active — for each rule, verify there is enforcement code that runs during execution (not just at boot):

   | Rule | Verification |
   |------|-------------|
   | 1. Tool schema validation | Tool calls validated before execution |
   | 2. Tool permission check | Agent permissions checked before tool execution |
   | 3. Message scope check | Message delivery checked against scope permission |
   | 4. Transition path check | State transitions follow declared `advances_to` |
   | 5. Guard evaluation | Guards evaluated before advancement |
   | 6. Event payload validation | Events validated against schema before publish |
   | 7. Accumulation idempotency | Duplicate events don't double-count |
   | 8. Permission source | Read from agent.permissions, not hardcoded |
   | 9. Scan mode source | Read from policy.scan_modes, not hardcoded |
   | 10. Manager fallback source | Read from agent.manager_fallback, not hardcoded |
   | 11. Workspace class source | Read from agent.workspace_class, not hardcoded |

6. Entity persistence tools exist — verify all 4:
   - `get_entity` — function exists, reads entity by ID, respects permissions
   - `save_entity_field` — function exists, validates field against entity_schema
   - `search_entities` — function exists, queries by stage/fields/metadata
   - `query_metrics` — function exists, returns aggregated metrics
   - `executor_sql.go` deleted from generic packages (raw SQL access removed)

7. DDL generation works — verify:
   - Function exists that reads `entity_schema` from package.yaml
   - Maps field types to DDL types (text→VARCHAR, etc.)
   - Generates CREATE TABLE statement
   - Called at boot step 12

8. Dynamic flow instance lifecycle complete (11 steps) — verify each:

   | Step | Check |
   |------|-------|
   | 1. create_flow_instance tool | Handler can call it |
   | 2. Template validation | Validates template exists AND mode == template |
   | 3. Instance uniqueness | Checks instance_id unique within template scope |
   | 4. Load template | Loads flow template contracts |
   | 5. Construct paths | `{template_id}/{instance_id}/{local_name}` |
   | 6. Register all | Nodes AND agents AND events registered |
   | 7. Wildcard expansion | Wildcard subscriptions expanded for new instance |
   | 8. Local subscriptions | Resolved for instance |
   | 9. Entity record | Created at initial_state |
   | 10. Start all | Both nodes AND agents started |
   | 11. Auto-emit | `auto_emit_on_create` field emitted |

9. URI addressing implemented — verify all three formats:
   - Local (no /): `scoring.requested` → current flow instance
   - Absolute (with /): `scoring/entity.shortlisted` → specific flow instance
   - Full URI: `empire://scoring/entity.shortlisted` → multi-root
   - Wildcards: `*/event` (direct children), `**/event` (any depth)

10. Permissions model complete — verify:
    - All 13 permissions defined
    - Enforcement code checks permissions before tool execution
    - Permission bundles expand correctly
    - Message scope hierarchy enforced (all > domain > peers)

11. Error model implemented — verify:
    - Handler retry with exponential backoff (1s, 2s, 4s, max 3)
    - Dead letter on max retries
    - Chain depth limit (50) with counter on events
    - Agent session retry (max 1) then dead letter

12. Timer model complete — verify all fields:
    - id, event, delay, recurring, start_on, cancel_on
    - Crash recovery: persisted timers checked on restart

13. Emit tool auto-generation — verify:
    - `emit_{event_name}` tools generated from agent's `emit_events`
    - Payload validated against events.yaml schema
    - Not listed in `tools_tier2`
    - Universal tools (agent_message, mailbox_send) auto-granted

14. Prompt templating — verify:
    - `{{variable}}` substituted from policy.yaml
    - Variables not in policy → left as-is

15. Namespace substitution — verify:
    - `namespace_prefix` substituted at boot
    - Boot check #6 (no collisions) works

### The second-product test

This is the ultimate completeness check. Mentally (or actually) attempt:
- Can a second product with different flows, agents, events, and entities boot on this platform?
- Does booting it require editing ANY file under `internal/runtime/`, `internal/events/`, `internal/store/`, `internal/config/`, or `internal/commgraph/`?
- If yes, each file that requires editing is a FAIL.

Run:
```
rg -i "empire|vertical|opco|factory|holding|hetzner|whatsapp|founder.?mode|scan.?campaign|mailbox" --type go internal/runtime/ internal/events/ internal/store/ internal/config/ internal/commgraph/ -l
```
Exclude explicitly product-owned subdirectories. Every remaining match is a FAIL.

### Hollow implementation checks

For EVERY boot validation check and runtime enforcement rule:
- Verify the check function is actually CALLED during boot/execution
- Trace the call chain from main() or boot entry point to the check function
- If the function exists but is never called, FAIL with "dead code — defined but never invoked"

For entity persistence tools:
- Verify they are registered in the tool registry
- Verify agents can actually invoke them (not just defined as functions)

For DDL generation:
- Verify it actually runs at boot, not just defined
- Verify it creates real tables, not just logs

### Report format

```
# Phase 3 Audit Report

Date: {date}
Build: PASS/FAIL
Tests: PASS/FAIL (N passed, N failed, N skipped)

## Boot Sequence (15 steps)

| Step | Name | Result | Evidence |
|------|------|--------|----------|
| 1 | load_platform_spec | PASS/FAIL | {file:line or "NOT FOUND"} |
...

## Boot Validation Checks (11 checks)

| # | Check | Defined | Called at boot | Result |
|---|-------|---------|----------------|--------|
| 1 | YAML parse | YES/NO | YES/NO | PASS/FAIL |
...

## Runtime Enforcement Rules (11 rules)

| # | Rule | Defined | Called at runtime | Result |
|---|------|---------|-------------------|--------|
| 1 | Tool schema validation | YES/NO | YES/NO | PASS/FAIL |
...

## Platform Completion Features

| Feature | Result | Evidence |
|---------|--------|----------|
| Entity persistence tools (4) | PASS/FAIL | |
| DDL generation | PASS/FAIL | |
| Flow instance lifecycle (11 steps) | PASS/FAIL | |
| URI addressing (3 formats + wildcards) | PASS/FAIL | |
| Permissions (13) | PASS/FAIL | |
| Error model | PASS/FAIL | |
| Timer model | PASS/FAIL | |
| Emit tool auto-gen | PASS/FAIL | |
| Prompt templating | PASS/FAIL | |
| Namespace substitution | PASS/FAIL | |

## Second-Product Test

Empire references in generic packages: {count}
Files requiring edits for second product: {list}
Result: PASS/FAIL

## Hollow Implementation Checks

{findings}

## Blocking Findings

{prioritized list of FAIL items}
```

---

## Phase 8 Auditor: Genericity Burn-Down

You are the Phase 8 auditor. The implementer claims Phase 8 (repo-wide genericity burn-down) is complete. Your job is to verify that generic code no longer embeds Empire taxonomy or scope primitives.

### Exit criteria to verify

1. `go build ./...` passes — run it, report result
2. `go test ./... -count=1` passes — run it, report result
3. Generic event structs no longer expose Empire vocabulary — verify:
   - `internal/events/types.go` does NOT define `VerticalID`
   - No Empire scope helpers in events package
4. No Empire namespace table in generic bus code — verify:
   ```
   rg "FactoryEventPrefixes|isFactoryEvent|resolveOpCoRecipients|factory_|opco_" --type go internal/runtime/bus/
   ```
   Every match is FAIL.
5. Generic runtime does not require Empire node IDs to boot — verify:
   - `workflow_nodes_runtime.go` uses registration API, not hardcoded node ID map
   - No `case "scoring-node"` or similar in generic code
6. No product-domain package on generic runtime critical path — verify:
   ```
   rg "internal/factory|internal/empire" --type go internal/runtime/ internal/events/ internal/store/ internal/config/
   ```
   Every import of product-owned code from generic code is FAIL.
7. Generic production files no longer contain Empire literals — run the comprehensive sweep:
   ```
   rg -i "empire|vertical|opco|factory|holding|hetzner|whatsapp|founder.?mode|scan.?campaign" --type go internal/runtime/ internal/events/ internal/store/ internal/config/ internal/commgraph/ -l
   ```
   Exclude:
   - `internal/runtime/pipeline/empire/`
   - `internal/runtime/productpolicy/empire/`
   - `internal/commgraph/empire/`
   - Any explicitly product-owned subdirectory

   Every remaining match is FAIL. List each file and the specific matches.

8. Second-product thought experiment — can a second product register its own:
   - Flows (via package.yaml) — without editing generic code?
   - Agents (via agents.yaml) — without editing generic code?
   - Events (via events.yaml) — without editing generic code?
   - Nodes (via registration API) — without editing generic code?
   - Config (via extension mechanism) — without editing generic config package?

   For each: trace the registration path. If it requires editing a generic file, FAIL.

9. Remaining Empire references are intentional and documented — for any Empire reference found in step 7 that was classified as "compatibility-only quarantine" rather than "blocker":
   - Is it documented?
   - Is it behind an explicit product composition boundary?
   - Would removing it break Empire without affecting the generic platform?

### Report format

```
# Phase 8 Audit Report

Date: {date}
Build: PASS/FAIL
Tests: PASS/FAIL

## Genericity Sweep

| Package | Empire refs | Files | Result |
|---------|-----------|-------|--------|
| runtime/pipeline/ (excl empire/) | {count} | {files} | PASS/FAIL |
| runtime/bus/ | {count} | {files} | PASS/FAIL |
| runtime/manager/ | {count} | {files} | PASS/FAIL |
| runtime/tools/ | {count} | {files} | PASS/FAIL |
| runtime/contracts/ | {count} | {files} | PASS/FAIL |
| runtime/workspace/ | {count} | {files} | PASS/FAIL |
| runtime/agents/ | {count} | {files} | PASS/FAIL |
| runtime/mcp/ | {count} | {files} | PASS/FAIL |
| events/ | {count} | {files} | PASS/FAIL |
| store/ | {count} | {files} | PASS/FAIL |
| config/ | {count} | {files} | PASS/FAIL |
| commgraph/ (excl empire/) | {count} | {files} | PASS/FAIL |

Total Empire references in generic code: {count}

## Registration Path Verification

| Registration | Generic-only? | Evidence |
|-------------|--------------|----------|
| Flows | PASS/FAIL | |
| Agents | PASS/FAIL | |
| Events | PASS/FAIL | |
| Nodes | PASS/FAIL | |
| Config | PASS/FAIL | |

## Blocking Findings

{prioritized list}
```
