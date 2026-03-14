# MAS Platform v1.1.0 — Post-Review Gap Inventory

**Date:** 2026-03-14 (updated 2026-03-15)
**Source:** Two independent review rounds, verified against live codebase
**Scope:** All behavioral gaps blocking a non-Empire product from running on the MAS runtime
**Status:** G-06 execution order sent to spec writer; all others actionable

---

## Gap Classification

- **P0 (Blocks boot):** System cannot start for a non-Empire product
- **P1 (Blocks correctness):** System boots but produces wrong behavior
- **P2 (Spec violation):** System works but violates declared spec contract
- **P3 (Hygiene):** Residual Empire coupling, broken CI, dead code

---

## P0: Blocks Boot

### G-01: Generic bootstrap path does not exist

**Severity:** P0
**Files:** `cmd/mas/main.go:35`
**Evidence:** `defaultContractsPath = "docs/specs/mas-platform/tests/generic-runtime/contracts"` — this directory does not exist. `go run ./cmd/mas -self-check=false -store=inmemory` fails before boot.

**Fix:** Create the directory with a minimal `package.yaml`, or change the default to point to an existing test contract bundle (e.g., `tests/test-guard-pass/contracts`). Alternatively, make `-contracts` flag required when no default is found.

**Estimated scope:** ~10 lines + 1 minimal contract directory

---

### G-02: Tool schemas loaded from repo filesystem, not runtime bundle

**Severity:** P0 (for alternate products)
**Files:** `internal/runtime/tools/contracts.go:49-51`, `contracts.go:129-135`
**Evidence:** `LoadContractSchemas()` calls `runtimecontracts.LoadWorkflowContractBundle(repoRoot())` where `repoRoot()` uses `runtime.Caller(0)` to find the Go source tree. This means tool schemas always come from the developer's repo checkout, not from the runtime bundle passed at boot.

**Impact:** A product with different tool schemas (different input_schema for `agent_message`, custom tools) would get the wrong validation. The executor already has `workflowSource` (semanticview.Source) which provides `ToolEntries()` — the same data.

**Fix:** Change `LoadContractSchemas()` to accept a `semanticview.Source` parameter (or use the executor's existing `workflowSource`). Same pattern as Tranche E for event schemas.

**Estimated scope:** ~30 lines changed

---

### G-03: CommGraph policy factory is nil in production

**Severity:** P0 (for policy-dependent features)
**Files:** `internal/commgraph/policy.go:12-16`, `registry.go:231-248`, `registry.go:325-350`
**Evidence:** `defaultPolicyFactory` is only set via `SetDefaultPolicyFactory()` in test files (`setup_test.go`, `authorization_matrix_test.go`). In production, it's nil. The registry also loads producer data and message authority from `repoRoot()` via sync.Once — same filesystem coupling as G-02.

**Impact:** CommGraph `ProducerRoles()` and `ProducerEventsForRole()` always reflect whatever contract bundle is at the repo root. A different product's agent roles and emit permissions won't be recognized.

**Fix:** Wire `SetDefaultPolicyFactory` from `NewRuntime()` using the booted contract bundle. Change `contractProducerData()` and `loadMessageAuthorityRegistry()` to accept a Source parameter.

**Estimated scope:** ~40 lines changed across commgraph + runtime.go

---

## P1: Blocks Correctness

### G-04: Permission enforcement not implemented

**Severity:** P1
**Files:** `internal/runtime/tools/authorizer.go:84-112`, `internal/runtime/contracts/workflow_contracts.go:1905-1921`, `cmd/mas/main.go:400`
**Evidence:**
- `AgentRegistryEntry` struct has no `permissions` or `permissions_bundle` field
- `classifyToolAuthorization()` has no permission-checking tier — it goes universal → emit_allowed → actor_config → default_allow
- Boot step 10 says "permission validation is outside Tranche A scope"
- Spec requires 13 permissions (`agent_fire`, `agent_hire`, `approve_spend`, `create_flow_instance`, etc.) enforced at tool execution time (platform-spec.yaml:331-360, 692)

**Impact:** Any agent can call any tool. An agent without `agent_fire` permission can fire agents. An agent without `create_flow_instance` permission can create flow instances.

**Fix:**
1. Add `Permissions []string` field to `AgentRegistryEntry`
2. Load permissions into agent config during `buildFlowAgentConfig`
3. Add a `toolAuthorizationPermission` tier to the authorizer between universal and emit_allowed
4. Map tool names to required permissions (e.g., `agent_fire` tool → `agent_fire` permission)
5. Enable boot step 10 validation

**Estimated scope:** ~120 lines across contracts, authorizer, flow_activation, main.go

---

### G-05: instance_id_from not evaluated as expression

**Severity:** P1
**Files:** `internal/runtime/pipeline/workflow_instance_activation.go:45-48`, `internal/runtime/masflowtest/catalog_runner_test.go:2086-2088`
**Evidence:**
- Production code: `plan.InstanceIDFrom` is used as a literal string
- Test harness: `catalogResolveString(handler.Action.InstanceIDFrom, payload, entity)` evaluates dotted paths like `payload.vertical_id`
- Contract expects `instance_id_from: payload.entity_id` to extract the value from the event payload

**Impact:** Flow instances always get a UUID instead of the semantically correct instance ID from the payload. This breaks instance identity, prevents idempotent re-creation, and causes orphaned state.

**Fix:** Use `workflowExpressionEvaluator` (already exists) or a simpler dotted-path resolver to evaluate `instance_id_from` against the trigger event payload.

**Estimated scope:** ~20 lines in workflow_instance_activation.go

---

### G-06: Five handler primitives parsed but never executed

**Severity:** P1 (but see nuance below)
**Files:** `internal/runtime/contracts/workflow_contracts.go:1861-1886`, `internal/runtime/engine/executor.go:19-49`
**Evidence:**
- Contract schema defines: `Query`, `Filter`, `Reduce`, `Count`, `Clear` as handler primitives
- Engine implements 13 steps: clear_gates, guard, accumulate, compute, fan_out, on_complete, rules, advances_to, sets_gate, data_writes, transform, emits, action
- No `stepQuery`, `stepFilter`, `stepReduce`, `stepCount`, `stepClear` functions exist
- Execution plan struct has no fields for these primitives

**Spec gap:** These 5 primitives are defined as handler fields (platform-spec.yaml:1375-1416) but are absent from the handler execution order dependency graph (lines 724-761). Spec writer is adding execution order placement. Confirmed V1.1 requirements.

**Likely execution order placement:**
- `Query`: before guard (pre-fetch entity data)
- `Filter`: after accumulate (prune items)
- `Reduce`: after filter (aggregate)
- `Count`: after reduce (counter increment)
- `Clear`: after action (state bucket reset, distinct from `clear_gates`)

**Blocked on:** Spec writer delivering updated execution order dependency graph.

**Estimated scope:** ~200 lines (5 new engine steps + execution plan fields)

---

### G-12: No retry or dead-letter infrastructure

**Severity:** P0 (operational safety)
**Files:** `internal/runtime/engine/executor.go`, `internal/runtime/pipeline/workflow_nodes_runtime.go`
**Evidence:** Handler execution failures are logged but not retried or persisted. No `dead_letter_events` table exists. A transient LLM timeout or database blip permanently loses the event.

**Fix:**
1. Add `RetryDispatcher` wrapper around handler execution with configurable retry count (default 3) and exponential backoff
2. Create `dead_letter_events` table (event_id, node_id, entity_id, error, attempts, created_at)
3. Add replay tooling or boot-time dead letter scan

**Estimated scope:** ~150 lines + DDL

---

### G-13: Panics in eventbus crash process

**Severity:** P0 (operational safety)
**Files:** `internal/runtime/bus/eventbus.go:80,225`
**Evidence:** Route table errors trigger `panic()` instead of returning errors. Any route misconfiguration crashes the entire runtime process.

**Fix:** Replace `panic()` calls with error returns. Callers already handle errors.

**Estimated scope:** ~10 lines

---

### G-14: Flow instance routes not persisted

**Severity:** P0 (operational safety)
**Files:** `internal/runtime/bus/routing_derivation.go`, `internal/runtime/manager/flow_activation.go`
**Evidence:** Flow instance routes exist only in memory. A crash between instance creation and recovery loses all routing state. Agents continue running but events can't reach them.

**Fix:** Add `flow_instance_routes` table populated during `AddFlowInstance()`. Recovery path reloads routes at boot.

**Estimated scope:** ~50 lines + DDL

---

### G-15: Event chain integrity boot check missing

**Severity:** P1
**Files:** `cmd/mas/main.go` (boot sequence), `internal/runtime/pipeline/workflow_contract_validation.go`
**Evidence:** Boot check #11 (event chain integrity) is not implemented. No code traces emit→subscribe chains to detect orphaned events (emitted but never consumed) or circular event paths.

**Fix:** Walk all node handler `emits` entries and verify each event type has at least one subscriber. Walk subscribe chains to detect cycles. Run at boot after route table is built.

**Estimated scope:** ~60 lines

---

### G-16: Bus-level payload validation missing

**Severity:** P1
**Files:** `internal/runtime/bus/eventbus_publish.go`
**Evidence:** Events published directly through the bus bypass schema validation. Only events emitted through the tool executor's `emit` path are validated against the schema registry. Direct `Publish()` calls (from engine actions, system events) skip validation entirely.

**Fix:** Add optional schema validation hook to `EventBus.Publish()` using the active event schema registry.

**Estimated scope:** ~40 lines

---

### G-17: create_flow_instance not exposed as agent-callable tool

**Severity:** P1
**Files:** `internal/runtime/tools/executor.go`, `internal/runtime/pipeline/workflow_nodes.go`
**Evidence:** `create_flow_instance` exists only as an engine action (handler primitive). Agents cannot programmatically create flow instances — they can only do so via events that trigger `action: create_flow_instance` handlers. Spec requires it as an agent-callable tool (platform-spec.yaml, permissions section lists `create_flow_instance` as a permission).

**Fix:** Add `create_flow_instance` to the tool executor with permission check (requires G-04). Input: flow_id, instance_id, initial payload. Delegates to existing activation machinery.

**Estimated scope:** ~80 lines

---

### G-18: Prompt schema guard never called at boot

**Severity:** P1
**Files:** `internal/runtime/contracts/prompt_schema_guard.go`, `cmd/mas/main.go`
**Evidence:** `ValidatePromptSchemaGuard()` exists and works (has tests) but is never invoked during the boot sequence. Prompt/schema mismatches are not caught until runtime.

**Fix:** Add call to `ValidatePromptSchemaGuard()` after contract bundle is loaded, before agent initialization.

**Estimated scope:** ~20 lines

---

### G-19: Accumulator timeout completion always returns false

**Severity:** P1
**Files:** `internal/runtime/engine/executor.go` (accumulate step)
**Evidence:** Accumulator timeout/deadline completion check is stubbed to always return `false`. An accumulator with `completion: timeout(5m)` never completes on timeout — it waits forever for explicit `all` or `count` completion.

**Fix:** Store accumulator start time, check deadline against current time in completion evaluator. Wire timer infrastructure to trigger re-evaluation on timeout.

**Estimated scope:** ~30 lines

---

### G-24: Flatten SQL migrations to contract-driven DDL

**Severity:** P1
**Files:** `internal/store/`, `internal/runtime/contracts/`
**Evidence:** DDL generation from contracts already exists (`GeneratePlatformTableDDLs`, `GenerateEntityTableDDLs`, `GenerateNodeStateTableDDLs`) but static migration files may coexist. The canonical path should be: contracts define schema → DDL generated at boot → applied to database. No incremental migration files.

**Fix:**
1. Ensure all tables are generated from contract bundle at boot
2. Remove any static migration files that duplicate contract-driven DDL
3. Add boot-time schema diff/validation (generated DDL vs. actual database schema)
4. Empire-specific tables (G-10) move to Empire product module's DDL generator

**Estimated scope:** ~80 lines + migration cleanup

---

## P2: Spec Violation

### G-07: Boot validation incomplete

**Severity:** P2
**Files:** `cmd/mas/main.go:400`, `internal/runtime/pipeline/workflow_contract_validation.go:402-403`
**Evidence:**
- Boot step 10 (permissions): "permission validation is outside Tranche A scope"
- Boot step 8 (required_agents): TODO comment — "root-level schema.yaml required_agents are not exposed on semanticview.Source today; this validation currently covers flow-scoped schemas"
- Spec requires both (platform-spec.yaml:940-946)

**Fix:** After G-04 lands (permissions), enable boot step 10. For required_agents, expose root-level schema entries through semanticview.Source and validate at boot.

**Estimated scope:** ~40 lines

---

### G-08: Hardcoded control-plane role restriction

**Severity:** P2
**Files:** `internal/runtime/tools/executor_system.go:14-15, 27-28, 70`
**Evidence:** System tools (nginx_reload, systemd_control, certbot_execute) check `actor.Role != "control-plane"` — hardcoded string. Should use permission enforcement from G-04 once implemented.

**Fix:** Replace hardcoded role check with permission check (e.g., `system_admin` permission). Defer until G-04 lands.

**Estimated scope:** ~10 lines (after G-04)

---

### G-09: Scan-mode compat shims and budget throttle

**Severity:** P2
**Files:** `internal/runtime/tools/emit_contract_compat.go:9-20`, `internal/runtime/agents/agent_llm.go:226,587,591`, `internal/runtime/manager/receipts.go:110`
**Evidence:**
- `NormalizeScanModeCompat`: maps "scan"/"discovery" → "scan" (Empire vocabulary)
- `receipts.go:110`: `strings.HasPrefix(eventType, "scan.")` — hardcoded budget throttle for scan events
- Spec says these should be contract/policy-driven

**Fix:**
- Move scan-mode normalization to Empire product module
- Replace `scan.` prefix check with policy-driven `throttle_suppress_prefixes` field
- A new product would never trigger these, so this is P2 not P1

**Estimated scope:** ~30 lines

---

## P3: Hygiene

### G-10: Empire tables in persistence layer

**Severity:** P3
**Files:** `internal/store/template_routing_store.go:12-30`, `internal/store/postgres_store_additional_test.go`, `internal/store/postgres_smoke_test.go`
**Evidence:** `org_templates` table with `bootstrap_routes`, `seeded_routes` — Empire-specific org template concept. Referenced in store tests.

**Note:** The DDL generation system (`store.GeneratePlatformTableDDLs`, `GenerateEntityTableDDLs`, `GenerateNodeStateTableDDLs`) is contract-driven and clean. The `org_templates` usage appears to be a legacy store that isn't part of the contract-driven path. The static migration files referenced by the review (`001_initial.sql`, `004_scan_campaigns.sql`) do not exist in the current tree — they may have been deleted already or live elsewhere.

**Fix:** Move `template_routing_store.go` to an Empire-specific store package, or remove if unused by the current runtime path.

**Estimated scope:** ~50 lines moved

---

### G-11: CI/test coverage integrity

**Severity:** P3
**Files:** `.github/workflows/ci.yml`, `Makefile`, `scripts/verify_wiring.py`, `TEST-CATALOG.md`
**Evidence per review:**
- Coverage gates fail against real package layout
- `verify_wiring.py` verifies nothing (referenced test is gone)
- Tier 9 and 10 are empty
- Tier 11 fixtures are mostly not executed

**Fix:** Audit and fix CI pipeline, remove dead verification scripts, populate or remove empty tiers.

**Estimated scope:** Variable — CI infrastructure work, not runtime code

---

### G-20: workflow_contracts.go is 4419 lines

**Severity:** P3
**Files:** `internal/runtime/contracts/workflow_contracts.go`
**Evidence:** Single file contains DTOs, loader, tree builder, and validation logic. Difficult to navigate, review, and test in isolation.

**Fix:** Decompose into: `contract_types.go` (DTOs), `contract_loader.go`, `contract_tree.go`, `contract_validation.go`.

**Estimated scope:** Refactor — no behavioral change

---

### G-21: pipeline/ package too large

**Severity:** P3
**Files:** `internal/runtime/pipeline/`
**Evidence:** Package contains orchestration, workflow model, state management, and node implementations in a flat structure.

**Fix:** Split into sub-packages: `orchestration/`, `workflow/`, `state/`. Or at minimum group files with clear naming prefixes.

**Estimated scope:** Refactor — no behavioral change

---

### G-22: String-based error matching

**Severity:** P3
**Files:** Various across `internal/runtime/`
**Evidence:** Error handling uses `strings.Contains(err.Error(), ...)` instead of sentinel errors or `errors.Is()`.

**Fix:** Define sentinel errors, use `errors.Is()` / `errors.As()` throughout.

**Estimated scope:** Refactor — no behavioral change

---

### G-23: CI test gate minimums too low

**Severity:** P3
**Files:** `.github/workflows/ci.yml`, `Makefile`
**Evidence:** Total test minimum is set to 25 but actual test count is much higher. Per-tier minimums for Tiers 6-8 are not asserted.

**Fix:** Raise total minimum to match actual count. Add per-tier minimum assertions.

**Estimated scope:** CI config changes

---

## Summary

| ID | Severity | Description | Scope | Depends on |
|----|----------|-------------|-------|------------|
| G-01 | P0 | Generic bootstrap path missing | ~10 lines | — |
| G-02 | P0 | Tool schemas from repo not bundle | ~30 lines | — |
| G-03 | P0 | CommGraph policy nil in production | ~40 lines | — |
| G-12 | P0 | No retry / dead-letter infrastructure | ~150 lines | — |
| G-13 | P0 | Panics in eventbus crash process | ~10 lines | — |
| G-14 | P0 | Flow instance routes not persisted | ~50 lines | — |
| G-04 | P1 | Permission enforcement not implemented | ~120 lines | — |
| G-05 | P1 | instance_id_from not evaluated | ~20 lines | — |
| G-06 | P1 | 5 handler primitives not executed | ~200 lines | Spec writer |
| G-15 | P1 | Event chain integrity boot check missing | ~60 lines | — |
| G-16 | P1 | Bus-level payload validation missing | ~40 lines | — |
| G-17 | P1 | create_flow_instance not agent-callable | ~80 lines | G-04 |
| G-18 | P1 | Prompt schema guard never called at boot | ~20 lines | — |
| G-19 | P1 | Accumulator timeout stubbed | ~30 lines | — |
| G-24 | P1 | Flatten SQL migrations to contract-driven DDL | ~80 lines | G-10 |
| G-07 | P2 | Boot validation incomplete | ~40 lines | G-04 |
| G-08 | P2 | Hardcoded control-plane role | ~10 lines | G-04 |
| G-09 | P2 | Scan-mode compat shims | ~30 lines | — |
| G-10 | P3 | Empire tables in store | ~50 lines | — |
| G-11 | P3 | CI/test coverage gaps | variable | — |
| G-20 | P3 | workflow_contracts.go 4419 lines | refactor | — |
| G-21 | P3 | pipeline/ package too large | refactor | — |
| G-22 | P3 | String-based error matching | refactor | — |
| G-23 | P3 | CI test gate minimums too low | CI config | G-11 |

**Totals:** 6× P0, 9× P1, 3× P2, 5× P3 = 23 gaps

---

## Phased Execution Plan

### Phase 5: Unblock Boot + Operational Safety
**Scope:** 6 gaps, ~290 lines
**Goal:** `cmd/mas` boots with any contract bundle; no silent failure modes

| Gap | Work | Est |
|-----|------|-----|
| G-01 | Create minimal generic contract dir or make `-contracts` required | ~10 lines |
| G-02 | `LoadContractSchemas()` accepts `semanticview.Source` instead of `repoRoot()` | ~30 lines |
| G-03 | Wire `SetDefaultPolicyFactory` from `NewRuntime()` using booted bundle | ~40 lines |
| G-13 | Replace `panic()` in eventbus.go:80,225 with error returns | ~10 lines |
| G-14 | `flow_instance_routes` table + populate in `AddFlowInstance()` + recovery reload | ~50 lines |
| G-12 | `RetryDispatcher` wrapper + `dead_letter_events` table + replay | ~150 lines |

**Verification:** `go run ./cmd/mas -contracts=<generic-bundle> -store=inmemory -self-check=true` boots clean. Inject handler failure → verify retry then dead-letter.

---

### Phase 6: Permission Enforcement + Expression Evaluation
**Scope:** 2 gaps, ~140 lines
**Goal:** Tool authorization is contract-driven; flow instance IDs are semantically correct

| Gap | Work | Est |
|-----|------|-----|
| G-04 | `Permissions []string` on agent config, permission tier in authorizer, tool→permission map, boot step 10 | ~120 lines |
| G-05 | Evaluate `instance_id_from` via expression evaluator against trigger payload | ~20 lines |

**Verification:** Agent without `agent_fire` permission → tool call rejected. `instance_id_from: payload.entity_id` → instance ID matches payload value.

---

### Phase 7: Boot Integrity + Runtime Completeness
**Scope:** 6 gaps, ~310 lines (G-06 blocked on spec writer)
**Goal:** All boot checks pass; all handler primitives execute; accumulator timeouts work

| Gap | Work | Est |
|-----|------|-----|
| G-18 | Call `ValidatePromptSchemaGuard()` in boot sequence | ~20 lines |
| G-15 | Event chain integrity check — trace emit→subscribe, detect orphans/cycles | ~60 lines |
| G-16 | Schema validation hook on `EventBus.Publish()` | ~40 lines |
| G-19 | Accumulator timeout — store start time, check deadline, wire timer | ~30 lines |
| G-17 | `create_flow_instance` tool with permission check (depends G-04) | ~80 lines |
| G-06 | 5 engine steps: query, filter, reduce, count, clear (blocked on spec writer) | ~200 lines |

**Note:** G-06 can slip to Phase 8 if spec writer hasn't delivered. All other items are unblocked.

**Verification:** Boot with missing prompt → validation error. Orphaned emit → boot warning. Direct bus publish with bad payload → rejected. Accumulator with `timeout(1s)` → completes after 1s. Agent calls `create_flow_instance` tool → flow instance created.

---

### Phase 8: Spec Compliance + Migration Cleanup
**Scope:** 5 gaps, ~210 lines
**Goal:** No spec violations; single DDL generation path

| Gap | Work | Est |
|-----|------|-----|
| G-07 | Enable boot steps 8 (required_agents) and 10 (permissions) | ~40 lines |
| G-08 | Replace hardcoded `control-plane` role with permission check | ~10 lines |
| G-09 | Move scan-mode normalization to Empire module; policy-driven throttle | ~30 lines |
| G-10 | Move `template_routing_store.go` to Empire-specific package | ~50 lines |
| G-24 | Remove static migrations; contract-driven DDL is the only path; boot-time schema diff | ~80 lines |

**Verification:** Boot with product missing required agent → error. System tool without `system_admin` → rejected. No Empire vocabulary in platform packages. `\dt` shows only contract-generated tables.

---

### Phase 9: Code Quality + CI Hardening
**Scope:** 5 gaps, variable
**Goal:** Clean codebase; CI catches regressions

| Gap | Work | Est |
|-----|------|-----|
| G-20 | Split `workflow_contracts.go` → types, loader, tree, validation | refactor |
| G-21 | Split `pipeline/` → orchestration, workflow, state | refactor |
| G-22 | Sentinel errors replacing string matching | refactor |
| G-11 | Fix CI pipeline, remove dead scripts, populate tiers 9-10 | variable |
| G-23 | Raise test gate minimums, add per-tier assertions | CI config |

**Verification:** All tests green. CI gates enforce actual coverage. No `strings.Contains(err.Error()` in runtime packages.

---

## Dependency Graph

```
Phase 5 (boot + safety)
  └→ Phase 6 (permissions + expressions)
       ├→ Phase 7 (boot integrity + runtime completeness)
       │    └→ [G-06 blocked on spec writer]
       └→ Phase 8 (spec compliance + migrations)
            └→ Phase 9 (code quality + CI)
```

## Estimated Total

| Phase | Gaps | Lines | Blocked |
|-------|------|-------|---------|
| 5 | 6 | ~290 | — |
| 6 | 2 | ~140 | — |
| 7 | 6 | ~310 | G-06 on spec writer |
| 8 | 5 | ~210 | G-07, G-08 on Phase 6 |
| 9 | 5 | variable | — |
| **Total** | **23** | **~950+ lines** | |
