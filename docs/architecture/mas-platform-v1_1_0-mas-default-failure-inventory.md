# MAS Platform v1.1.0 — Post-Review Gap Inventory

**Date:** 2026-03-14
**Source:** Independent review findings, verified against live codebase
**Scope:** All behavioral gaps blocking a non-Empire product from running on the MAS runtime

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

**Nuance:** The spec's handler execution order (platform-spec.yaml:724-761) does NOT include these five primitives in the dependency graph. They appear in the field definitions section (lines 1375-1416) but not in the execution model. This suggests they may be aspirational/deferred rather than currently required.

**Recommended approach:** Determine whether these are V1.1 requirements or V1.2 aspirational. If V1.1:
- `Query`: pre-execution entity lookup, runs before guard
- `Filter`: predicate on accumulated items, runs after accumulate
- `Reduce`: aggregation on accumulated items, runs after filter
- `Count`: counter increment, runs after reduce
- `Clear`: state bucket reset, runs after action

If aspirational, remove from contract schema to prevent confusion.

**Estimated scope:** ~200 lines if implementing all 5, ~20 lines if removing from schema

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

## Summary

| ID | Severity | Description | Scope |
|----|----------|-------------|-------|
| G-01 | P0 | Generic bootstrap path missing | ~10 lines |
| G-02 | P0 | Tool schemas from repo not bundle | ~30 lines |
| G-03 | P0 | CommGraph policy nil in production | ~40 lines |
| G-04 | P1 | Permission enforcement not implemented | ~120 lines |
| G-05 | P1 | instance_id_from not evaluated | ~20 lines |
| G-06 | P1 | 5 handler primitives not executed | ~200 or ~20 lines |
| G-07 | P2 | Boot validation incomplete | ~40 lines |
| G-08 | P2 | Hardcoded control-plane role | ~10 lines (after G-04) |
| G-09 | P2 | Scan-mode compat shims | ~30 lines |
| G-10 | P3 | Empire tables in store | ~50 lines |
| G-11 | P3 | CI/test coverage gaps | variable |

## Recommended Execution Order

Phase 1 — **Unblock boot** (G-01, G-02, G-03): ~80 lines, makes `cmd/mas` bootable with any contract bundle
Phase 2 — **Correctness** (G-04, G-05): ~140 lines, permissions and flow instance identity
Phase 3 — **Spec compliance** (G-06 decision, G-07, G-08): ~50-250 lines depending on G-06 decision
Phase 4 — **Polish** (G-09, G-10, G-11): variable, can be incremental
