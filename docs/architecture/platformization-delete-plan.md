# Platformization: Keep / Delete / Rewrite Plan

**Date:** 2026-03-13
**Total codebase:** ~102,800 Go lines + ~18,000 TypeScript lines
**Goal:** Generic MAS platform that boots any product from contracts + WorkflowModule
**Exhaustive:** Every directory and file in the repo is classified below.

## Status Update

As of the current tree, this plan is **partially executed**.

### Completed
- [x] `internal/models/` contents deleted (directory now empty)
- [x] `internal/protocolheaders/` contents deleted (directory now empty)
- [x] `internal/factory/` remains empty
- [x] `internal/ops/` contents deleted (directory now empty)
- [x] `internal/specaudit/` contents deleted (directory now empty)
- [x] `cmd/empire/` contents deleted (directory now empty)
- [x] `internal/store/empire_compat.go` deleted
- [x] `internal/runtime/scanmode/` contents deleted (directory now empty)
- [x] `internal/runtime/corpusobs/` contents deleted (directory now empty)
- [x] `internal/commgraph/empire/` contents deleted (directory now empty)
- [x] `internal/runtime/tools/executor_emit_normalization.go` deleted
- [x] `internal/runtime/tools/executor_emit_guardrails.go` deleted
- [x] `internal/runtime/tools/executor_sql.go` deleted
- [x] `internal/runtime/manager/opco.go` deleted
- [x] `internal/runtime/bus/cycle_tracker.go` deleted
- [x] Most Empire orchestration files in `internal/runtime/pipeline/` deleted and replaced by a thin platform coordinator

### Still Open
- [ ] several cleanup/rewrite items in kept packages remain open (Sections 4.3 through 4.8)

---

## Executive Summary

| Action | Go Lines | Files |
|--------|----------|-------|
| DELETE | ~48,000 | ~220 |
| KEEP | ~46,000 | ~180 |
| CLEANUP | ~2,000 lines changed | ~15 files |
| NEW CODE | ~1,500 | 5-6 new files |
| NET RESULT | ~47,500 | ~185 |

| Action | TypeScript Lines | Dirs |
|--------|-----------------|------|
| DELETE | 0 | — |
| KEEP | ~18,000 | all 17 feature dirs |
| NEW | ~200 | 1 adapter file |
| UPDATE | ~200 | 2-3 API client files |

---

# PART 1: GO PACKAGES — COMPLETE INVENTORY

Every Go package in the repo is listed below with its verdict.

---

## 1. DELETE — Entire Packages (no salvage)

### 1.1 `internal/empire/` — 7,387 lines, 42 files — DELETE ALL
- Old extraction scaffolding (config/, factory/, hooks/, models/, payloads/, pipeline/, store/)
- Zero generic runtime imports from it
- Only consumer: `cmd/empire/main.go` (also being deleted)

### 1.2 `internal/models/` — 56 lines, 9 files — DELETE ALL
- Pure type alias re-exports (`type Agent = runtime.Agent`)
- No logic, no value

### 1.3 `internal/protocolheaders/` — 21 lines, 1 file — DELETE ALL
- `X-Empire-*` HTTP headers
- Product-specific protocol, not platform

### 1.4 `internal/factory/` — 0 lines, empty — DELETE
- Empty legacy directory

### 1.5 `internal/ops/` — 498 lines, 3 files — DELETE ALL
- Empire portfolio monitoring and diagnostics (73 Empire references)
- Product-specific operational logic

### 1.6 `internal/specaudit/` — 858 lines, 3 files — DELETE ALL
- Empire specification auditing and compliance checking (15 Empire references)
- Product-specific audit logic

### 1.7 `cmd/empire/` — 12,566 lines, 58 files — DELETE ALL
- Empire-specific CLI subcommands: telegram, verticals, pipeline, scan_shards, monitor, operator, template, etc.
- `cmd/empire/main.go` (994 lines): wires 14 Empire subsystems
- Replaced by `cmd/mas/main.go` (357 lines, already clean)

### 1.8 `internal/dashboard/*.go` — 15,913 lines, 55 files — DELETE ALL
- Go HTTP server backend (988 Empire references)
- Rewrite as thin API server (~300 lines) — see Section 4.1

### 1.9 `internal/store/empire_compat.go` — 122 lines — DELETE
- Forwarding shims to old Empire store interfaces

### 1.10 `internal/runtime/productpolicy/` — DELETE ALL
- Contains `empire/` subdir (product-specific policy)
- Product policy should live in the Empire WorkflowModule, not generic runtime

### 1.11 `internal/runtime/scanmode/` — 47 lines, 1 file — DELETE
- `NormalizeMode()`, `DefaultMode()`, `ExpectedScannerCount()`
- Empire discovery scan mode logic (saas_gap, saas_trend, local_services, corpus)

### 1.12 `internal/runtime/corpusobs/` — 131 lines, 1 file — DELETE
- Empire corpus observation/context tracking (4 Empire references)

### 1.13 `internal/commgraph/empire/` — 79 lines, 1 file — DELETE
- Empire-specific communication graph policy

---

## 2. DELETE — Individual Files Within Kept Packages

### 2.1 `internal/runtime/pipeline/` — DELETE these files (Empire orchestration)

| File | Lines | Reason |
|------|-------|--------|
| `coordinator.go` | 542 | Empire subsystem fields, domain structs, hardcoded event switches |
| `coordinator_discovery.go` | 923 | 100% discovery aggregation logic |
| `coordinator_scoring.go` | 916 | 100% scoring orchestration logic |
| `coordinator_scan.go` | 575 | Empire scan campaign assignment |
| `coordinator_subsystems.go` | 45 | ScanCoordinator/ScoringState/ValidationGate struct defs |
| `coordinator_validation.go` | 48 | Empire validation gate dispatch |
| `coordinator_projection.go` | 436 | Empire state bucket projection |
| `coordinator_projection_snapshot.go` | 42 | Empire snapshot helpers |
| `coordinator_state.go` | 198 | Empire state persistence (scan_accumulators, validation_pipelines) |
| `workflow_instance_projection.go` | 1,155 | 100% Empire scoring/validation/scan state buckets |
| `coordinator_workflow_projection.go` | 162 | Empire workflow projection |
| `coordinator_projection_expected_agents.go` | 15 | Empire agent expectation |
| `coordinator_runtime_support.go` | 28 | Empire runtime helpers |
| `payload_factory.go` | 305 | Empire-specific payload builders (BuildVertical*, BuildScan*) |
| `scan_campaign_compat.go` | 111 | Empire scan campaign compatibility |
| `coordinator_scan_compat.go` | 48 | Empire scan compat shims |
| `lifecycle_compat.go` | 189 | Empire lifecycle compatibility wrappers |
| `engine_builtin_compat.go` | 109 | Empire engine built-in compat |
| `scan_normalization.go` | 11 | Empire scan mode normalization |
| `workflow_compat_helpers.go` | 63 | Empire compatibility helpers |
| `workflow_hook_runtime.go` | 26 | Empire workflow hook runtime |
| `workflow_node_scan.go` | 78 | Empire scan node |
| `portfolio_node.go` | 58 | Empire portfolio node |
| `module_hooks.go` | ~200 | Wrong intermediate cut — Empire types in generic package |
| **Subtotal** | **~5,300** | |

### 2.2 `internal/runtime/pipeline/` — DELETE test files for deleted code

| File | Lines | Reason |
|------|-------|--------|
| All `*_test.go` files testing deleted code | ~3,000 est. | Tests for Empire orchestration |
| `platformization_skip_test.go` | 8 | Temporary skip marker |
| `short_skip_test.go` | 18 | Temporary skip marker |
| `generic_test_module.go` | 78 | Empire test module (if Empire-specific) |

### 2.3 `internal/runtime/tools/` — DELETE these files

| File | Lines | Reason |
|------|-------|--------|
| `executor_emit_normalization.go` | ~200 | Empire scan mode normalization, geography hardcoding |
| `executor_emit_guardrails.go` | ~250 | 10 hardcoded Empire role-event transition rules |
| `executor_sql.go` | 207 | Raw SQL access — spec says delete |

### 2.4 `internal/runtime/manager/` — DELETE these files

| File | Lines | Reason |
|------|-------|--------|
| `opco.go` | 144 | SpawnOpCo/TeardownOpCo — Empire operating company lifecycle |

### 2.5 `internal/runtime/bus/` — DELETE these files

| File | Lines | Reason |
|------|-------|--------|
| `cycle_tracker.go` | 397 | OpCoCycleTracker, hardcoded `opco_cto` escalation role |

### 2.6 `docs/specs/` — DELETE old spec archives

| Path | Reason |
|------|--------|
| `docs/specs/empireai-v2_6_0/` | Old spec version, already superseded |
| `docs/specs/empireai-v2_6_0.tar` | Archive of above |
| `docs/specs/mas-platform-v1.1.0-8.tar` | Old platform spec archive |
| `docs/specs/mas-platform-v1.1.0-14.tar` | Old platform spec archive |

---

## 3. KEEP — Complete Package Inventory

Every kept package is listed. Nothing is omitted.

### 3.1 `internal/runtime/engine/` — 3,784 lines, 16 files — KEEP ALL
- 12-step handler execution engine
- Zero product imports, fully generic
- Interfaces, executor, ordered steps, atomicity

### 3.2 `internal/runtime/pipeline/` — KEEP these files (~7,600 lines)

| File | Lines | Role |
|------|-------|------|
| `module.go` | 44 | WorkflowModule interface (cleanup — see 4.2) |
| `workflow.go` | 317 | WorkflowDefinition, generic workflow types |
| `workflow_nodes.go` | 419 | WorkflowNode, generic node types |
| `workflow_nodes_runtime.go` | 97 | Node runtime dispatch |
| `workflow_transition_engine.go` | 1,711 | State machine transitions (cleanup — see 4.3) |
| `workflow_instance_store.go` | 431 | workflow_instances CRUD — spec-aligned |
| `workflow_contract_validation.go` | 736 | Boot-time contract validation |
| `workflow_expression_evaluator.go` | 408 | Guard/expression evaluation |
| `workflow_expression_context_builder.go` | 78 | Expression context |
| `workflow_timer_lifecycle.go` | 330 | Timer primitives |
| `state_machine.go` | 25 | State machine core |
| `scheduler.go` | 235 | Timer scheduler |
| `runtime_support.go` | 344 | Generic runtime helpers |
| `runtime_interfaces.go` | 109 | Runtime interface definitions |
| `runtime_ids.go` | 9 | Runtime ID constants |
| `transitions.go` | 163 | Transition types |
| `persistence.go` | 67 | Generic persistence interface |
| `sharding.go` | 378 | ShardPlanner — generic |
| `shard_dispatcher.go` | 851 | Shard dispatch — generic |
| `engine_adapter.go` | 465 | Engine ↔ pipeline bridge |
| `engine_bridge.go` | 206 | Engine bridge helpers |
| `handler_engine_builtins.go` | 177 | Built-in handler steps |
| `handler_preview.go` | 163 | Handler preview/dry-run |
| `guard_action_registry.go` | 166 | Guard + action registries |
| `system_node_runner.go` | 264 | System node execution |
| `declarative_default_node.go` | 211 | Declarative node defaults |
| `declarative_workflow_node.go` | 49 | Declarative node interface |
| `background_workflow_node.go` | 71 | Background node |
| `pipeline_mode_resolution.go` | 71 | Mode resolution |
| `pipeline_helpers.go` | 72 | Generic helpers |
| `directive_parser.go` | 70 | Directive parsing |
| `recovery.go` | 82 | State recovery |

### 3.3 `internal/runtime/contracts/` — 9,469 lines, 13 files — KEEP (with cleanup)
- Contract loader, prompt resolver, schema registry
- Cleanup needed: remove Empire-default paths (see 4.4)

### 3.4 `internal/runtime/bus/` — ~2,325 lines, 13 files — KEEP (minus cycle_tracker.go)
- EventBus, routing, subscriptions, interceptors, transaction support, dead-letter

### 3.5 `internal/runtime/manager/` — ~2,559 lines, 9 files — KEEP (minus opco.go, with cleanup)
- Agent manager, bootstrap, lifecycle, receipts, flow activation
- Cleanup needed: remove hardcoded role functions (see 4.5)

### 3.6 `internal/runtime/tools/` — ~4,462 lines, 27 files — KEEP (minus deleted files, with cleanup)
- Tool executor, authorizer, handler registry, human tasks, agent tools
- Cleanup needed: emit_registry.go schemas, authorizer hardcoding (see 4.6)

### 3.7 `internal/runtime/workspace/` — 539 lines, 1 file — KEEP
- Workspace manager, generic lifecycle interface

### 3.8 `internal/runtime/agents/` — 634 lines, 2 files — KEEP (with cleanup)
- Agent LLM runtime, event handling, tool filtering
- Cleanup: remove OpCo-specific context (30 Empire refs, minor)

### 3.9 `internal/runtime/llm/` — 3,013 lines, 17 files — KEEP (with cleanup)
- Anthropic API runtime, CLI runtime, session management, budget guard, conversation persistence
- Cleanup: remove OpCo-aware session routing (28 Empire refs, minor)

### 3.10 `internal/runtime/flowmodel/` — 1,165 lines, 9 files — KEEP ALL (clean)
- Flow-level domain models, state definitions
- Zero Empire references

### 3.11 `internal/runtime/sessions/` — 1,335 lines, 7 files — KEEP ALL (clean)
- Session registry, session metadata
- Minimal Empire references

### 3.12 `internal/runtime/core/` — 747 lines, 12 files, 5 subdirs — KEEP ALL (clean)
- `core/identity/` — EntityID, NodeID, FlowID, ActionKey, GuardKey, WorkflowURI (189 lines)
- `core/values/` — Bucket state container, Context wrapper (332 lines)
- `core/state/` — Instance state, MailboxItem (47 lines)
- `core/paths/` — Contract path construction (114 lines)
- `core/sharding/` — Shard distribution (65 lines)
- No Empire references

### 3.13 `internal/runtime/mcp/` — 1,257 lines, 5 files — KEEP (with cleanup)
- MCP protocol gateway, diagnostics
- Cleanup: remove OpCo-aware routing (28 Empire refs, minor)

### 3.14 `internal/runtime/semanticview/` — 584 lines, 6 files — KEEP (with cleanup)
- Source interface, BundleSource, FileSource, PreviewSource, scope context
- Cleanup: remove vertical/portfolio agent scoping (10 Empire refs, minor)

### 3.15 `internal/runtime/actorctx/` — 22 lines, 1 file — KEEP ALL
- Actor context wrapper, minimal

### 3.16 `internal/runtime/actors/` — 75 lines, 2 files — KEEP (with cleanup)
- AgentConfig, MandateDocument
- Cleanup: remove vertical binding, geography/founder directives (4 Empire refs)

### 3.17 `internal/runtime/registry/` — 134 lines, 2 files — KEEP (with cleanup)
- Agent registration instructions
- Cleanup: minor Empire refs (4 refs)

### 3.18 `internal/runtime/rterrors/` — 97 lines, 1 file — KEEP ALL (clean)
- Runtime error type definitions
- Zero Empire references

### 3.19 `internal/runtime/sharedjson/` — 147 lines, 1 file — KEEP ALL (clean)
- JSON encoding/decoding utilities
- Zero Empire references

### 3.20 `internal/runtime/*.go` (root files) — 2,504 lines, 12 files — KEEP (with cleanup)
- Runtime bootstrap, budget, inbound event handling, MCP diagnostics
- Cleanup: remove Empire default wiring, budget OpCo refs (51 Empire refs in budget.go)

### 3.21 `internal/commgraph/` — 978 lines, 6 files — KEEP (minus empire/ subdir, with cleanup)
- Communication graph, authority model, policy, registry
- Cleanup: align authority to contract permissions (3 Empire refs in root files)

### 3.22 `internal/store/` — ~5,040 lines, 23 files — KEEP (minus empire_compat.go)
- PostgreSQL store, schedule store, entity storage, migration management

### 3.23 `internal/config/` — 294 lines, 2 files — KEEP (with cleanup)
- Configuration loading
- Cleanup: remove Empire-specific config refs (22 Empire refs)

### 3.24 `internal/events/` — 73 lines, 1 file — KEEP ALL
- Event type definitions (core platform type)
- Minor Empire refs (3 — likely just event type constants, fine)

### 3.25 `internal/digest/` — 144 lines, 2 files — KEEP (with cleanup)
- Event/action digest aggregation
- Cleanup: remove Empire refs (12 refs)

### 3.26 `internal/mailbox/` — 932 lines, 5 files — KEEP (with cleanup)
- Human task delivery, interaction state — platform concept
- Cleanup: remove Empire refs (11 refs, minor)

### 3.27 `internal/promptcontracts/` — 551 lines, 2 files — KEEP (with cleanup)
- Prompt schema validation and contract enforcement
- Cleanup: remove Empire refs (7 refs, minor)

### 3.28 `internal/templateops/` — 1,868 lines, 7 files — KEEP (with cleanup)
- Dynamic workflow instantiation, variable substitution — platform concept
- Cleanup: remove Empire refs (121 refs — significant, needs careful audit)

### 3.29 `cmd/mas/main.go` — 357 lines — KEEP
- Generic boot entrypoint, no Empire imports

### 3.30 Test Infrastructure — KEEP ALL

| Package | Lines | Files | Role |
|---------|-------|-------|------|
| `internal/testutil/` | 341 | 2 | Test utilities and helpers |
| `internal/runtime/testkit/` | 86 | 1 | Testing helpers |
| `internal/runtime/testcases/` | 459 | 10 | Reusable test scenarios |
| `internal/runtime/masflowtest/` | 1,662 | 7 | MAS flow catalog runner (cleanup: OpCo refs) |
| `internal/runtime/testdata/` | — | 14 YAML | Generic MAS bundle fixture |

---

## 4. REWRITE — New Code to Write

### 4.1 Dashboard API Server — ~300-400 lines NEW
**File:** `internal/dashboard/server.go` (single file or small package)

Thin HTTP server replacing 15,913 lines of deleted Go backend.

**Generic endpoints (query platform tables directly):**
- `GET /api/instances` — list workflow_instances
- `GET /api/instances/:id` — instance detail + accumulator + transitions
- `GET /api/instances/summary` — aggregate stats (replaces `/dashboard/api/holding`)
- `GET /api/instances/funnel` — group by state (replaces `/dashboard/api/funnel`)
- `GET /api/agents` — list agents
- `GET /api/agents/:id/prompt` — agent prompt
- `PUT /api/agents/:id/prompt` — update prompt
- `GET /api/events` — query events
- `GET /api/events/:id` — event detail
- `GET /api/events/stream` — SSE stream
- `GET /api/tasks` — human tasks
- `POST /api/tasks/:id/complete` — approve task
- `POST /api/tasks/:id/reject` — reject task
- `GET /api/conversations/:agent_id` — conversation history
- `GET /api/health` — health check
- `POST /api/directive` — send directive
- `POST /api/control/runtime` — pause/resume/reset
- `GET /api/budget` — budget status
- `GET /api/pipeline/graph` — workflow DAG visualization

### 4.2 `internal/runtime/pipeline/module.go` — REWRITE (~50 lines)
Strip to only the 5-method WorkflowModule interface:
```go
type WorkflowModule interface {
    SemanticSource() semanticview.Source
    WorkflowDefinition() *WorkflowDefinition
    WorkflowNodes() []WorkflowNode
    GuardRegistry() GuardRegistry
    ActionRegistry() ActionRegistry
}
```
Delete all Empire types, policy interfaces, and optional provider interfaces.

### 4.3 `internal/runtime/pipeline/workflow_transition_engine.go` — CLEANUP
- Remove validation gate references (g1-g4, reconcileValidationGates)
- Remove verticalID usage — replace with entityID from payload
- Keep the generic transition engine logic (~1,200 lines survive)

### 4.4 `internal/runtime/contracts/` — CLEANUP (3 files)
- `workflow_contracts.go`: Remove `DefaultWorkflowContractsDir()` returning hardcoded `empire/contracts`
- `prompts.go`: Remove hardwired Empire contract root for prompt loading
- `schema_registry_generated.go` (3,779 lines): Regenerate from product bundle, not Empire source paths

### 4.5 `internal/runtime/manager/` — CLEANUP (2 files)
- `helpers.go`: Delete `IsGrowthRole()`, `IsProactiveHeartbeat()`, `IsEmergencyAllowedFlow()` — 3 functions with 12 hardcoded Empire agent names
- `runtime.go`: Remove 3 locations hardcoding `"empire-coordinator"` as default manager

### 4.6 `internal/runtime/tools/` — CLEANUP (2 files)
- `emit_registry.go`: Delete 89 hardcoded Empire event schemas in `StrictDefaultEventSchemas`
- `authorizer.go`: Remove hardcoded tool allowlists

### 4.7 Minor Cleanups Across Kept Packages

| Package | Empire Refs | Cleanup Effort |
|---------|-------------|----------------|
| `internal/runtime/agents/` | 30 | Remove OpCo context, minor |
| `internal/runtime/llm/` | 28 | Remove OpCo routing, minor |
| `internal/runtime/mcp/` | 28 | Remove OpCo routing, minor |
| `internal/runtime/semanticview/` | 10 | Remove vertical scoping, minor |
| `internal/runtime/actors/` | 4 | Remove vertical binding, minor |
| `internal/runtime/registry/` | 4 | Minor refs |
| `internal/runtime/*.go` (root) | 51 | Budget OpCo refs, default wiring |
| `internal/commgraph/` | 3 | Align authority to contracts |
| `internal/config/` | 22 | Remove Empire config refs |
| `internal/digest/` | 12 | Remove Empire refs |
| `internal/mailbox/` | 11 | Remove Empire refs |
| `internal/promptcontracts/` | 7 | Remove Empire refs |
| `internal/templateops/` | 121 | **Significant** — needs careful audit |
| `internal/runtime/masflowtest/` | 13 | Remove OpCo refs in flow_activation.go |
| `internal/store/` | 294 | Remove Empire table refs (non-compat files) |

### 4.8 Empire WorkflowModule — ~200-300 lines NEW
**File:** `internal/products/empire/module.go` (or similar)

Thin WorkflowModule implementation that:
- Returns Empire's SemanticSource from `docs/specs/mas-platform/empire/contracts/`
- Returns WorkflowNodes built from Empire contract YAML
- Registers Empire-specific guards and actions
- Contains Empire payload types, policy interfaces, and domain logic deleted from generic packages

---

## 5. Dashboard UI (TypeScript) — KEEP ALL, Add TS Adapter

**Strategy:** The Go API server (Section 4.1) serves 100% generic platform data.
Empire-specific vocabulary and field mapping lives in a thin TypeScript adapter layer.
No TS views are deleted. No Go code carries Empire display logic.

### 5.1 KEEP — All feature directories (~18,000 lines, zero deletions)

| Directory | Role | Change Needed |
|---|---|---|
| `src/features/holding/` | Kanban board of entities, detail view | Wire through TS adapter |
| `src/features/portfolio/` | Operator workbench, focus navigation | Wire through TS adapter |
| `src/features/pipeline/` | Funnel, shard scans, lifecycle trace | Wire through TS adapter |
| `src/features/operations/` | Operational triage | Wire through TS adapter |
| `src/features/agents/` | Agent list, prompt editing | No change |
| `src/features/events/` | Event log, event detail | No change |
| `src/features/workflow/` | Workflow instance state | No change |
| `src/features/flow/` | Flow visualization | No change |
| `src/features/graph/` | Communication graph | No change |
| `src/features/tasks/` | Human-in-the-loop task queue | No change |
| `src/features/control/` | Runtime control | No change |
| `src/features/health/` | Health checks | No change |
| `src/features/incidents/` | Incident log | No change |
| `src/features/logs/` | Runtime logs | No change |
| `src/features/observability/` | Observability dashboard | No change |
| `src/features/digest/` | Digest summary | No change |
| `src/features/overview/` | Overview | Wire through TS adapter |

### 5.2 NEW — Empire TS Adapter (~150-200 lines)
**File:** `src/adapters/empire.ts`

Thin mapping layer that transforms generic platform API responses into the
shapes the existing Empire views already expect. Keeps all Empire vocabulary
in one TS file, not scattered across views or the Go server.

### 5.3 UPDATE — API client files (~200 lines changed)
- `src/api/dashboardPortfolio.ts` — point at generic `/api/instances` endpoints
- `src/api/holding.ts` (if exists) — same
- Import adapter in hooks (e.g., `useHoldingController.ts` calls `instanceToVertical()`)
- Views themselves are untouched — they receive the same prop shapes they always did

### 5.4 Why TS Adapter Over Go Adapter

| | Go adapter | TS adapter |
|---|---|---|
| Go API stays generic? | No — product logic leaks back in | **Yes — 100% platform** |
| Second product needs? | New Go adapter + registration | New TS adapter only |
| Complexity | Plugin/routing system in Go | One import in hook files |
| Where Empire vocab lives | Split across Go + TS | **Single file: `adapters/empire.ts`** |

---

## 6. Tests

### 6.1 DELETE — Empire-specific test tiers
- `tests/tier9-empire-integration/` — 14 subdirs, Empire flow integration tests
- `tests/tier10-empire-policy/` — 7 subdirs, Empire policy tests
- All `*_test.go` files in deleted directories (`internal/empire/`, `cmd/empire/`)

### 6.2 KEEP — Generic platform test tiers
- `tests/tier1-primitives/` — 33 subdirs, basic workflow primitives
- `tests/tier2-accumulation/` — 8 subdirs, accumulation patterns
- `tests/tier3-list-processing/` — 13 subdirs, list operations
- `tests/tier4-cross-entity/` — 5 subdirs, cross-entity operations
- `tests/tier5-flow-lifecycle/` — 10 subdirs, flow lifecycle
- `tests/tier6-event-loop/` — 10 subdirs, event loop semantics
- `tests/tier7-composition/` — 7 subdirs, flow composition
- `tests/tier8-boot-verification/` — 40+ subdirs, boot-time verification
- `tests/tier11-flow-composition/` — 20 subdirs, advanced composition
- All `*_test.go` in kept packages

### 6.3 UPDATE — Tests in cleaned packages
- Pipeline tests referencing deleted Empire types need updating
- Manager tests referencing hardcoded roles need updating
- Tools tests referencing Empire event schemas need updating

---

## 7. Non-Go Files — Complete Inventory

### 7.1 Contracts and Specs

| Path | Action | Reason |
|------|--------|--------|
| `docs/specs/mas-platform/platform/` | KEEP | Platform spec authority |
| `docs/specs/mas-platform/empire/contracts/` | KEEP | Empire product contracts (70 files) |
| `docs/specs/mas-platform/tests/` | KEEP | Test fixtures |
| `contracts/event-catalog.yaml` | KEEP | Platform event catalog |
| `contracts/system-nodes.yaml` | KEEP | Platform system nodes |
| `contracts/test-vectors/` | KEEP | Test vectors |
| `contracts/ddl-canonical.sql` | KEEP | Canonical DB schema |
| `docs/specs/empireai-v2_6_0/` | DELETE | Superseded |
| `docs/specs/empireai-v2_6_0.tar` | DELETE | Old archive |
| `docs/specs/mas-platform-v1.1.0-*.tar` | DELETE | Old archives |

### 7.2 Database Migrations

| Path | Action | Reason |
|------|--------|--------|
| `migrations/` (25 SQL files, 1,620 lines) | KEEP ALL | Schema evolution history |

Note: Migrations reference Empire-specific tables (verticals, scan_accumulators, etc.).
These tables still exist in the DB for Empire; they just aren't used by the generic runtime.
No migration changes needed — the platform ignores tables it doesn't use.

### 7.3 Configuration

| Path | Action | Reason |
|------|--------|--------|
| `configs/agents/` (15+ YAML files) | KEEP | Agent config templates |
| `configs/agents/templates/` (5 templates) | KEEP | Agent prompt templates |

Note: Agent configs contain Empire agent names. Post-platformization, these are
loaded by the Empire WorkflowModule, not by the platform kernel.

### 7.4 Scripts

| Path | Action | Reason |
|------|--------|--------|
| `scripts/generate_event_schema_registry/` | KEEP + UPDATE | Generates schema_registry_generated.go |
| `scripts/runtime_payload_audit.go` | KEEP | Generic audit utility |

Note: The schema registry generator imports `empireai/internal/runtime/contracts` —
it needs updating to accept a product contract path argument rather than
hardcoding Empire.

### 7.5 Infrastructure

| Path | Action | Reason |
|------|--------|--------|
| `Dockerfile.orchestrator` | KEEP + UPDATE | Update entrypoint from cmd/empire to cmd/mas |
| `Dockerfile.dashboard` | KEEP | Dashboard build |
| `Dockerfile.workspace` | KEEP | Dev environment |
| `docker-compose.yml` | KEEP + UPDATE | Update service references |
| `deploy/nginx/dashboard.conf` | KEEP | Nginx config |
| `Makefile` | KEEP + UPDATE | Update build targets |
| `.github/` | KEEP + UPDATE | CI workflows |

### 7.6 Data

| Path | Action | Reason |
|------|--------|--------|
| `data/test-signals-25.jsonl` | KEEP | Test fixture |

### 7.7 Documentation

| Path | Action | Reason |
|------|--------|--------|
| `docs/architecture/` | KEEP | Implementation plans, handoffs, audits |
| `docs/reports/` | KEEP | Test/audit reports |
| `go.mod` / `go.sum` | KEEP | Module dependencies |

---

## 8. Execution Order

```
Phase A: Mass deletion (1 hour)
  1. Delete entire directories: internal/empire/, internal/models/,
     internal/protocolheaders/, internal/factory/, internal/ops/,
     internal/specaudit/, cmd/empire/, old specs
  2. Delete internal/store/empire_compat.go
  3. Delete internal/dashboard/*.go (keep ui/ untouched)
  4. Delete internal/runtime/productpolicy/, scanmode/, corpusobs/
  5. Delete internal/commgraph/empire/
  6. Delete pipeline Empire files (Section 2.1, ~24 files)
  7. Delete tools Empire files (Section 2.3, 3 files)
  8. Delete manager/opco.go, bus/cycle_tracker.go

Phase B: Fix compile errors (2-3 hours)
  9. Rewrite pipeline/module.go to clean interface
  10. Clean workflow_transition_engine.go (remove g1-g4, verticalID)
  11. Clean manager/helpers.go, manager/runtime.go
  12. Clean contracts/ (remove Empire defaults)
  13. Clean tools/ (remove hardcoded schemas + guardrails)
  14. Clean minor Empire refs across kept packages (Section 4.7)
  15. Fix remaining import errors across codebase

Phase C: Rebuild (2-3 hours)
  16. Write thin generic dashboard API server (~300 lines)
  17. Write Empire WorkflowModule (~200-300 lines)
  18. Write TS Empire adapter (~150-200 lines in src/adapters/empire.ts)
  19. Update TS API client files to hit generic endpoints (~200 lines)
  20. Update schema registry generator script
  21. Regenerate schema_registry from product bundle
  22. Fix and run tests

Phase D: Verify (1 hour)
  23. `go build ./...` passes
  24. Tiers 1-8, 11 tests pass
  25. `cmd/mas/main.go` boots with Empire contracts
  26. Dashboard UI loads — holding/portfolio/pipeline views show instance data
  27. Zero `grep -r "empire\|vertical\|opco" internal/runtime/` hits
     (excluding comments and contract-loading paths)
  28. All Empire vocabulary confined to: contracts, WorkflowModule,
     TS adapter, cmd/mas boot wiring

Phase E: Infrastructure cleanup (30 min)
  29. Update Dockerfile.orchestrator entrypoint
  30. Update docker-compose.yml service references
  31. Update Makefile build targets
  32. Update CI workflows
```

---

## 9. Litmus Test (from arch-a.md)

> Can a second product boot by supplying contracts, a product module, and a `main.go`, without editing generic code under `internal/runtime/`, `internal/commgraph/`, or `internal/models/`?

After this plan executes: **yes.** The second product provides:
1. A contract directory (like `docs/specs/mas-platform/acme/contracts/`)
2. A `WorkflowModule` implementation (~200 lines)
3. A `cmd/acme/main.go` that calls `runtime.Boot(module)`
4. A TS adapter file (`src/adapters/acme.ts`) for dashboard views

No generic code changes required.

---

## 10. Packages NOT in Plan (Verification Checklist)

Every package below has been explicitly classified above. If a package exists
in the repo and is NOT listed here, it is a gap.

### Classified as DELETE:
- [x] `internal/empire/` (+ all subdirs: config, factory, hooks, models, payloads, pipeline, store)
- [x] `internal/models/`
- [x] `internal/protocolheaders/`
- [x] `internal/factory/`
- [x] `internal/ops/`
- [x] `internal/specaudit/`
- [x] `internal/store/empire_compat.go`
- [x] `internal/runtime/productpolicy/`
- [x] `internal/runtime/scanmode/`
- [x] `internal/runtime/corpusobs/`
- [x] `internal/commgraph/empire/`
- [x] `cmd/empire/`
- [x] `internal/dashboard/*.go`

### Classified as KEEP:
- [x] `internal/runtime/engine/`
- [x] `internal/runtime/pipeline/` (partial — see Sections 2.1, 3.2)
- [x] `internal/runtime/contracts/`
- [x] `internal/runtime/bus/` (partial — minus cycle_tracker.go)
- [x] `internal/runtime/manager/` (partial — minus opco.go)
- [x] `internal/runtime/tools/` (partial — minus 3 files)
- [x] `internal/runtime/workspace/`
- [x] `internal/runtime/agents/`
- [x] `internal/runtime/llm/`
- [x] `internal/runtime/flowmodel/`
- [x] `internal/runtime/sessions/`
- [x] `internal/runtime/core/` (+ identity, values, state, paths, sharding)
- [x] `internal/runtime/mcp/`
- [x] `internal/runtime/semanticview/`
- [x] `internal/runtime/actorctx/`
- [x] `internal/runtime/actors/`
- [x] `internal/runtime/registry/`
- [x] `internal/runtime/rterrors/`
- [x] `internal/runtime/sharedjson/`
- [x] `internal/runtime/*.go` (root files)
- [x] `internal/commgraph/` (root files)
- [x] `internal/store/` (minus empire_compat.go)
- [x] `internal/config/`
- [x] `internal/events/`
- [x] `internal/digest/`
- [x] `internal/mailbox/`
- [x] `internal/promptcontracts/`
- [x] `internal/templateops/`
- [x] `internal/testutil/`
- [x] `internal/runtime/testkit/`
- [x] `internal/runtime/testcases/`
- [x] `internal/runtime/masflowtest/`
- [x] `internal/runtime/testdata/`
- [x] `internal/dashboard/ui/`
- [x] `cmd/mas/`
- [x] `contracts/`
- [x] `docs/specs/mas-platform/`
- [x] `docs/architecture/`
- [x] `migrations/`
- [x] `configs/`
- [x] `scripts/`
- [x] `deploy/`
- [x] `data/`
- [x] `tests/` (tiers 1-8, 11)

### Classified as DELETE (tests):
- [x] `tests/tier9-empire-integration/`
- [x] `tests/tier10-empire-policy/`
