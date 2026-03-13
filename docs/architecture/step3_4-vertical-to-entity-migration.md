# Step 3.4: vertical_id → entity_id Migration — Implementer Handoff

**Date:** 2026-03-13
**Repo:** `/Users/youmew/dev/empireai`
**Language:** Go (backend), TypeScript (dashboard UI)
**Database:** PostgreSQL
**Authoritative spec:** `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

---

## 1. What Is This Project

EmpireAI is a **generic Multi-Agent System (MAS) orchestration platform**. It coordinates AI agents through declarative workflow contracts — YAML files that define flows, agents, events, handlers, guards, and state machines.

The first product built on this platform is called **"Empire"** — an autonomous AI holding company that discovers, validates, and operates software businesses. Empire was built simultaneously with the platform, which caused ~6,800 Empire-specific references to leak into generic platform code.

The **platformization effort** is extracting Empire-specific logic so the platform can run any product, not just Empire. The litmus test: *"Can a second product boot by supplying contracts, a WorkflowModule, and a main.go, without editing generic code?"*

## 2. Key Architecture Concepts

- **WorkflowModule interface** — the product/platform boundary. Each product implements 5 methods: `SemanticSource()`, `WorkflowDefinition()`, `WorkflowNodes()`, `GuardRegistry()`, `ActionRegistry()`. Generic code NEVER imports product code.
- **Contract bundles** — YAML files under `docs/specs/mas-platform/` defining flows, agents, events, schemas. The platform loads these at boot and derives behavior from them.
- **Empire contracts** live at `docs/specs/mas-platform/empire/contracts/` — these are **product-owned** and allowed to use Empire vocabulary (verticals, scoring, discovery, etc.).
- **Platform contracts** live at `docs/specs/mas-platform/platform/contracts/` — generic.
- **Event bus** — agents communicate through typed events routed by subscriptions derived from contracts.
- **12-step handler execution engine** — in `internal/runtime/engine/`, fully generic, zero product imports.
- **"Vertical"** is Empire's word for a business entity (a software company it's building). In the generic platform, this concept is just "entity." This migration renames that everywhere.

## 3. Where We Are — Phase 3 Progress

The project has 4 phases. Phases 1-2 are complete. Phase 3 (Empire Extraction) is ~60% done:

| Step | Description | Status |
|------|-------------|--------|
| 3.1 | Build generic test bundle | Not started |
| 3.2 | Rewrite 224 tests with generic vocabulary | Not started |
| 3.3 | Move product E2E coverage to product packages | Partially done |
| **3.4** | **Delete VerticalID from codebase** | **THIS TASK** |
| 3.5 | Extract Empire from config, factory, store, tools | ~80% done |
| 3.6 | Genericize cmd/empire → cmd/mas | Done |
| 3.7 | Remove remaining Empire vocabulary | ~90% done (cleanup stream complete) |

### What the cleanup stream accomplished (before this task)

Over the past cleanup stream, the previous implementer:

- **Deleted** ~48,000 lines of Empire-specific code across ~220 files
- **Deleted** entire packages: `internal/empire/`, `internal/models/`, `internal/factory/`, `internal/ops/`, `internal/specaudit/`, `cmd/empire/`, `internal/dashboard/*.go` (backend), `internal/runtime/productpolicy/`, `internal/runtime/scanmode/`, `internal/runtime/corpusobs/`, `internal/commgraph/empire/`
- **Deleted** Empire orchestration files in pipeline/ (~24 files: coordinator_discovery.go, coordinator_scoring.go, coordinator_scan.go, scan_campaign_manager.go, validation_orchestrator.go, lifecycle_orchestrator.go, payload_factory.go, etc.)
- **Cleaned** tools/ package — removed hardcoded tool switch statements, role mappings, scan modes. Authorization now delegates to commgraph.
- **Cleaned** runtime.go — removed hardcoded coordinator fallback, timer.portfolio_digest, system-admin special cases
- **Cleaned** commgraph/registry.go — removed hardcoded whatsapp/email producer events, founder_input.response
- **Cleaned** inbound.go — generic provider gateway, no hardcoded whatsapp handling
- **Cleaned** core/sharding/config.go — generic primary/secondary stages instead of MarketResearch/TrendResearch
- **Cleaned** budget.go — scopes renamed to system/entity/global
- **Made generator path-configurable** — `scripts/generate_event_schema_registry/main.go` accepts `MAS_CONTRACTS_DIR` env/CLI arg

### What this task completes

The `vertical_id` → `entity_id` rename is the last major mechanical step in Phase 3. After this, what remains is test rewrites (Steps 3.1-3.2) and then Phase 3 exit criteria verification.

## 4. Key Reference Files

| File | Purpose |
|------|---------|
| `docs/architecture/implementer-handoff.md` | Original 4-phase plan (read Phase 3, Steps 3.1-3.7 for full context) |
| `docs/architecture/platformization-delete-plan.md` | Exhaustive keep/delete/rewrite plan for every package |
| `docs/architecture/arch-a.md` | Architectural plan (P1-P7 dependency order) |
| `docs/specs/mas-platform/platform/contracts/platform-spec.yaml` | The authoritative platform spec |
| `contracts/ddl-canonical.sql` | Canonical database schema (source of truth for DB structure) |
| `internal/runtime/engine/` | The generic 12-step handler engine (fully clean — do not touch) |
| `internal/runtime/pipeline/module.go` | WorkflowModule interface definition |
| `cmd/mas/main.go` | Generic boot entrypoint |

## 5. Boundary Rules — READ CAREFULLY

1. **This task is ONLY the `vertical_id` → `entity_id` rename.** Do not fix, refactor, or clean up anything else you encounter. If you see other issues, note them but do not fix them.
2. **Do not modify anything under `docs/specs/mas-platform/empire/`** — that is product-owned contract vocabulary. Empire contracts are allowed to say "vertical." The platform doesn't care what product contracts call their entities.
3. **Do not modify the TypeScript UI** (`internal/dashboard/ui/`) — the TS layer will be handled separately via an adapter pattern. ~393 TS hits exist and are known.
4. **Do not modify historical docs** (`docs/architecture/`, `docs/reports/`) — they are historical records.
5. **Do not modify historical migration files** (001-025) — they are executed migration history.
6. **Do not mix this with other work.** Dedicated branch, dedicated commits.
7. **No bridge / no compatibility layer.** Per the Phase 3 spec: one coordinated pass — break everything, fix everything, green. No dual-read from both `vertical_id` and `entity_id`.

## 6. Why the Previous Implementer Was Replaced

The previous implementer was competent at execution but could not hold scope. Specific failure patterns:
- When asked for an **audit**, they would start **fixing things** instead
- When told "do not modify Empire contracts," they would propose renaming Empire contract vocabulary to generic terms
- They mixed cleanup, refactoring, and unrelated improvements into every task
- After context got long, they started drifting — repeating the same mistakes that had been corrected earlier

**You must hold scope.** Do exactly what is specified. If something seems wrong or missing, ask — do not improvise.

---

## 7. The Migration — Stream A: Database

Write migration `026_rename_vertical_to_entity.sql`.

### Tables needing column rename: `vertical_id` → `entity_id`

| # | Table | Nullable? | Notes |
|---|-------|-----------|-------|
| 1 | `events` | YES | |
| 2 | `agents` | YES (NULL for system-scoped) | |
| 3 | `template_migrations` | NOT NULL | |
| 4 | `routing_rules` | NOT NULL | |
| 5 | `mailbox` | YES | |
| 6 | `webhook_events` | NOT NULL | |
| 7 | `deployments` | YES | |
| 8 | `vertical_metrics` | YES | Table also needs rename (see below) |
| 9 | `spend_ledger` | YES (NULL for system-scoped) | |
| 10 | `human_tasks` | YES | |
| 11 | `runtime_log` | YES | |
| 12 | `cycle_counters` | NOT NULL, ON DELETE CASCADE | |
| 13 | `scoring_digest_buffer` | NOT NULL | Also has `vertical_name` column → `entity_name` |
| 14 | `validation_pipelines` | PRIMARY KEY | |

### Array column rename: `vertical_ids` → `entity_ids`

| Table | Column |
|-------|--------|
| `scan_directives` | `vertical_ids UUID[]` |

### Table renames

| Current | Target |
|---------|--------|
| `verticals` | `entities` |
| `vertical_metrics` | `entity_metrics` |

### Indexes to drop/recreate (8+)

| Current Index Name | Table | New Name |
|-------------------|-------|----------|
| `idx_events_vertical` | events | `idx_events_entity` |
| `idx_agents_vertical` | agents | `idx_agents_entity` |
| `idx_spend_vertical` | spend_ledger | `idx_spend_entity` |
| `idx_human_tasks_vertical` | human_tasks | `idx_human_tasks_entity` |
| `idx_routing_rules_unique` | routing_rules | `idx_routing_rules_unique` (update column ref) |
| `idx_rlog_vertical` | runtime_log | `idx_rlog_entity` |
| `idx_vertical_metrics_vertical_period_desc` | vertical_metrics | `idx_entity_metrics_entity_period_desc` |
| `idx_deployments_vertical_env_version` | deployments | `idx_deployments_entity_env_version` |

### FK constraints to update

- All 14 tables have `REFERENCES verticals(id)` → `REFERENCES entities(id)`
- `cycle_counters_vertical_id_fkey` → `cycle_counters_entity_id_fkey`
- `verticals.parent_id REFERENCES verticals(id)` → `entities.parent_id REFERENCES entities(id)` (self-referential)

### Other schema items

- `vertical_approval` mailbox type CHECK value → `entity_approval`
- `session_per_vertical` conversation mode value → `session_per_entity`

### Canonical DDL update

- Update `contracts/ddl-canonical.sql` to reflect all renames above

### Migration files (historical — do NOT modify)

These files contain `vertical_id` references but are historical migrations. Do NOT edit them:
- `migrations/001_initial.sql` through `migrations/025_*.sql`

---

## 8. The Migration — Stream B: Go Code

### Rename map

| Current | Target | Occurrences |
|---------|--------|-------------|
| `verticalID` (variable/param) | `entityID` | ~140 |
| `vertical_id` (SQL strings) | `entity_id` | ~45 |
| `verticals` (SQL table name) | `entities` | ~15 |
| `RequireVertical` (struct field) | `RequireEntity` | 4 |
| `SessionPerVerticalScoped` (const) | `SessionPerEntityScoped` | 5 |
| `filterOutVerticalScopedAgentIDs` (function) | `filterOutEntityScopedAgentIDs` | 2 |
| `seedVerticalAndAgent` (test function) | `seedEntityAndAgent` | 1 |
| `AuthorizeManageForTest` param `targetVerticalID` | `targetEntityID` | 1 |
| `legacyVerticalID` (variable) | `legacyEntityID` | 6 |
| `PerVertical` in naming | `PerEntity` | scattered |

### Files to modify (33 files)

**Store layer (11 files — update SQL strings + variables):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/store/events.go` | 10 | SQL `vertical_id` → `entity_id`, variables |
| `internal/store/event_receipt_store.go` | 8 | SQL columns, `legacyVerticalID` variable |
| `internal/store/agent_store.go` | 3 | SQL column |
| `internal/store/mailbox.go` | 5 | SQL columns |
| `internal/store/inbound.go` | 4 | SQL columns |
| `internal/store/schedule_store.go` | 6 | SQL columns |
| `internal/store/template_routing_store.go` | 6 | SQL columns |
| `internal/store/postgres_smoke_test.go` | 14 | Variables, SQL, table name `verticals` |
| `internal/store/postgres_helpers_test.go` | 8 | Variables, SQL, table names |
| `internal/store/manager_retry_policy_test.go` | 15 | Variables, function name, SQL, table name |
| `internal/store/postgres_store_additional_test.go` | 60+ | Variables, SQL, table name (heaviest test file) |

**Runtime core (5 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/budget.go` | 25 | Params, variables, SQL columns |
| `internal/runtime/diagnostics.go` | 1 | SQL column in INSERT |
| `internal/runtime/eventbus.go` | 6 | Function name, params, variables |
| `internal/runtime/runtime.go` | 2 | Variable |
| `internal/runtime/mcp_hooks.go` | 4 | Params, variables |

**Pipeline (4 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/pipeline/workflow_nodes.go` | 3 | `RequireVertical` struct field |
| `internal/runtime/pipeline/declarative_workflow_node.go` | 1 | `RequireVertical` reference |
| `internal/runtime/pipeline/declarative_default_node.go` | 1 | `RequireVertical` reference |
| `internal/runtime/pipeline/workflow_timer_lifecycle.go` | 18 | Params, variables (heaviest pipeline file) |

**Manager (4 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/manager/bootstrap.go` | 2 | Param |
| `internal/runtime/manager/types.go` | 2 | Interface method params |
| `internal/runtime/manager/receipts.go` | 9 | Variables |
| `internal/runtime/manager/flow_activation.go` | 2 | Param, variable |

**Bus (2 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/bus/eventbus_routing.go` | 6 | Function name, params, variables |
| `internal/runtime/bus/hooks.go` | 1 | Interface method param |

**LLM (2 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/llm/types.go` | 3 | Interface method params |
| `internal/runtime/llm/conversation.go` | 3 | `SessionPerVerticalScoped` const |

**Tools (2 files):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/tools/executor.go` | 2 | Test helper param |
| `internal/runtime/tools/executor_human_tasks.go` | 2 | SQL columns |

**Agents (1 file):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/agents/agent_llm.go` | 6 | `SessionPerVerticalScoped` ref, variables |

**MCP (1 file):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `internal/runtime/mcp/gateway.go` | 2 | Variable |

**Scripts (1 file):**

| File | Hit count | Key changes |
|------|-----------|-------------|
| `scripts/runtime_payload_audit.go` | 2 | Payload key names |

---

## 9. Stream C: TypeScript UI (DEFERRED — do not do now)

~393 hits across 60+ TS/TSX/CSS files. These will be handled by the TS adapter pattern (`src/adapters/empire.ts`) in a separate workstream. The UI is Empire-owned and is allowed to use Empire vocabulary internally.

**Do not touch the TypeScript.**

---

## 10. Execution Order

```
1. Write migration 026_rename_vertical_to_entity.sql
   - Table renames, column renames, index recreates, FK updates
   - Update contracts/ddl-canonical.sql

2. Update store layer (11 files)
   - All SQL strings: vertical_id → entity_id, verticals → entities
   - All Go variables in store files

3. Update runtime callers (22 files)
   - Go variables/params: verticalID → entityID
   - Constants: SessionPerVerticalScoped → SessionPerEntityScoped
   - Struct fields: RequireVertical → RequireEntity
   - Function names: filterOutVerticalScopedAgentIDs → filterOutEntityScopedAgentIDs

4. Regenerate schema registry
   - Run: MAS_CONTRACTS_DIR=docs/specs/mas-platform go generate ./internal/runtime/contracts/...

5. Run full test suite
   - go test ./... -count=1
   - Fix any fallout

6. Verify
   - grep -r "verticalID\|vertical_id\|VerticalID\|verticals" internal/ --include="*.go"
     should return zero hits (excluding empire/ subdirs and schema_registry_generated.go)
```

---

## 11. Config files to update

| File | Change |
|------|--------|
| `configs/empire.yaml` | `verticals_dir` → `entities_dir` (if still referenced) |
| `configs/empire.docker.yaml` | Same |
| `docker-compose.yml` | Comment update only |

---

## 12. What success looks like

1. `go build ./...` passes
2. `go test ./... -count=1` passes
3. `grep -r "verticalID\|vertical_id\|VerticalID\|RequireVertical\|SessionPerVerticalScoped" internal/ --include="*.go"` returns zero hits (excluding `empire/` subdirs and `schema_registry_generated.go`)
4. The migration file `026_rename_vertical_to_entity.sql` is complete and correct
5. `contracts/ddl-canonical.sql` reflects the new names
6. No files outside the 33 listed files + migration + DDL were modified
7. No Empire contracts were touched
8. No TypeScript was touched
9. No historical docs or migrations were edited

---

## 13. NOT in scope

- TypeScript UI (deferred to adapter workstream)
- Historical migration files (001-025)
- Historical docs (`docs/architecture/`, `docs/reports/`)
- Empire contract YAML under `docs/specs/mas-platform/empire/`
- Test rewrites beyond what's needed for the rename (Steps 3.1-3.2 are separate)
- Any cleanup, refactoring, or improvements beyond the rename
- Bug fixes, feature work, or architectural changes
