# Step 3.4: vertical_id → entity_id Migration

**Date:** 2026-03-13
**Scope:** Rename `vertical_id` → `entity_id` and `verticals` → `entities` across DB schema, Go code, and tests.
**Predecessor:** Platform cleanup stream (complete). Runtime logic is clean. This is a schema/storage migration.

---

## Boundary Rules

1. **This task is ONLY the rename.** Do not fix, refactor, or clean up anything else.
2. **Do not modify anything under `docs/specs/mas-platform/empire/`** — that is product-owned contract vocabulary. Empire contracts are allowed to say "vertical."
3. **Do not modify the TypeScript UI** — the TS layer will be handled separately via an adapter pattern.
4. **Do not modify historical docs** (`docs/architecture/`, `docs/reports/`) — they are historical records.
5. **Do not mix this with other work.** Dedicated branch, dedicated commits.

---

## Stream A: Database Migration (do first)

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
- `migrations/001_initial.sql`
- `migrations/002_v2_0.sql`
- `migrations/008_*.sql`
- `migrations/016_*.sql`
- `migrations/018_*.sql`
- `migrations/021_*.sql`
- `migrations/022_*.sql`
- `migrations/025_*.sql`

---

## Stream B: Go Code (after migration is written)

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
| `internal/store/postgres_helpers_test.go` | 8 | Variables, SQL, table names `verticals`, `vertical_metrics` |
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

## Stream C: TypeScript UI (DEFERRED — do not do now)

~393 hits across 60+ TS/TSX/CSS files. These will be handled by the TS adapter pattern (`src/adapters/empire.ts`) in a separate workstream. The UI is Empire-owned and is allowed to use Empire vocabulary internally.

---

## Execution Order

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
   - Run updated generator against current contracts

5. Run full test suite
   - go test ./... -count=1
   - Fix any fallout

6. Verify
   - grep -r "verticalID\|vertical_id\|VerticalID\|verticals" internal/ --include="*.go"
     should return zero hits (excluding empire/ subdirs and schema_registry_generated.go)
```

---

## Decision: No Bridge

Per the original Phase 3 spec (Step 3.4): "Do NOT rename — delete. No bridge."

This means:
- No dual-read from both `vertical_id` and `entity_id`
- No compatibility layer
- One coordinated pass: break everything, fix everything, green
- The migration runs, the code updates, tests pass — done

---

## Config files to update

| File | Change |
|------|--------|
| `configs/empire.yaml` | `verticals_dir` → `entities_dir` (if still referenced) |
| `configs/empire.docker.yaml` | Same |
| `docker-compose.yml` | Comment update only |

---

## NOT in scope

- TypeScript UI (deferred to adapter workstream)
- Historical migration files (001-025) — do not edit
- Historical docs (`docs/architecture/`, `docs/reports/`)
- Empire contract YAML under `docs/specs/mas-platform/empire/`
- Any cleanup, refactoring, or improvements beyond the rename
