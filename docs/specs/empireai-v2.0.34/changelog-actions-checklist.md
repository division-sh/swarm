# EmpireAI Changelog-Actions Checklist (v2.0.34)

Generated from spec changelog v2.0.1 through v2.0.34.
Every unchecked item is a potential bug if the codebase doesn't match.

Canonical references:
- `contracts/agent-tools.yaml` — 28 agents
- `contracts/event-catalog.yaml` — 161 events
- `contracts/ddl-canonical.sql` — 36 tables

Action prefixes: `DROP` (remove code/config), `EDIT` (modify existing), `ADD` (create new),
`MIGRATE` (database schema change), `RENAME` (file/identifier rename), `VERIFY` (grep/test check).

---

## 1. Removals (DROP)

- [ ] DROP COLUMN: `human_tasks.tool_call_id` (v2.0.20)
  - File: `contracts/ddl-canonical.sql` — already removed
  - VERIFY: `grep -r "tool_call_id" internal/ migrations/` → 0 results

- [ ] DROP ROUTE: seeded route `devops.deploy_complete → qa-agent` (v2.0.20)
  - File: `internal/runtime/bootstrap.go` or seed data
  - VERIFY: `grep -r "qa.*deploy_complete\|deploy_complete.*qa" internal/ configs/` → 0 results

- [ ] DROP EVENT: `devops.port_allocated` (v2.0.20)
  - Files: `internal/events/types.go`, EventSchemaRegistry
  - VERIFY: `grep -r "port_allocated" internal/ configs/` → 0 results

- [ ] DROP AGENT: `scoring-coordinator` (v2.0.19)
  - Files: `configs/agents/scoring-coordinator.yaml`, roster in `configs/empire.yaml`
  - VERIFY: `grep -r "scoring.coordinator\|scoring-coordinator\|ScoringCoordinator" internal/ configs/` → 0 results

- [ ] DROP EVENT: `scoring.dimensions_complete` (v2.0.19)
  - Files: `internal/events/types.go`, EventSchemaRegistry
  - VERIFY: `grep -r "dimensions_complete" internal/` → 0 results

- [ ] DROP MODE: `automation_micro` from `defaultModes` campaign list (v2.0.28)
  - File: `internal/runtime/pipeline.go` (handleScanRequested)
  - VERIFY: `grep -r "automation_micro.*mode\|defaultModes.*automation_micro" internal/` → 0 results (rubric still exists)

- [ ] DROP PATTERN: JSON envelope emission support (v2.0.15)
  - File: `internal/runtime/tools.go` or response parser
  - VERIFY: `grep -r "emit_events.*json\|parseResponseJSON\|envelope" internal/` → 0 results

## 2. Database Migrations (MIGRATE)

- [ ] MIGRATE: `ALTER TABLE deployments ALTER COLUMN deployed_by TYPE TEXT` (v2.0.29)
  - Was: `UUID REFERENCES agents(id)` — type mismatch since `agents.id` is TEXT
  - File: Create migration `XXX_fix_deployed_by_type.sql`
  - VERIFY: `psql -c "\d deployments"` shows `deployed_by TEXT`

- [ ] MIGRATE: CREATE TABLE `pipeline_transitions` (v2.0.13)
  - File: `contracts/ddl-canonical.sql` lines matching `CREATE TABLE pipeline_transitions`
  - VERIFY: `psql -c "\d pipeline_transitions"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `shards` (v2.0.16)
  - VERIFY: `psql -c "\d shards"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `prompt_overrides` (v2.0.3)
  - VERIFY: `psql -c "\d prompt_overrides"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `scoring_digest_buffer` (v2.0.24)
  - VERIFY: `psql -c "\d scoring_digest_buffer"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `cycle_counters` (v2.0.22)
  - VERIFY: `psql -c "\d cycle_counters"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `runtime_log` (v2.0.14)
  - VERIFY: `psql -c "\d runtime_log"` matches canonical DDL with all 6 indexes

- [ ] MIGRATE: ADD COLUMN `scope_key` to `conversations` + `agent_sessions` (v2.0.19)
  - VERIFY: `psql -c "\d conversations"` shows `scope_key TEXT`
  - VERIFY: Unique index `idx_conversations_scope` exists on `(agent_id, scope_key) WHERE status='active'`

- [ ] MIGRATE: Verify `inbound_events` retention = 7 days (v2.0.21)
  - VERIFY: pg_cron or application-level cleanup configured

- [ ] MIGRATE: CREATE TABLE `runtime_config` (v2.0.33, migration 003)
  - VERIFY: `psql -c "\d runtime_config"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `pipeline_receipts` (v2.0.33, migration 009)
  - VERIFY: `psql -c "\d pipeline_receipts"` matches canonical DDL with unique index

- [ ] MIGRATE: CREATE TABLE `scan_accumulators` (v2.0.33, migration 012)
  - VERIFY: `psql -c "\d scan_accumulators"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `pending_dedup_candidates` (v2.0.33, migration 012)
  - VERIFY: `psql -c "\d pending_dedup_candidates"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `validation_pipelines` (v2.0.33, migration 012)
  - VERIFY: `psql -c "\d validation_pipelines"` with unique active index

- [ ] MIGRATE: CREATE TABLE `pipeline_processed_events` (v2.0.33, migration 012)
  - VERIFY: `psql -c "\d pipeline_processed_events"` matches canonical DDL

- [ ] MIGRATE: CREATE TABLE `template_prompt_drafts` (v2.0.33, migration 011)
  - VERIFY: `psql -c "\d template_prompt_drafts"` matches canonical DDL

- [ ] MIGRATE: ADD COLUMN `verticals.slug` + unique index (v2.0.33)
  - VERIFY: `psql -c "\d verticals"` shows `slug TEXT NOT NULL`
  - VERIFY: `idx_verticals_slug_geo` unique index exists

- [ ] MIGRATE: ADD COLUMNS `scan_campaigns.directive_id`, `.strategic_context`, `.deadline_at` (v2.0.33)
  - VERIFY: `psql -c "\d scan_campaigns"` shows all three columns

- [ ] MIGRATE: ADD unique index `routing_rules(vertical_id, event_pattern, subscriber_id) WHERE active` (v2.0.33)
  - VERIFY: `psql -c "\di idx_routing_rules_unique"` exists

## 3. Agent Config Reconciliation (EDIT)

### Model tier verification
- [ ] EDIT: `configs/agents/empire-coordinator.yaml` — verify `model_tier: sonnet`
- [ ] EDIT: `configs/agents/factory-cto.yaml` — verify `model_tier: sonnet`
- [ ] EDIT: `configs/agents/holding-devops.yaml` — verify `model_tier: haiku`
- [ ] EDIT: `configs/agents/operations-analyst.yaml` — verify `model_tier: sonnet`
- [ ] EDIT: `configs/agents/spec-auditor.yaml` — verify `model_tier: sonnet`, `type: holding` (v2.0.33 fix C3)
- [ ] EDIT: `configs/agents/discovery-coordinator.yaml` — verify `model_tier: sonnet`
- [ ] EDIT: `configs/agents/analysis-agent.yaml` — verify `model_tier: sonnet`
- [ ] EDIT: `configs/agents/business-research.yaml` — verify `conversation_mode: session_per_vertical`
- [ ] EDIT: `configs/agents/lightweight-spec.yaml` — verify `conversation_mode: session_per_vertical`

### Subscription fixes
- [ ] EDIT: `configs/agents/factory-cto.yaml` — subscriptions must include `opco.steady_state_reached`, `opco.escalation` (no wildcards, v2.0.33 fix C1)
  - VERIFY: `grep -r "opco\.\*\." configs/agents/` → 0 results
- [ ] EDIT: `configs/agents/operations-analyst.yaml` — subscriptions must include `opco.ceo_report`, `opco.steady_state_reached` (no wildcards, v2.0.33 fix C1)
- [ ] EDIT: `configs/agents/empire-coordinator.yaml` — add `opco.escalation`, `devops.capacity_warning`, `cto.extraction_recommended` (v2.0.29)

### Tool fixes
- [ ] EDIT: `configs/agents/holding-devops.yaml` — add `mailbox_send` to tools (v2.0.29)
- [ ] EDIT: `configs/agents/templates/opco-ceo.yaml` — add `mailbox_send` to tools (v2.0.33 fix H1)
- [ ] EDIT: `configs/agents/templates/vp-product.yaml` — add `mailbox_send` to tools (v2.0.33 fix H1)
- [ ] EDIT: `configs/agents/templates/vp-growth.yaml` — add `mailbox_send` to tools (v2.0.33 fix H1)
- [ ] EDIT: `configs/agents/templates/marketing-agent.yaml` — add `mailbox_send` to tools (v2.0.33 fix H1)
- [ ] EDIT: `configs/agents/templates/support-agent.yaml` — add `mailbox_send` to tools (v2.0.33 fix H1)

### Missing configs
- [ ] ADD: `configs/agents/market-research.yaml` — from agent-tools contract (v2.0.33)
- [ ] ADD: `configs/agents/trend-research.yaml` — from agent-tools contract (v2.0.33)
- [ ] RENAME: `configs/agents/prebrand.yaml` → `configs/agents/pre-brand-agent.yaml` (v2.0.33)
  - VERIFY: `grep -r "prebrand\.yaml" .` → 0 results

## 4. Event Schema Registration (EDIT)

- [ ] EDIT: `internal/events/types.go` — add 30 new event type constants from event-catalog v2.0.33
  - VERIFY: All 161 events in `contracts/event-catalog.yaml` have matching Go constants

- [ ] EDIT: EventSchemaRegistry — add schemas for 30 new events, fix 5 payload field names:
  - `source.scraped`: field `source` (was `source_type`)
  - `portfolio.digest_compiled`: field `digest_text` (was `digest_content`)
  - `vertical.health_warning`: field `severity` (was `warning_type`)
  - `spec.validation_failed`: field `issues` (was `failures`)
  - `devops.rollback_complete`: field `active_version` (was `restored_version`)
  - VERIFY: Run wiring verifier — 0 FAIL, 0 WARN on payload field checks

## 5. Interceptor Handler Verification (VERIFY)

All handlers per v2.0.1–v2.0.33 changes:
- [ ] VERIFY: `handleScanRequested` — spawns MRA with 3 modes (not 4)
- [ ] VERIFY: `handleSubAgentComplete` — routes MRA/TRA/Scanner completion
- [ ] VERIFY: `handleScanCompleted` — cycles campaign to next mode or emits `campaign.completed`
- [ ] VERIFY: `handleDiscoveryReport` — dual-path: SaaS gap ≥50 AND automation-micro ≥50 independently
- [ ] VERIFY: `handleVerticalDiscovered` — dedup check, creates vertical record
- [ ] VERIFY: `handleScoreDimensionComplete` — ScoringAccumulator, no SC agent
- [ ] VERIFY: `handleShortlisted` → `handleResearchCompleted` → `handleSpecApproved` → full pipeline chain
- [ ] VERIFY: `computeComposite()` — uses correct rubric per discovery_mode
- [ ] VERIFY: EC rejection filtering — EC never receives rejected verticals

## 6. State Machine Guards (VERIFY)

- [ ] VERIFY: RevisionCount max 3 (spec → cto review cycle)
- [ ] VERIFY: InnerRevisionCount max 5 (spec draft → review cycle)
- [ ] VERIFY: Campaign backpressure — no new scan while active verticals > threshold
- [ ] VERIFY: OpCo cycle detection — `cycle_counters` table prevents infinite build loops
- [ ] VERIFY: Packaging timeout — ready_for_review reached within bounded time
- [ ] VERIFY: Crash recovery — `pipeline_receipts` prevent duplicate processing on replay

## 7. Cross-Reference Integrity (VERIFY)

- [ ] VERIFY: Every agent in `agent-tools.yaml` has matching YAML config in `configs/agents/`
  - Run: `python3 -c "import yaml; agents=yaml.safe_load(open('contracts/agent-tools.yaml')); [print(a) for a in agents]"` and check each has config file
- [ ] VERIFY: Every event in `event-catalog.yaml` has EventSchemaRegistry entry
  - Run: Wiring verifier with 0 unregistered events
- [ ] VERIFY: `ddl-canonical.sql` matches live schema — diff all 36 tables
- [ ] VERIFY: agent-tools.yaml emit_events × event-catalog.yaml = zero gaps
- [ ] VERIFY: agent-tools.yaml subscriptions × event-catalog.yaml = zero gaps
- [ ] VERIFY: Wiring verifier final score: 0 FAIL, 0 WARN

## 8. v2.0.30 Review Findings Reconciliation

Status: All critical/high findings from v2.0.30 external review were addressed in v2.0.33.
This section tracks the remediation for audit trail purposes.

### Agent-Tools (6/10 → 10/10)
- [x] C1: Wildcard subscriptions → direct event names (v2.0.33)
  - VERIFY: `grep -r "opco\.\*\." contracts/agent-tools.yaml` → 0 results
- [x] C2: OpCo worker subscription model documented via `subscriptions_bootstrap` field (v2.0.33)
- [x] C3: Spec Auditor → `model_tier: sonnet`, `type: holding` (v2.0.33)
- [x] H1: `mailbox_send` added to CEO, Marketing, Support, VP Product, VP Growth (v2.0.33)
- [x] H2: `scanner-agent` added as ephemeral template agent (v2.0.33)
- [x] H3: Chief of Staff 8 subscriptions from config (v2.0.33)
- [x] H4: OpCo CTO 7 subscriptions_bootstrap from config (v2.0.33)
- [x] Medium: max_turns, conversation_mode, schedule tool fixes throughout (v2.0.33)

### Event-Catalog (4/10 → 9/10)
- [x] 34 missing events added (v2.0.33): scanner per-type, OpCo lifecycle, human workflow, operational, agent-produced
- [x] Payload field mismatches fixed (v2.0.33): source.scraped, portfolio.digest_compiled, vertical.health_warning, spec.validation_failed, devops.rollback_complete aligned with EventSchemaRegistry
- [x] `trend.identified` payload: `market_intersection` field added (v2.0.33)
- [x] `category.assessed` payload: `automation_micro` field already present (v2.0.33)
- [x] Consumer listing gaps fixed (v2.0.33): devops.rollback_failed → [holding-devops, audit], template.migration_completed → [empire-coordinator, audit], template.migration_failed → [empire-coordinator, audit]
- [x] `vertical.shortlisted` emitter confirmed as `runtime` — commgraph entry was stale (v2.0.33)
- [ ] VERIFY: `delivery_channel` field accurate for all 161+ events — run channel-vs-actual check

### DDL (6/10 → 9/10)
- [x] 7 missing tables added (v2.0.33): runtime_config, pipeline_receipts, scan_accumulators, pending_dedup_candidates, validation_pipelines, pipeline_processed_events, template_prompt_drafts
- [x] scan_campaigns columns added: directive_id, strategic_context, deadline_at (v2.0.33)
- [x] verticals.slug + unique index added (v2.0.33)
- [x] Scope-aware unique indexes on conversations/agent_sessions (v2.0.33)
- [x] routing_rules uniqueness constraint added (v2.0.33)
- [ ] VERIFY: `psql -f contracts/ddl-canonical.sql` on fresh database → 0 errors

### Main Spec Structural
- [x] §17 Contracts section written (v2.0.33) — authority rules, test verification, maintenance policy
- [x] §16 directory structure updated with `contracts/` directory (v2.0.33)
- [ ] TODO: §16 still lists `internal/agents/`, `internal/claude/`, `internal/tools/` — confirm these match actual codebase or update
- [ ] TODO: `prebrand.yaml` → `pre-brand-agent.yaml` naming inconsistency (spec uses both)
- [ ] TODO: Section 4 (190 KB) and Appendix B (211 KB) exceed 80 KB — consider splitting in future revision

## §9 — v2.0.34 DDL Schema Alignment & Implementer Audit Reconciliation

Source: v2.0.33-checklist-audit.md (implementer), v2.0.33 external review (reviewer)

### DDL Schema Realignment (CRITICAL — all 7 runtime tables rewritten)

Previous canonical DDL schemas were spec-writer inventions that didn't match Go structs or implementer migrations.
All 7 tables rewritten to match Go struct definitions (§4.2.2.2, §4.2.2.3) and actual migration schemas.

- [x] `runtime_config`: key-value → config-snapshot (id UUID PK, config_yaml TEXT, config_hash) — matches migrations/003
- [x] `pipeline_receipts`: multi-handler → single-receipt (event_id UUID PK) — matches migrations/009 and RecoverFromCrash()
- [x] `scan_accumulators`: count-based → JSONB-based (completed_by, reports) — matches ScanAccumulator struct
- [x] `pending_dedup_candidates`: UUID+vertical_id → candidate JSONB, existing_id, dedup_event_id — matches PendingCandidate struct
- [x] `validation_pipelines`: row-per-stage → row-per-vertical with G1-G4 gates, revision counters — matches ValidationPipeline struct
- [x] `pipeline_processed_events`: per-handler → per-event only (event_id PK) — matches idempotency pattern
- [x] `template_prompt_drafts`: versioned workflow → simple role→prompt (role TEXT PK) — matches hot-reload pattern
- [x] `scan_campaigns.strategic_context`: TEXT → JSONB — matches migrations/010
- [x] `shards.status`: CHECK constraint added (was comment-only)
- [x] `spend_ledger.agent_id`: FK to agents(id) added (was bare TEXT)
- [x] `prompt_overrides`: ON DELETE CASCADE added
- [x] `cycle_counters`: ON DELETE CASCADE added
- [ ] VERIFY: `psql -f contracts/ddl-canonical.sql` on fresh DB → 0 errors (was UNVERIFIED in implementer audit)
- [ ] VERIFY: Live schema diff (migrations-built DB vs canonical DDL) → 0 structural differences

### Implementer Audit Findings Absorbed

- [x] `handleSubAgentComplete` → `handleScanCompletion` — spec function name corrected to match `internal/runtime/` implementation
- [x] `prebrand-agent` → `pre-brand-agent` — Appendix A ID normalized to match Appendix B and configs
- [x] §16 directory tree: `internal/agents/` subtree removed → replaced with `internal/pipeline/` matching actual codebase
- [x] §3.1 ASCII diagrams: Scoring Coordinator ghost removed from both diagrams
- [x] v2.0.32 changelog: Backfilled (was title-only — most substantial revision had zero documentation)

### Implementer Audit PARTIAL Items (test fixture cleanup — not blocking)

- [ ] EDIT: Remove `"scoring-coordinator"` string labels from test fixtures (runtime agent removed but test synthetic sources still reference it)
- [ ] EDIT: Remove legacy `emit_events` JSON envelope payloads from regression test fixtures (runtime is emit-tool only since v2.0.19)

### Implementer Config Naming Conventions (checklist wording fix)

- [x] Checklist referenced `market-research.yaml` / `trend-research.yaml` — repo uses `market-research-agent.yaml` / `trend-research-agent.yaml`. Checklist corrected.

### Reviewer Remaining Items (deferred — needs implementer migration data)

- [ ] EDIT: Populate `subscriptions_bootstrap` for 7 OpCo workers (PM=3, TechWriter=1, Backend=1, Frontend=1, QA=1, DevOps=3, Marketing=3) — cross-reference `configs/agents/templates/routes.yaml` bootstrap entries
- [ ] EDIT: Add `schedule` tool to 6 OpCo workers (PM, CTO, Tech Writer, Backend, Frontend, DevOps)
- [ ] EDIT: Add `human_task_request` tool to OpCo CEO
- [ ] EDIT: Add 6 missing events to catalog: opco.routing_updated, customer_message, human_task.assigned, review.product_spec_feedback, review.deploy_feedback, runtime.reset
- [ ] EDIT: Fix ~8 payload mismatches (scan.requested missing campaign_context, source.scraped raw_data→evidence, cycle_limit_reached missing vertical_id/recommendation)
- [ ] EDIT: scan.completed intercepted flag → false (it's not intercepted by pipeline coordinator)
- [ ] EDIT: scan.started is phantom (no code emits it) — either add emission or remove from catalog
- [ ] EDIT: Register 7 pipeline_coordinator deferred-emitted events in commgraph runtimeEmittedEvents
- [ ] EDIT: Reduce permissive default schemas (currently ~66%) — add real payload definitions
