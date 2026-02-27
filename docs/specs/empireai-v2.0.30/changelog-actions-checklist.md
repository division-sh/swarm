# EmpireAI Retroactive Changelog-Actions Checklist

Generated from spec changelog v2.0.1 through v2.0.29.
Every unchecked item is a potential bug if the codebase doesn't match.

Cross-reference against `contracts/agent-tools.yaml`, `contracts/event-catalog.yaml`, 
and `contracts/ddl-canonical.sql` for canonical values.

---

## Removals Requiring Codebase Action

- [ ] **DROP COLUMN `tool_call_id` from `human_tasks`** — removed v2.0.20. Was used for async tool_result injection pattern, replaced by event-based targeted delivery.
- [ ] **Remove seeded route `devops.deploy_complete (staging) → QA Agent`** — removed v2.0.20. QA assignment now via CTO agent_message chain. Route count 28→27.
- [ ] **Remove `devops.port_allocated` event** — removed v2.0.20 (infrastructure provisioning fix). Port allocation is synchronous inside SpawnOpCo, no event needed. Remove from EventSchemaRegistry, event catalog, producer registry.
- [ ] **Remove Scoring Coordinator agent entirely** — absorbed into runtime v2.0.19. Remove: `scoring-coordinator.yaml` config, roster entry in `empire.yaml`, `empire init` seed. Replaced by `computeComposite()` in runtime (§4.2.2.8).
- [ ] **Remove `scoring.dimensions_complete` event** — removed v2.0.19. ScoringAccumulator flows directly into `computeComposite()`, no intermediate event. Remove from EventSchemaRegistry.
- [ ] **Remove `automation_micro` from `defaultModes` campaign list** — removed v2.0.28. Was 4 modes, now 3 (`saas_gap`, `saas_trend`, `local_services`). Dual assessment integrated into MRA single pass.
- [ ] **Remove JSON envelope emission support** — removed v2.0.15. Agents no longer return `{"emit_events":[...]}` in response text. All emissions via typed `emit_*` tools.

## Migrations Requiring DDL Action

- [ ] **`ALTER TABLE deployments ALTER COLUMN deployed_by TYPE TEXT`** — fixed v2.0.29. Was `UUID REFERENCES agents(id)` but `agents.id` is `TEXT`. Type mismatch would cause FK errors.
- [ ] **CREATE TABLE `pipeline_transitions`** — added v2.0.13, missing from §8.1 until v2.0.21. Verify table exists with all indexes per `ddl-canonical.sql`.
- [ ] **CREATE TABLE `shards`** — added v2.0.16, missing from §8.1 until v2.0.21. Verify table exists with all indexes per `ddl-canonical.sql`.
- [ ] **CREATE TABLE `prompt_overrides`** — added v2.0.3. Verify table exists per `ddl-canonical.sql`.
- [ ] **CREATE TABLE `scoring_digest_buffer`** — added v2.0.24. Verify table exists per `ddl-canonical.sql`.
- [ ] **CREATE TABLE `cycle_counters`** — added v2.0.22. Verify table exists per `ddl-canonical.sql`.
- [ ] **CREATE TABLE `runtime_log`** — added v2.0.14. Verify table exists with all indexes per `ddl-canonical.sql`.
- [ ] **ADD COLUMN `scope_key` to `conversations`** — added v2.0.19. Required for `session_per_vertical` mode.
- [ ] **ADD COLUMN `scope_key` to `agent_sessions`** — added v2.0.19. Required for `session_per_vertical` mode.
- [ ] **CREATE INDEX `idx_conversations_scope`** — added v2.0.19. Covers (agent_id, scope_key, status) WHERE scope_key IS NOT NULL.
- [ ] **Verify `inbound_events` retention = 7 days** — aligned v2.0.21. Was inconsistent (24h vs 7d). Canonical: 7 days. Check cleanup cron.

## Agent Config Verification

### Model Tiers (verify each matches `agent-tools.yaml`)
- [ ] Empire Coordinator: **Sonnet**
- [ ] Factory CTO: **Sonnet**
- [ ] Holding DevOps: **Haiku**
- [ ] Operations Analyst: **Sonnet**
- [ ] Spec Auditor: **Haiku**
- [ ] Discovery Coordinator: **Sonnet**
- [ ] Analysis Agent: **Sonnet**
- [ ] Validation Coordinator: **Sonnet**
- [ ] Business Research Agent: **Sonnet**
- [ ] Lightweight Spec Agent: **Sonnet**
- [ ] Spec Reviewer: **Haiku**
- [ ] Market Research Agent: **Sonnet**
- [ ] Trend Research Agent: **Sonnet**
- [ ] Pre-Brand Agent: **Sonnet**
- [ ] Tech Writer: **Sonnet** (fixed v2.0.1, was incorrectly Haiku)
- [ ] QA Agent: **Haiku**
- [ ] OpCo DevOps: **Haiku**
- [ ] Support Agent: **Haiku**

### Conversation Modes (verify each matches `agent-tools.yaml`)
- [ ] BRA: **session_per_vertical** (changed v2.0.19, was `session`)
- [ ] LSA: **session_per_vertical** (changed v2.0.19, was `session`)
- [ ] Discovery Coordinator: **task** (changed v2.0.9, was `session`)
- [ ] Validation Coordinator: **task** (changed v2.0.9, was `session`)
- [ ] Scoring Coordinator: **REMOVED** (v2.0.19)

### Subscription Changes (verify each agent's subscriptions match `agent-tools.yaml`)
- [ ] EC: `scan.completed` **REMOVED** (v2.0.9) — runtime handles scan cycling
- [ ] EC: `scan.requested` **REMOVED** (v2.0.9) — runtime handles directive translation
- [ ] EC: `vertical.needs_more_data` **REMOVED** (v2.0.9) — runtime handles more-data loop
- [ ] EC: `campaign.completed` **ADDED** (v2.0.9) — replaces scan.completed
- [ ] EC: `scoring.contested` **ADDED** (v2.0.21)
- [ ] EC: `opco.escalation`, `devops.capacity_warning`, `cto.extraction_recommended` **ADDED** (v2.0.29)
- [ ] DC: subscriptions changed to ONLY `dedup.ambiguous` + `synthesis.needed` (v2.0.9). Removed `scan.requested`.
- [ ] VC: subscription changed to ONLY `validation.package_ready` (v2.0.9). Removed all gate events.
- [ ] BRA: `spec.draft_ready` **ADDED** (v2.0.10) — was missing, spec drafts silently dropped
- [ ] BRA: `validation.more_data_needed` **ADDED** (v2.0.9)
- [ ] BRA: `spec_review.passed` and `spec_review.issues_found` **ADDED** (v2.0.5)
- [ ] VC: `max_turns_per_task` reduced to **5** (v2.0.9, was 20+)

### Tool Changes (verify each agent's tools match `agent-tools.yaml`)
- [ ] `agent_message` is **universal** — injected into ALL agents automatically (v2.0.23). Remove explicit listings in individual YAMLs (redundant but harmless).
- [ ] Holding DevOps: `mailbox_send` **ADDED** (v2.0.29) — needed for destructive DDL escalation
- [ ] Holding DevOps: `agent_message` **ADDED** (v2.0.20) — for cross-tier deploy result delivery
- [ ] All agents: `emit_*` tools auto-injected from EventSchemaRegistry (v2.0.15). Verify GenerateEmitTools() uses `agent-tools.yaml` emit_events lists.

### Prompt Rewrites (verify each prompt matches spec appendix)
- [ ] Empire Coordinator: full rewrite v2.0.2 (constraint-first), updated v2.0.7 (removed scan cycling), v2.0.9 (removed directive translation), v2.0.21 (added scoring.contested), v2.0.24 (EC rejection filtering)
- [ ] Discovery Coordinator: full rewrite v2.0.9 (judgment-only, removed accumulation)
- [ ] Validation Coordinator: full rewrite v2.0.9 (packaging-only, removed gate tracking)
- [ ] Factory CTO: rewrite v2.0.5 (CASE A/B distinction)
- [ ] Business Research Agent: rewrite v2.0.5 (per-event rules), v2.0.9 (more_data_needed handler)
- [ ] Lightweight Spec Agent: rewrite v2.0.5 (unique-per-vertical enforcement)
- [ ] Spec Auditor: rewrite v2.0.6 (three-tier validation framework)
- [ ] Analysis Agent: CREATED v2.0.17 (was missing entirely)
- [ ] Market Research Agent: rewrite v2.0.28 (dual-lens assessment: SaaS gap + automation-micro)
- [ ] Holding DevOps: updated v2.0.22 (migration safety), v2.0.20 (agent_message for results)
- [ ] OpCo DevOps: updated v2.0.22 (rollback policy), v2.0.20 (results via agent_message)
- [ ] OpCo CTO: updated v2.0.22 (cycle detection handling)

## Runtime Pipeline Coordinator Verification

### Interceptor Handlers (verify all 26+ handlers exist in PipelineCoordinator.Intercept())
- [ ] `handleScanRequested` — directive → campaign creation → sub-agent delegation (v2.0.8)
- [ ] `handleSubAgentComplete` — scan completion accumulation (v2.0.8)
- [ ] `handleScanCompleted` — campaign mode cycling + backpressure check (v2.0.7/v2.0.8)
- [ ] `handleDiscoveryReport` — accumulation of category.assessed/trend.identified/source.scraped (v2.0.8), dual-path assessment for automation_micro (v2.0.28)
- [ ] `handleVerticalDiscovered` — dedup + rubric selection + scoring.requested emission (v2.0.18)
- [ ] `handleScoreDimensionComplete` — ScoringAccumulator + computeComposite() (v2.0.18/v2.0.19)
- [ ] `handleShortlisted` — create ValidationPipeline + validation.started + brand.requested (v2.0.8)
- [ ] `handleResearchCompleted` — set G1 (v2.0.8)
- [ ] `handleResearchRejected` — kill pipeline (v2.0.8)
- [ ] `handleSpecApproved` — set G2, emit cto.spec_review_requested (v2.0.8)
- [ ] `handleSpecValidated` — route spec.validation_passed based on tier (v2.0.9)
- [ ] `handleCTOApproved` — set G3, checkGates() (v2.0.8)
- [ ] `handleCTORevision` — reset G2+G3, increment RevisionCount, max 3 (v2.0.9)
- [ ] `handleCTOVetoed` — kill pipeline (v2.0.8)
- [ ] `handleBrandReady` — set G4, checkGates() (v2.0.8)
- [ ] `handleBrandRevision` — reset G4, emit brand.revision_needed (v2.0.12)
- [ ] `handleMoreData` — reset G1, emit validation.more_data_needed (v2.0.9)
- [ ] `handleSpecRevisionNeeded` — inner loop tracking, max 5 cycles (v2.0.12)
- [ ] `handleDedupResolved` — match dedup_id, process pending candidate (v2.0.21)
- [ ] EC rejection filtering on `vertical.scored` — rejections to scoring_digest_buffer, not EC (v2.0.24)
- [ ] `computeComposite()` — hard gates, per-rubric weights, per-rubric viability floors (v2.0.19/v2.0.25)

### State Machine Guards (verify these invariants are enforced in code)
- [ ] RevisionCount max 3 (outer CTO loop) — escalate to mailbox on breach
- [ ] InnerRevisionCount max 5 (BRA↔LSA↔Reviewer loop) — park vertical on breach
- [ ] SpecVersion counter — stale spec events dropped silently (v2.0.11)
- [ ] Post-packaging guard — all gate events dropped after Status=packaged/rejected
- [ ] scan.completed with no matching campaign — graceful no-op
- [ ] Campaign backpressure — pause at 5+ pending mailbox items, resume on decision
- [ ] Crash recovery — RecoverFromCrash() replays unreceipted pipeline events (v2.0.11)
- [ ] Packaging timeout — retry at 10min, escalate at 20min (v2.0.11)
- [ ] OpCo cycle detection — 5 same-type events in 4h → cycle_limit_reached (v2.0.22)

## EventSchemaRegistry Verification

- [ ] **91+ explicit schemas** — all agent-emitted events should have explicit schemas (v2.0.21). Verify against `event-catalog.yaml` (129 events total, ~91 agent-emitted need schemas).
- [ ] **33 events added in v2.0.29 audit** — verify these are in the registry (see `event-catalog.yaml` section "EVENTS DISCOVERED DURING CONTRACT EXTRACTION")

## Event Routing Classification (§4.2)

- [ ] `isFactoryEvent()` uses **type prefix whitelist**, NOT vertical_id presence (v2.0.4)
- [ ] Factory prefixes: `system.`, `scan.`, `vertical.`, `scoring.`, `validation.`, `research.`, `spec.`, `spec_review.`, `cto.`, `brand.`, `template.`, `budget.`, `human_task.`, `analyst.`, `portfolio.`, `source.`, `score.`, `category.`, `trend.`, `devops.`, `opco.`, `campaign.`, `dedup.`, `synthesis.`, `market_research.`, `trend_research.`, `scanner.`, `mailbox.`
- [ ] OpCo internal: short names without dots (`bug_reported`, `build_complete`, etc.) + `qa.*` + `inbound.*`

## Observability Verification

- [ ] `pipeline_transitions` table written by every interceptor handler (before/after state, drop reasons)
- [ ] `runtime_log` table written by EventBus, interceptors, AgentManager, scheduler
- [ ] Dashboard Tab 2 shows per-vertical pipeline cards with gate status
- [ ] Telegram alerts for: runtime errors, stuck pipelines, budget thresholds, stale mailbox items
- [ ] `scoring_digest_buffer` read at digest time for rejection summary

## Cross-Reference Integrity

- [ ] Every agent in `agent-tools.yaml` has a matching config file and roster entry
- [ ] Every event in `event-catalog.yaml` has a matching EventSchemaRegistry entry
- [ ] `ddl-canonical.sql` matches the actual database schema (run diff)
- [ ] `agent-tools.yaml` emit_events × `event-catalog.yaml` = zero gaps (validated v2.0.29)
- [ ] `agent-tools.yaml` subscriptions × `event-catalog.yaml` = zero gaps (validated v2.0.29)
