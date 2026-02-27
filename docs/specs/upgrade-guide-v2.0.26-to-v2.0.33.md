# EmpireAI Upgrade Guide: v2.0.26 → v2.0.33

**For:** Implementer currently on v2.0.26
**From:** Spec writer
**Date:** 2026-02-26

This covers 7 spec revisions. Read in order — later versions depend on earlier ones.

---

## TL;DR — What changed

The spec now ships **three machine-readable contract files** that are authoritative over prose. Your verifier and tests should load these directly. The MRA does dual assessment in one pass. The Scoring Coordinator ghost is fully cleaned up. Several subscription/tool gaps were found and fixed. An external reviewer audited v2.0.30 and we've resolved all findings.

**New deliverables in the archive:**

```
contracts/
├── agent-tools.yaml         # 28 agents, every subscription/tool/emit_event
├── event-catalog.yaml       # 161 events, every emitter/consumer/delivery_channel/payload
└── ddl-canonical.sql        # 36 tables, FK-ordered, `empire init` runs this directly
changelog-actions-checklist.md  # 227 lines, typed & executable
```

**Rule: contracts win over prose.** If the spec prose says one thing and a contract file says another, the contract file is correct.

---

## Step 1: Drop in the contract files (v2.0.29)

Copy `contracts/` directory to your repo root. These are the new source of truth.

### Immediate code changes

- [ ] **MIGRATION:** `ALTER TABLE deployments ALTER COLUMN deployed_by TYPE TEXT;`
  - Was UUID, but `agents.id` is TEXT. Type mismatch would cause FK errors.
  - VERIFY: `grep -r "UUID.*agents\|agents.*UUID" internal/` → 0 results

- [ ] **EDIT:** `configs/agents/holding-devops.yaml` — add `mailbox_send` to tools list
  - Agent prompt says "create a mailbox item" for destructive DDL but the tool wasn't wired

- [ ] **EDIT:** `configs/agents/empire-coordinator.yaml` — add 3 subscriptions:
  - `opco.escalation`
  - `devops.capacity_warning`
  - `cto.extraction_recommended`
  - These events target EC but EC wasn't subscribed. Events were silently dropped.

- [ ] **EDIT:** EventSchemaRegistry — add 33 missing event schemas
  - Full list in `contracts/event-catalog.yaml` under "EVENTS DISCOVERED DURING CONTRACT EXTRACTION"
  - Includes: all budget.*, human_task.*, portfolio.*, cto.*, devops.* events, plus OpCo internal events

---

## Step 2: MRA dual assessment (v2.0.28)

`automation_micro` is no longer a separate scan phase. MRA now evaluates both SaaS gap AND automation-micro in a single pass per subcategory.

- [ ] **EDIT:** `handleScanRequested` — remove `automation_micro` case from the mode switch
- [ ] **EDIT:** `defaultModes` — change from `[automation_micro, saas_gap, saas_trend, local_services]` to `[saas_gap, saas_trend, local_services]`
- [ ] **EDIT:** `handleDiscoveryReport` — when processing `category.assessed`, check TWO paths:
  - SaaS gap `signal_strength ≥ 50` → emit `vertical.discovered` with `mode: "saas_gap"`
  - `automation_micro.signal_strength ≥ 50` → emit second `vertical.discovered` with `mode: "automation_micro"`
  - Both can fire for the same subcategory
- [ ] **EDIT:** `category.assessed` in EventSchemaRegistry — add optional `automation_micro` object with `signal_strength`, `evidence`, `opportunity_hypothesis`
- [ ] **EDIT:** MRA prompt (§B.10) — complete rewrite. Now does dual-lens: Dimension A (SaaS Gap, 5 criteria) + Dimension B (Automation-Micro, 4 criteria)
- [ ] VERIFY: `modeToRubric["automation_micro"]` still maps to Rubric C — unchanged
- [ ] VERIFY: `expectedAgentsPerMode["automation_micro"]` still = 1 — backward compat

---

## Step 3: Scoring Coordinator ghost cleanup (v2.0.27)

SC was absorbed into runtime in v2.0.19, but stale references remained. If you have any of these, remove them:

- [ ] DELETE: `configs/agents/scoring-coordinator.yaml` if it exists
- [ ] VERIFY: `grep -r "scoring.coordinator\|scoring-coordinator\|ScoringCoordinator" internal/ configs/` → 0 results
- [ ] VERIFY: Roster/seed data does not spawn a scoring-coordinator agent
- [ ] VERIFY: `handleScoreDimensionComplete` calls `computeComposite()` directly (no SC delivery)

---

## Step 4: Verifier alignment (v2.0.30)

Fix stale emitter/consumer attributions that cause false ORPHAN_EMISSION warnings.

- [ ] **EDIT:** EventSchemaRegistry — these events' emitter is `runtime`, not Discovery Coordinator:
  - `scan.completed` — emitter: runtime, consumer: runtime
  - `scan.started` — emitter: runtime, consumer: runtime
  - `vertical.discovered` — emitter: runtime, consumer: runtime
- [ ] **EDIT:** DC producer registry — DC only emits `dedup.resolved` and `synthesis.resolved`
- [ ] **ADD:** Terminal event policy to verifier:
  - `consumer: audit` → skip ORPHAN_EMISSION check
  - `consumer: mailbox` → skip ORPHAN_EMISSION check
  - `consumer: runtime` (intercepted) → skip consumer subscription check
- [ ] **ADD:** Three-tier orphan scope:
  - Tier 1: factory/holding events → strict subscription check
  - Tier 2: OpCo static events → strict subscription check
  - Tier 3: OpCo routing-table/agent_message events → emitter-only check

---

## Step 5: delivery_channel field (v2.0.31)

Every event in `event-catalog.yaml` now has a `delivery_channel` field. **This is what your verifier should read** instead of inferring from `routing`/`intercepted`/comments.

Six values:

| Channel | Count | Verifier rule |
|---------|-------|---------------|
| `eventbus_static` | 64 | Check consumer has matching subscription |
| `eventbus_routing_table` | 23 | Check emitter has event in emit_events; skip consumer sub check |
| `runtime` | 21 | Check interceptor handler exists; skip consumer sub check |
| `agent_message` | 2 | Skip subscription check entirely |
| `mailbox` | 7 | Terminal — skip orphan check |
| `audit` | 14 | Terminal — skip orphan check |

- [ ] **EDIT:** Verifier — read `delivery_channel` from YAML, apply rules per table above
- [ ] VERIFY: `cto.tech_spec_feedback` has `delivery_channel: agent_message` (was ambiguously `routing: static`)
- [ ] VERIFY: `cto.tech_spec_review_requested` has `delivery_channel: agent_message`

---

## Step 6: Agent-tools reconciliation (v2.0.32)

Major contract sync. The reviewer found 30+ discrepancies between contracts and configs. All fixed in `agent-tools.yaml` v2.0.33:

- [ ] **EventBus does NOT use glob patterns.** If your configs have `opco.*.steady_state_reached` or `opco.*.cto_escalation`, replace with direct event names: `opco.steady_state_reached`, `opco.escalation`
  - VERIFY: `grep -r "opco\.\*\." configs/` → 0 results

- [ ] **OpCo worker subscription model:** Workers use `subscriptions_bootstrap` (routing table), not static EventBus subscriptions. The contract has a new field for this. Workers with `subscriptions_bootstrap: []` receive work exclusively via `agent_message` — this is by design, not a bug.

- [ ] **Spec Auditor:** Must be `model_tier: sonnet`, `type: holding` (was incorrectly haiku/factory)

- [ ] **Add `mailbox_send`** to tools for: opco-ceo, opco-marketing, opco-support, opco-head-of-product, opco-head-of-growth

- [ ] **Add `scanner-agent`** to agent registry — ephemeral template, id: `scanner-{type}-{scan_id}`, spawned per scan type (google_maps, instagram, reviews, directories, yelp)

- [ ] **Chief of Staff:** 8 subscriptions (product_report, growth_report, feature_deployed, churn_risk, build_complete, prelaunch_ready, support_critical, channel_blocked)

- [ ] **OpCo CTO:** 7 routing-table subscriptions (product_spec_ready, technical_spec_ready, qa.validation_passed, qa.validation_failed, bug_reported, feature_request, cycle_limit_reached)

- [ ] **Add missing configs:**
  - `configs/agents/market-research.yaml`
  - `configs/agents/trend-research.yaml`
  - Consider: `prebrand.yaml` → `pre-brand-agent.yaml` (naming inconsistency)

---

## Step 7: Remaining event catalog fixes (v2.0.33)

- [ ] **EDIT:** `trend.identified` schema — add `market_intersection` field to payload
- [ ] **EDIT:** `devops.rollback_failed` — Holding DevOps subscribes to this (for retry/escalation). Consumer is `[holding-devops, audit]`, not just `audit`
- [ ] **EDIT:** `template.migration_completed` / `template.migration_failed` — EC subscribes to both. Consumer is `[empire-coordinator, audit]`
- [ ] **EDIT:** Scanner event emitters (scanner.*.scan_complete) — emitter is `scanner-agent` template, not individual `scanner-{type}` entries
- [ ] **ADD** to EC subscriptions: `board.directive`, `board.chat`
- [ ] **ADD** to Operations Analyst subscriptions: `budget.threshold_crossed`
- [ ] **ADD** to OpCo CEO subscriptions: `founder_input.response`, `opco.escalation_response`
- [ ] **ADD** to Holding DevOps subscriptions: `ops.agent_failed`

---

## Step 8: DDL — 7 missing tables (v2.0.32)

`ddl-canonical.sql` now has all 36 tables. If your DB is missing any of these, create them:

- [ ] `runtime_config` (migration 003)
- [ ] `pipeline_receipts` (migration 009) — crash recovery depends on this
- [ ] `scan_accumulators` (migration 012)
- [ ] `pending_dedup_candidates` (migration 012)
- [ ] `validation_pipelines` (migration 012)
- [ ] `pipeline_processed_events` (migration 012)
- [ ] `template_prompt_drafts` (migration 011)

Also:
- [ ] `verticals` — add `slug TEXT NOT NULL` column + `idx_verticals_slug_geo` unique index
- [ ] `scan_campaigns` — add `directive_id`, `strategic_context`, `deadline_at` columns
- [ ] `routing_rules` — add unique index on `(vertical_id, event_pattern, subscriber_id) WHERE active`
- [ ] `conversations` + `agent_sessions` — add `scope_key TEXT` + scope-aware unique indexes

VERIFY: `psql -f contracts/ddl-canonical.sql` on a fresh database → 0 errors

---

## Step 9: Spec structure changes (v2.0.33)

- New **§17 Contracts** section documents authority rules, test verification patterns, maintenance policy
- **§16 directory structure** now includes `contracts/` directory
- **§18 Open Questions** (was §17)

---

## Validation checklist

After applying all changes, run:

```bash
# 1. Cross-validate contracts
python3 -c "
import yaml
agents = yaml.safe_load(open('contracts/agent-tools.yaml'))
events = yaml.safe_load(open('contracts/event-catalog.yaml'))
# every emit_event has catalog entry
# every subscription has catalog entry  
# every catalog emitter is a real agent
# every event has delivery_channel
"

# 2. Wiring verifier
empire verify --contracts contracts/

# 3. Schema check
psql -f contracts/ddl-canonical.sql  # fresh DB, 0 errors

# Target: 0 FAIL, 0 WARN
```

---

## Files to hand off

Give the implementer `empireai-v2.0.33.tar.gz` which contains:

| File | Lines | Description |
|------|-------|-------------|
| `empireai-v2_0_33.md` | 13,496 | Full spec |
| `contracts/agent-tools.yaml` | 624 | 28 agents |
| `contracts/event-catalog.yaml` | 1,760 | 161 events with delivery_channel |
| `contracts/ddl-canonical.sql` | 746 | 36 tables |
| `changelog-actions-checklist.md` | 227 | Executable checklist with VERIFY steps |
