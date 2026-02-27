# EmpireAI Upgrade Guide: v2.0.26 → v2.0.34

**For:** Implementer currently on v2.0.26
**From:** Spec writer
**Date:** 2026-02-26
**Archive:** `empireai-v2.0.34.tar.gz` (282KB)

This covers 8 spec revisions (v2.0.27–v2.0.34). Apply in order.

---

## ⚠️ CRITICAL: Use v2.0.34 DDL, not v2.0.33

If you received the v2.0.33 archive earlier, **do not use its `ddl-canonical.sql`**. The 7 runtime tables added in v2.0.32/33 had invented schemas that didn't match the Go structs in the spec. v2.0.34 rewrites all 7 to match exactly. See Step 1.

---

## What's in the archive

```
empireai-v2_0_34.md               # Full spec (13,542 lines)
contracts/
├── agent-tools.yaml              # 28 agents, tools/subscriptions/emit_events
├── event-catalog.yaml            # 153 events, delivery_channel, payloads
└── ddl-canonical.sql             # 36 tables, FK-ordered, empire init runs this
changelog-actions-checklist.md    # Typed actions with VERIFY steps
```

**Rule: contracts win over spec prose.** §17 documents this. The verifier and tests should load these YAML/SQL files directly.

---

## Step 1: DDL — Schema from scratch (v2.0.29 + v2.0.32 + v2.0.34)

Run `contracts/ddl-canonical.sql` on a fresh database. This creates all 36 tables with correct schemas.

The 7 runtime tables now match their Go struct definitions:

| Table | Go struct / usage it matches |
|-------|------------------------------|
| `validation_pipelines` | `ValidationPipeline` struct §4.2.2.2 — G1-G4 boolean gates, revision_count, inner_revision_count, spec_version |
| `scan_accumulators` | `ScanAccumulator` struct §4.2.2.3 — completed_by JSONB, reports JSONB, pending_dedup count |
| `pipeline_receipts` | `writePipelineReceipt()` + `RecoverFromCrash()` — event_id UUID PK, no handler column |
| `pending_dedup_candidates` | `PendingCandidate` struct §4.2.2.3 — candidate JSONB, existing_id, dedup_event_id |
| `runtime_config` | `empire init` config snapshot — id UUID PK, config_yaml TEXT, config_hash |
| `pipeline_processed_events` | Simple idempotency guard — event_id UUID PK |
| `template_prompt_drafts` | Hot-reload pattern — role TEXT PK, prompt TEXT |

Other DDL fixes in v2.0.34:
- [ ] `scan_campaigns.strategic_context` is `JSONB` (not TEXT)
- [ ] `shards.status` has CHECK constraint
- [ ] `spend_ledger.agent_id` has FK to agents(id)
- [ ] `prompt_overrides` and `cycle_counters` have ON DELETE CASCADE

**VERIFY:** `psql -f contracts/ddl-canonical.sql` on fresh DB → 0 errors

---

## Step 2: Agent wiring fixes (v2.0.29 + v2.0.32)

These are subscription/tool gaps found during contract extraction and external review.

- [ ] `deployments.deployed_by` type: UUID → TEXT (agents.id is TEXT)
  ```sql
  ALTER TABLE deployments ALTER COLUMN deployed_by TYPE TEXT;
  ```
- [ ] `holding-devops.yaml` — add `mailbox_send` to tools (prompt says "create mailbox item" but tool was missing)
- [ ] `empire-coordinator.yaml` — add 3 missing subscriptions: `opco.escalation`, `devops.capacity_warning`, `cto.extraction_recommended`
- [ ] `opco-cto.yaml` — add `build_blocked`, `build_progress` to emit_events
- [ ] `opco-marketing.yaml` — add `outreach_digest`, `channel_blocked`, `spend_needed` to emit_events
- [ ] 5 OpCo agents get `mailbox_send`: opco-ceo, opco-marketing, opco-support, opco-head-of-product, opco-head-of-growth
- [ ] EventSchemaRegistry — add 33 missing event schemas (listed in event-catalog.yaml, marked as v2.0.29 audit additions)

**VERIFY:** `grep -r "UUID.*deployed_by\|deployed_by.*UUID" internal/` → 0 results

---

## Step 3: MRA dual assessment (v2.0.28)

`automation_micro` is no longer a separate scan phase. MRA evaluates both SaaS gap AND automation-micro in one pass.

- [ ] `handleScanRequested` — remove `automation_micro` case from mode switch
- [ ] `defaultModes` — change `[automation_micro, saas_gap, saas_trend, local_services]` → `[saas_gap, saas_trend, local_services]`
- [ ] `handleDiscoveryReport` — dual-path from `category.assessed`:
  - SaaS gap `signal_strength ≥ 50` → emit `vertical.discovered` with `mode: "saas_gap"`
  - `automation_micro.signal_strength ≥ 50` → emit second `vertical.discovered` with `mode: "automation_micro"`
  - Both can fire for same subcategory
- [ ] `category.assessed` schema — add optional `automation_micro` object: `{signal_strength, evidence, opportunity_hypothesis}`
- [ ] MRA prompt — complete rewrite: dual-lens (Dimension A: SaaS Gap 5 criteria + Dimension B: Automation-Micro 4 criteria)

**VERIFY:** `modeToRubric["automation_micro"]` still maps to Rubric C (unchanged)

---

## Step 4: Scoring Coordinator cleanup (v2.0.27 + v2.0.34)

SC was absorbed into runtime in v2.0.19. These are leftover references.

- [ ] DELETE: `configs/agents/scoring-coordinator.yaml` if it exists
- [ ] Roster/seed data must NOT spawn a scoring-coordinator agent
- [ ] `handleScoreDimensionComplete` calls `computeComposite()` directly (no SC delivery)

**VERIFY:** `grep -ri "scoring.coordinator\|scoring-coordinator\|ScoringCoordinator" internal/ configs/` → 0 results

---

## Step 5: Verifier overhaul (v2.0.30 + v2.0.31)

The verifier now reads `delivery_channel` from event-catalog.yaml instead of inferring delivery from comments.

**Discovery event emitter corrections:**
- [ ] `scan.completed` → emitter: runtime, consumer: runtime (NOT Discovery Coordinator)
- [ ] `scan.started` → emitter: runtime, consumer: runtime
- [ ] `vertical.discovered` → emitter: runtime, consumer: runtime
- [ ] DC producer registry → DC only emits `dedup.resolved`, `synthesis.resolved`

**delivery_channel rules (6 values):**

| Channel | Count | Verifier check |
|---------|-------|----------------|
| `eventbus_static` | 64 | Consumer has matching subscription ✓ |
| `eventbus_routing_table` | 23 | Emitter has emit_event; skip consumer sub check |
| `runtime` | 21 | Interceptor handler exists; skip consumer sub check |
| `agent_message` | 2 | Skip subscription check entirely |
| `mailbox` | 7 | Terminal — skip orphan check |
| `audit` | 14 | Terminal — skip orphan check |

- [ ] Verifier reads `delivery_channel` from YAML, applies rules per channel
- [ ] Terminal events (`audit`, `mailbox`) exempt from ORPHAN_EMISSION
- [ ] Three-tier orphan scope: factory strict → OpCo static strict → OpCo routing/agent_message emitter-only

---

## Step 6: Agent-tools reconciliation (v2.0.32)

- [ ] **No glob patterns.** EventBus doesn't support them. Replace `opco.*.steady_state_reached` → `opco.steady_state_reached`
  - **VERIFY:** `grep -r "opco\.\*\." configs/` → 0 results
- [ ] OpCo workers use `subscriptions_bootstrap` (routing table), not static EventBus. Workers with `subscriptions_bootstrap: []` receive work via `agent_message` — by design
- [ ] Spec Auditor: `model_tier: sonnet`, `type: holding`
- [ ] `scanner-agent`: ephemeral template, id pattern `scanner-{type}-{scan_id}`, spawned per scan type
- [ ] Chief of Staff: 8 subscriptions (see agent-tools.yaml)
- [ ] OpCo CTO: 7 routing-table subscriptions (see agent-tools.yaml)
- [ ] `prebrand.yaml` → rename to `pre-brand-agent.yaml`

---

## Step 7: Event catalog fixes (v2.0.33)

- [ ] `trend.identified` schema — add `market_intersection` field
- [ ] `devops.rollback_failed` consumer: `[holding-devops, audit]` (not just audit)
- [ ] `template.migration_completed` / `template.migration_failed` consumer: `[empire-coordinator, audit]`
- [ ] EC additional subscriptions: `board.directive`, `board.chat`
- [ ] Operations Analyst: add `budget.threshold_crossed` subscription
- [ ] OpCo CEO: add `founder_input.response`, `opco.escalation_response` subscriptions
- [ ] Holding DevOps: add `ops.agent_failed` subscription

---

## Step 8: Spec structure changes (v2.0.33 + v2.0.34)

These are reference changes — no code impact, but know about them:

- **§17 Contracts** — new section documenting authority rules, test verification, maintenance
- **§18 Open Questions** — renumbered (was §17)
- **§3.1 diagrams** — "Scoring" removed from both ASCII diagrams (replaced with "Spec Auditor" / "Scoring Pipeline (runtime)")
- **§16 directory tree** — `internal/agents/` removed (was stale), replaced with `internal/pipeline/` matching PipelineCoordinator code
- **v2.0.32 changelog** — was empty, now fully documented

---

## Quick validation after all steps

```bash
# 1. Fresh DB from canonical DDL
psql -f contracts/ddl-canonical.sql   # → 0 errors

# 2. No scoring coordinator ghosts
grep -ri "scoring.coordinator\|scoring-coordinator" internal/ configs/  # → 0

# 3. No glob subscriptions
grep -r "opco\.\*\." configs/  # → 0

# 4. No UUID references to agents.id
grep -r "UUID.*agents\|agents.*UUID" internal/  # → 0

# 5. Cross-validate contracts (load YAML, check every emit_event/subscription has catalog entry)
empire verify --contracts contracts/   # → 0 FAIL, 0 WARN
```

---

## Known open items (not yet in spec — flagged by reviewer)

These were identified in the v2.0.33 review but deferred. Track them but don't block on them:

1. **subscriptions_bootstrap data:** 7 of 9 OpCo workers have `subscriptions_bootstrap: []` in the contract but their configs + routes.yaml have real subscriptions. PM=3, Tech Writer=1, Backend=1, Frontend=1, QA=1, DevOps=3, Marketing=3. Needs cross-referencing.
2. **`schedule` tool:** Missing from 6 worker entries (PM, CTO, Tech Writer, Backend, Frontend, DevOps). Configs and prompts reference it.
3. **`human_task_request` tool:** Missing from OpCo CEO entry. Config has it.
4. **6 missing events:** `opco.routing_updated`, `customer_message`, `human_task.assigned`, `review.product_spec_feedback`, `review.deploy_feedback`, `runtime.reset`
5. **~8 payload mismatches** between catalog and schemas (scan.requested missing campaign_context, source.scraped field name mismatch, etc.)
6. **~66% of event schemas** still use permissive default template — should be tightened over time
7. **OpCo Support** subscription name mismatch: contract says `inbound.{vertical}.whatsapp_message`, config says `customer_message`
