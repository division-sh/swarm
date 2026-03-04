# EmpireAI v2.0.43 — Analyst Derivation Loop + Competition Gap Anchors

**Version:** 2.0.43  
**Previous:** 2.0.42  
**Date:** 2026-02-28  
**Status:** Contracts updated, pending implementation

---

## Summary

Adds an analyst derivation loop that allows the Analysis Agent to propose alternative
opportunity hypotheses during scoring. When the analyst scores a dimension below 65 and
understands why, it can emit up to 2 modified hypotheses per parent that address the
specific weakness. Derived verticals re-enter the scoring pipeline, scored by a different
analyst instance to avoid confirmation bias.

Also refines competition_gap score anchors to account for price-disruption wedge —
distinguishing "crowded at $29/month" from "crowded at $299/month."

---

## Critical Issues Resolved (from v1/v2 review)

### Issue 1: Event Namespace Inconsistency ✅
**Problem:** Original proposal used `opportunity.derived`, `opportunity.discovered`,
`opportunity.scored` but spec uses `vertical.*` namespace throughout.  
**Fix:** All events use `vertical.*` namespace. New event is `vertical.derived`.
No grep-and-replace needed — adapted the proposal to match existing contracts.

### Issue 2: Trigger Point Misalignment ✅  
**Problem:** Proposal said derivation triggers "after analyst emits opportunity.scored"
but analyst doesn't emit that event — ScoringNode emits `vertical.scored`.  
**Fix:** `vertical.derived` is emitted by the analysis-agent after all 11
`score.dimension_complete` events are emitted (i.e., after the analyst finishes
all dimension scoring). The `emit_vertical_derived` tool is available during the
scoring session's final step. The scoring-node handles `vertical.derived` as a
separate pre-filter flow, independent of the per-vertical state machine.

### Issue 3: Unbounded Branching Factor ✅
**Problem:** Width only governed by prompt instruction ("QUALITY OVER QUANTITY").
5 derivations × 5 per child = 250 scoring cycles from 10 initial verticals.  
**Fix:** Hard branching limit of **max 2 derivations per parent** enforced at the
tool execution layer (not just prompt). Combined with depth cap of 2:  
worst case = N × 2 × 2 = 4N total scoring cycles (manageable).  
New verification gate `xval-derived-branching-limit` validates this invariant.

### Issue 4: Schema Alignment ✅
**Problem:** Proposal adds `parent_scores`, `parent_opportunity_name`,
`parent_weak_dimensions` to `discovery_context` but EventSchemaRegistry/DDL
don't know about these.  
**Fix:**  
- DDL: Added `parent_id`, `generation_depth`, `generator_agent_id`,
  `derivation_rationale` columns to `verticals` table with indexes and CHECK constraint.
- Event catalog: `vertical.derived` payload fully specified with all fields.
- `discovery_context` carries `parent_opportunity_name` and
  `parent_weak_dimensions[{dimension, reason}]` — intentionally omits
  `parent_scores` (anti-bias: scoring analyst must NOT see parent's actual scores).
- EventSchemaRegistry entry required (upgrade action `v2043-emit-vertical-derived-tool`).

### Issue 5: Campaign Termination Tracking ✅
**Problem:** Rule "no scoring.requested AND no vertical.derived pending" conceptually
sound but tracking "pending" events across async EventBus is tricky.  
**Fix:** Added `campaign_termination` section to system-nodes with explicit tracking:
- `active_scoring_accumulators`: count of ScoringAccumulator instances with
  state ∈ {waiting, accumulating, evaluating, awaiting_contest_resolution}
  grouped by campaign_id
- `pending_derivation_count`: count of vertical.derived events received but
  not yet processed through derivation_pre_filter, grouped by campaign_id
- Campaign completes when both counters reach 0 (or time/budget cap hit)
- 4 termination rules evaluated in priority order: time_cap → budget_exhausted
  → queue_drain → depth_exhaustion

---

## Contract Changes

### event-catalog.yaml
- ADD: `vertical.derived` event (emitter: analysis-agent, consumer: scoring-node)
  - Full payload specification with 13 fields
  - Constraints: generation_depth_max=2, max_derivations_per_parent=2,
    5 valid modification_types, anti-bias guarantee documented

### agent-tools.yaml
- EDIT: `analysis-agent.emit_events` — added `vertical.derived`

### system-nodes.yaml
- EDIT: `scoring-node.subscribes_to` — added `vertical.derived`
- EDIT: `scoring-node.produces` — added `vertical.discovered` (re-emitted for derived verticals)
- ADD: `scoring-node.analyst_assignment` — generator exclusion logic with Go pseudocode
- ADD: `scoring-node.derivation_pre_filter` — 7 pre-filter checks for vertical.derived
  (depth_cap, signal_strength, blocking_red_flags, icp_positive, evidence_completeness,
  name_dedup, budget_check) with on_pass/on_reject actions
- ADD: `competition_gap_anchors` — revised anchors (20/50/80) with 5 evaluation rules
  (price_floor_comparison, price_gap_threshold, segment_specificity,
  downmarket_tier_check, cost_structure_moat)
- ADD: `campaign_termination` — 4 termination rules with tracking counters

### ddl-canonical.sql
- EDIT: `verticals` table — added columns: parent_id (UUID FK), generation_depth (INT),
  generator_agent_id (TEXT), derivation_rationale (JSONB)
- ADD: `idx_verticals_parent` partial index (WHERE parent_id IS NOT NULL)
- ADD: `idx_verticals_depth` index
- ADD: `chk_generation_depth` CHECK constraint (0-2)
- EDIT: `discovery_mode` column comment — added 'derived' to valid values

### verification-gates.yaml
- ADD: 6 new gates:
  - `xval-derived-depth-cap` (must_pass) — zero verticals with depth > 2
  - `xval-derived-different-analyst` (must_pass) — anti-bias enforcement
  - `xval-derived-has-parent` (must_pass) — referential integrity
  - `xval-derived-rationale-present` (should_pass) — data quality
  - `xval-derived-branching-limit` (must_pass) — max 2 children per parent
  - `xval-vertical-derived-event-in-catalog` (must_pass) — contract consistency

### upgrade-actions.yaml
- ADD: 8 new actions:
  - `v2043-ddl-verticals-derivation-columns` (migrate, must)
  - `v2043-ddl-discovery-mode-derived` (migrate, should)
  - `v2043-scoring-node-vertical-derived-subscription` (code, must)
  - `v2043-scoring-node-analyst-exclusion` (code, must)
  - `v2043-emit-vertical-derived-tool` (code, must)
  - `v2043-analysis-agent-prompt-derivation` (prompt, must)
  - `v2043-competition-gap-anchors` (prompt, must)
  - `v2043-campaign-termination-derivation` (code, should)

---

## What This Does NOT Change

- Rubric dimensions, weights, or thresholds (unchanged)
- Pre-filter checks 1-5 (unchanged, check 6-7 added for derivation)
- EC decision-making logic (enhanced presentation, same authority)
- Scoring Node composite computation (unchanged)
- MRA/Scanner architecture (unchanged — derivation is a scoring-layer feature)
- Contested dimension handling (unchanged)

---

## Deferred Decisions

- **Vertical → Opportunity rename:** Reviewed and agreed in principle but explicitly
  deferred. Not executed in v2.0.43 to avoid massive grep-and-replace risk.
  Will be a separate version bump when undertaken.
- **Multi-analyst exclusion for depth-2:** Currently only excludes immediate parent's
  generator. Excluding both depth-0 and depth-1 generators would require 3+ analyst
  instances — too strict initially. Can be tightened later.
- **EventSchemaRegistry Go code:** Schema entry for `vertical.derived` documented
  in upgrade action but not yet written as Go code (spec ahead of implementation,
  as with other v2.0.42+ schemas).
