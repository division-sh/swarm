# EmpireAI v2.0.44 — Corpus Discovery Mode + Platform Capability Registry + Pre-filter Hardening

**Version:** 2.0.44  
**Previous:** 2.0.43  
**Date:** 2026-03-04  
**Status:** Contracts updated, pending implementation

---

## Summary

Adds corpus discovery mode — a simplified discovery path where pre-collected demand signals (job postings, app store reviews, forum threads) are fed into the same pipeline that taxonomy-walking uses. Validates the thesis that better inputs produce better outputs: two corpus campaigns produced 3x higher viable rates (29.1% vs 10.5%) compared to the original taxonomy scan (0% shortlisted).

Also introduces the Platform Capability Registry (defining what EmpireAI can build today vs. planned capabilities), Opportunity Pattern Classification (7 archetypes for product opportunities), and pre-filter hardening based on empirical results from 618 total signals analyzed.

## Changes

### Corpus Discovery Mode (§6.1.1, §4.2.2.3, §4.2.2.2, §B.10.1)
- Added `corpus` to `discovery_mode` enum
- Added `corpus_path` field to `scan.requested` payload
- Added `corpus` to `expectedAgentsPerMode` (1 agent — MRA)
- Added `corpus` to `completionSignals` (market_research.scan_complete)
- Added corpus case to `handleScanRequested` — reads JSONL, batches at 25, dispatches to MRA
- Added `corpus_signals` and `mode` fields to `market_research.scan_assigned` payload
- Added MRA corpus mode prompt variant (§B.10.1) — interprets raw signals instead of walking taxonomy
- Added §6.1.1 documenting corpus mode flow, file format, and validated results

### Platform Capability Registry (§1 — Design Principles subsection)
- New section defining current capabilities (Tier 1: pure digital), planned (Tier 2: async messaging), and future (Tier 3: voice/browser automation)
- Cost profiles per capability
- Registry usage: MRA tags opportunities with `required_capabilities`, EC uses for portfolio sequencing

### Opportunity Pattern Classification (§3.2.2)
- New section defining 7 opportunity archetypes: platform_parasitic, freelancer_replacement, data_asymmetry, api_middleware, compliance_regulatory, ai_wrapper, workflow_automation, unknown
- Pattern validation data from corpus runs (ai_wrapper highest avg score, workflow_automation most common)
- MRA tags every opportunity with primary pattern
- Scoring rubric renumbered to §3.2.3

### Pre-filter Hardening (§4.2.2.3)
- Signal threshold raised from 50 to 55 (stage 2)
- New blocking red flags (stage 3): phone_led_sales, enterprise_procurement, relationship_networking, physical_presence_required, support_mode_phone_video
- Evidence completeness raised to ≥2 independent source URLs (stage 5)
- New retention primitive gate (stage 6): requires ≥1 of recurring_data, workflow_embedding, integration_lock_in, compliance_cadence, team_collaboration
- Pre-filter stages renumbered (now 9 stages, was 8)

### Event Schema Changes
- `category.assessed`: added opportunity_pattern, signal_sources, required_capabilities
- `market_research.scan_assigned`: added mode, corpus_signals
- `scan.requested`: added corpus_path, corpus to mode enum

### DDL Changes
- `verticals` table: added `opportunity_pattern TEXT`, `signal_sources JSONB`, `required_capabilities JSONB`

## Touches

contracts:
  - event-catalog.yaml (category.assessed, market_research.scan_assigned, scan.requested)
  - ddl-canonical.sql (verticals table — 3 new columns)
  - verification-gates.yaml (14 new gates)
  - upgrade-actions.yaml (10 new actions)
  - agent-tools.yaml (no changes — MRA subscription already correct)
  - agent-config-map.yaml (no changes needed)

spec:
  - §1 Design Principles → new Platform Capability Registry subsection
  - §3.2.2 Opportunity Pattern Classification (new, renumbers Scoring Rubric to §3.2.3)
  - §4.2.2.2 handleScanRequested → corpus case added
  - §4.2.2.3 Discovery Accumulation → expectedAgentsPerMode, completionSignals, pre-filter cascade updated
  - §5.4 Factory Events → category.assessed payload updated
  - §6.1.1 Corpus Discovery Mode (new section)
  - §B.10.1 MRA Corpus Mode Prompt Variant (new appendix entry)

## Empirical Validation

This version is uniquely grounded in empirical data:
- **v1 corpus run (228 signals):** 10.5% viable rate, 4 distinct products, 1 scoring 75+ (InvoiceBridge at 76). 89.5% NOT_VIABLE driven by poor input targeting (warehouse workers, receptionists, forklift operators).
- **v2 corpus run (390 signals):** 29.1% viable rate, 10 distinct products, 3 scoring 75+ (BillBridge Legal 78, BuildBooks 77, BidCraft+PermitPulse 75). 30 industries, 169 cities, no single industry >17%.
- **Key finding:** "The agents are smart, the inputs were broken." Better targeting (Tier A proven titles + Tier B exploratory) tripled viable rate without any pipeline changes.
- **Construction vertical dominance:** 22 viable signals (19.5%), highest avg signal strength (66.4), supports 3-4 products selling to same ICP.
- **Tier 2 messaging impact:** Adding email/SMS would improve 84% of viable opportunities by avg +12 percentage points automation.
