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
  - event-catalog.yaml: ADD corpus_path to scan.requested payload; ADD corpus to mode enum; ADD corpus_signals and mode to market_research.scan_assigned; ADD opportunity_pattern, signal_sources, required_capabilities to category.assessed; ADD assigned_analysis_agent_id and excluded_analysis_agent_id to scoring.requested
  - ddl-canonical.sql: ADD opportunity_pattern TEXT, signal_sources JSONB, required_capabilities JSONB to verticals table
  - verification-gates.yaml: 13 new v2.0.44 gates + 4 automated gate definitions; `automated` field added to all 50 gates; spec_version bumped to 2.0.44; YAML structure fixed (v2.0.44 gates converted from map syntax to list syntax); v2.0.43 gate path reference fixed (contracts-v2043 → contracts-v2044)
  - upgrade-actions.yaml: 10 new v2.0.44 actions; header version bumped to 2.0.44/2.0.43; mislabeled v2.0.42 section header corrected
  - system-nodes.yaml: ADD fallback_policy: best_effort to analyst_assignment; ADD fallback_description with logging requirements; version attribution corrected (v2.0.43 features re-attributed from v2.0.44)
  - agent-tools.yaml: no changes — MRA subscription already correct
  - agent-config-map.yaml: no changes needed
  - spec-writer-guide.md: NEW — spec writer onboarding guide with 3 corrections applied (upgrade-actions format note, additionalProperties policy, automated gates documentation)
  - CHANGELOG-v2.0.43.md: format standardized (## Contract Changes → ## Touches)

spec (prose changes):
  - §1 Design Principles → new Platform Capability Registry subsection (Tier 1/2/3 capabilities)
  - §3.2 pre-filter summary → updated to v2.0.44 rules (threshold 55, 9 stages, retention primitive, 5 new blocking flags)
  - §3.2.2 Opportunity Pattern Classification (new section, 7 archetypes)
  - §3.2.3 Scoring Rubric → slimmed: per-dimension tables removed, replaced with summary + pointer to system-nodes.yaml
  - §4.2.2.2 handleScanRequested → corpus case added
  - §4.2.2.3 Discovery Accumulation → expectedAgentsPerMode, completionSignals, pre-filter cascade updated
  - §4.5.1 Event Emission Tools → EventSchemaRegistry Go code block removed (~1,367 lines), replaced with pointer to event_emit_tools.go + event-catalog.yaml + TestContractCompliance
  - §5.4-5.6 Event tables → consolidated into §5.4 Event Catalog Summary (~486 lines removed), 15-row domain summary table + delivery channel patterns + pointer to event-catalog.yaml
  - §6.1.1 Corpus Discovery Mode (new section)
  - §8.1 Core Tables → inline DDL removed (~546 lines), replaced with prose table catalog grouped by domain + pointer to ddl-canonical.sql
  - §4.2.2 runtime tables → 5 embedded DDL blocks replaced with one-line pointers to ddl-canonical.sql
  - §6.4 runtime_log DDL → replaced with column summary + pointer
  - Appendix B inline changelog → removed (~1,991 lines), replaced with pointer to standalone CHANGELOG files
  - §15.0 Event Wiring Verifier → Go test implementation reference added
  - §17.3 Test Verification Against Contracts → rewritten with 6 automated compliance gates
  - §B.10.1 MRA Corpus Mode Prompt Variant (new appendix entry)
  - Cross-references fixed: 6 stale §5.5 references, 3 stale §8.1 references, 2 stale §4.2.2.3 EventSchemaRegistry references, verifier Python extract_schemas() updated

spec slimming summary:
  - Inline changelog: −1,991 lines
  - Event tables §5.4-5.6: −485 lines
  - EventSchemaRegistry §4.5.1: −1,367 lines
  - Inline DDL §8.1 + §4.2.2 + §6.4: −625 lines
  - Scoring rubric detail tables §3.2.3: −139 lines
  - Total: 14,558 → ~9,950 lines (−4,600 lines / 32% reduction)

## Empirical Validation

This version is grounded in empirical data from two corpus campaigns:
- **v1 corpus run (228 signals):** 10.5% viable rate, 4 distinct products. 89.5% NOT_VIABLE driven by poor input targeting.
- **v2 corpus run (390 signals):** 29.1% viable rate, 10 distinct products, 3 scoring 75+ (BillBridge Legal 78, BuildBooks 77, BidCraft+PermitPulse 75).
- **Key finding:** Better targeting tripled viable rate without pipeline changes.
- **Tier 2 messaging impact:** Adding email/SMS would improve 84% of viable opportunities by avg +12pp automation.
