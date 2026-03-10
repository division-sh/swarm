# EmpireAI v2.0.45 — Typed Payloads + MRA Prompt Calibration

**Version:** 2.0.45
**Previous:** 2.0.44
**Date:** 2026-03-05

## Summary

Two changes driven by the first live corpus campaign:

1. **Typed payloads in event-catalog.yaml.** Payload fields now carry JSON Schema types (string, number, integer, boolean, object, array) instead of bare field names. 197 types from the Go EventSchemaRegistry, 403 inferred from naming conventions. Eliminates the `{"type": "any"}` backfill that caused Draft 2020-12 schema validation failures across 10 active agents (159 invalid fields).

2. **MRA corpus prompt calibration.** Red flag definitions added with explicit examples of what IS and IS NOT each flag type. Fixes over-flagging of `complex_integration` and `high_feature_count` that caused 7/7 corpus opportunities to be pre-filter rejected. Also adds retention primitive requirements and the `opportunity_pattern` vs `red_flag type` distinction that caused the `compliance_regulatory` enum confusion.

## Touches

contracts:
  - event-catalog.yaml: payload changed from list to typed dict (600 fields). scan.completed/campaign.completed naming standardized (total_discovered → verticals_discovered, added verticals_skipped). opco.spinup_requested founder_directives fixed (array → string). emitter_type field added to all 167 events. vertical.shortlisted emitter corrected (runtime → scoring-node). vertical.discovered gains alternate_emitters: [scoring-node].
  - agent-tools.yaml: opco.escalation_response added to empire-coordinator emit_events (M1). scoring-node subscriptions synced with system-nodes.yaml — added vertical.derived, scoring.contest_resolved (L1). scoring-node produces synced — added vertical.discovered (L2). Documentation header restored (N1).
  - system-nodes.yaml: anti-bias fallback_policy: best_effort added. Emitter convention, emitter_type taxonomy, payload exhaustiveness policy, consumer_type resolution rules, terminal event convention documented in header (L4). ICP workflow_anchor_tokens list added (9 tokens with matching rule).
  - verification-gates.yaml: Gate 7 (agent_prompts) added. 8 gate paths fixed from contracts-v2044/ to contracts/ (L3). Total: 53 gates, 20 automated.
  - prompts/market-research-agent.corpus.md: red flag definitions, capability tiers (Tier 1/2/3), retention primitives, "emit more filter less" guidance, self-filter threshold lowered from 55 to 40.
  - prompts/market-research-agent.md: full rewrite with v2.0.45 payload structure, capability tiers, red flag definitions, retention primitives, self-filter threshold 40.
  - prompts/analysis-agent.md: full rewrite with v2.0.45 universal rubric (11 dimensions), derivation loop instructions (when/how/caps), hard gate definitions.
  - prompts/empire-coordinator.md: derivation awareness added to vertical.scored, vertical.marginal, campaign.completed handlers.
  - All contract headers bumped to 2.0.45

spec:
  - Version bumped to 2.0.45
  - Corpus mode documented as single-mode (no cycling to saas_trend/local_services)
