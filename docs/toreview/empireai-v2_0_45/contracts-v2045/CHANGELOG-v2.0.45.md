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
  - event-catalog.yaml: payload changed from list of field names to dict of field_name: type. All 167 events, 600 fields typed.
  - prompts/market-research-agent.corpus.md: RED FLAG DEFINITIONS section added with blocking/passthrough classification, positive and negative examples for complex_integration and high_feature_count, retention primitives, enum value guardrails
  - All contract headers bumped to 2.0.45

spec:
  - Version bumped to 2.0.45
