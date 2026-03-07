# EmpireAI v2.0.48 — Prompt Schema Discipline + Template Conversion

**Version:** 2.0.48
**Previous:** 2.0.47
**Date:** 2026-03-05

## Summary

Two changes: prompt-schema discipline (remove inline payload structures that contradict tool schemas) and template variable conversion for all holdco prompts.

## Changes

### Prompt-schema discipline

Prompts now say "see tool schema" for structured fields instead of describing payload shapes inline. The tool definition sent to the LLM carries the exact schema — prompts should not compete with it. This fixes the `derivation_rationale` string-vs-object mismatch that caused a wasted turn on the BidForge AI scoring run.

**Affected prompts:** analysis-agent, market-research-agent (both modes), trend-research-agent, discovery-coordinator.

**Principle:** Prompts tell agents WHEN and WHY to use a tool. Tool definitions tell them HOW. When a prompt describes payload structure, it contradicts the schema and causes shape mismatches.

### Template variable conversion

6 holdco prompts converted to use {{variable}} syntax referencing prompt-variables.yaml. 33 total template tokens across: empire-coordinator (7), analysis-agent (3), market-research-agent.corpus (10), market-research-agent (9), trend-research-agent (3), discovery-coordinator (1).

9 prompts verified as needing no variables (they receive values via event payloads and tool schemas).

All 19 prompts audited: zero hardcoded values that should be variables, zero inline structures contradicting tool schemas.

## Touches

contracts:
  - prompts/analysis-agent.md: derivation_rationale and build_sketch → "see tool schema". 3 {{variables}}.
  - prompts/market-research-agent.corpus.md: build_sketch, evidence, signal_sources, required_capabilities → "see tool schema". 10 {{variables}}.
  - prompts/market-research-agent.md: same fields cleaned. 9 {{variables}}.
  - prompts/trend-research-agent.md: build_sketch, evidence → "see tool schema". Redundant "MUST be object" warning removed. 3 {{variables}}.
  - prompts/discovery-coordinator.md: inline payloads removed. 1 {{variable}}.
  - prompts/empire-coordinator.md: 7 {{variables}} (unchanged from v2.0.47).
  - prompt-manifest.sha256: regenerated.

## Post-Update Verification

```
cd contracts/prompts && sha256sum -c ../prompt-manifest.sha256
```
