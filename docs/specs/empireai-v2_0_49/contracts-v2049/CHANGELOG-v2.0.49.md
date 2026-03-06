# EmpireAI v2.0.49 — Gemini Audit Fixes + Scanner Prompt + Consistency Hardening

**Version:** 2.0.49
**Previous:** 2.0.48
**Date:** 2026-03-05

## Summary

Addresses findings from three-reviewer audit cycle (Claude auditor, implementer feedback, Gemini reviewer). Adds scanner-agent prompt, fixes derivation pre-filter gap, resolves OpCo leadership routing ambiguity, and corrects derivation handler ownership.

## Changes

### From Gemini audit (4 findings)

1. **retention_primitive gate added to derivation_pre_filter** (High). Derived verticals now pass the same retention check as discovered verticals. Prevents "zombie" verticals without SaaS defensibility entering scoring.

2. **vertical.derived handler ownership corrected** (High). upgrade-actions.yaml now correctly points to ScoringNode, not PipelineCoordinator. References RFC-001 v2 deferred event chaining rationale.

3. **OpCo leadership routing cleaned** (Medium). All subscriptions_bootstrap entries moved to static subscriptions for CEO, CoS, HoP, HoG. Header clarified: "leadership uses static only, no bootstrap." Eliminates double-delivery risk.

4. **Branching terminology fixed** (Medium). analysis-agent prompt: "per vertical" → "per parent vertical" + templated with {{max_derivations_per_parent}}. Matches xval-derived-branching-limit gate.

### From implementer feedback (2 findings)

5. **scanner-agent.md prompt created**. Was the only active agent without a prompt file. Covers Phases 1-3 (synthetic adapter) and Phase 4+ (real scraping). Prompt count: 19 → 20.

6. **empireai-current.md symlink** documented as upgrade action (v2047-symlink-current-spec). Implementer must create: `ln -s empireai-v2_0_49.md empireai-current.md`.

## Touches

contracts:
  - system-nodes.yaml: retention_primitive check added to derivation_pre_filter
  - upgrade-actions.yaml: v2048-vertical-derived-handler corrected to ScoringNode
  - agent-tools.yaml: OpCo leadership subscriptions_bootstrap → subscriptions. Header clarified.
  - prompts/analysis-agent.md: "per vertical" → "per parent vertical" + {{max_derivations_per_parent}}
  - prompts/scanner-agent.md: NEW (20th prompt file)
  - prompt-manifest.sha256: regenerated (20 files)
  - All contract headers bumped to 2.0.49

spec:
  - Version bumped to 2.0.49

## Post-Update Verification

```
cd contracts/prompts && sha256sum -c ../prompt-manifest.sha256
```

All 20 files must show OK.

## Remaining Known Gaps

- 9 OpCo agent prompts missing (opco-pm, opco-cto, opco-tech-writer, opco-backend, opco-frontend, opco-qa, opco-devops, opco-marketing, opco-support) — fix before OpCo spinup
- scoring-node has no prompt (correct — it's a Go system node, not an LLM agent)
- Campaign time_cap: contract says 24h, implementer needs to verify runtime matches

### From runtime errors (mailbox_send)

7. **mailbox_send formalized as universal Tier 2 tool** (Critical). Was referenced in prose but had no contract definition — no schema, no type enum, not in universal tools list. Now: defined in spec §4.5 with full schema (type enum, priority enum, subject, payload), added to agent-tools.yaml header, and documented as universal tool alongside agent_message. MCP tool gateway must generate the tool definition with the type enum matching the DB CHECK constraint.

## Additional Touches

contracts:
  - agent-tools.yaml: mailbox_send schema added to header as universal tool
  - prompt-variables.yaml: mailbox_types and mailbox_priorities added
  - upgrade-actions.yaml: v2049-mailbox-send-schema and v2049-scanner-prompt added

spec:
  - §4.5: mailbox_send added to universal Tier 2 tools with full schema table
  - Universal tools line updated: agent_message → agent_message, mailbox_send

### Tool schema contract (new)

8. **tool-schemas.yaml created** (Critical). Defines input schemas for all 21 Tier 2 tools (2 universal + 19 per-agent). The MCP tool gateway should generate tool definitions from this file — same pattern as emit_* tools from EventSchemaRegistry. Eliminates the class of bug where prompts describe tool parameters differently from the runtime implementation. 10 tools have enum-constrained fields that the schema enforces.

## Additional Touches (tool-schemas)

contracts:
  - tool-schemas.yaml: NEW — 21 tool schemas with JSON Schema draft 2020-12 format
  - spec-writer-guide.md: section 2.9 added for tool-schemas.yaml
  - upgrade-actions.yaml: v2049-tool-schemas added
