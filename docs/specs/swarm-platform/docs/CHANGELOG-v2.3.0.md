# EmpireAI v2.3.0 — Structural Simplification + Permissions + Genericity

**Version:** 2.3.0
**Previous:** 2.2.2
**Date:** 2026-03-08

## Summary

Major structural simplification: 15 contract files reduced to 8. Permissions model, persistence model, directive routing, event schema format, and prompt templating formalized in platform-spec. All redundant and non-runtime files eliminated. Prose and guide rewritten for new structure.

## Structural Changes

### Files eliminated (9)
| File | Reason |
|------|--------|
| workflow.yaml | State machine derived from nodes.yaml handlers |
| hooks.yaml | 22 guards — 18 orphaned, 4 inlined on handlers, platform builtins to platform-spec |
| ddl.sql (x2) | Implementer artifact — entity_schema + state_schema are the contracts |
| agent-config-map.yaml | Convention-derived: configs/agents/{agent-id}.yaml |
| upgrade-actions.yaml | Historical migration tracker, not a runtime contract |
| verification-gates.yaml | Test infrastructure — belongs in Go test suite |
| tooling.lock | Redundant with package.yaml version |
| prompt-manifest.sha256 | Orphaned integrity check |

### Files renamed (drop postfix, scoped by directory)
nodes-empire.yaml → nodes.yaml, events-empire.yaml → events.yaml, agents-empire.yaml → agents.yaml, tools-empire.yaml → tools.yaml, policy-empire.yaml → policy.yaml

### events.yaml slimmed (63KB → 24KB)
Routing/consumer/emitter fields removed — derived from agents.yaml + nodes.yaml. Retained payload schemas only. 175 events (3 new for directive routing).

### sql_execute removed
Agents no longer have raw DB access. Platform auto-generates typed persistence tools from entity_schema.

### mailbox_send removed from per-agent tools_tier2
Universal tool — injected by platform, not listed per-agent.

## New Platform Capabilities

### Permissions model
10 platform permissions. 4 Empire bundles (coordinator/lead/specialist/worker). All 29 agents assigned. Replaces hardcoded authority in Go policy packages.

### Persistence model
Auto-generated tools from entity_schema: get_entity, save_entity_field, search_entities, query_metrics.

### Tool ownership
Platform owns ALL tool serving. handler_type: platform_builtin (2) or workflow_registered (18). One file per workflow.

### Directive handling
system.directive routed by system node with typed rules. Deterministic, no LLM.

### Event schema format
events.yaml = payload schemas only. Routing derived at startup from agents.yaml + nodes.yaml.

### Prompt templating
{{variable}} substitution from policy.yaml. Simple string replacement.

### Database responsibility model (§17.2)
Three tiers: contract-defined schemas (entity_schema, state_schema — implementer derives DDL), platform-owned tables (implementer designs independently), workflow business tables (implementer designs based on agent needs).

## Canonical Contract Set

```
platform/
  package.yaml           # platform identity
  platform-spec.yaml     # platform definition (14 sections)

{workflow_name}/
  package.yaml           # workflow identity + entity schema
  nodes.yaml             # state machine + handlers + guards + state schemas
  events.yaml            # event payload schemas (175)
  agents.yaml            # agent registry with permissions (29)
  tools.yaml             # tool schemas (20)
  policy.yaml            # thresholds + bundles (45 vars)
  prompts/               # agent prompts (20)
```

## Audit Fix Summary

From v2.3.0 audit (30 findings):
- F1: Spec title → v2.3.0
- F2: Arithmetic 9+2=11
- F3: Flat contract paths → empire/ paths
- F4: §16 directory tree rewritten
- F5: §17 contract descriptions rewritten
- F6: Tool count 21 → 20
- F7: spec-writer-guide fully rewritten for v2.3.0
- F11: 3 missing events added (budget.adjustment_requested, policy.change_requested, directive.unhandled)
- F12: §17.2 Database Responsibility Model added
- F13: Orphan events marked _status: planned
- F14: review.deploy_feedback added to opco-cto emit_events
- F15: campaign.completed added to scan-orchestrator produces
- F16: dedup.ambiguous added to discovery-aggregator produces
- F17: Directory renamed contracts
- F18: agents.yaml header → v2.3.0
- F19: §5.10 naming convention updated
- F20: DDL references cleaned
- F21: Redundant nodes.yaml listing deduplicated in §16
- F22: Prompt count consistent (20)
- F29: mailbox_send removed from 8 agents' per-agent tools_tier2
- F30: platform-spec version aligned to 2.3.0

Deferred to implementer: F8-F10 (runtime code), F25-F27 (prompt drift), F28 (migration 026)
