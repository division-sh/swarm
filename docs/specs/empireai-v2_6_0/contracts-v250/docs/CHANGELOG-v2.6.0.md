# EmpireAI v2.6.0 — Normative Handler Execution Order

**Version:** 2.6.0
**Previous:** 2.5.0
**Date:** 2026-03-09

## Summary

Formalizes the handler execution order as a normative section in platform-spec.yaml. Previously only in SPEC-COMPLIANCE.md informally. All platform implementations must follow this order.

## Handler Execution Order (10 steps)

```
1. guard         → evaluate condition, reject if false
2. accumulate    → track arrival, wait if incomplete
3. compute       → run platform computation primitive
4. on_complete   → evaluate conditional branches
5. advances_to   → update entity state
6. sets_gate     → set gate flag
7. data_accumulation → write payload to entity
8. emits         → publish follow-up event
9. rules         → typed routing (alternative to 5+8)
10. action hook  → call product hook if not a platform primitive
```

Steps are optional — engine skips absent fields. Steps 1-10 execute in a single transaction. Rollback on failure. Idempotent under retry.

## Key properties

- **Transaction boundary:** All 10 steps in one DB transaction
- **Idempotency:** Duplicate delivery produces same result
- **on_complete priority:** If step 4 advances state or emits, steps 5 and 8 are skipped (branch already handled them)
- **rules alternative:** Step 9 is an alternative path for directive-style handlers (replaces steps 5+8)

## Platform-spec section count: 21

## Flows are fully self-contained

Each flow now carries everything it needs — installable from marketplace as a complete package:

| Flow | Agents | Prompts | Tools | Policy | Handlers |
|------|--------|---------|-------|--------|----------|
| discovery | 4 | 5 | — | 3 keys (scan_modes) | 9 |
| scoring | 1 | 1 | — | 6 keys (thresholds) | 4 |
| validation | 7 | 7 | 3 | 1 key (revision_max) | 15 |
| operating | 13 | 4 | 14 | 2 keys (park/capacity) | 15 |

Project level (spans flows):
- 3 agents (empire-coordinator, operations-analyst, holding-devops)
- 3 prompts
- 11 shared tools (platform builtins + multi-flow tools)
- 36 shared policy keys (permission bundles + shared thresholds)
- 1 system node (portfolio-node, 4 handlers)

### Per-flow file set
```
{flow}/
  nodes.yaml      # system nodes + handlers (required)
  events.yaml     # event payloads (required)
  schema.yaml     # pins, states, namespace, required_agents (required)
  agents.yaml     # flow agents (required)
  prompts/        # agent prompts (required)
  tools.yaml      # flow-specific tools (optional)
  policy.yaml     # flow-specific thresholds (optional)
```

### What this enables
- Install a flow from marketplace: it brings agents, prompts, tools, policy, nodes, events, schema
- No cross-flow agent leakage — each flow's agents are scoped to that flow
- Project-level agents explicitly span flows (empire-coordinator observes all flows)
- Project-level policy (permission bundles) inherited by all flows
