# EmpireAI v3.0.0

**Version:** 3.0.0
**Previous:** 2.6.0 (monolith)
**Platform:** >=1.0.0
**Date:** 2026-03-09

## Summary

First release on the independent MAS Platform. Major version bump from v2.x (monolith) to v3.0 (flow-based, platform-dependent).

## Breaking changes from v2.x

- Monolithic spec split into Platform Spec (v1.0.0) + Empire Product Spec (v3.0.0)
- Flows are self-contained packages with package.yaml, agents, prompts, tools, policy
- Root flow replaces "project" concept — empire/ is just another flow
- Runtime reads from runtime/ bridge (merged flat files) until multi-file loader implemented
- Entity renamed to "flow instance" in vocabulary
- Stage renamed to "state"

## What's in v3.0.0

### 4 flows
| Flow | Agents | Prompts | Handlers | Purpose |
|------|--------|---------|----------|---------|
| discovery | 4 | 5 | 9 | Scan, signals, verticals |
| scoring | 1 | 1 | 4 | Multi-dimension scoring |
| validation | 7 | 7 | 15 | Research, spec, CTO, brand |
| operating | 13 | 4 | 15 | Build, launch, operate, grow |

### Root flow (empire)
- 3 agents (empire-coordinator, operations-analyst, holding-devops)
- 1 system node (portfolio-node, 4 handlers)
- 109 cross-flow events
- 11 shared tools, 36 shared policy keys

### Totals
- 52 handlers, 6 nodes, 28 agents, 179 events, 20 tools

### Platform dependency
Requires MAS Platform >=1.0.0. Uses: flow model, execution primitives, permissions, persistence, prompt templating, handler execution order.

## Migration from v2.x

See `docs/CHANGELOG-v2.3.0.md` through `docs/CHANGELOG-v2.6.0.md` for the step-by-step evolution from monolith to flow-based architecture.

## v3.0.1

### All handlers declarative
8 product hooks eliminated. Empire is now 100% YAML — zero Go code needed.
52 handlers use platform patterns: rules, fan_out, accumulate, reduce, query, guard, clear, data_accumulation, advances_to, emits, sets_gate.

### Handler count: 51
Up from 48. Fan_out sub-handlers added for scan dispatch (category, trend, source).

### Event count: 181
58 root + 123 flow (events relocated to correct flows).
