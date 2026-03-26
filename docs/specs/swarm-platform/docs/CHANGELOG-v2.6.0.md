# EmpireAI v2.6.0

**Version:** 2.6.0
**Previous:** 2.5.0
**Date:** 2026-03-09

## Summary

Unified flow model, prose split, self-contained flow packages. See `platform/CHANGELOG.md` for platform-level changes.

## Platform changes (see platform/CHANGELOG.md)
- Unified flow model — no separate "project" concept
- Normative 10-step handler execution order
- Recursive flow nesting
- Self-contained flow packages with package.yaml at every level
- Execution primitives (91% declarative)
- Compliance guidelines (40 checks)

## Empire-specific changes

### Prose split
- `platform/platform-spec.md` — platform prose (235 lines, zero Empire references)
- `empire/empire-spec.md` — product prose (3,522 lines)
- Monolithic spec eliminated

### Flows are self-contained
| Flow | Agents | Prompts | Tools | Policy | Handlers |
|------|--------|---------|-------|--------|----------|
| discovery | 4 | 5 | — | 3 keys | 9 |
| scoring | 1 | 1 | — | 6 keys | 4 |
| validation | 7 | 7 | 3 | 1 key | 15 |
| operating | 13 | 4 | 14 | 2 keys | 15 |
| root (empire) | 3 | 3 | 11 shared | 36 shared | 4 |

### package.yaml added to all flows
discovery, scoring, validation, operating each have package.yaml declaring identity and (empty) child flows list. Root flow (empire/) already had package.yaml.

### schema.yaml added to root flow
Root flow now has schema.yaml declaring its external pins.

### Runtime bridge updated
runtime/ contains 5 merged files (nodes, events, agents, tools, policy) for legacy loader compatibility.

### Counts
- 51 handlers across 6 nodes
- 28 agents (3 root + 25 in flows)
- 181 events (109 root + 66 flow)
- 20 tools (11 shared + 9 flow-specific)
- 4 flows + root
