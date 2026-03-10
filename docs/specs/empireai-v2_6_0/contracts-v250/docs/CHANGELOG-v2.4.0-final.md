# EmpireAI v2.4.0 Final — Engine Primitives + Compliance Guidelines + Runtime Bridge

**Version:** 2.4.0
**Previous:** 2.2.2 (implementer baseline)
**Date:** 2026-03-09

## Summary

v2.4.0 introduces the flow composition model, engine execution primitives, namespace templating, required agent roles, and a runtime compatibility bridge. This is the spec foundation for the genericity endgame (Phases 9-11).

## Cumulative Changes from v2.2.2

### Architecture: Flow Composition
- 4 flows: discovery (9 handlers), scoring (4), validation (15), operating (15)
- 1 project-level node: portfolio-node (4 handlers)
- 47 total handlers across 6 system nodes
- Flows auto-wire via event subscriptions. Handoffs that create new flow instances are declarative in package.yaml.
- Each flow has: nodes.yaml, events.yaml (payload schemas), schema.yaml (pins + states + required agents)

### Platform Spec: Engine Primitives (NEW)
8 generic handler patterns the engine interprets from YAML:
- advance_state, emit_event, set_gate, guard_check, data_write, conditional_branch, type_routing, record_evidence

3 platform execution primitives for complex patterns:
- **accumulate** — track N items arriving over time, fire on completion. Covers 7 handlers.
- **dispatch** — fan out to multiple agent instances. Covers 1 handler.
- **query_format** — read state, compile summary, emit. Covers 1 handler.

Handler classification: 34 generic (72%) + 9 primitive (19%) + 4 product code (9%) = 47 total.
With primitives, 91% of handlers are YAML-driven. 4 handlers need product-specific Go in approved packages.

### Platform Spec: Namespace Templating (NEW)
- Flows declare namespace_prefix in schema.yaml
- Projects assign namespace per flow instance in package.yaml
- Platform substitutes prefix at boot (e.g. vertical.scored → ticket.scored for reuse)

### Platform Spec: Required Agents (NEW)
- Flows declare required_agents sockets in schema.yaml
- Each socket: role, subscribes_to, emits, description
- Platform validates at boot that all roles are fulfilled by agents.yaml entries
- Discovery: 3 roles, Scoring: 1, Validation: 5, Operating: 2

### Platform Spec: Permissions Model
- 10 platform permissions, 5 Empire bundles (coordinator, lead, specialist, worker, system_node)
- All 28 agents assigned. Platform enforces at tool execution and message delivery.

### Platform Spec: Persistence Model
- sql_execute removed. Platform auto-generates typed persistence tools from entity_schema.
- Agent data access is permissioned, validated, storage-agnostic.

### Platform Spec: Directive Routing
- system.directive handled by portfolio-node with typed rules
- Deterministic routing, no LLM interpretation

### Runtime Bridge (NEW)
The runtime loader reads flat files from empire/runtime/:
- `runtime/nodes.yaml` — 6 nodes merged flat with full metadata
- `runtime/events.yaml` — 175 events with owning_node, runtime_handling, consumer, payload

Target structure in flows/ shows the architecture we're migrating toward. When runtime supports multi-file loading + routing derivation, switch to flows/ and delete runtime/.

### Files Eliminated (from v2.2.2)
workflow.yaml, hooks.yaml, ddl.sql (x2), agent-config-map.yaml, upgrade-actions.yaml, verification-gates.yaml, tooling.lock, prompt-manifest.sha256

### Spec Compliance Guidelines (NEW)
docs/SPEC-COMPLIANCE.md — implementer reference for handler compliance, event compliance, agent compliance, flow compliance, entity compliance, version compliance, CI guard rules, and Phase 11 migration path.

## File Structure

```
platform/
  package.yaml
  platform-spec.yaml          # 17 sections including engine_primitives

empire/
  package.yaml                # flows, handoffs, entity_schema
  agents.yaml                 # 28 agents with permissions
  nodes.yaml                  # portfolio-node (project-level)
  events.yaml                 # 109 project-level event payloads
  tools.yaml                  # 20 tools
  policy.yaml                 # 45 vars + 5 bundles
  prompts/                    # 20 prompts
  flows/
    discovery/                # 3 files: nodes, events, schema
    scoring/
    validation/
    operating/
  runtime/                    # bridge for current loader
    nodes.yaml                # 6 nodes merged flat
    events.yaml               # 175 events with full metadata

docs/
  SPEC-COMPLIANCE.md          # implementer guidelines
  CHANGELOG-*.md              # version history
  spec-writer-guide.md        # spec writer reference
```

## Counts

| What | Count |
|------|-------|
| Flows | 4 |
| System nodes | 6 (5 flow + 1 project) |
| Event handlers | 47 (43 flow + 4 project) |
| YAML-executable handlers | 43 (91% with primitives) |
| Product code handlers | 4 (9%) |
| Events | 175 (66 flow + 109 project) |
| Agents | 28 |
| Tools | 20 |
| Prompts | 20 |
| Policy variables | 45 |
| Permission bundles | 5 |
| Entity schema fields | 30 (8 groups) |
| Platform-spec sections | 17 |
