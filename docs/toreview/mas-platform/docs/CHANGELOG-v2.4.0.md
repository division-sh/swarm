# EmpireAI v2.4.0 — Flow Composition Model

**Version:** 2.4.0
**Previous:** 2.3.0
**Date:** 2026-03-08

## Summary

Introduces the Flow composition model. A Flow is the platform's fundamental building block — a self-contained package with typed input/output pins (events and data), system nodes, and required agent roles. Flows compose via events: if one flow's output pin matches another's input pin, they're connected. Like IC chips on a circuit board.

Empire is now a project composing 4 flows + project-level coordination nodes.

## Flow Model (platform-spec)

### What is a Flow
A marketplace-distributable package of 3 files: nodes.yaml (handlers), events.yaml (payloads), schema.yaml (data pins). Analogous to an IC chip with a datasheet.

### Pins
- Input event pins (required) — events the flow subscribes to
- Output event pins (optional) — events the flow emits
- Input data pins (configurable) — entity fields the flow reads
- Output data pins (always written) — entity fields the flow writes
- No two flows may write the same field (short circuit = boot error)

### Namespacing
Flow templates use entity.* as generic event prefix. Projects assign namespace per instance. Platform substitutes at boot. Same flow can run multiple times with different namespaces.

### Required agent roles
Flows declare sockets. Projects plug agents in. Boot fails if unfulfilled.

### Boot validation
Platform checks at startup: all required input pins wired, all roles fulfilled, no data pin conflicts, no namespace collisions.

## Project Model (platform-spec)

### Structure
```
{project}/
  package.yaml           # project manifest + flow instances
  agents.yaml            # project-level agents
  nodes.yaml             # project-level system nodes (handoffs, portfolio)
  policy.yaml            # shared policy
  tools.yaml             # shared tools
  prompts/               # agent prompts
  flows/
    {flow}/              # each installed flow
      nodes.yaml
      events.yaml
      schema.yaml
```

### Project-level nodes
- handoff-node: manages flow-to-flow transitions (4 handlers)
- portfolio-node: cross-flow monitoring, budget, directives (4 handlers)

## Empire Flow Decomposition

| Flow | Handlers | States | System Node |
|------|----------|--------|-------------|
| discovery | 9 | pre-flow | scan-orchestrator + discovery-aggregator |
| scoring | 4 | discovered → shortlisted/marginal/killed | scoring-node |
| validation | 15 | researching → ready_for_review | validation-orchestrator |
| operating | 15 | approved → winding_down | lifecycle-orchestrator |
| project-level | 8 | — | handoff-node + portfolio-node |

Total: 53 handlers (47 flow + 4 project).

## Handoff Chain

discovery emits vertical.discovered → scoring consumes it
scoring emits vertical.shortlisted → validation consumes it (via handoff-node)
validation emits vertical.ready_for_review → operating consumes it (via handoff-node)
operating emits opco.spinup_requested → handoff-node creates opco flow instance

## Terminology

- stage → state (workflow state)
- entity → flow instance (the thing moving through states)
- Flow = building block / IC chip
- Project = circuit board
- Events = wires
- Schema pins = component I/O
- Marketplace = component catalog

## Touches

- platform/platform-spec.yaml: flow_model, project_model, vocabulary (flow + state)
- empire/package.yaml: rewritten with flow instances
- empire/nodes.yaml: project-level nodes (handoff-node, portfolio-node)
- empire/flows/discovery/: nodes.yaml, events.yaml, schema.yaml
- empire/flows/scoring/: nodes.yaml, events.yaml, schema.yaml
- empire/flows/validation/: nodes.yaml, events.yaml, schema.yaml
- empire/flows/operating/: nodes.yaml, events.yaml, schema.yaml

## Post-audit amendments

### handoff-node eliminated
All 6 handlers were passthroughs — events auto-wire via subscriptions. The one real action (create opco flow instance) is now a declarative handoff in package.yaml:
```yaml
handoffs:
  - event: opco.spinup_requested
    creates_flow: operating
    namespace: opco
```
Platform reads this at boot and creates instances when the trigger event fires.

### entity_schema restored to package.yaml
8 groups, 30 fields. Implementer derives entity table DDL from this.

### vertical.approved emitter specified
mailbox.item_decided handler in lifecycle-orchestrator now explicitly emits vertical.approved or vertical.killed via typed rules.

### Platform-spec reconciled
- Old workflow_definition format removed, pin-based format only
- Compliance rules rewritten for flow model
- Handoffs model added (declarative, not node-based)

### Final counts
- 47 handlers (43 flow + 4 project)
- 6 system nodes (5 flow + 1 project)
- 175 events, 28 agents, 20 tools, 4 flows

## Iteration 2 Audit Fixes

### Interceptor prose purge
1,800 lines of interceptor/pipeline-coordinator prose replaced with 32-line system node description. §4.2.2 rewritten. Zero interceptor references remain. Spec: ~7,200 → ~5,250 lines.

### Events deduplication
Root events.yaml carried all 175 events, overlapping with per-flow events. Fixed: root now has 109 project-level events (agent-to-agent, cross-flow), per-flow files have 66 flow events. Zero overlap, 175 total.

### HIGH fixes
- H1: vertical.resumed cross-flow state leak — removed advances_to:researching, emits event instead
- H2: Interceptor prose purged (see above)
- H3: Handler count 51→47, node parenthetical 5+2→5+1, agent count 29→28
- H4: configure_routing added to specialist permission bundle (7 agents)
- H5: vertical.approved added to operating/schema.yaml output pins

### MEDIUM fixes
- M2: Unreachable scoring state 'scoring' removed from schema
- M6: events.yaml header → v2.4.0

### LOW fixes
- L3: scoring-node execution_type → system_node

### Structure clarification
```
empire/
  events.yaml               # 109 project-level events (agent-to-agent, cross-flow)
  nodes.yaml                # portfolio-node (4 handlers)
  flows/
    discovery/events.yaml   # 21 flow events
    scoring/events.yaml     # 11 flow events
    validation/events.yaml  # 23 flow events
    operating/events.yaml   # 17 flow events (includes 6 not in root)
```
No file contains events from another file. Platform merges at boot.

## Iteration 2 — Audit Remediation

### Interceptor prose purge
1,800 lines of interceptor/pipeline-coordinator prose replaced with 32-line system node description. §4.2.2 fully rewritten. Zero interceptor references remain. Spec reduced from ~7,200 to ~5,250 lines.

### Flow coherence fixes
- HIGH 1: vertical.resumed no longer advances_to researching (cross-flow state leak). Now emits event for validation flow.
- HIGH 4: Specialist permission bundle now includes configure_routing (matches 7 agents' actual permissions).
- HIGH 5: vertical.approved added to operating/schema.yaml output pins.
- MEDIUM 2: Unreachable scoring state 'scoring' removed from schema.
- LOW 3: scoring-node execution_type → system_node.

### Events deduplication
Root events.yaml deduplicated: 175 → 109 project-level events. 66 flow-owned events live only in per-flow events.yaml. Zero overlap. Total still 175.

### Count corrections
- Handlers: 47 (43 flow + 4 project)
- Nodes: 6 (5 flow + 1 project)
- Agents: 28 (scoring-node removed from agents.yaml)
- Events: 175 (109 project + 66 flow)

### Version alignment
All headers → v2.4.0 (events.yaml, agents.yaml, platform-spec).

## Runtime Compatibility Layer (from implementer Phase 9 feedback)

### Problem
The runtime loader reads one flat nodes file and one events file with full routing metadata. The v2.4.0 flow structure (4 per-flow files) and payload-only events broke runtime loading.

### Solution: empire/runtime/ directory
Two merged files the current loader reads:

- `runtime/nodes.yaml` — all 6 nodes merged from flows/*/nodes.yaml + project nodes.yaml. Auto-generated, not source of truth.
- `runtime/events.yaml` — all 175 events with full metadata (owning_node, runtime_handling, consumer, consumer_type, delivery_channel, payload). Derived from flows + agents.yaml + nodes.yaml.

### Migration path
1. Runtime currently reads `empire/runtime/nodes.yaml` and `empire/runtime/events.yaml`
2. When implementer builds multi-file loader → switch to reading `empire/flows/*/nodes.yaml` + `empire/nodes.yaml`
3. When implementer builds routing derivation → switch to payload-only `empire/flows/*/events.yaml` + `empire/events.yaml`
4. Delete `empire/runtime/` directory

### Key implementer finding preserved
- `vertical.derived` stays scoring-node owned (not transition/interceptor-owned) — hardest Phase 7 semantic issue
- Event visibility and transition ownership remain distinct (spec.approved dual-use rule valid)
- All 5 flow-level node identities preserved exactly in merged file
