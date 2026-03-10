# EmpireAI Spec Writer Guide — v2.5.0

## 1. Architecture: Flows

Empire is a project composing 4 flows + 1 project-level node.

| Flow | Handlers | What it does |
|------|----------|-------------|
| discovery | 9 | Scan, accumulate signals, produce verticals |
| scoring | 4 | Score dimensions, classify (shortlist/marginal/reject) |
| validation | 15 | Research, spec, CTO review, brand — 4-gate pipeline |
| operating | 15 | Build, launch, operate, grow, wind down |
| project (portfolio-node) | 4 | Monitoring, budget, directives |

Total: 47 handlers across 6 system nodes.

## 2. File Structure

```
platform/
  package.yaml                  # platform manifest
  platform-spec.yaml            # platform definition (16 sections)

empire/
  package.yaml                  # project manifest + entity schema + flow instances + handoffs
  agents.yaml                   # 28 agents with permissions
  events.yaml                   # 109 project-level event payloads (agent-to-agent, cross-flow)
  nodes.yaml                    # project-level: portfolio-node (4 handlers)
  policy.yaml                   # 45 variables + 5 permission bundles
  tools.yaml                    # 20 tools with handler_type
  prompts/                      # 20 agent prompt files
  flows/
    discovery/                  # 9 handlers, 21 events
      nodes.yaml
      events.yaml
      schema.yaml
    scoring/                    # 4 handlers, 11 events
      nodes.yaml
      events.yaml
      schema.yaml
    validation/                 # 15 handlers, 23 events
      nodes.yaml
      events.yaml
      schema.yaml
    operating/                  # 15 handlers, 17 events (includes 6 from old lifecycle split)
      nodes.yaml
      events.yaml
      schema.yaml
```

### Event ownership
- Per-flow events.yaml: events that flow's nodes handle or produce (66 total)
- Project-level events.yaml: agent-to-agent and cross-flow events (109 total)
- Zero overlap between the two levels
- Total: 175 events

## 3. Vocabulary

- **Flow** — self-contained building block (IC chip analogy). Has pins, nodes, events, schema.
- **State** — where a flow instance is in its lifecycle (was "stage")
- **Flow instance** — a specific entity moving through a flow (was "entity")
- **Pin** — typed input/output on a flow (events + data fields)
- **Namespace** — event prefix for a flow instance (e.g., "vertical", "opco")
- **Handoff** — declarative rule in package.yaml that creates a new flow instance when a trigger event fires

## 4. Authority Hierarchy

1. Flow nodes.yaml wins for state machine behavior
2. agents.yaml wins for agent configuration
3. events.yaml wins for payload schemas (flow-level for flow events, project-level for agent events)
4. policy.yaml wins for thresholds
5. package.yaml wins for entity schema

## 5. Common Tasks

### Adding a handler to an existing flow
1. Add handler to the owning node in `flows/{flow}/nodes.yaml`
2. Add event to node's `subscribes_to`
3. Add payload schema to `flows/{flow}/events.yaml`
4. If `advances_to` a new state, update `flows/{flow}/schema.yaml`
5. If emits a new event consumed by another flow, add to schema output pins

### Adding an agent-to-agent event
1. Add payload schema to project-level `events.yaml`
2. Add event to emitting agent's `emit_events` in `agents.yaml`
3. Add event to consuming agent's `subscriptions` in `agents.yaml`

### Adding a new flow
1. Create directory under `flows/`
2. Create `nodes.yaml` (system nodes + handlers)
3. Create `events.yaml` (payload schemas for flow events)
4. Create `schema.yaml` (pins: input/output events + data, states, namespace)
5. Add flow instance to `package.yaml` flows list
6. If the flow creates new entity types, add handoff to `package.yaml` handoffs
7. Provide agents that fulfill required roles

### Adding a tool
1. Add schema to `tools.yaml` with handler_type
2. Add tool name to relevant agents' `tools_tier2` in `agents.yaml`
3. Ensure agent has permission for the tool's required capability

## 6. Key Principles

- Events ARE the composition mechanism — no explicit wiring between flows
- State machine is derived from handler `advances_to` — no separate workflow file
- Routing is derived from agents.yaml + nodes.yaml at boot — not in events.yaml
- No two flows may write the same entity field
- Flows don't reach into other flows' state spaces — use events for cross-flow transitions
- Handoffs that create new flow instances are declarative in package.yaml
- DDL is an implementer artifact — entity_schema + state_schema are the contracts

## 7. Counts

| What | Count |
|------|-------|
| Flows | 4 |
| System nodes | 6 (5 flow + 1 project) |
| Event handlers | 47 (43 flow + 4 project) |
| Events | 175 (66 flow + 109 project) |
| Agents | 28 |
| Tools | 20 |
| Prompts | 20 |
| Policy variables | 45 |
| Permission bundles | 4 |
| Entity schema groups | 8 (30 fields) |
