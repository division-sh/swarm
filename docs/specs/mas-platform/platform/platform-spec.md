# MAS Platform Specification v1.2.0

> **Note:** The authoritative specification is `platform/contracts/platform-spec.yaml`. This prose document is a high-level overview. When in conflict, the YAML wins.


## 1. Purpose

This document defines the Multi-Agent System (MAS) Platform — a generic runtime for composing and executing AI agent workflows. The platform runs **flows** (self-contained building blocks) that are wired together via events. It is product-agnostic: any domain can build on it.

The platform's contracts live in `platform/contracts/`. Products (like a product) provide their own contracts in separate directories.

## 2. Core Concepts

### 2.1 Flows

A **flow** is the platform's fundamental building block — analogous to an IC chip in circuit design. Each flow is a self-contained package with:

- **System nodes** (`nodes.yaml`) — deterministic event handlers that orchestrate state transitions
- **Events** (`events.yaml`) — typed payload schemas for inter-component communication
- **Schema** (`schema.yaml`) — input/output pins (events + data), states, namespace, required agent roles
- **Agents** (`agents.yaml`) — LLM agents that do reasoning work within the flow
- **Tools** (`tools.yaml`, optional) — tool schemas for agent capabilities
- **Policy** (`policy.yaml`, optional) — flow-specific thresholds and configuration
- **Prompts** (`prompts/`) — agent system prompts with `{{variable}}` templating

Flows compose via events: if one flow's output event pin matches another flow's input event pin, they're connected. No explicit wiring needed.

### 2.2 Nesting

Flows nest recursively. A flow's `package.yaml` declares child flows. Each child is a directory under `flows/` with the same structure. There is no depth limit. There is no separate "root flow" concept — the root flow IS the root flow.

```
{root_flow}/                   # root flow (= the root flow)
  package.yaml                 # manifest: name, version, flows, handoffs
  schema.yaml                  # root flow pins
  nodes.yaml                   # root-level system nodes
  agents.yaml                  # root-level agents
  flows/
    {child_flow}/              # child flow (same structure)
      package.yaml             # child manifest, may declare its own flows
      schema.yaml
      nodes.yaml
      agents.yaml
      prompts/
      flows/                   # grandchild flows (if any)
        ...
```

The platform discovers flows by walking the tree: read `package.yaml`, register the flow, recurse into each child declared in `flows:` list. Uniform at every level.

### 2.3 Events

Events are the universal communication mechanism. Every interaction — agent output, state transition, timer, human input — is an event with a typed payload. The platform:

- Validates payloads against schemas before publish
- Routes events to subscribers (system nodes + agents)
- Persists events for recovery
- Derives routing at boot from `agents.yaml` subscriptions + `nodes.yaml` subscribes_to

Events do not declare their own routing. Routing is derived.

### 2.4 System Nodes

Deterministic Go components that subscribe to events, process them through declared handlers, and emit new events. They are NOT LLM agents — they execute predefined logic. The state machine lives in handler `advances_to` fields.

### 2.5 Agents

LLM-powered components that subscribe to events, reason about them, and emit response events. Each agent has:

- Model tier (which LLM)
- Subscriptions (which events it receives)
- Emit events (which events it can produce)
- Tools (which capabilities it has)
- Permissions (what platform actions it's authorized for)
- Workspace class (isolation group)
- Manager fallback (escalation target)

## 3. Pins and Wiring

Each flow declares typed input/output pins in `schema.yaml`:

- **Input event pins** (required) — events that trigger the flow. Must be wired at boot.
- **Output event pins** (optional) — events the flow produces. Subscribers consume if present.
- **Input data pins** (configurable) — entity fields the flow reads.
- **Output data pins** — entity fields the flow writes. No two flows may write the same field (short circuit = boot error).

### 3.1 Namespacing

Flow schemas declare `namespace_prefix` — the event prefix in their contracts. Root flows assign a namespace per flow instance in `package.yaml`. The platform substitutes at boot, enabling flow reuse: the same scoring flow can score leads (namespace=lead) or tickets (namespace=ticket).

### 3.2 Required Agents

Flow schemas declare `required_agents` — role sockets the root flow must fill. Each entry specifies: role name, events it must subscribe to, events it must emit. Boot fails if any role is unfulfilled.

## 4. Handler Execution Order

When a system node receives an event matching a declared handler, the platform executes handler fields in this exact order. Steps are optional — the engine skips absent fields.

1. **guard** — Evaluate condition against entity state + policy. Reject if false.
2. **accumulate** — Track this event arrival. If completion condition not met, stop and wait.
3. **compute** — Execute platform computation primitive (weighted_average, etc.).
4. **on_complete** — Evaluate conditional branches. Execute first match.
5. **advances_to** — Update entity state. Skipped if on_complete already advanced.
6. **sets_gate** — Set entity gate flag.
7. **data_accumulation** — Write event payload fields to entity.
8. **emits** — Publish follow-up event. Skipped if on_complete already emitted.
9. **rules** — Typed routing (alternative to steps 5+8 for directive-style handlers).
10. **action hook** — Call registered product hook if not a platform primitive.

Steps 1-10 execute in a single database transaction. Rollback on failure. Idempotent under retry.

## 5. Execution Primitives

### 5.1 Generic Patterns

The engine interprets these handler fields natively:

| Field | Behavior |
|-------|----------|
| `advances_to` | Set entity.state to target |
| `emits` | Publish follow-up event |
| `sets_gate` | Set entity.gates.{name} = true |
| `guard` | Evaluate condition, block if false |
| `data_accumulation` | Write payload fields to entity |
| `on_complete` | Conditional branching |
| `rules` | Typed routing |
| `record_evidence` | Append payload to accumulator |

### 5.2 Accumulate

Track multiple events for same entity. Fire on completion. Platform handles idempotency, crash recovery, timeout.

- `accumulate.track` — expected items list
- `accumulate.completion` — all | threshold | timeout
- `accumulate.on_complete` — what to do when done
- `accumulate.on_timeout` — optional timeout handler

### 5.3 Dispatch

Fan-out work to multiple agents from single trigger.

- `dispatch.targets` — target mapping
- `dispatch.event_per_target` — event per target
- `dispatch.track_completion` — set up accumulation for responses

### 5.4 Query and Format

Read entity state across instances, compile summary, emit digest.

- `query.scope` — which entities
- `query.fields` — which fields
- `format.template` — summary structure

### 5.5 Product Hooks

Handlers the engine cannot execute generically. Product-specific code registered at bootstrap. The engine calls hooks by action name. A well-designed flow should have <10% hooks.

## 6. Permissions Model

10 platform permissions: `agent_hire`, `agent_fire`, `agent_reconfigure`, `configure_routing`, `approve_spend`, `message_all`, `message_domain`, `message_peers`, `mailbox_send`, `human_task_request`.

Agents declare permissions in `agents.yaml`. Platform enforces at tool execution and message delivery time. Permission bundles in `policy.yaml` provide shorthand.

## 7. Tool Model

Platform owns all tool serving (MCP gateway + schema validation). Tools have `handler_type`: `platform_builtin` (platform provides) or `workflow_registered` (product provides). One `tools.yaml` per flow + root-level shared tools.

## 8. Persistence Model

Platform auto-generates typed persistence tools from `entity_schema` in `package.yaml`: `get_entity`, `save_entity_field`, `search_entities`, `query_metrics`. Agents use these — never raw SQL.

Three-tier database responsibility:
- **Contract-defined** — entity_schema + state_schema (implementer derives DDL)
- **Platform-owned** — event store, routing tables (implementer designs)
- **Business tables** — operational data (implementer designs based on agent needs)

## 9. Prompt Templating

Agent prompts are markdown files with `{{variable}}` placeholders. Platform substitutes from `policy.yaml` at agent session creation. Simple string replacement, fail-open for unknown variables.

## 10. Dynamic Flow Instances

Flows declared as `mode: template` have zero instances at boot. System nodes create instances at runtime by calling the `create_flow_instance` platform tool.

```yaml
# In package.yaml — declare the template
flows:
  - id: child_flow
    flow: child_flow
    mode: template

# In nodes.yaml — node handler creates instances
entity.created:
  action: create_flow_instance
  template: child_flow
  instance_id_from: payload.entity_id
```

Same pattern as agent sharding: nodes orchestrate instance creation. The platform provides the tool. The node provides the logic.

## 11. Nested Flows

A flow may contain sub-flows in a `flows/` subdirectory. The parent flow has its own orchestration node (tracking sub-flow completion) while sub-flows do the work. Same structure at every level. Same auto-wiring via events.

## 12. Boot Validation

Platform checks at startup:

1. All YAML files parse
2. Flow directories match package.yaml declarations
3. All required input event pins are wired
4. All required_agents roles are fulfilled
5. No data pin write conflicts
6. No namespace collisions
7. All tools in agents' tools_tier2 exist
8. Agent permissions sufficient for their tools
9. All emit_events have payload schemas
10. entity schema covers all data_accumulation targets
11. All required_agents roles fulfilled after namespace substitution

## 13. Runtime Enforcement

During execution:

1. Tool calls validated against schema
2. Tool calls checked against permissions
3. Message delivery checked against scope
4. State transitions follow declared advances_to paths
5. Guards evaluated before advancement
6. Events validated against payload schemas
7. Accumulation is idempotent
8. Permission checks read from agent.permissions (not hardcoded)
9. Scan mode behavior read from policy.scan_modes (not hardcoded)
10. Manager fallback read from agent.manager_fallback (not hardcoded)
11. Workspace class read from agent.workspace_class (not hardcoded)

## 14. Contract-Driven Policy

Behavior the platform reads from contracts instead of hardcoded functions:

- `agents.yaml` → `permissions`, `workspace_class`, `manager_fallback`
- `policy.yaml` → `scan_modes`, `permission_bundles`, thresholds
- Flow `schema.yaml` → `namespace_prefix`, `required_agents`

## 15. Product Boundary

Product-specific code lives only in approved product packages. Generic runtime contains zero product references. Product hooks registered at bootstrap. CI guard enforced.

## 16. Contract Formats

See `platform/contracts/platform-spec.yaml` for the formal YAML definitions of all contract formats: flow_package, schema_definition, node_definition, agent_registry, event_payload, root flow_manifest.
