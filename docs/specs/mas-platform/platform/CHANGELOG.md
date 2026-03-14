# MAS Platform Changelog

## v1.1.1 (2026-03-13)

### Dependency graph — 5 primitives positioned
`query`, `filter`, `reduce`, `count`, `clear` were defined as handler fields but missing from the dependency graph. Now specified with `execution_position` in the spec:

```
query → clear_gates → guard → accumulate → filter → reduce → count → compute
  → on_complete / rules
    → {advances_to, sets_gate, data_accumulation}
      → payload_transform → emits → action → clear
```

### Handler execution: dependency graph replaces flat 10-step list
The old numbered execution order is replaced by a causal dependency graph. Steps with dependencies execute in order. Independent steps (advances_to, sets_gate, data_accumulation) commit atomically in any order. All stale "10-step" references removed.

### Boot verification expanded to 16 checks
Added checks 12-16:
- CHECK 12: Gate references (structural check)
- CHECK 13: Policy conflict detection
- CHECK 14: Event chain cycle detection (DFS traversal of node emit graph)
- CHECK 15: Dialect compliance (guard shape, on_complete ordering, condition prefixes, self-emit)
- CHECK 16: Single node handler per event (no two nodes may handle the same event)

### Single node per event rule
Each event may be handled by at most one system node. Boot fails on duplicates. Agents may subscribe freely. Rationale: unambiguous state authority.

### CEL expression language formalized
All guards, rule conditions, on_complete branch conditions, and filter expressions use CEL. Context: entity, payload, policy, accumulated, fan_out.

### Terminal state: unconditional event rejection
When an entity reaches a terminal state, ALL subsequent events for that entity are rejected. No guard runs, no handler runs. Unconditional. No configuration override.

### Accumulate dedup_by field
New sub-field on `accumulate`. Default: sender (session ID). Override with payload field path (e.g., `payload.dimension`) for content-identified accumulation.

### Dead letter structured schema
10-field schema for pipeline.dead_letter events: original_event, original_payload, entity_id, flow_instance, failure_type, error_message, retry_count, chain_depth, handler_node, timestamp.

### Payload shaping semantics
Specified as engine contract (not adapter concern). Default: only entity_id carried to emitted events. All other fields require explicit payload_transform with CEL expressions. No implicit forwarding.

### clear_gates scope
Clarified: clears ALL entity gates, not scoped to node or flow schema. Entity has one flat gate map.

### Timer model
Durable timers with start_on/cancel_on lifecycle. Persisted across restarts.

### Error model
3-retry exponential backoff. Dead letter after exhaustion. Chain depth limit: 50.

### Emit-tool auto-generation
Agent emit_events list → platform generates emit_{event_name} tools. Dots converted to underscores.

### Boot verification reference implementation
verify.py (16 checks) included as reference. Platform spec's `boot_verification` section lists all checks with severity levels.

## v1.1.0 (2026-03-09)
[... previous content preserved below ...]


## v1.1.0 (2026-03-09)

### Engine specification
Complete engine spec added to platform-spec.yaml (10 sections):
flow_tree_walker, uri_addressing, contract_merger, agent_sharding, dynamic_instance_lifecycle, boot_sequence, event_loop, state_management, accumulation_engine, agent_session_management.

### Flow modes
static (one instance at boot), template (created at runtime via create_flow_instance).

### Addressing model
Local (no slash), Absolute (slash-separated path), Wildcard (asterisk matches instances).

## v1.0.0 (2026-03-09)

### Unified flow model
Flows all the way down. No separate project concept. Recursive nesting. Self-contained packages.

### Zero product hooks
All 8 former hooks replaced with declarative patterns. 100% YAML.

### Pre-1.0 History
Platform concepts developed within the monolithic spec (v2.0.x–v2.6.0). See docs/ for full history.
