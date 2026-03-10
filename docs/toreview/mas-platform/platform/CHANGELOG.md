# MAS Platform Changelog

## v1.1.0 (2026-03-09)

### Engine specification
Complete engine spec added to platform-spec.yaml (10 sections):
- flow_tree_walker: recursive discovery, max depth 99
- uri_addressing: hierarchical paths with /, local vs absolute, wildcards
- contract_merger: path-keyed registries, no conflicts via scoping
- agent_sharding: nodes orchestrate parallelism, platform provides mechanics
- dynamic_instance_lifecycle: runtime flow creation via create_flow_instance tool
- boot_sequence: 15 ordered steps, fail-fast
- event_loop: 8-step lifecycle, per-entity serialization, atomicity, crash recovery
- state_management: entity state, accumulator, gates — all scoped by entity + flow path
- accumulation_engine: tracking, completion modes, idempotency
- agent_session_management: session lifecycle, tool validation, turn budget

### Flow modes
- `mode: static` — one instance at boot
- `mode: template` — zero instances at boot, created by nodes at runtime

### Handoffs removed
Replaced by node-driven instance creation via `create_flow_instance` platform tool. Same pattern as agent sharding — nodes orchestrate everything.

### create_flow_instance platform tool
New builtin tool. Validates uniqueness, loads template, constructs paths, registers in runtime, expands wildcards, starts nodes and agents.

### Addressing model
- Local: `event_name` (no slash = same flow)
- Absolute: `scoring/entity.shortlisted` (slash = path from root)
- Wildcard: `operating/*/event_name` (matches template instances)
- No type suffix, no URI scheme for single-root deployments

## v1.0.0 (2026-03-09)

### Unified flow model — flows all the way down
- **No separate "project" concept.** A flow IS the universal unit at every level. The root flow is what was previously called "the project."
- **Every flow has `package.yaml`.** Declares identity, version, child flows, handoffs. Same manifest at every nesting level.
- **Recursive nesting.** A flow's `package.yaml` declares child flows in `flows:` list. Each child is a directory under `flows/` with the same structure. No depth limit.
- **Discovery algorithm:** Walk the tree from root: read package.yaml → register flow → recurse into `flows:` children. One recursive function.
- `project_model` removed from platform-spec.yaml. Absorbed into `flow_model`.
- `project_manifest` renamed to `flow_manifest` in contract formats.
- `schema.yaml` required at every level including root flow.

### Normative handler execution order
10-step execution order formalized in platform-spec.yaml → `handler_execution_order`:

```
1. guard         → evaluate condition, reject if false
2. accumulate    → track arrival, wait if incomplete
3. compute       → platform computation primitive
4. on_complete   → conditional branches
5. advances_to   → update entity state
6. sets_gate     → set gate flag
7. data_accumulation → write payload to entity
8. emits         → publish follow-up event
9. rules         → typed routing (alternative to 5+8)
10. action hook  → product hook
```

All steps in one transaction. Idempotent under retry.

### Self-contained flow packages
Each flow carries everything needed for marketplace distribution:

| File | Required | Purpose |
|------|----------|---------|
| package.yaml | ✓ | Manifest: name, version, child flows, handoffs |
| schema.yaml | ✓ | Pins, states, namespace, required_agents |
| nodes.yaml | optional | System nodes + event handlers |
| events.yaml | optional | Event payload schemas |
| agents.yaml | optional | Agent definitions |
| tools.yaml | optional | Flow-specific tools |
| policy.yaml | optional | Flow-specific thresholds |
| prompts/ | if agents | Agent prompt files |
| flows/ | if nesting | Child flow directories |

### Execution primitives
- 8 generic patterns (advances_to, emits, sets_gate, guard, data_accumulation, on_complete, rules, record_evidence)
- 3 platform primitives (accumulate, dispatch, query_and_format)
- Product hooks for the remaining ~9% of handlers
- Coverage target: 91% declarative

### Contract-driven policy
- `agents.yaml` fields: `workspace_class`, `manager_fallback` — replaces hardcoded Go functions
- `policy.yaml` fields: `scan_modes`, `permission_bundles` — data-driven behavior
- 14 of 20 Go policy functions replaced by contract reads

### Compliance guidelines
44 checks across 5 categories:
- Boot validation: 11 checks
- Runtime enforcement: 11 checks
- Handler execution: 10 rules
- Testing: 9 requirements
- Product boundary: 4 rules

### Runtime bridge
`runtime/` directory at any flow level provides merged flat files for legacy loaders. Contains nodes.yaml, events.yaml, agents.yaml, tools.yaml, policy.yaml — all merged from the flow tree.

## Pre-1.0 History

Platform concepts were developed within the the monolithic spec monolithic spec (v2.0.x through v2.6.0). See `docs/CHANGELOG-*.md` for the full monolith history. Platform v1.0.0 is the first independent release after the spec split.

### Zero product hooks
All 8 former product hooks replaced with declarative patterns:
- dispatch_scanners → rules + fan_out
- init_scoring → rules + data_accumulation + emits
- derivation_pre_filter → guard + count
- resolve_contest → reduce (pick_or_average)
- process_dedup → rules + data_accumulation
- process_synthesis → rules + data_accumulation
- compile_digest → query + emits
- reset_pipeline_state → clear

### New platform patterns
- List processing: fan_out, filter, reduce, count, group_by
- Cross-entity: query (aggregation from entity_schema), clear (bulk state reset)
- Platform tools: create_entity, query_entities

### Coverage: 100% declarative (51 handlers, 0 product hooks)

