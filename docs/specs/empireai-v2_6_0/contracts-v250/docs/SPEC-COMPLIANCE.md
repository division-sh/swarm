# Spec Compliance Guidelines — v2.4.0

## What the Runtime Must Read

### From empire/runtime/ (current loader)
The runtime loader reads these flat files:
- `empire/runtime/nodes.yaml` — 6 merged system nodes, 47 handlers
- `empire/runtime/events.yaml` — 175 events with full metadata (owning_node, runtime_handling, consumer, payload)

### From empire/ (direct)
- `empire/agents.yaml` — 28 agents with subscriptions, emit_events, tools_tier2, permissions
- `empire/tools.yaml` — 20 tool schemas with handler_type
- `empire/policy.yaml` — 45 template variables + 5 permission bundles
- `empire/prompts/` — 20 agent prompt files with {{variable}} placeholders
- `empire/package.yaml` — entity_schema (8 groups, 30 fields), flow instances, handoffs

### From platform/
- `platform/platform-spec.yaml` — platform definition (vocabulary, formats, rules, primitives)

## Handler Compliance

### 47 handlers across 6 nodes

Every handler in runtime/nodes.yaml must have a corresponding implementation:

| Node | Flow | Handlers | Implementation |
|------|------|----------|---------------|
| scan-orchestrator | discovery | 4 | Split executor Handle() |
| discovery-aggregator | discovery | 5 | Split executor Handle() |
| scoring-node | scoring | 4 | Split executor Handle() |
| validation-orchestrator | validation | 15 | Split executor Handle() |
| lifecycle-orchestrator | operating | 15 | Split executor Handle() |
| portfolio-node | project | 4 | Split executor Handle() |

### Handler classification for Phase 11

34 handlers are YAML-generic (advances_to, emits, sets_gate, guard, data_accumulation, rules):
- All 15 validation-orchestrator handlers
- 13 of 15 lifecycle-orchestrator handlers
- 2 of 4 scoring-node handlers
- 2 of 4 portfolio-node handlers
- 2 of 4 scan-orchestrator handlers (timer handlers)

9 handlers use platform primitives (accumulate, dispatch, query_format):
- scan-orchestrator: scan.requested (dispatch), *.scan_complete (accumulate), timer.scan_timeout (accumulate)
- discovery-aggregator: category.assessed, trend.identified, source.scraped (accumulate)
- scoring-node: score.dimension_complete (accumulate + compute)
- portfolio-node: timer.portfolio_digest (query_format)
- scan-orchestrator: timer.campaign_deadline (accumulate)

4 handlers require product code in approved empire package:
- discovery-aggregator: dedup.resolved, synthesis.resolved (merge logic)
- scoring-node: scoring.contest_resolved (arbitration)
- portfolio-node: runtime.reset (cleanup)

### Phase 11 migration path

1. Build generic handler executor for the 34 YAML-generic handlers
2. Build accumulate primitive — covers 7 handlers
3. Build dispatch primitive — covers 1 handler
4. Build query_format primitive — covers 1 handler
5. Register product hooks for 4 remaining handlers
6. Verify: only 4 Go files in empire product package

## Event Compliance

### 175 events in runtime/events.yaml

Every event must have:
- `payload` — field names and types (for emit tool validation)
- `runtime_handling` — consuming | dual_delivery | passthrough
- `owning_node` — which system node processes it (if any)
- `consumer` — who receives it
- `consumer_type` — agent | system_node | hybrid

### Routing derivation (target model)

When the runtime supports it, routing will be derived at boot:
- If a node subscribes_to an event → owning_node = that node
- If agents also subscribe → dual_delivery
- If only a node subscribes → consuming
- If no node subscribes → passthrough

Until then, runtime/events.yaml carries the explicit metadata.

## Agent Compliance

### 28 agents in agents.yaml

Every agent must have:
- subscriptions list matching events it handles
- emit_events list matching events it produces
- tools_tier2 list — all tools must exist in tools.yaml
- permissions list — must cover all tools the agent calls
- permissions_bundle — must match a bundle in policy.yaml

### Permission enforcement

The platform checks permissions before tool execution:
- agent_hire/fire/reconfigure — only coordinator and lead bundles
- configure_routing — coordinator, lead, and specialist bundles
- message_all — coordinator only
- message_domain — coordinator and lead
- message_peers — all agents

## Flow Compliance (target structure)

### 4 flows in empire/flows/

Each flow directory must contain:
- `nodes.yaml` — system nodes with event_handlers
- `events.yaml` — event payload schemas (payload only)
- `schema.yaml` — pins (input/output events + data), states, namespace_prefix, required_agents

### Boot validation (when multi-file loading is implemented)

The platform will validate at boot:
1. All required input event pins are wired
2. All required agent roles are fulfilled
3. No data pin write conflicts between flows
4. Namespace substitution produces no collisions
5. All handler guards reference valid entity fields or policy values
6. All emitted events have payload schemas

## Entity Compliance

### Entity schema in package.yaml

The entity_schema (8 groups, 30 fields) defines the entity table structure. The implementer derives DDL from this. The platform auto-generates persistence tools from this schema.

Fields are populated by data_accumulation declarations on handlers:
- discovery flow writes: name, geography, raw_signals, signal_strength, discovery_mode
- scoring flow writes: composite_score, scoring_rubric, scores
- validation flow writes: business_brief, mvp_spec, cto_feasibility, brand, validation_kit
- operating flow writes: full_spec, deploy_config, live_url, launch_targets

No two flows write the same field.

## Version Compliance

- empire/package.yaml version must match platform/package.yaml platform_version
- All file headers should reference the current version
- CHANGELOG must exist for each version bump

## CI Guard (Phase 10)

Non-test Go files outside approved product packages must not contain:
- `Empire`, `empire-`, `empire_`, `empirecoordinator`
- Approved product packages: `internal/runtime/pipeline/empire/`, `internal/runtime/productpolicy/empire/`
