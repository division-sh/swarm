# Swarm Platform Changelog

## Unreleased

### Clarified: Path alpha role-scoped entity-tool contracts are canonical
Opted-in flows (`tool_surface.role_scoped_entity_tools: true`) now have a canonical platform-spec home for generated current-entity read/save/update tools, non-lossy typed reads, generated closed schemas, and legacy entity-tool retirement. Fully opted-in role-scoped actors do not receive the legacy entity surface names (`create_entity`, `get_entity`, `get_subject_status`, `query_entities`, `search_entities`, `query_metrics`, `save_entity_field`). Older changelog entries describing those tools as universally auto-granted are historical records for the pre-Path alpha model, not current opted-in actor-surface truth.

### Clarified: CLI native_tools are provider-native only
The platform spec now makes the shipped CLI rule explicit: `bash`, `web_search`, and `file_io` are provider-native capabilities only. The platform does not inject fallback tools to satisfy `native_tools` on CLI, unsupported capabilities fail closed, and visible native-tool surface must equal callable truth for the same turn.

### Removed: subject-link runtime cleanup surfaces
The runtime no longer exposes subject-link schema/tool/operator cleanup surfaces. Business correlation moves through explicit authored payload/config fields, and causal debugging uses `source_event_id`. Final canonical platform-spec adoption remains tracked separately.

### Removed: cross-flow runtime entity reads
Agent-facing entity tools no longer expose cross-flow entity state through read-only `get_entity` or explicit foreign `entity_type` query/search/metrics targets. Cross-flow handoff data must move through declared event payload fields or `create_flow_instance` `config_from` bindings; causal debugging uses `source_event_id`.

## v1.6.0 (2026-04-02)

### Breaking: Flow-Scoped Entity State (`flow_model.state_composition`)

Core principle: **state is flow-local, lineage is cross-flow.**

One entity belongs to exactly one flow. No shared mutable state across flow boundaries. `flow_states` map removed. `current_state` is singular and unambiguous within the owning flow.

**Cross-flow handoff**: when a business object moves between flows, the receiving flow creates its own entity via `create_entity: true`. Data crosses via event payloads or `create_flow_instance` `config_from` bindings. Cross-flow reads and writes are prohibited by the platform for agent-facing entity tools.

**Boot validation**: input pin handlers in stateful flows MUST declare `create_entity: true`. Boot error otherwise.

**Subject-link note:** the original v1.6 flow-scoped entity draft introduced a platform-owned business-link column and lifecycle status tool. That model has since been superseded by explicit authored payload/config business correlation plus `source_event_id` causal proof.

**Runtime implementation required:**
- [ ] Enforce cross-flow write prohibition in save_entity_field
- [ ] Boot validation: input pin + stateful flow → require create_entity: true
- [ ] Remove flow_states write path

**Backward compatibility:** enforced at platform >= 1.6.0. Products on >= 1.5.0 use the previous shared-row model.

## v1.5.0 (2026-04-02)

### New: Run Model (`run_model` section)

Platform-level execution context. Every event, delivery, session, turn, and entity mutation belongs to exactly one run.

**New tables:**
- `runs` — run registry with lifecycle states (running, paused, completed, failed, cancelled, forked), fork lineage (forked_from_run_id, forked_from_event_id)
- `entity_mutations` — append-only mutation log (entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step)

**`run_id` column added to:**
- events, event_deliveries, agent_sessions, agent_turns

**Run lifecycle:** creation (system.started, external event, fork), causal chain propagation, completion detection, pause/resume.

**Fork:** timestamp-based system fork with optional contract swap. Historical drafts used `swarm fork --run <id> --at <event_id|timestamp> [--contracts <path>]`; v1 retires top-level `swarm fork` in favor of a future separately gated control/API owner. System auto-detects pending work at fork point. Reconstructs all entity states via mutation log reverse-apply.

**Flight recorder:** query pattern over events + entity_mutations + agent_turns + event_deliveries. Run reconstruction, entity timeline, drift detection.

**Builder API:** REST endpoints for run list/detail/events/mutations/fork/pause/resume/cancel.

**Runtime implementation required:**
- [ ] `runs` table + boot creation
- [ ] `entity_mutations` table + boot creation
- [ ] `run_id` column on events, event_deliveries, agent_sessions, agent_turns
- [ ] run_id propagation in bus publish path (child inherits from parent)
- [ ] Mutation logging in save_entity_field tool executor
- [ ] Mutation logging in system node handler executor (data_accumulation, compute, sets_gate, advances_to, clear)
- [ ] Future separately gated fork control/API surface (top-level `swarm fork` is retired in v1)

### New: Lineage Model (replaces trace_id)

**`trace_id` removed** from events and agent_turns DDL (column + index). Lineage now tracked via:
- `run_id` — flat grouping (all events in a run)
- `source_event_id` — causal tree (parent → child, recursive CTE)
- `runs.forked_from_run_id` + `runs.forked_from_event_id` — cross-run fork lineage

Postgres recursive CTE examples provided for causal chain and causal tree queries.

**Runtime implementation required:**
- [ ] Drop trace_id column from events (migration)
- [ ] Drop trace_id column from agent_turns (migration)
- [ ] Remove all trace_id references from Go runtime code
- [ ] Ensure source_event_id set on all emitted events

### New: Expression Context Specification (`handler_specification.expression_context`)

Explicitly defines namespaces available in CEL expressions (guard, on_complete, rules, data_accumulation, filter):

| Namespace | Resolves to | Available in |
|-----------|------------|--------------|
| `entity.*` | entity_state.fields | all expressions |
| `accumulated.*` | accumulator items array | on_complete, filter only |
| `payload.*` | inbound event payload | all expressions |
| `policy.*` | merged policy context | all expressions |

Key rule: `entity.*` does NOT include accumulated items. Dimension scores, fan-out results, and per-item data live in `accumulated.*` only. No implicit promotion.

**Boot validation rule:** for every `entity.X` in a condition/expression, verify at least one upstream handler writes X. Warning if no writer found.

**Runtime implementation required:**
- [ ] Expression field reference validation in `swarm verify` / boot checks
- [ ] Extract `entity.*` references from condition/check/expression strings
- [ ] Cross-reference against data_accumulation writes and compute.store_as
- [ ] Boot warning for unresolved references

### New: `create_entity` Handler Field (`handler_specification.create_entity`)

New handler field that mints a fresh entity_id before any other processing. All subsequent handler steps operate on the new entity. Execution position: first in the dependency graph (before query, guard, everything).

Key interactions specified:
- `create_entity` + `accumulate` = **PROHIBITED** (boot error). Accumulate needs same entity across events; create_entity mints a new one each time.
- `create_entity` + `fan_out` = ALLOWED. One entity, multiple fan-out events.
- `create_entity` + `guard` = ALLOWED, but guard sees empty entity state.

Dependency graph updated to show `create_entity` as first step. `handler_field_compliance` and `dialect_compliance` boot checks updated.

**Runtime implementation required:**
- [ ] Implement create_entity as first step in handler execution
- [ ] Boot error if create_entity + accumulate on same handler

### New: `compute.keys` and `operation_requirements` (`handler_specification.compute`)

`compute` now has explicit requirements for `weighted_average` operation:
- `keys.dimension_key` — field name in accumulated items identifying the dimension
- `keys.score_keys` — field name(s) containing numeric scores
- Boot error if weighted_average is missing keys

This was the root cause of the early scoring zero-composite bug — without keys, the runtime couldn't map accumulated items to dimensions.

### Changed: Universal Tools List Expanded (`engine.agent_session_management`)

Universal tools (auto-granted to all agents without explicit listing) expanded from 2 to 7:
- agent_message, mailbox_send (existing)
- get_entity, save_entity_field, query_entities, query_metrics, search_entities (new)

### Changed: `event_deliveries` DDL

Added columns: `in_progress` to status CHECK, `reason_code`, `active_session_id`, `started_at`. Added index on `active_session_id`.

**Runtime implementation required:**
- [ ] Migration to add new columns

### Changed: `event_receipts` DDL

Added outcomes: `terminal_reject`, `waiting`, `fanned_out`. Added `reason_code` column.

**Runtime implementation required:**
- [ ] Migration to add new outcomes and column

### Changed: Stateless Flow Entity Behavior (`flow_model.stateless_flows`)

Clarified: stateless does NOT mean entityless. If a handler in a stateless flow writes entity-scoped fields (data_accumulation, accumulate, compute), the platform auto-materializes an entity. The entity has no `current_state` and no lifecycle transitions, but persists accumulator state and computed values. Distinction: stateless = no state machine. Entityless = no entity at all (rare).

### New: Boot Check `input_pin_wiring` (check #33)

Warning severity. Fires when a flow's required input event pin has no emitter — no other flow's node or agent produces this event. Check count: 32 → 33 (21 error, 12 warning).

### Changed: `configure_routing` Moved to `planned_tools`

`configure_routing` removed from `platform_builtin_tools` and moved to a new `planned_tools` section. The permission exists (as an authorization gate) but the tool is not yet implemented. Planned for v1.5.

### Version bump: 1.4.0 → 1.5.0

---

### Empire Contract Changes (same session)

Product-level, not platform spec. Listed for implementer awareness.

**Scoring: on_complete shortlist condition fixed.** `entity.build_complexity` / `entity.automation_completeness` → `accumulated.filter()`. Root cause of the 10/11 accumulator bug — old condition referenced non-existent entity fields, on_complete evaluation failed, transaction rolled back on 11th dimension.

**Scoring: derivation flow reworked (two-phase).** Old: single handler emitted `scoring.derived_requested` directly to agent, no child entity creation. New: Phase 1 (`vertical.derived`) guards on parent + increments child_count + emits `scoring.derived_requested`. Phase 2 (`scoring.derived_requested` handler, new) creates child entity + emits `scoring.derived_ready`. Agent subscribes to `scoring.derived_ready`. Anti-bias preserved: primary agent never scores derived verticals.

**Scoring: derivation guard simplified.** Removed `entity.scores.icp_crispness` and `entity.scores.retention_architecture` (nested fields that don't exist). Guards can't access accumulated data.

**Validation: revision counters wired.** Added `data_accumulation` with `expression: entity.revision_count + 1` to 4 spec-revision handlers + `brand_revision_count + 1` to brand revision handler. Previously referenced in guards but never incremented.

**Signal-search: missing events.** Added `timer.search_campaign_timeout`. Removed duplicate cross-scope events that caused ambiguous alias in merge logic.

**Misc:** factory-cto orphan subscription removed. portfolio-coordinator gets `policy.change_requested`. Signals-mode MRA tool name fixed. pre-brand-agent stale emit removed. Validation output pins completed. Scoring schema pins cleaned. Cross-flow-exits docs updated.

## v1.4.0 (2026-03-27)

### Breaking: tools_tier2 renamed to tools
The agent field `tools_tier2` is renamed to `tools`. The tier naming was an Empire legacy — the platform has no tool tiers. The loader accepts `tools_tier2` as a deprecated alias with a boot warning.

### New: native_tools — provider-adaptive agent capabilities
Agents can now declare `native_tools` (bash, web_search, file_io) in their contract. The platform either enables the provider's native implementation or injects a platform fallback tool — the agent sees the same interface either way. All execution respects workspace mounts. Eliminates the need for custom MCP servers for simple infrastructure operations.

Historical note: this `v1.4.0` entry describes the original rollout model. The current authoritative contract for the shipped CLI path is the clarified rule above under `Unreleased`: CLI native tools are provider-native only, the platform does not inject fallback tools to satisfy `native_tools` on CLI, and visible surface must equal callable truth.

### New: platform.budget_threshold_crossed event
Budget monitoring is a platform concern — the platform owns spend_ledger and knows the spend. New event in the platform event catalog with level field (warning/throttle/emergency/ok). Thresholds read from policy.yaml. One event, no separate events per level.

### Fixed: platform_builtin_tools list was incomplete
Added 7 missing builtins: agent_fire, agent_hire, agent_reconfigure, configure_routing, human_task_decide, human_task_request, schedule. List is now 13 tools total. These operate on platform tables and work on every deployment — they were always builtins, the spec just didn't list them.

### Rewritten: tool_model — three-layer implementation model
Replaced the vague `workflow_registered` / `api_call` handler types with three explicit implementation classes:

- **platform_builtin** — tools the platform ships (agent_message, query_entities, etc.). Always available. List is exhaustive.
- **mcp** — tools from external MCP servers. Platform connects as MCP client, discovers via tools/list, forwards via tools/call. No per-tool handler code.
- **http** — tools defined declaratively as HTTP request templates in tools.yaml. Platform ships one generic HTTP executor. No per-tool handler code.

Key rule: the generic runtime ships platform_builtin handlers ONLY. All other tools are executed via the MCP client or HTTP executor.

### New: mcp_client — MCP server registration and tool discovery
Products register MCP servers in policy.yaml under `mcp_servers`. Each server gets a namespace prefix (e.g., `pg.list_tables`). Platform discovers tools at boot and routes calls at runtime. Agents only see MCP tools listed in their tools_tier2 — the full server catalog is never injected into agent context.

### New: http_tool_definition — declarative HTTP tools
Tools defined as URL + method + headers + body templates with `{{input.field}}` and `{{credentials.key}}` variable substitution. Products add API integrations without writing handler code.

### New: credential_store — secure credential management
Platform-provided interface for storing and retrieving secrets (API keys, tokens). Tools and MCP servers reference credentials by key name. The platform resolves to values at call time only. Never logged or persisted in event store.

### Deprecated: workflow_registered, api_call handler types
Accepted by the loader for backward compatibility. Normalized to http if an http block is present, otherwise boot warning.

## v1.3.0 (2026-03-25)

### pipeline.dead_letter → platform.dead_letter
Renamed to complete the platform.* namespace unification. All platform events now use the platform.* prefix with no exceptions.

### New: platform_events catalog (section 15)
Platform-owned operational events now have an authoritative catalog in the platform spec:
- `platform.dead_letter` (reference — schema in §engine.error_model)
- `platform.runtime_log` (reference — schema in §platform_tables.diagnostics_encoding)
- `platform.agent_failed`, `platform.agent_panic`, `platform.event_quarantined`, `platform.dead_letter_escalation`, `platform.reset`, `platform.auth_required`, `platform.paused` — new, with full payload schemas
- `platform.*` namespace reserved — boot error if products define events with this prefix
- Go source rule: every hardcoded event name must appear in catalog or be loaded from contracts

### Breaking: permission_scoping added, message_all/message_domain removed
- Removed `message_all` and `message_domain` from platform permission list
- Added `message_flow` — scoped to same flow instance
- `message_peers` unchanged — same manager_fallback parent, same flow instance
- New `permission_scoping` sub-section in §permissions_model defines:
  - Messaging scope: flow-instance-local only, cross-flow rejected
  - Management scope: same flow instance + target below caller in manager_fallback chain
  - Routing scope: self + manageable agents
  - manager_fallback semantics: escalation path (events), not messaging/management grant

### New: workspace_model (section 14)
Defines filesystem surfaces available to agent sessions. Three standard mount points, product-configurable scoping:
- `/workspace` — read-write, scope configured per workspace class (per-agent or per-flow-instance)
- `/data` — read-only, global, same mount across all workspace classes
- `/opt/swarm/contracts` — read-only, global, populated from contract loader at boot

Products define workspace classes in policy.yaml with a `workspace_scope` for /workspace. Platform validates at boot.

Key rules:
- /data is identical across all classes — no agent is denied access based on workspace class
- Mount paths are invariant across local, Docker, and production deployments
- Boot validation checks workspace class definitions exist and mounts are satisfiable
- Product-generic: no product-specific classes in platform spec

## v1.2.0

### Critical: emit payload default changed

**Default emit: entity base → trigger overlay → platform force.** Entity fields are the base layer, trigger payload overlays (trigger wins on collision), platform forces entity_id/trigger_event_type/current_state. payload_transform replaces entity+trigger layers.

### Additional spec clarifications
- produces field: optional, platform derives at boot
- required_agents: empty subscriptions/emits valid for managerial roles  
- Stateless flows: discovery-style flows documented (initial_state: null, states: [])
- Core vs extension fields: 7 core (guard, advances_to, sets_gate, data_accumulation, emits, rules, on_complete) + 11 extensions

### Audit remediation: vocabulary, CEL, dialect, docs

**Vocabulary cleaned** — guard and action definitions removed "code-backed function ID" language. Guards are CEL expressions. Actions are platform-only (create_flow_instance, record_evidence).

**Template syntax → CEL** — scoring on_complete conditions converted from `{{composite_shortlist}}` to `policy.composite_shortlist`. Operating guard converted from pseudo-code to valid CEL. All conditions now parseable CEL.

**Discovery cleanup** — source: → source_event: in aggregator data_accumulation. Empty signal handlers given accumulate declarations. Scan-orchestrator wildcard vs explicit subscriptions noted.

**Developer guide** — state_schema → entity_schema in quick-start. Payload transform note added for emitted events with custom fields.

**Product brief** — numbers updated to current (62 handlers, 29 agents, 195 events, 21 tools).

**Handoff** — duplicate Q&A sections labeled by date.

### Test writer feedback: observation model + 6 fixes

**observation_model section** — new top-level section defining exactly what emitted_events, entity_state, entity_fields, and handler_outcome contain from an external observer's perspective. Defines agent vs node observability asymmetry. Documents emission naming (fully-qualified URI in parent context).

**Computed write shape** — {target_field, expression} added to canonical data_accumulation. CEL expressions like "entity.counter + 1" now have a non-deprecated path.

**Custom tool schema** — enumerated required (description), optional (handler_type, input_schema, output_schema, parameters, returns), and invalid (endpoint, type) fields for tools.yaml definitions.

**Chain depth boundary** — clarified: intercepted event NOT emitted, handler outcome is success (handler succeeded, emission was intercepted), dead_letter records the downstream event.

**stateless alias** — conversation_mode: stateless accepted by loader, normalized to task internally.

## v1.2.0 (2026-03-16)

### Spec restructure: 23 sections → 12

Unified handler specification, eliminated redundant sections, consolidated compliance, cleaned terminology.

**Deleted sections (7):**
- builtin_hooks — product-specific guards/actions removed from platform spec
- directive_pattern — example, not a primitive
- compliance_rules — merged into compliance
- agent_registry_fields — redundant with handler_specification
- policy_driven_behavior — vague, covered by engine sections
- entity_schema_format — merged into contract_formats
- event_schema — merged into contract_formats

**Merged sections (6 → 2):**
- execution_primitives + handler_execution_order + system_node_specification → handler_specification
- compliance_rules + compliance_guidelines → compliance
- file_layout + entity_schema_format + event_schema + persistence_model → contract_formats

**New content:**
- hello_world_example — minimal 5-file flow example in contract_formats
- required_agents fulfillment rule — agent YAML key must match role name
- Minimum flow package defined once (was defined three times)
- platform_builtin_tools consolidated to one authoritative list in tool_model

**Terminology:**
- "stage" → "state" (normalized throughout)
- "project" → "flow" (no separate project concept)
- subscriptions_bootstrap — removed (was deprecated)
- Legacy data_accumulation shapes — removed from spec (loader still accepts)

**Coherence fixes:**
- create_flow_instance: handler action only, not a tool
- Timer shape: one canonical shape (runtime), glossary shape removed
- "a entity" → "a vertical" (typo)

## v1.1.1 (2026-03-13)

### Platform tables DDL (12 tables)
New `platform_tables` section in platform-spec.yaml declares the full DDL for all platform-owned infrastructure tables:
- events (append-only event store)
- event_deliveries (per-subscriber delivery tracking)
- event_receipts (handler completion acknowledgement, idempotency)
- entity_state (replaces workflow_instances — current state per entity)
- agents (runtime agent registry)
- agent_sessions (per-entity agent sessions)
- routing_rules (materialized subscription table, built at boot)
- mailbox (human-in-the-loop task queue)
- spend_ledger (LLM token and API cost tracking)
- flow_instances (static and dynamic instance registry)
- timers (durable timer store)
- dead_letters (failed events after retry exhaustion)

Legacy `workflow_state` section removed (superseded by `entity_state` table).

### Catalog fixture spec gaps resolved (3 areas)

**Loader-derived defaults** — schema.name derived from directory, schema.namespace from flow path, agent id from YAML map key, agent emit_events defaults to []. All optional in YAML.

**data_accumulation canonical shape** — one shape: writes is list of strings or {source_field, target_field} objects. source_event optional (defaults to trigger). Legacy shapes (bare string, dict) accepted but deprecated.

**Terminal state absorbing semantics** — terminal states are absorbing, no reopen mechanism. Backward transitions allowed for non-terminal states only. Reopen pattern: create new entity with copied data.

### Phase 8 final decisions (6)

**workflow_instances replacement** — split across entity_state (current state, gates, fields, accumulator), flow_instances (template, config, version), timers (timer state), events (transition history). state_change_history deleted (reconstructible from event store). credentials deleted from platform (product-owned secret).

**No entity registry table** — entity_state IS the registry. Added slug, name, entity_type as top-level columns for fast lookup. Generic store reads query entity_state directly.

**entity_metrics deleted** — metrics are entity fields in entity_state.fields JSONB. Spend aggregated from spend_ledger. Canonical fields documented.

**prompt_overrides deleted from platform** — delete PromptOverridePersistence and tests. Product-owned concern.

**schema_version kept** — new platform table. Tracks applied DDL version. ApplyMigrationFile deleted (no file-based migrations). Boot compares version, generates diff, applies.

**Test bootstrap** — platform_tables from spec only. No ddl-canonical.sql. No migrations/. Generic tests use platform tables. Product tests load contracts.

### event_receipts widened, mailbox finalized, diagnostics encoding defined

**event_receipts** — `entity_id` nullable, `handler_node` replaced by generic `subscriber_type` + `subscriber_id`. Added `duration_ms`, `idempotency_key`. UNIQUE on (event_id, subscriber_id). Replaces both pipeline_receipts and system_node_ledger.

**mailbox** — payload-centric model confirmed. All human_tasks become `item_type='human_task'`. Added `from_agent`, `summary`, `decision_notes`, `notified`, `source_event_id`. Old columns (context, timeout_at, priority) replaced by payload, expires_at, severity.

**diagnostics** — no dedicated table. runtime_log encoded as `events` with `event_name='platform.runtime_log'`, scope=global. pipeline_transitions encoded in `event_receipts` (state_before/after, side_effects, duration_ms, outcome).

### Task mode stateless, provider session ID policy
Task conversation mode is truly stateless — no agent_sessions row, no persistence. Provider session IDs stored in `agent_sessions.runtime_state.provider_session_id`, not in session_id PK.

### LLM backend separated from session mode
`agents.llm_backend` (api/cli_test/mock/local) tracks the invocation backend. `agent_sessions.runtime_mode` (task/session/session_per_entity) tracks conversation scope. These are orthogonal — same session scope can use different backends.

### routing_rules absorbs flow_instance_routes
`routing_rules` expanded to handle both contract patterns and materialized instance routes. No separate `flow_instance_routes` table needed. Added `is_materialized`, `materialized_from`, `source_flow`, `status`. Full lifecycle documented: boot → create_flow_instance → terminate → dispatch.

### Events, mailbox, agents tables extended for global scope + runtime descriptors

**events** — `entity_id` and `flow_instance` now nullable. Added `scope` column (entity/flow/global). Global events: system bootstrap, runtime recovery, admin commands.

**mailbox** — `entity_id` and `flow_instance` now nullable. Added `scope` and `severity` (normal/urgent/critical). Global mailbox items: recovery-failure alerts, system operational decisions.

**agents** — expanded to full runtime descriptor. Added `role`, `parent_agent_id`, `config` (JSONB), `subscriptions`, `emit_events`, `tools`, `permissions` (all JSONB, materialized from contracts at boot). `flow_instance` and `entity_id` now nullable for root-level and unscoped agents.

Consistent scoping model across all platform tables: entity_id/flow_instance nullable with explicit scope column. Unblocks remaining G-24 store migrations.

### Agent sessions relaxed for global/flow scope
`entity_id` and `flow_instance` now nullable. Added `scope_key` (runtime lookup key) and `runtime_mode`. Three session scopes:
- **entity**: scope_key = entity_id. One session per agent × entity.
- **flow**: scope_key = flow_instance. One session per agent × flow.
- **global**: scope_key = "global". One session per agent system-wide.

UNIQUE constraint changed from (agent_id, entity_id) to (agent_id, scope_key). Matches live runtime's (agentID, runtimeMode, scopeKey) acquisition pattern. Unblocks G-24 session store migration.

### Timers table relaxed for global schedules
`entity_id`, `flow_instance`, and `owner_node` are now nullable. Three timer scopes:
- **entity-scoped**: entity_id + flow_instance set. Cancelled on terminal state.
- **flow-scoped**: flow_instance set, entity_id NULL. Cancelled on flow termination.
- **global**: both NULL. Platform-level recurring work (scans, health checks, digests).

Added `owner_agent` (agents can own timers), `recurrence_cron` (cron expressions), `global_recurring` task_type. Unblocks G-24 schedule_store migration.

### Platform tables extended (3 tables)
Runtime semantics added to agent_sessions, timers, and events tables:

**agent_sessions** — added lease_holder/lease_expires_at (distributed locking), runtime_state (ephemeral agent data), scope (entity/flow/global session sharing), status, flow_instance.

**timers** — added fire_payload (context delivered when timer fires, replaces schedules table), owner_node, task_type (timer/scheduled_task/deadline), fired_at, expired status.

**events** — added idempotency_key (deduplication for external ingestion), source_event_id (causal chain for flight recorder), produced_by_type (node/agent/platform/external).

These extensions resolve the 3 schema mismatches blocking G-24 legacy SQL deletion.


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
10-field schema for platform.dead_letter events: original_event, original_payload, entity_id, flow_instance, failure_type, error_message, retry_count, chain_depth, handler_node, timestamp.

### Payload shaping semantics
Specified as engine contract. Default: trigger payload merged with entity state fields. payload_transform for explicit override.

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
