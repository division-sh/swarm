# MAS Platform Changelog

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
- `/opt/mas/contracts` — read-only, global, populated from contract loader at boot

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
