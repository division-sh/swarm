# Swarm Builder API Reference

**Version:** 0.1.0
**Status:** Deprecated adapter reference — existing legacy builder methods documented from source, proposed methods retained only as historical notes.
**Transport:** JSON-RPC 2.0 over HTTP and WebSocket
**Authority:** Not authoritative. The canonical user-facing API contract is `docs/specs/swarm-platform/platform/contracts/platform-spec.yaml` under `api_specification`, with `docs/specs/swarm-platform/platform/contracts/openrpc.json` generated from that section. Legacy builder endpoints are adapter surfaces until migrated or removed by later #665 slices.

---

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/rpc` | POST | JSON-RPC 2.0 |
| `/api/rpc` | POST | Alias |
| `/ws` | GET | WebSocket (RPC + channel subscriptions) |
| `/api/ws` | GET | Alias |

## JSON-RPC 2.0 Envelope

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "method": "engine.ping",
  "params": {}
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": { ... }
}
```

**Error:**
```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "error": {
    "code": -32601,
    "message": "method not found",
    "data": { "method": "foo.bar" }
  }
}
```

---

## Error Codes

| Code | Meaning |
|------|---------|
| -32700 | Parse error (malformed JSON) |
| -32601 | Method not found |
| -32602 | Invalid params (missing required field) |
| -32004 | Resource unavailable (controller not configured, entity not found) |
| -32000 | Internal error (runtime failure) |

---

## Methods

### Engine

#### `engine.ping`

Health check.

**Params:** none
**Returns:**
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

---

### Project Lifecycle

#### `project.open`

Load contracts from a directory, validate, boot runtime.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_dir` | string | yes | Path to contracts root (package.yaml location) |

**Returns:** `BuilderProjectStatus`
```json
{
  "project_dir": "/path/to/contracts",
  "loaded": true,
  "workflow_name": "generic-swarm-bundle",
  "workflow_version": "1.0.0"
}
```

#### `project.reload`

Hot-reload contracts. If `project_dir` is omitted, reloads current project.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_dir` | string | no | Path to contracts root. Defaults to current. |

**Returns:** `BuilderProjectStatus`

#### `project.close`

Shut down runtime, unload project.

**Params:** none
**Returns:** `BuilderProjectStatus` (empty, `loaded: false`)

---

### Run Control

#### `run.start`

Start a debug run. Injects input events into the runtime bus.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Unique run identifier |
| `inputs` | object | no | Map of `event_name` to payload object. Each is published to the bus. If payload lacks `entity_id`, defaults to `run_id`. |
| `breakpoints` | string[] | no | Node IDs to break on |

**Returns:**
```json
{
  "run_id": "test-1",
  "status": "started"
}
```

**Behavior:**
- Attaches runtime logger hook for event streaming
- Publishes each input event to the bus
- Emits `run.started` on the WebSocket channel
- Starts a background goroutine awaiting bus quiescence (30s timeout)

#### `run.stop`

Stop a run and reset runtime state.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run to stop |

**Returns:** `{run_id, status: "stopped"}`

#### `run.pause`

Pause event ingress. Runtime stops processing new events.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run to pause |

**Returns:** `{run_id, status: "paused"}`

#### `run.continue`

Resume a paused run. Optionally submit a human decision.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run to resume |
| `instance_ids` | string[] | no | Scope to specific instances |
| `decision` | string | no | Human decision: `approved`, `rejected`, `deferred` |

**Returns:** `{run_id, status: "running"}`

**Behavior:** If `decision` is set and a `human.task_waiting` is pending, publishes `human_task.{decision}` to the bus before resuming.

#### `run.step`

Execute one handler step, then pause.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run to step |
| `node_id` | string | no | Specific node to step through |
| `instance_id` | string | no | Specific entity instance |

**Returns:** `{run_id, status: "running"}`

**Behavior:** Resumes runtime, pauses again after the next handler execution matching the node/instance filter.

#### `run.retry`

Retry a failed handler.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run ID |
| `node_id` | string | no | Node that failed |
| `instance_id` | string | no | Entity instance |

**Returns:** `{run_id, status: "running"}`

#### `run.skip`

Skip a blocked handler. If a human task is pending, submits "deferred."

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Run ID |
| `node_id` | string | no | Node to skip |
| `instance_id` | string | no | Entity instance |

**Returns:** `{run_id, status: "running"}`

#### [PROPOSED] `run.inject`

Inject an event into an active run without stopping it.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |
| `event_name` | string | yes | Event to inject |
| `payload` | object | yes | Event payload |

**Returns:** `{run_id, event_id, status: "injected"}`

#### [PROPOSED] `run.add_breakpoint`

Add a breakpoint to a running session.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |
| `node_id` | string | yes | Node to break on |

**Returns:** `{run_id, breakpoints: [...]}`

#### [PROPOSED] `run.remove_breakpoint`

Remove a breakpoint from a running session.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |
| `node_id` | string | yes | Node to unbreak |

**Returns:** `{run_id, breakpoints: [...]}`

#### [PROPOSED] `run.list_breakpoints`

List breakpoints for a run.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |

**Returns:** `{run_id, breakpoints: [...], tripped: [...]}`

---

### State Inspection

#### `state.list_instances`

List all flow instances.

**Params:** none
**Returns:**
```json
{
  "instances": [
    { "instance_id": "...", "flow_template": "...", "mode": "...", "status": "..." }
  ]
}
```

#### `state.get_entity`

Get entity state, gates, and accumulator data.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `instance_id` | string | yes | Entity/instance ID |

**Returns:**
```json
{
  "entity": {
    "state": "assigned",
    "priority_score": 82.5,
    "...": "..."
  },
  "gates": {
    "g_reviewed": true,
    "g_approved": false
  },
  "accumulated": {
    "dimensions_received": { "...": "..." }
  }
}
```

#### [PROPOSED] `state.get_event_history`

Query events for an entity or flow instance.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `entity_id` | string | no | Filter by entity |
| `flow_instance` | string | no | Filter by flow instance |
| `event_name` | string | no | Filter by event name |
| `limit` | integer | no | Max results (default 100) |
| `offset` | integer | no | Pagination offset |

**Returns:**
```json
{
  "events": [
    {
      "event_id": "...",
      "event_name": "order.created",
      "entity_id": "...",
      "flow_instance": "...",
      "payload": { "..." },
      "produced_by": "...",
      "produced_by_type": "node",
      "chain_depth": 0,
      "source_event_id": "...",
      "created_at": "2026-03-26T..."
    }
  ],
  "total": 47
}
```

#### [PROPOSED] `state.list_agents`

List registered agents with status.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `flow_instance` | string | no | Filter by flow instance |
| `status` | string | no | Filter: active, paused, terminated |

**Returns:**
```json
{
  "agents": [
    {
      "agent_id": "workflow-coordinator",
      "role": "workflow_coordinator",
      "model_tier": "sonnet",
      "conversation_mode": "session",
      "status": "active",
      "turn_count": 3,
      "last_active_at": "2026-03-26T..."
    }
  ]
}
```

#### [PROPOSED] `state.get_agent_session`

Get agent session detail including conversation history.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `agent_id` | string | yes | Agent to inspect |
| `scope_key` | string | no | Session scope key (entity_id, flow_instance, or "global") |

**Returns:**
```json
{
  "session_id": "...",
  "agent_id": "workflow-coordinator",
  "scope": "global",
  "turn_count": 3,
  "conversation": [ "..." ],
  "runtime_state": { "..." },
  "status": "active"
}
```

#### [PROPOSED] `state.get_dead_letters`

Query dead-lettered events.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `flow_instance` | string | no | Filter by flow instance |
| `failure_type` | string | no | handler_error, chain_depth_exceeded, retry_exhausted |
| `entity_id` | string | no | Filter by entity |
| `limit` | integer | no | Max results (default 50) |

**Returns:**
```json
{
  "dead_letters": [
    {
      "dead_letter_id": "...",
      "original_event": "review.completed",
      "original_payload": { "..." },
      "entity_id": "...",
      "flow_instance": "processing",
      "failure_type": "retry_exhausted",
      "error_message": "...",
      "retry_count": 3,
      "handler_node": "ticket-router",
      "created_at": "2026-03-26T..."
    }
  ]
}
```

#### [PROPOSED] `state.list_timers`

List active timers.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `entity_id` | string | no | Filter by entity |
| `flow_instance` | string | no | Filter by flow instance |
| `status` | string | no | active, fired, cancelled |

**Returns:**
```json
{
  "timers": [
    {
      "timer_id": "...",
      "timer_name": "sla_timeout",
      "entity_id": "...",
      "fire_event": "timer.sla_breach",
      "fire_at": "2026-03-27T...",
      "recurring": false,
      "status": "active"
    }
  ]
}
```

#### [PROPOSED] `state.get_routing`

Query materialized routing rules.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_pattern` | string | no | Filter by event name/pattern |
| `subscriber_id` | string | no | Filter by subscriber |
| `flow_instance` | string | no | Filter by flow instance |

**Returns:**
```json
{
  "rules": [
    {
      "rule_id": "...",
      "event_pattern": "order.created",
      "subscriber_type": "node",
      "subscriber_id": "ticket-router",
      "flow_instance": "processing",
      "is_wildcard": false,
      "status": "active"
    }
  ]
}
```

#### [PROPOSED] `state.list_mailbox`

Query pending mailbox items.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `status` | string | no | pending, decided, expired |
| `entity_id` | string | no | Filter by entity |
| `item_type` | string | no | Filter by type |

**Returns:**
```json
{
  "items": [
    {
      "item_id": "...",
      "entity_id": "...",
      "item_type": "order_approval",
      "from_agent": "review-coordinator",
      "severity": "normal",
      "summary": "Vertical X ready for review",
      "payload": { "..." },
      "status": "pending",
      "created_at": "2026-03-26T..."
    }
  ]
}
```

---

### Contract Introspection

#### [PROPOSED] `contract.get_flows`

List all loaded flows with hierarchy.

**Params:** none
**Returns:**
```json
{
  "flows": [
    {
      "flow_id": "intake",
      "path": "flows/intake",
      "mode": "static",
      "parent": null,
      "initial_state": null,
      "terminal_states": [],
      "states": [],
      "node_count": 2,
      "agent_count": 4,
      "event_count": 20
    }
  ]
}
```

#### [PROPOSED] `contract.get_node`

Get a system node's full declaration.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `node_id` | string | yes | Node identifier |
| `flow_id` | string | no | Flow scope (for disambiguation) |

**Returns:**
```json
{
  "id": "ticket-router",
  "execution_type": "system_node",
  "subscribes_to": ["..."],
  "produces": ["..."],
  "event_handlers": { "...": { "..." } },
  "state_schema": { "..." },
  "timers": [ "..." ]
}
```

#### [PROPOSED] `contract.get_agents`

List agents for a flow (or all).

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `flow_id` | string | no | Filter by flow |

**Returns:**
```json
{
  "agents": [
    {
      "agent_id": "workflow-coordinator",
      "type": "root",
      "role": "workflow_coordinator",
      "model_tier": "sonnet",
      "subscriptions": ["..."],
      "emit_events": ["..."],
      "tools": ["..."],
      "permissions_bundle": "coordinator",
      "workspace_class": "shared",
      "manager_fallback": null
    }
  ]
}
```

#### [PROPOSED] `contract.get_events`

List event schemas for a flow (or all).

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `flow_id` | string | no | Filter by flow |

**Returns:**
```json
{
  "events": [
    {
      "event_name": "order.created",
      "flow": "intake",
      "payload": {
        "customer_name": "string",
        "region": "string",
        "...": "..."
      }
    }
  ]
}
```

---

### Credential Management

Credentials are write-only in the builder. The API never returns secret values after write — only metadata.

#### `credentials.list`

List all credential keys with metadata.

**Params:** none
**Returns:**
```json
{
  "credentials": [
    {
      "key": "sendgrid_api_key",
      "source": "file",
      "writable": true,
      "present": true,
      "updated_at": "2026-03-27T14:30:00Z"
    },
    {
      "key": "meta_graph_token",
      "source": "env",
      "writable": false,
      "present": true,
      "updated_at": null
    },
    {
      "key": "whois_api_key",
      "source": null,
      "writable": true,
      "present": false,
      "updated_at": null
    }
  ],
  "required_by": {
    "sendgrid_api_key": ["email_api"],
    "meta_graph_token": ["instagram_handle_check", "whatsapp_name_check", "instagram_api"],
    "whois_api_key": ["domain_availability_check"]
  }
}
```

`required_by` maps each credential key to the tools and MCP servers that reference it. Derived from tools.yaml `credentials` fields and policy.yaml `mcp_servers.*.credentials_key`.

#### `credentials.set`

Store a credential value. Writes to the file store only. Cannot overwrite env-sourced credentials (use env vars directly).

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | yes | Credential key name |
| `value` | string | yes | Secret value (transmitted once, never returned) |

**Returns:**
```json
{
  "key": "sendgrid_api_key",
  "source": "file",
  "writable": true,
  "present": true,
  "updated_at": "2026-03-27T14:35:00Z"
}
```

**Error:** If the key exists in the env tier, returns error: "Credential 'sendgrid_api_key' is sourced from environment variable. Update the env var directly."

#### `credentials.delete`

Remove a credential from the file store. Cannot delete env-sourced credentials.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | yes | Credential key to delete |

**Returns:** `{key, deleted: true}`

**Error:** If the key exists only in the env tier: "Cannot delete env-sourced credential."

#### [PROPOSED] `credentials.test`

Test a credential by attempting a lightweight operation with the tool or MCP server that uses it.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | string | yes | Credential key to test |

**Returns:**
```json
{
  "key": "sendgrid_api_key",
  "status": "ok",
  "tested_via": "email_api",
  "latency_ms": 230
}
```

**Behavior:** For MCP server credentials, calls `tools/list` on the server. For HTTP tool credentials, makes a HEAD or GET request to the tool's URL. Returns status and latency.

---

### Validation

#### `validate.full`

Run full contract validation (same checks as boot).

**Params:** none
**Returns:**
```json
{
  "status": "pass",
  "errors": [],
  "warnings": [
    {
      "check_id": "gate_info",
      "severity": "warning",
      "message": "sets_gate 'g_reviewed' but no guard reads entity.gates.g_reviewed",
      "flow_path": "processing",
      "node_id": "order-orchestrator",
      "suggestion": "Add a guard checking this gate, or remove the sets_gate"
    }
  ],
  "summary": {
    "errors": 0,
    "warnings": 4,
    "flows_checked": 5,
    "duration_ms": 12
  }
}
```

---

### Timer Manipulation (debug only)

#### [PROPOSED] `run.fire_timer`

Force-fire a timer immediately (debug shortcut for testing timeout paths).

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |
| `timer_id` | string | yes | Timer to fire |

**Returns:** `{run_id, timer_id, status: "fired"}`

#### [PROPOSED] `run.cancel_timer`

Cancel an active timer.

**Params:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `run_id` | string | yes | Active run |
| `timer_id` | string | yes | Timer to cancel |

**Returns:** `{run_id, timer_id, status: "cancelled"}`

---

## WebSocket Protocol

### Client Frames

**Subscribe:**
```json
{ "type": "subscribe", "channel": "engine:health" }
```

**Unsubscribe:**
```json
{ "type": "unsubscribe", "channel": "engine:health" }
```

**RPC (over WebSocket):**
```json
{ "type": "rpc", "id": "1", "method": "engine.ping", "params": {} }
```

### Server Frames

**Event:**
```json
{ "type": "event", "channel": "run:events:test-1", "data": { ... } }
```

**RPC Response:**
```json
{ "jsonrpc": "2.0", "id": "1", "result": { ... } }
```

### Channels

| Channel | Data | Interval | Description |
|---------|------|----------|-------------|
| `engine:health` | `BuilderEngineHealth` | 5s heartbeat | Runtime, database, project status |
| `run:events:{run_id}` | `RunEventEnvelope` | real-time | Run event stream. Replays history on subscribe. |

### `BuilderEngineHealth` Schema

```json
{
  "status": "ok | degraded",
  "version": "0.1.0",
  "timestamp": "2026-03-26T...",
  "ready": true,
  "runtime": { "..." },
  "database": { "..." },
  "database_error": "",
  "project": {
    "project_dir": "/path/to/contracts",
    "loaded": true,
    "workflow_name": "my-project",
    "workflow_version": "4.2.0"
  }
}
```

---

## Run Event Types

Events streamed on `run:events:{run_id}` channel.

### Run Lifecycle

| Type | When | Payload |
|------|------|---------|
| `run.started` | Run begins | `{run_id}` |
| `run.completed` | Bus quiescence reached | `{run_id, summary: {duration_ms, total_events}}` |
| `run.failed` | Run error or timeout | `{run_id, error}` |
| `run.stopped` | Manual stop via `run.stop` | `{run_id}` |
| `run.paused` | Breakpoint, step, or human task | `{run_id, reason}` — reason: `node_breakpoint`, `step_complete`, `human_task_waiting` |
| `run.resumed` | Continue/step/retry/skip | `{run_id, mode, instance_ids, decision}` |

### Debugging

| Type | When | Payload |
|------|------|---------|
| `run.breakpoint_hit` | Execution hit a breakpoint node | `{reason: "node_breakpoint"}` + `node_id`, `instance_id` |
| `handler.retried` | Handler retried via `run.retry` | `node_id`, `instance_id` |
| `handler.skipped` | Handler skipped via `run.skip` | `node_id`, `instance_id` |

### Human-in-the-Loop

| Type | When | Payload |
|------|------|---------|
| `human.task_waiting` | `human_task.requested` emitted | `{decision_options: ["approved", "rejected", "deferred"]}` + `node_id`, `instance_id` |
| `human.task_submitted` | Decision submitted via `run.continue` | `{decision}` + `node_id`, `instance_id` |

### Engine Activity

| Type | When | Payload |
|------|------|---------|
| `event.fired` | Event published to bus | `{event_name, source, payload}` + `instance_id`, `node_id` |
| `runtime.log` | Any runtime log entry | `{level, component, action, event_type, agent_id, detail, error}` + `instance_id`, `node_id` |

### Common Envelope Fields

Every run event has:
```json
{
  "id": "uuid",
  "type": "event.fired",
  "timestamp": "2026-03-26T...",
  "node_id": "ticket-router",
  "instance_id": "entity-uuid",
  "payload": { "..." }
}
```

`node_id` and `instance_id` are present when applicable, omitted otherwise.

---

## Implementation Notes

- Run event buffer is capped at 128 events per session (oldest evicted)
- WebSocket subscribes to `run:events:{run_id}` replay buffered events on connect
- Completion timeout is 30s (hardcoded, should be configurable per-run)
- Human decision normalization: `approve` → `approved`, `reject` → `rejected`, `defer` → `deferred`
- `run.skip` auto-submits `deferred` if a human task is pending
- Runtime pause/resume uses global ingress control (`runtimebus.PauseRuntimeIngress`)
