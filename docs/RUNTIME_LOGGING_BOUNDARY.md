# Runtime Logging Boundary

This note defines the intended split between persisted run diagnostics and process-global logs.

## Run-bound diagnostics

Use persisted runtime logs when the failure or warning belongs to an active run, event, delivery, turn, or session.

These diagnostics should go through the structured runtime log path:

- `diaglog.RunLog(...)`
- `RuntimeLogger`
- `bus.LogRuntime(...)`

Expected fields when available:

- `run_id`
- `trace_id`
- `event_id`
- `event_type`
- `agent_id`
- `entity_id`
- `session_id`
- `component`
- `action`
- `level`

Run-bound diagnostics are the canonical source for:

- `swarm status`
- postmortem debugging
- correlation across events, deliveries, sessions, and turns

If a code path already has a `context.Context` for a live event or turn, it should prefer persisted runtime logging over plain process logging.

## Process-global logs

Use process-global logs only when there is no truthful run context.

These are appropriate for:

- boot and shutdown
- health server lifecycle
- raw SQL/debug traces
- pre-runtime startup failures
- store or infrastructure warnings that occur outside a run

These should go through:

- `diaglog.ProcessLog(...)`
- or direct process logging when that is genuinely lower-level/debug-only

Process-global logs are not the canonical operator view for run health.

## Rule of thumb

- If the operator will ask "what happened in this run?", persist it as a run-bound runtime log.
- If there is no honest `run_id`/event/turn/session context, keep it process-global.
