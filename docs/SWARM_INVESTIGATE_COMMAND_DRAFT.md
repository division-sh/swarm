# Swarm Investigate Command Draft

## Purpose

`swarm investigate` should become the operator-facing CLI entrypoint for runtime diagnosis.

It should not introduce a new diagnostics subsystem. It should compose the canonical read
owners and supported helper surfaces that already exist:

- `make run-clear`
- `go run ./cmd/swarm status`
- health/readiness endpoints
- dashboard/API read surfaces
- canonical store-backed run-debug readers

The command should reduce direct SQL usage by making the supported diagnostic surfaces
discoverable, consistent, and scriptable from one place.

## Design Rules

1. `swarm investigate` is a thin orchestrator over existing canonical owners.
2. It must not add ad hoc SQL queries when a canonical read owner already exists.
3. CLI, dashboard, and API should consume the same underlying read models where possible.
4. The command should expose supported-path diagnosis first, then deeper drill-down surfaces.
5. Any new subcommand should name its canonical owner explicitly before implementation.
6. A follow-on subcommand is only valid if it wraps a real existing owner or a clearly identified future owner.
7. If no single owner exists yet, the command must stay explicitly thin and composed rather than inventing a second CLI-side read model.

## Current Capabilities We Already Have

### Supported Startup And Repro

- `make run-clear`
  - Best for supported-path startup, readiness, launcher failures, and real run initialization.
  - Owner: [run_clear.sh](/Users/youmew/dev/swarm/scripts/run_clear.sh)

- `go run ./cmd/swarm verify --contracts ... --platform-spec ...`
  - Best cheap compatibility gate for boot-time contract validity and verifier/runtime disagreement.
  - Owner: [main.go](/Users/youmew/dev/swarm/cmd/swarm/main.go)

### Run Diagnosis

- `go run ./cmd/swarm status`
  - Already provides:
    - latest-run resolution
    - run table status
    - operational state
    - blocking layer
    - blocking reason
    - event counts
    - delivery counts
    - recent events
    - recent mutations
    - dead letters
    - runtime warning/error summary
    - agent turn summary
  - Supports:
    - `--run-id`
    - `--json`
    - `--logs`
    - `--logs-all`
    - `--component`
  - Important split today:
    - shared run data comes from the canonical run-debug read surface
    - extra interpretation like:
      - operational state
      - blocking layer
      - blocking reason
      - heuristics
      still lives in CLI code
  - Current CLI owner: [main.go](/Users/youmew/dev/swarm/cmd/swarm/main.go#L334)
  - Current canonical read owner: [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go#L337)

- canonical run-debug readers
  - `ResolveLatestRunDebugRunID`
  - `ListRunDebugRuns`
  - `LoadRunDebugReport`
  - `LoadRunDebugTrace`
  - Owner: [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go)

### Joined Trace

- `GET /api/runs/{runID}/trace`
  - Gives one joined run trace across:
    - event
    - delivery
    - active session / audit session
    - turn
  - API owner: [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L742)
  - Canonical read owner: [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go#L578)

### Health And Readiness

- `GET /healthz`
- `GET /readyz`
- `GET /api/health`
- `GET /api/healthz`
  - Best for startup truth and lightweight runtime confirmation.
  - Important split today:
    - outer runtime health server owns the simple probe surfaces:
      - `/healthz`
      - `/readyz`
    - dashboard/API handler owns the structured JSON health surfaces:
      - `/api/health`
      - `/api/healthz`
  - Current outer probe owner: [main.go](/Users/youmew/dev/swarm/cmd/swarm/main.go#L1072)
  - Current dashboard/API owner: [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L238)

### Event And Runtime Diagnostic Surfaces

- `GET /api/events`
- `GET /api/events/{id}`
- `GET /api/events/flow`
  - Canonical event/delivery inspection without direct SQL.
  - Existing live-diagnosis capability:
    - flow-event streaming through `/api/events/flow`
  - Owners:
    - [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L600)
    - [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L163)

- `GET /api/runtime/logs`
  - Structured runtime log surface with filters:
    - `type`
    - `source`
    - `entity_id`
    - `component`
    - `level`
    - `error_code`
    - `order`
  - Owners:
    - [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L700)
    - [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L353)

- `GET /api/runtime/incidents`
  - Aggregated incident surface over canonical runtime logs.
  - Owners:
    - [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L722)
    - [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L417)

These are already credible current-head operator surfaces:

- runtime logs
- runtime incidents

They are real follow-on candidates for `swarm investigate`, not speculative future capabilities.

### Agent / Conversation / Entity / Instance Surfaces

- `GET /api/agents`
- `GET /api/agents/{id}`
- `POST /api/agents/{id}/actions/directive`
- `POST /api/agents/{id}/actions/restart`
- `POST /api/agents/{id}/actions/replay`

- `GET /api/conversations`
- `GET /api/conversations/{sessionID}`

- `GET /api/mailbox`
- `GET /api/mailbox/{id}`

- `GET /api/instances`
- `GET /api/instances/{id}`
- `GET /api/instances/aggregate`

These already provide operator-visible drill-down without querying:

- `agent_sessions`
- `agent_conversation_audits`
- `agent_turns`
- `mailbox`
- `flow_instances`
- `entity_state`

Primary API owner: [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L241)

Entity-oriented diagnosis is already stronger than a typical “future follow-on” category, even though the current APIs are still instance-shaped:

- `/api/instances/{id}`
- `/api/instances/aggregate`

These are real current-head no-SQL diagnosis surfaces, not just placeholders.

Future-facing CLI note:

- `entity` is the better long-run operator concept than deprecated `subject`
- current head still exposes most of that diagnosis through instance-oriented surfaces
- `swarm investigate entities` is honest only if it wraps those existing surfaces or a later real entity-diagnosis owner

Legacy note:

- the old subject status API was removed with the post-boundary subject-link cleanup
- `subject` is deprecated and must not be treated as a future-facing `swarm investigate` surface

These are not only passive read surfaces.

Current head already has actionable agent-level control surfaces that matter for recovery and repro:

- directive
- restart
- replay

Those surfaces matter operationally, but they are a better fit for a future `swarm control` family than for `swarm investigate`.

### Process Log

- `/tmp/swarm-empire.log`
  - Still useful for:
    - boot sequence
    - startup failures
    - top-level runtime errors
  - This remains a useful fallback surface, but should not be the default detailed run diagnosis path.

## Problem With The Current UX

The capability exists, but it is fragmented.

An operator currently has to remember some combination of:

- `make run-clear`
- `swarm status`
- `curl /healthz`
- `curl /readyz`
- `curl /api/runtime/logs`
- `curl /api/runtime/incidents`
- `curl /api/runs/{runID}/trace`
- `curl /api/events/...`
- conversation/agent/entity-instance endpoints

That is workable for expert users, but not a good default operator workflow.

There is also unnecessary command duplication today:

- `swarm status` already covers the main run-diagnosis path
- `swarm investigate run` would overlap with it unless `investigate run` becomes the replacement

This draft assumes:

- `swarm investigate run` replaces `swarm status`
- `swarm status` is deprecated rather than kept as a parallel long-term surface

## Proposed Command Shape

`swarm investigate` should be a top-level family with focused subcommands.

### First-Slice Subcommands

- `swarm investigate runs`
- `swarm investigate run`
- `swarm investigate trace`
- `swarm investigate health`

These four are enough to cover the most common diagnosis path:

1. is the runtime alive?
2. which runs exist?
3. which run am I investigating?
4. is the run actually progressing?
5. where is it blocked?
6. what happened across events, deliveries, sessions, and turns?
7. if none of the above are available, the startup diagnosis story is still TBD

### Follow-On Subcommands

- `swarm investigate logs`
- `swarm investigate incidents`
- `swarm investigate event`
- `swarm investigate events`
- `swarm investigate agent`
- `swarm investigate conversation`
- `swarm investigate entities`
- `swarm investigate entity`
- `swarm investigate startup` (TBD)

These follow-ons are only legitimate if they stay owner-backed.

That means:

- `investigate agent`
  - valid only if it wraps the existing agent reader
- `investigate conversation`
  - valid only if it wraps the existing conversation reader
- `investigate startup`
  - important, but still TBD because there is no single current startup owner and the right scope is not settled

This family should not become a catch-all umbrella for “anything diagnostic.”

## Draft CLI Contract

### `swarm investigate runs`

Purpose:
- let the operator choose the run to investigate without falling back to SQL, ad hoc API calls, or guesswork

Possible flags:
- `--limit <n>`
- `--json`

Behavior:
- list recent runs from the canonical run list owner
- default to a reasonable recent limit
- show a compact table with:
  - run id
  - root event type
  - run table status
  - started / created time
  - last update
  - ended time when present
  - event count
  - entity count
- if canonical run-list ownership later adds turn count, include it there rather than deriving it ad hoc in the CLI

Canonical owner:
- [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go#L260)

### `swarm investigate run`

Purpose:
- operator-oriented run diagnosis using:
  - the canonical run-debug report
  - plus the current run-interpretation layer that still lives in CLI code

Possible flags:
- `--run-id <uuid>`
- `--json`
- `--logs`
- `--logs-all`
- `--component <name>`

Behavior:
- default to latest run if `--run-id` is omitted
- replace `swarm status` instead of becoming a second long-term command
- reuse the existing `status` path initially rather than re-implementing run interpretation in a second place
- print:
  - run table status
  - operational state
  - blocking layer
  - blocking reason
  - event counts
  - delivery summary
  - dead letters
  - recent mutations
  - runtime warnings/errors
  - recent events
  - agent turns

Canonical persisted read owner:
- [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go#L337)

Current interpretation owner that still needs to be absorbed under the new command:
- [main.go](/Users/youmew/dev/swarm/cmd/swarm/main.go#L406)
- especially:
  - `projectRunOperationalStatus(...)`
  - `deriveRunStatusHeuristics(...)`

Long-run product gap:
- if `investigate run` is meant to become the main diagnosis surface, operational interpretation needs a real shared owner
- specifically for:
  - operational state
  - blocking layer
  - blocking reason
- today those still live as CLI-side interpretation rather than a shared run-diagnosis owner

### `swarm investigate trace`

Purpose:
- show the joined run trace across event, delivery, session, and turn

Possible flags:
- `--run-id <uuid>`
- `--limit <n>`
- `--json`

Behavior:
- load rows through the canonical run trace reader
- do not reconstruct joins inside the CLI
- treat this as the execution spine of the run, not the full debug transcript
- default output should stay compact and help the operator answer:
  - what happened
  - where it stopped
  - what object to inspect next
- richer detail should come through `--json`, not by overloading the default table output

Suggested compact default fields:
- event name
- entity id
- event time
- subscriber / delivery target
- delivery status
- session id
- turn id
- turn outcome / turn error
- retry count when present

Canonical owner:
- [run_debug_read_surface.go](/Users/youmew/dev/swarm/internal/store/run_debug_read_surface.go#L578)

Important current limit:
- `trace` is a key command, but it is not complete per-turn or per-tool diagnosis
- deeper investigation still often requires:
  - conversation detail
  - turn blocks
  - runtime logs
  - tool-call / tool-result surfaces
  - provider transcript inspection in harder cases
- `--stream` is a desirable future extension, but it is not a current-head capability
- if `trace --stream` is added later, it needs a real live-trace owner rather than ad hoc CLI polling dressed up as canonical trace

### `swarm investigate health`

Purpose:
- quick startup/readiness truth without running ad hoc curls

Possible flags:
- `--addr <host:port>`
- `--json`

Behavior:
- compose the current health/readiness surfaces
- for now, call:
  - `/healthz`
  - `/readyz`
  - `/api/health`
- print a compact diagnosis:
  - healthy / not healthy
  - ready / not ready
  - any error payloads

Current owners:
- outer probe owner in [main.go](/Users/youmew/dev/swarm/cmd/swarm/main.go#L1072)
- dashboard/API health owner in [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L238)

Intended end state:
- one shared health/readiness owner computes:
  - alive
  - ready
  - detailed checks
- probe endpoints and API endpoints may still present that shared truth differently

Important current limit:
- `investigate health` is not full startup diagnosis
- it answers alive/ready truth, but not the full startup story

## Draft Follow-On CLI Contract

### `swarm investigate logs`

Purpose:
- filtered structured runtime diagnostics

Flags:
- `--run-id <uuid>` if/when run-scoped filtering is directly exposed
- `--component <name>`
- `--level <level>`
- `--entity-id <id>`
- `--source <source>`
- `--error-code <code>`
- `--limit <n>`
- `--json`

Canonical owner:
- [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L353)

Owner note:
- this is already a real current-head operator surface, not a speculative addition

### `swarm investigate incidents`

Purpose:
- higher-level incident view for operator triage

Flags:
- `--since-hours <n>`
- `--component <name>`
- `--level <level>`
- `--mcp-only`
- `--limit <n>`
- `--json`

Canonical owner:
- [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L417)

Owner note:
- this is already a real current-head operator surface, not a speculative addition

### `swarm investigate event`

Purpose:
- inspect a specific event and its delivery lifecycle

Flags:
- `--event-id <id>`
- `--json`

Canonical owner:
- [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L264)

### `swarm investigate events`

Purpose:
- inspect or stream live event activity without dropping to SQL

Possible flags:
- `--type <event-type>`
- `--source <producer>`
- `--entity-id <id>`
- `--subscriber <id>`
- `--limit <n>`
- `--stream`
- `--json`

Behavior:
- without `--stream`, wrap the existing event list surface
- with `--stream`, wrap the existing live flow-event streaming surface
- this is especially useful for live diagnosis when persisted run summaries are too slow or too coarse

Current owners:
- event list/detail owner in [observability_sql.go](/Users/youmew/dev/swarm/internal/dashboard/server/observability_sql.go#L163)
- flow-event stream surface in [server.go](/Users/youmew/dev/swarm/internal/dashboard/server/server.go#L668)

Owner rule:
- valid only if it stays a thin wrapper over the existing event list and flow-stream surfaces
- not valid if it invents a second event-correlation model in the CLI

### `swarm investigate agent`

Purpose:
- inspect one agent and its operator-visible current state

Owner rule:
- valid only if it wraps the existing agent reader directly
- not valid as a new CLI summary layer with independent agent-state inference

### `swarm investigate conversation`

Purpose:
- inspect canonical conversation / turn / tool-call / turn-block state

Owner rule:
- valid only if it wraps the existing conversation reader directly
- not valid if it invents a second CLI-side summary of turn or tool state

Long-run product gap:
- current head still does not provide a clean operator-grade no-SQL surface for:
  - what tools were advertised
  - what tools were actually called
  - what failed
  - what the model saw
- deeper turn/tool diagnosis still falls back to conversation detail, turn blocks, runtime logs, and sometimes provider transcripts
- the likely long-run owner is a first-class composed conversation/session/turn diagnosis surface, not CLI heuristics

### `swarm investigate entities`

Purpose:
- list the entities created, touched, or currently visible for the investigation scope without dropping to SQL

Possible flags:
- `--run-id <uuid>`
- `--type <entity-type>`
- `--aggregate`
- `--limit <n>`
- `--json`

Current backing surfaces:
- `/api/instances`
- `/api/instances/{id}`
- `/api/instances/aggregate`

Intended behavior:
- when `--run-id` is provided, list the entities associated with that run if the backing owner can do so honestly
- otherwise, list a useful current entity-oriented slice from the existing instance-backed readers
- default output should help the operator choose the specific entity to inspect next

Owner note:
- the command name should be `entities`, because that is the future-facing operator concept
- current head still exposes this mostly through instance-oriented readers rather than a dedicated entity diagnosis owner
- this command is only honest if it stays a thin wrapper over those existing readers or a later real shared entity-diagnosis owner
- if run-scoped entity listing does not exist cleanly yet, the CLI should not fake it with ad hoc correlation logic

### `swarm investigate entity <entity-id>`

Purpose:
- inspect one specific entity in detail

Possible flags:
- `--json`

Current backing surfaces:
- `/api/instances/{id}`
- `/api/instances/aggregate`

Intended behavior:
- show the current known state for the chosen entity
- show the related instance/runtime context that already exists on current head
- make it obvious what run, flow, or recent activity this entity is tied to when the backing owner exposes that truth

Owner note:
- the required entity identifier should be positional, not a required flag
- this command is only honest if it stays backed by existing instance-oriented readers today or a later real entity-diagnosis owner
- it should not invent a second CLI-side entity summary model

### `swarm investigate startup` (TBD)

Purpose:
- eventually diagnose startup for a concrete runtime instance or endpoint without relying on ad hoc shell glue

Possible flags:
- TBD

Current state:
- do not commit to a v1 contract yet
- current startup evidence is still split across:
  - health/readiness surfaces
  - process logs
  - launcher/process state
  - control-plane reachability
- `make` and helper scripts are convenience wrappers, not canonical diagnosis owners
- the right command shape is still unresolved because startup is instance-scoped rather than one global truth

Current owners:
- none as a single credible startup diagnosis owner

Why this is TBD:
- startup is important, but the current repo does not provide one credible canonical owner over boot/readiness/process-log/control-plane state
- adding the command now would mostly create glue logic over scattered surfaces
- revisit this after there is a stronger scope and owner story

## Output Modes

Every subcommand should support:

- human-readable default output
- `--json`

Optional later mode:
- `--summary`

The JSON output should mirror the canonical owner structs as closely as possible.

## Recommended First Implementation Slice

Implement only:

1. `swarm investigate runs`
2. `swarm investigate run`
3. `swarm investigate trace`
4. `swarm investigate health`

Why:

- they already have enough real owner support to implement cleanly
- they provide immediate value
- they avoid direct SQL for the most common diagnosis path
- they do not require new semantics, only consolidation
- they still leave deep per-turn / per-tool diagnosis and startup diagnosis as follow-ons instead of pretending first slice is complete

## Explicit Non-Goals

- no new store-side heuristic projections
- no direct SQL duplication in the CLI when a canonical owner already exists
- no attempt to absorb all dashboard capabilities into one PR
- no new normative platform-spec contract in the first slice
- no replacement of `make run-clear` in the first slice
- no claim that `trace` alone makes run diagnosis complete
- no umbrella “everything diagnostic” command family built on new CLI-side read models
- no mixed diagnosis/control umbrella; existing actions belong in a future `swarm control` family

## Suggested Implementation Structure

### CLI Routing

Add a new top-level command family in `cmd/swarm/main.go`:

- `swarm investigate run`
- `swarm investigate runs`
- `swarm investigate trace`
- `swarm investigate health`

For `run` specifically:

- `swarm investigate run` should replace `swarm status`
- do not preserve both as long-term operator commands

### Owner Consumption

- `investigate runs`
  - should consume `ListRunDebugRuns`
  - should not derive turn count ad hoc unless the canonical run-list owner exposes it
- `investigate run`
  - should consume the canonical run-debug report
  - should absorb the current `status` interpretation logic instead of duplicating it again
- `investigate trace`
  - should call `LoadRunDebugTrace`
- `investigate health`
  - should compose the existing health/readiness surfaces first
  - should move to one shared health/readiness owner if that owner is created
- `investigate events`
  - should wrap the existing event list and flow-stream surfaces
  - should not invent a second event-correlation model in the CLI
- `investigate startup`
  - should compose the existing helper, health/readiness, process-log, and control-plane startup surfaces
  - should stay thin and should not replace `make run-clear` in the first slice

### Shared Rules

- resolve latest run only through `ResolveLatestRunDebugRunID`
- refuse to operate when canonical run-debug capabilities are unavailable
- fail closed rather than silently degrading to partial table reads

## Capability Gaps Still Left After This Draft

Even with `swarm investigate`, a few gaps remain:

- there is no single CLI wrapper today for the full observability API family
- startup diagnosis is still composed from multiple existing surfaces rather than one clean owner
- live event diagnosis still needs its CLI wrapper to be implemented over the existing event list / flow-stream surfaces
- some deep turn-level diagnosis still depends on conversation and runtime-log surfaces rather than one compact CLI view
- stalled-run diagnosis still lacks a canonical shared operational-state owner; current `status`/`investigate run` interpretation remains partly CLI-side
- per-turn / per-tool diagnosis still lacks a first-class operator-grade no-SQL owner and often forces transcript spelunking or deep conversation/runtime-log fallback
- live trace streaming does not exist yet as a first-class owner-backed surface

Those are good follow-on issues, but they should not block the first slice.

## Recommended Tracker Framing

This should be framed as:

- operator-surface consolidation over existing canonical read owners

Not as:

- a new diagnostics subsystem
- a spec-first initiative
- a broad observability redesign

## Bottom Line

We already have most of the diagnosis capability needed to avoid direct SQL.

What is missing is not mainly data. It is a single supported operator entrypoint.

`swarm investigate` should provide that entrypoint by composing:

- canonical run listing
- the current `status` run interpretation path, under a new home
- canonical run trace
- health/readiness
- later logs/incidents/event/agent/conversation drill-down

without creating a second model of runtime truth.
