# Dashboard Generic Server Audit

## Goal

Build a generic Go server that exposes platform/runtime data with no Empire-specific concepts, then add a TypeScript adapter layer that maps those generic resources into the dashboard's current views.

The boundary should be:

- Go server: generic resources and generic queries only
- TypeScript adapter: transforms generic server responses into dashboard-specific view models

## Frontend Reality

The dashboard currently calls a mixed API surface:

- generic-looking endpoints under `/api/*`
- legacy dashboard/product-shaped endpoints under `/dashboard/api/*`

The wrappers live in:

- `internal/dashboard/ui/src/api/dashboardCore.ts`
- `internal/dashboard/ui/src/api/dashboardRuntime.ts`
- `internal/dashboard/ui/src/api/dashboardWorkflow.ts`
- `internal/dashboard/ui/src/api/dashboardPortfolio.ts`
- `internal/dashboard/ui/src/api/dashboardAgentConsole.ts`
- `internal/dashboard/ui/src/api/agents.ts`
- `internal/dashboard/ui/src/api/flow.ts`
- `internal/dashboard/ui/src/api/holding.ts`
- `internal/dashboard/ui/src/api/health.ts`

The key frontend property is that its TS contracts are loose and normalization-heavy:

- `internal/dashboard/ui/src/types/core.ts`
- `internal/dashboard/ui/src/types/runtime.ts`
- `internal/dashboard/ui/src/types/workflow.ts`
- `internal/dashboard/ui/src/types/portfolio.ts`

That makes an adapter strategy viable.

## Current Implementation Status

The generic server is no longer hypothetical.

Implemented in Go:

- `internal/dashboard/server/server.go`
- `internal/dashboard/server/conversations_sql.go`
- mounted from `cmd/mas/main.go` on the existing health/admin HTTP server

Currently mounted generic endpoints:

- `GET /api/health`
- `GET /api/agents`
- `GET /api/conversations`
- `GET /api/conversations/:agentID`
- `GET /api/mailbox`
- `GET /api/mailbox/:id`
- `GET /api/instances`
- `GET /api/instances/:id`
- `GET /api/instances/aggregate`

Implemented in TypeScript:

- generic resource clients:
  - `src/api/resources/agents.ts`
  - `src/api/resources/conversations.ts`
  - `src/api/resources/health.ts`
  - `src/api/resources/instances.ts`
  - `src/api/resources/mailbox.ts`
- adapters:
  - `src/adapters/agents.ts`
  - `src/adapters/conversations.ts`
  - `src/adapters/digest.ts`
  - `src/adapters/funnel.ts`
  - `src/adapters/health.ts`
  - `src/adapters/holding.ts`
  - `src/adapters/holdingDetail.ts`
  - `src/adapters/incidentArtifacts.ts`
  - `src/adapters/mailbox.ts`
  - `src/adapters/overview.ts`
  - `src/adapters/trace.ts`

Current migration state:

- already generic and in use:
  - events
  - event detail
  - runtime logs
  - runtime incidents
  - graph
  - pipeline/workflow flow
  - tasks
- generic-only or generic-first:
  - agents
  - health
  - overview
  - digest
  - conversations
  - conversation artifacts
  - holding
  - holding detail (runtime-owned fields)
  - mailbox
-  - funnel
  - trace
- still legacy-backed or partially legacy-overlaid:
  - control targets/actions
  - holding detail business artifacts
  - shard scans

## Important Correction

The generic Go server should not expose Empire concepts like:

- verticals
- holding
- portfolio
- campaigns
- opcos
- funnel

Those are product projections, not platform resources.

Note on shard scans:

- a dedicated shard-scan operator resource can still be generic if it is framed as runtime scan execution state
- what should be avoided is a dashboard/Empire-specific projection of shard scans

If the dashboard wants those views, the TS adapter should derive them from generic resources such as:

- workflow instances
- events
- tasks
- mailbox items
- agents
- schedules
- graphs

## Current Endpoint Inventory

### Current Read Endpoints Used by the Dashboard

- `GET /dashboard/api/overview`
- `GET /dashboard/api/digest`
- `GET /dashboard/api/agents`
- `GET /dashboard/api/health`
- `GET /dashboard/api/control/targets`
- `GET /dashboard/api/funnel`
- `GET /dashboard/api/holding`
- `GET /dashboard/api/holding/vertical`
- `GET /dashboard/api/pipeline/shards`
- `GET /dashboard/api/pipeline/shards/:id`
- `GET /dashboard/api/verticals/:slug/trace`
- `GET /dashboard/api/conversations`
- `GET /dashboard/api/conversations/:agentID`
- `GET /dashboard/api/conversations/:agentID/artifacts`
- `GET /api/tasks`
- `GET /api/tasks/stats`
- `GET /api/mailbox`
- `GET /api/events`
- `GET /api/events/:id`
- `GET /api/runtime/logs`
- `GET /api/runtime/incidents`
- `GET /api/graph`
- `GET /api/pipeline/graph`
- `GET /api/agents/:id/prompt`
- `GET /api/agents/:id/prompt/diff`
- `GET /api/events?stream=true...`
- `GET /api/events/flow?stream=true...`

### Current Write Endpoints Used by the Dashboard

- `POST /api/tasks/:id/claim`
- `POST /api/tasks/:id/complete`
- `POST /api/tasks/:id/reject`
- `POST /api/mailbox/:id/decide`
- `PUT /api/agents/:id/prompt`
- `DELETE /api/agents/:id/prompt`
- `POST /api/chat/:agentID`
- `POST /dashboard/api/control/directive`
- `POST /dashboard/api/control/chat`
- `POST /dashboard/api/control/agents/restart`
- `POST /dashboard/api/control/agents/replay`
- `POST /dashboard/api/control/events/requeue`
- `POST /dashboard/api/control/runtime`
- `POST /dashboard/api/control/seed-org`
- `POST /dashboard/api/control/verticals/create`
- `POST /api/pipeline/shards/:id/:action`

## Existing Generic Backend Seams

There is no existing `internal/dashboard` Go server package in this tree. The dashboard is currently proxied by nginx to orchestrator routes:

- `deploy/nginx/dashboard.conf`

But the runtime/store layer already exposes generic persistence seams we can build on:

- agents: `internal/store/agent_store.go`
- mailbox: `internal/store/mailbox.go`
- conversations / turns: `internal/store/llm_store.go`
- events / receipts: `internal/store/events.go`, `internal/store/event_receipt_store.go`
- schedules: `internal/store/schedule_store.go`
- flow-instance routes: `internal/store/flow_instance_routes.go`
- workflow instances: `internal/runtime/pipeline/workflow_instance_store.go`
- aggregate instance helpers: `internal/store/digest.go`

So the Go server can be mostly a thin query/serialization layer plus some aggregation code.

## Correct Generic Resource Model

The generic server should speak in these platform terms:

- health
- agents
- conversations
- events
- runtime logs
- runtime incidents
- tasks
- mailbox items
- workflow instances
- schedules
- graphs / workflow topology
- directives / operator actions

Everything else should be derived from those.

Practical migration status:

- `trace` is now derived in TypeScript from generic `/api/events`
- `shard scans` remain the main unresolved portfolio/pipeline legacy surface because there is no generic runtime scan resource yet

## Recommended Generic Go API

Serve only `/api/*`.

### Read Resources

- `GET /api/health`
  - runtime/process/store health

- `GET /api/agents`
  - list agents with generic filters
  - example filters: `status`, `state`, `role`, `entity_id`, `limit`

- `GET /api/agents/:id`
  - single agent detail

- `GET /api/agents/:id/prompt`
- `GET /api/agents/:id/prompt/diff`

- `GET /api/conversations`
  - list active conversations by agent

- `GET /api/conversations/:agent_id`
  - full conversation detail

- `GET /api/conversations/:agent_id/artifacts`
  - optional, if artifacts are truly a generic runtime concern

- `GET /api/events`
  - generic event query
  - filters should be generic: `event_name`, `source`, `entity_id`, `flow_instance`, `subscriber`, `since`, `limit`

- `GET /api/events/:id`

- `GET /api/events/stream`
  - SSE for generic event stream

- `GET /api/runtime/logs`
  - generic log query
  - filters: `level`, `component`, `agent_id`, `entity_id`, `error_code`, `since`, `limit`

- `GET /api/runtime/incidents`
  - generic incident aggregation

- `GET /api/tasks`
- `GET /api/tasks/stats`

- `GET /api/mailbox`
- `GET /api/mailbox/:id`

- `GET /api/instances`
  - list workflow instances with generic filters
  - filters: `workflow_name`, `current_state`, `entity_id`, `flow_instance`, `updated_since`, `limit`

- `GET /api/instances/:id`
  - instance detail
  - should include current state, metadata, config, transitions, timers, buckets

- `GET /api/instances/:id/events`
  - event timeline scoped to an instance or entity

- `GET /api/instances/:id/tasks`
  - optional convenience join if task-to-instance linkage is generic enough

- `GET /api/instances/:id/mailbox`
  - optional convenience join if mailbox-to-instance linkage is generic enough

- `GET /api/instances/aggregate`
  - generic aggregation endpoint
  - examples:
    - `group_by=current_state`
    - `group_by=workflow_name`
    - `group_by=metadata.some_key`
    - `filter.workflow_name=...`

- `GET /api/schedules`
  - list active schedules/timers

- `GET /api/graphs/system`
  - generic system/resource graph

- `GET /api/graphs/workflow`
  - generic workflow graph / topology
  - query options like `view=design|runtime|replay`, `workflow_name`, `entity_id`, `start`, `end`

### Write Resources

- `PUT /api/agents/:id/prompt`
- `DELETE /api/agents/:id/prompt`

- `POST /api/chat/:agent_id`
  - if direct agent chat is intended as a generic operator action

- `POST /api/directives`
  - generic operator directive submission

- `POST /api/agents/:id/restart`
- `POST /api/agents/:id/replay`

- `POST /api/tasks/:id/claim`
- `POST /api/tasks/:id/complete`
- `POST /api/tasks/:id/reject`

- `POST /api/mailbox/:id/decide`

- `POST /api/events/:id/requeue`

- `POST /api/runtime/control`
  - `pause`, `resume`, `reset_state`, `reset_db` if those are kept

Notes:

- no `/api/verticals`
- no `/api/holding`
- no `/api/funnel`
- no `/api/portfolio`
- no `/api/shards` unless shards are promoted to an actual generic platform concept

## What the TypeScript Adapter Should Do

The adapter should translate generic resources into the dashboard’s current product views.

Examples:

- "overview"
  - compose from `instances/aggregate`, `agents`, `tasks`, `mailbox`, `health`

- "digest"
  - compose from `instances`, `runtime/incidents`, `tasks`, `agents`

- "holding"/"portfolio"
  - derive from `instances` plus instance metadata and recent events

- "funnel"
  - derive from `instances/aggregate?group_by=current_state`

- "vertical trace"
  - derive from `events` filtered by generic instance/entity attributes

- "control targets"
  - derive from `agents`

So the adapter owns product semantics like:

- which metadata key corresponds to an Empire vertical slug
- which workflow names belong to a board view
- which state buckets correspond to "pipeline pressure"

That logic should not live in the generic Go server.

## Recommended TS Refactor Shape

Add a resource client layer plus adapters.

Suggested structure:

- `internal/dashboard/ui/src/api/serverClient.ts`
  - raw transport helpers

- `internal/dashboard/ui/src/api/resources/`
  - `agents.ts`
  - `instances.ts`
  - `events.ts`
  - `tasks.ts`
  - `mailbox.ts`
  - `conversations.ts`
  - `runtime.ts`
  - `graphs.ts`
  - `health.ts`

- `internal/dashboard/ui/src/adapters/`
  - `overviewAdapter.ts`
  - `digestAdapter.ts`
  - `portfolioAdapter.ts`
  - `workflowAdapter.ts`
  - `runtimeAdapter.ts`

Then existing wrappers like `dashboardPortfolio.ts` and `holding.ts` become adapter/composition modules instead of endpoint-specific clients.

## What the Go Server Needs Beyond Existing Store Calls

Some endpoints can be very thin over current store/runtime calls:

- agents
- mailbox
- conversations
- schedules
- workflow instances

Some still need generic read-model code:

- event list/detail query APIs suitable for UI filtering
- runtime log and incident query APIs
- graph/topology endpoints
- instance aggregation endpoint
- prompt diff endpoint
- directive/control endpoints

That is still generic server work, but it should remain domain-neutral.

## Remaining Gap Matrix

### Ready for Generic Migration with TS Adapters

- mailbox
  - generic data is present
  - TS currently derives summary counts from generic items

- conversations
  - generic list/detail is present
  - TS currently adapts generic rows to the existing conversation UI contract

- events / logs / incidents
  - these are already effectively generic
  - remaining work is mostly cleanup and normalization, not a boundary change

- workflow graph / pipeline flow
  - already generic enough

### Needs More Generic Backend Data Before Migration

- agents
  - current dashboard agent views depend on triage/runtime fields not available from the current generic `/api/agents` shape
  - examples: attention/stuck state, pending counts, breaker proximity, turn pressure, runtime lock state, recent tool outcome
  - those can still be exposed generically, but the server needs more read-model aggregation first

- health
  - current generic `/api/health` is intentionally sparse
  - current health view still expects auth, spend, workflow audit, container, and contract diagnostics
  - either the generic health surface must grow, or the TS adapter must compose health from several generic endpoints

### Should Stay TS-Composed, Not Become Go Product APIs

- overview
- digest
- holding
- portfolio
- funnel
- trace views keyed by Empire metadata
- control target pickers that encode product-specific operator semantics

These should be assembled in TS from generic resources, not encoded into dedicated Go endpoints.

## Best Migration Path

1. Build the generic Go server under a new package, for example:
   - `internal/dashboard/server/`

2. Implement the first generic resources:
   - health
   - agents
   - conversations
   - tasks
   - mailbox
   - events
   - instances
   - graphs

3. Add TS resource clients that target only `/api/*`.

4. Add TS adapters that reconstruct current dashboard data models from those generic resources.

5. Remove `/dashboard/api/*` dependencies from the UI.

6. Delete compatibility routes once the adapter migration is complete.

## Recommended Minimum Viable Generic API

To unlock most of the dashboard without Empire leakage, the first server tranche should include:

- `GET /api/health`
- `GET /api/agents`
- `GET /api/agents/:id/prompt`
- `GET /api/agents/:id/prompt/diff`
- `GET /api/conversations`
- `GET /api/conversations/:id`
- `GET /api/events`
- `GET /api/events/:id`
- `GET /api/events/stream`
- `GET /api/runtime/logs`
- `GET /api/runtime/incidents`
- `GET /api/tasks`
- `GET /api/tasks/stats`
- `POST /api/tasks/:id/claim`
- `POST /api/tasks/:id/complete`
- `POST /api/tasks/:id/reject`
- `GET /api/mailbox`
- `POST /api/mailbox/:id/decide`
- `GET /api/instances`
- `GET /api/instances/:id`
- `GET /api/instances/aggregate`
- `GET /api/graphs/system`
- `GET /api/graphs/workflow`

That is the clean generic core.

## Recommendation

Do not preserve the current dashboard API shape on the Go side.

The correct architecture is:

- generic Go resource server
- TypeScript adapter that maps generic resources into dashboard views

That keeps the Go side reusable and prevents Empire-specific view semantics from fossilizing into platform APIs.
