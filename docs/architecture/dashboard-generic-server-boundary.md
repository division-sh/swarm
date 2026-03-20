# Dashboard Generic Server Boundary

## Purpose

Capture the current stopping point for the dashboard genericization work:

- what is now on the generic Go + TS adapter path
- what remains intentionally product-side
- what is deferred because the runtime does not yet expose a proper generic resource

This is the decision document to use going forward, not a speculative migration plan.

## Boundary Rule

The boundary is strict:

- Go server exposes only generic platform/runtime resources and generic operator actions
- TypeScript adapters compose those generic resources into dashboard/Empire views
- Empire concepts do not belong in the generic Go API

## Generic Go Surface

The generic server is mounted from:

- `cmd/mas/main.go`

Implemented under:

- `internal/dashboard/server/server.go`
- `internal/dashboard/server/conversations_sql.go`
- `internal/dashboard/server/agents_sql.go`

Current generic runtime resources:

- `GET /api/health`
- `GET /api/agents`
- `GET /api/agents/{id}`
- `GET /api/conversations`
- `GET /api/conversations/{agentID}`
- `GET /api/mailbox`
- `GET /api/mailbox/{id}`
- `GET /api/instances`
- `GET /api/instances/{id}`
- `GET /api/instances/aggregate`
- `POST /api/agents/{id}/actions/directive`
- `POST /api/agents/{id}/actions/restart`
- `POST /api/agents/{id}/actions/replay`
- `POST /api/runtime/actions`

The dashboard already also uses existing generic runtime endpoints outside `internal/dashboard/server`, including:

- `/api/events`
- `/api/events/{id}`
- `/api/runtime/logs`
- `/api/runtime/incidents`
- `/api/tasks`
- `/api/tasks/stats`

## Views Now On The Generic Path

These dashboard surfaces are now generic-only or generic-first through TS adapters:

- agents
- health
- overview
- digest
- conversations
- conversation artifacts
- mailbox
- holding
- holding detail for runtime-owned facts
- funnel
- trace
- most control actions:
  - directive
  - restart
  - replay
  - pause
  - resume
  - reset state

## Intentionally Product-Side

These should stay out of the generic Go API unless they are redefined as true platform concepts:

- `createVertical`
- `seedOrg`
- `requeueEvent`
- holding/business artifacts such as:
  - scores
  - business brief
  - MVP spec
  - brand
  - validation kit
  - deploy config
  - launch targets
  - spend summaries tied to product views

Those can still be shown in the dashboard, but they should be composed in TS or served by a product-specific layer, not the generic platform server.

## Deferred: Shard Scans

`shard scans` are the one major unresolved dashboard surface.

Current assessment:

- they are not an Empire concept
- but they also do not yet have a first-class generic runtime resource in this repo
- today, the repo exposes:
  - sharding config
  - some `scan_id` diagnostics/logging
- it does not expose a reusable persisted model for:
  - scan runs
  - shard executions
  - shard progress
  - shard retry state
  - shard spend/result aggregates

Because of that, `shard scans` remain legacy-backed for now.

If we want to migrate them cleanly, the right next step is not another dashboard adapter. It is a runtime design task:

- define a generic operator/runtime resource for scan runs and shard executions
- persist it
- expose it under `/api/*`

Until that exists, `shard scans` should be treated as deferred.

## Practical Rule For Future Work

Before adding a new dashboard endpoint to the generic Go server, ask:

1. Is this a reusable runtime/platform fact?
2. Would another product reasonably consume this same resource shape?
3. Can it be named without Empire vocabulary?

If the answer is no, keep it out of the generic server.

## Current Status

The dashboard genericization is far enough along that remaining work should be treated as one of:

- product-side UI composition
- a new genuine runtime resource design
- dead-code cleanup of legacy fetch paths after stabilization

It should not drift back into adding Empire-shaped read models to the generic Go server.
