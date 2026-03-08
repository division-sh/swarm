# Dashboard Improvement Plan

## Purpose

This document is the working reference for the next dashboard product and UX improvements.

It is separate from `ARCHITECTURE.md`.

- `ARCHITECTURE.md` describes how the SPA is structured internally.
- This file describes what the dashboard should become from an operator/user perspective.

## Implementation Status

- Phase 1 `Overview`: completed
- Phase 2 `Observability`: completed
- Phase 3 fold `Convos` into `Agents`: completed
- Phase 4 `Workflow`: completed
- Phase 5 `Portfolio`: completed
- Phase 6 `Operations`: completed
- Phase 7 `Health` rebalance: completed

## Current Assessment

The current dashboard is powerful but too flat.

Problems with the current tab model:

- there are too many top-level tabs
- several tabs are really subviews of the same workflow
- the navigation is optimized for implementers, not operators
- important cross-surface actions require too much tab hopping
- there is no strong landing page that answers "what needs attention right now?"

Current top-level tabs:

- `agents`
- `digest`
- `events`
- `logs`
- `incidents`
- `flow`
- `convos`
- `graph`
- `control`
- `tasks`
- `pipeline`
- `holding`
- `health`

## Target Information Architecture

Target top-level navigation:

1. `Overview`
2. `Operations`
3. `Observability`
4. `Agents`
5. `Workflow`
6. `Portfolio`
7. `Health`

Notes:

- `Health` can eventually be folded into `Overview` if the extra depth is not needed.
- `Digest` should stop being a top-level tab.
- `Convos` should stop being a top-level tab.

### Target Mapping

- `Overview`
  - new
  - combines digest summary, high-signal health, workflow audit warnings, stuck agents, pending mailbox/tasks, and funnel/holding KPIs
- `Operations`
  - replaces `control` + `tasks`
  - contains control actions, mailbox queue, and human task queue
- `Observability`
  - replaces `events` + `logs` + `incidents`
  - contains event tracing, runtime logs, and incident triage
- `Agents`
  - keeps `agents`
  - absorbs `convos`
  - agent detail becomes the home for turns, prompt state, direct chat, directives, and conversation history
- `Workflow`
  - replaces `flow` + `graph`
  - contains the org graph, workflow flow graph, and flow/runtime/replay inspection
- `Portfolio`
  - replaces `pipeline` + `holding`
  - contains funnel, shard scans, vertical kanban, and holding/workflow-state triage
- `Health`
  - keeps deep infra and contract inspection
  - should not carry broad operational triage that belongs in `Overview`

## Phased Plan

### Phase 1: Introduce `Overview`

Status:

- completed on 2026-03-08
- implemented as a new top-level `overview` tab
- digest summary now appears in `Overview`
- `Digest` remains route-accessible for now but is no longer top-level in primary nav

Goal:

- create a landing page for operators and developers

Scope:

- add a new `overview` top-level tab
- move digest summary out of standalone `Digest`
- show high-signal cards only:
  - runtime running/stopped
  - stuck agents
  - incident count
  - pending mailbox items
  - open/pending-review tasks
  - workflow audit warnings
  - holding drift / timer / revision counts
  - pipeline throughput summary
- add quick links from each card into the detailed surface

Acceptance criteria:

- an operator can open the dashboard and know what needs attention in under 10 seconds
- `Digest` no longer needs to be top-level for daily usage

### Phase 2: Consolidate `Observability`

Status:

- completed on 2026-03-08
- implemented as a new top-level `observability` tab
- old `events`, `logs`, and `incidents` routes remain accepted and land in the unified surface
- `Events`, `Logs`, and `Incidents` are no longer primary top-level nav items

Goal:

- merge runtime debugging surfaces into one place

Scope:

- create a single `Observability` tab with submodes or local nav:
  - `Events`
  - `Logs`
  - `Incidents`
- preserve current filters and detail panes
- add deep links between submodes:
  - event -> logs
  - incident -> logs
  - incident -> conversation
  - event -> agent
- add saved filter presets for common debugging patterns

Acceptance criteria:

- `events`, `logs`, and `incidents` no longer need to be top-level tabs
- common debugging flows require fewer tab switches

### Phase 3: Fold `Convos` into `Agents`

Status:

- completed on 2026-03-08
- conversation history now lives inside the agent dropdown/console
- old `convos` routes remain accepted and redirect into `Agents`
- `Convos` is no longer a primary top-level nav item

Goal:

- make agent debugging centered around the selected agent rather than a separate tab

Scope:

- move conversation history into the agent detail surface
- retain turn/tool-call inspection
- preserve deep-link support from other tabs into selected agent + selected conversation state
- keep "copy conversation" behavior

Acceptance criteria:

- `convos` is removed as a top-level tab
- all useful conversation workflows still exist within `Agents`

### Phase 4: Merge `Workflow`

Status:

- completed on 2026-03-08
- implemented as a new top-level `workflow` tab
- old `flow` and `graph` routes remain accepted and land in the unified surface
- `Flow` and `Graph` are no longer primary top-level nav items

Goal:

- unify topology and workflow execution views

Scope:

- merge `graph` and `flow` into one `Workflow` tab
- add local mode switch:
  - `Org Graph`
  - `Workflow Design`
  - `Runtime Flow`
  - `Replay`
- keep existing graph and flow inspectors
- add stronger cross-linking between selected nodes/edges and runtime/vertical context

Acceptance criteria:

- users no longer need to decide between `Graph` and `Flow` before entering the area
- both topology and transition inspection are reachable from one place

### Phase 5: Merge `Portfolio`

Status:

- completed on 2026-03-08
- implemented as a new top-level `portfolio` tab
- old `pipeline` and `holding` routes remain accepted and land in the unified surface
- `Pipeline` and `Holding` are no longer primary top-level nav items

Goal:

- unify portfolio progression and holding-state triage

Scope:

- merge `pipeline` and `holding` into one `Portfolio` tab
- use local sections or subnav:
  - `Funnel`
  - `Shards`
  - `Holding Board`
  - `Workflow Triage`
- keep holding detail modal
- add saved triage presets:
  - drift only
  - active timers
  - revisioned
  - stale stage

Acceptance criteria:

- users can move from funnel to shard scan to vertical detail without leaving the portfolio area
- `pipeline` and `holding` no longer need to be top-level tabs

### Phase 6: Merge `Operations`

Status:

- completed on 2026-03-08
- implemented as a new top-level `operations` tab
- old `control` and `tasks` routes remain accepted and land in the unified surface
- `Control + Mailbox` and `Tasks` are no longer primary top-level nav items

Goal:

- unify human-in-the-loop actions and operational commands

Scope:

- merge `control` and `tasks` into one `Operations` tab
- keep local sections or subnav:
  - `Mailbox`
  - `Tasks`
  - `Control`
- add direct "take action" flows from mailbox/task to the relevant agent or vertical
- make mailbox/task counts more prominent in the local nav

Acceptance criteria:

- operational actions are grouped together
- control and human-review workflows feel like one area, not two unrelated tabs

### Phase 7: Rebalance `Health`

Status:

- completed on 2026-03-08
- `Health` is now framed explicitly as the deep diagnostics page
- `Overview` remains the primary landing and triage surface
- `Health` keeps infra, auth, spend, contract, workflow audit, and vertical deploy diagnostics

Goal:

- keep `Health` as a deep diagnostics area, not a second overview page

Scope:

- keep:
  - runtime status
  - spend
  - auth
  - containers
  - contract versions
  - verification summary
  - contract paths
- remove broad operational summary cards that are better placed in `Overview`
- optionally show digest summary here only as a secondary detail if `Overview` is not implemented yet

Acceptance criteria:

- `Health` becomes a deeper diagnostics page
- `Overview` becomes the primary landing page

## Cross-Cutting Improvements

These should happen opportunistically during the phased work above.

### Global Search

Add one global search/open box that can jump to:

- agent ID
- vertical slug or ID
- event ID
- task ID
- conversation/agent ID
- shard scan ID

### Cross-Surface Deep Links

Every major surface should expose quick navigation to related records:

- open agent
- open vertical
- open event
- open logs
- open conversation
- open task

### Saved Presets

Add saved or built-in presets for:

- observability filters
- holding triage filters
- task queues
- incident time windows

### Better "Why This Matters" Summaries

Prefer summaries that explain urgency:

- stuck agents
- stale stages
- workflow drift
- overdue timers
- high-severity incidents
- pending approvals / pending mailbox items

### Better Default Ordering

Prefer urgency-first ordering over raw recency where applicable:

- stuck first
- drift first
- overdue first
- pending human action first

## Recommended Execution Order

1. `Overview`
2. `Observability`
3. fold `Convos` into `Agents`
4. `Workflow`
5. `Portfolio`
6. `Operations`
7. `Health` rebalance

## Delivery Notes

- keep each phase shippable on its own
- prefer local subnavigation inside consolidated tabs over adding more top-level tabs
- preserve deep-link capability during migrations
- do not mix architecture refactors with broad IA changes unless they are directly required

## Post-Plan Cleanup

Delivered on 2026-03-08:

- consolidated tabs now show local subview counts where useful
- local subview labels were tightened to match the new IA
- obvious legacy deep-link labels in the agent surface were renamed to match the consolidated areas

## Out of Scope For Now

- persisted verification-gate execution history
- runtime/CI changes outside the dashboard package
- adding a global client state library
- major visual redesign unrelated to operator flow
