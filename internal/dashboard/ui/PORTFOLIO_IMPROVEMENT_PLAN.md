# Portfolio Improvement Plan

## Purpose

This document is the working reference for improving the `Portfolio` tab from a merged wrapper into a real operator surface.

It focuses on two things:

- the current business logic and data model already available inside the dashboard
- the phased product/UX improvements needed to turn that data into a serious portfolio command center

This file is intentionally separate from:

- `ARCHITECTURE.md`
- `DASHBOARD_IMPROVEMENT_PLAN.md`

Those files describe overall SPA structure and top-level dashboard IA. This file is specifically about `Portfolio`.

## Current State

### Current UI Shape

The current `Portfolio` tab is a simple merged wrapper:

- `Holding Board`
- `Funnel + Shards`

Implementation:

- `src/features/portfolio/PortfolioView.jsx`
- `src/features/holding/HoldingView.jsx`
- `src/features/pipeline/PipelineView.jsx`

What it does well:

- top-level navigation is cleaner than the old `holding` + `pipeline` split
- workflow-state badges exist in holding
- shard scan detail exists in pipeline
- vertical trace exists in pipeline

What it does not do yet:

- no shared portfolio focus
- no unified vertical/run selection
- no urgency-first triage summary
- no strong cross-pivots between holding, funnel, shards, workflow, and operations

### Current Business Logic Model

The good news is that the dashboard already has enough portfolio logic and data to support a much stronger UI without changing the API first.

#### 1. Holding Domain

Current holding data already includes:

- campaign list
- visible verticals
- per-vertical:
  - stage
  - workflow current stage
  - stage entered time
  - revision count
  - active timer count
  - composite score
  - geography
  - workflow version
- aggregated workflow summary:
  - drift
  - active timers
  - revisioned
  - stage-entered tracking

Relevant files:

- `src/features/holding/HoldingView.jsx`
- `src/features/holding/useHoldingViewState.js`
- `src/features/holding/HoldingVerticalDetail.jsx`
- `src/features/holding/HoldingWorkflowPanel.jsx`

#### 2. Pipeline / Funnel Domain

Current pipeline data already includes:

- funnel throughput
- stuck verticals
- shard scan summaries
- shard scan detail rows
- lifecycle trace rows for a selected vertical

Relevant files:

- `src/features/pipeline/PipelineView.jsx`
- `src/features/pipeline/usePipelineController.js`

#### 3. Workflow Cross-Link Data

The portfolio area already has indirect access to:

- workflow trace via `traceVerticalFlow`
- workflow topology/trace via the global `Workflow` tab
- holding detail modal with persisted workflow instance state

That means the missing work is mostly coordination and UX, not raw data availability.

#### 4. Controller Shape

Current app/controller assembly:

- `useDashboardPipelineAssembly`
- `useDashboardPipelineController`

This already produces four domain controllers:

- `flow`
- `graph`
- `pipeline`
- `holding`

That means a stronger portfolio layer can be built by composing existing controllers rather than inventing a new API first.

## Current Problems

### Product / UX Problems

1. `Portfolio` is a wrapper, not a workspace.
- The tab only toggles between two existing views.
- It does not have its own triage model.

2. There is no shared portfolio focus.
- Pipeline uses `traceVertical`.
- Holding uses `openHoldingVerticalDetail`.
- The wrapper does not own a common vertical/run context.

3. The current surfaces are not urgency-first.
- `HoldingView` is useful, but still board-centric.
- `PipelineView` is useful, but still engineering-table-centric.

4. Cross-surface pivots are weak.
- A stuck funnel item should open:
  - holding detail
  - workflow trace
  - shard context if present
  - operations context if human action exists
- that flow is not first-class yet

5. The business logic is visible, but not synthesized.
- We already know about drift, timers, revisions, stuck verticals, throughput, and shard failures.
- The UI does not yet combine those into a single “what needs attention now?” model.

### Architectural Problems

These are not blockers, but they matter:

- `PortfolioView.jsx` currently owns no real domain logic
- there is no portfolio-specific controller or derived-state hook
- the portfolio area does not yet own shared selection/filter state

## Target State

The end state for `Portfolio` should be:

- one portfolio workspace
- one shared vertical/run focus
- one urgency-first triage layer
- consistent pivots into:
  - `Workflow`
  - `Operations`
  - `Agents`
  - holding detail

### Target Mental Model

Operators should be able to answer these questions quickly:

1. Which portfolio items need attention right now?
2. Is the problem workflow drift, shard execution, human review, or low-quality discovery?
3. What is the fastest next action for this vertical?
4. Can I jump directly into the right supporting surface?

## Phased Plan

### Phase 1: Add A Real Portfolio Focus Layer

Goal:

- introduce a shared `selected portfolio item` concept at the `Portfolio` wrapper level

Scope:

- add a `Portfolio Focus` card in `PortfolioView`
- show:
  - selected vertical
  - current db stage
  - workflow stage
  - drift
  - timers
  - revisions
  - latest trace activity
- add quick pivots:
  - `Open Holding Detail`
  - `Open Workflow Trace`
  - `Open Workflow Topology`
  - `Open Operations`

Implementation notes:

- likely add `usePortfolioFocusState`
- portfolio wrapper should own selected vertical id/slug
- holding and pipeline should both read/update that focus

Acceptance:

- selecting a vertical in either subview updates one shared portfolio focus card

### Phase 2: Add Portfolio Triage Summary

Goal:

- synthesize current business logic into a true triage layer

Scope:

- add summary cards for:
  - drift
  - active timers
  - revisioned
  - stuck funnel items
  - failed/timed-out shards
  - ready-for-review / human-needed items
- add top attention lists:
  - stale stage entrants
  - drifted verticals
  - timer-heavy verticals
  - retry-needed shard scans

Implementation notes:

- likely add `usePortfolioDerivedState`
- this should consume existing holding + pipeline data only

Acceptance:

- an operator can identify the top portfolio risks without opening a subview first

### Phase 3: Upgrade Funnel + Shards Into A Triage Surface

Goal:

- make the current pipeline area operator-first instead of table-first

Scope:

- split the view into:
  - attention summary
  - stuck verticals
  - shard failures / retries
  - trace table
- bring failed/timed-out shards higher in the layout
- make stuck vertical rows and shard rows update shared portfolio focus
- add one-click actions:
  - `Trace`
  - `Holding Detail`
  - `Retry`
  - `Workflow`

Acceptance:

- `PipelineView` feels like a triage console, not just raw tables

### Phase 4: Upgrade Holding Into A Portfolio Execution Board

Goal:

- make holding cards/rows operationally connected to the rest of the system

Scope:

- add direct actions from holding cards or preview rows:
  - `Trace`
  - `Workflow`
  - `Focus`
- make board selections update shared portfolio focus
- add a compact “execution context” row on cards when data exists:
  - timers
  - revisions
  - drift
  - last activity

Acceptance:

- holding is no longer an isolated board

### Phase 5: Add Cross-Surface Portfolio Pivots

Goal:

- turn `Portfolio` into a routing hub for the right downstream tool

Scope:

- from portfolio focus, allow one-click pivots into:
  - `Workflow` trace for selected vertical
  - `Workflow` topology for selected vertical
  - `Operations` if a task/mailbox path exists
  - `Agents` if a primary/related agent can be inferred
- preserve selection context where possible

Acceptance:

- tab switches become intentional handoffs, not fresh starts

Status:

- completed
- `Portfolio` now derives downstream task/mailbox/agent context from existing dashboard data
- shared focus can pivot directly into:
  - `Operations > Tasks`
  - `Operations > Control/Mailbox`
  - `Agents`
  - `Workflow`
- holding and funnel attention rows now expose downstream `Ops` / `Agent` handoffs when context exists

### Phase 6: Add Presets And Saved Views

Goal:

- support repeated triage workflows

Scope:

- portfolio presets:
  - `drift only`
  - `timers`
  - `revisions`
  - `stale`
  - `human-needed`
  - `shard failures`
- save locally at first

Acceptance:

- operators can reopen common portfolio investigations quickly

Status:

- completed
- built-in presets now exist for:
  - drift only
  - timers
  - revisions
  - stale
  - human-needed
  - shard failures
- portfolio now persists:
  - selected subview
  - focused vertical
  - three local saved views with focus/filter context

### Phase 7: Consider A Dedicated Portfolio Workbench

Goal:

- only if needed after the previous phases

Scope:

- evaluate whether `Portfolio` should evolve toward a workbench layout similar to `Workflow`
- possible panels:
  - `Board`
  - `Funnel`
  - `Shard Runs`
  - `Trace`
  - `Detail`

Important note:

- do not jump to this too early
- first prove the triage model in a lighter wrapper design

Status:

- implemented
- `Portfolio` now uses a light Dockview workbench with:
  - `Overview`
  - `Triage`
  - `Board`
  - `Funnel`
- existing holding/pipeline components remain authoritative
- the workbench adds structure without forcing a new portfolio-specific backend model

## Recommended Execution Order

1. shared portfolio focus
2. portfolio triage summary
3. pipeline triage upgrade
4. holding cross-pivots
5. cross-surface portfolio pivots
6. presets
7. evaluate workbench

## Out Of Scope For The First Pass

- backend schema changes
- new persistent portfolio entities
- full workbench/docking conversion
- full eval/test coverage expansion

## Definition Of Done

We can consider `Portfolio` to be in a strong state when:

- it has a shared selected vertical/run concept
- it surfaces urgency first
- holding and pipeline cooperate instead of just coexisting
- pivots into `Workflow` and `Operations` are one click away
- the operator does not need to manually reconstruct the business state from raw tables
