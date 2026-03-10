# Dashboard Test Plan

## Goal

Keep the dashboard safe to evolve by covering:
- backend API contracts
- top-level routing and tab/workbench wiring
- high-value operator interactions
- accessibility regressions
- a few domain-heavy pure helpers

The goal is not exhaustive UI testing.
The goal is fast, stable coverage for the surfaces most likely to regress.

## Current Baseline

### Backend

Go tests already cover:
- query/read contract endpoints
- mutation contracts
- degraded-state behavior
- alias parity
- event list/detail consistency

Primary area:
- [`internal/dashboard`](../dashboard)

### Frontend Static/Unit

Current checks:
- `npm run typecheck`
- `npm run lint`
- `npm run check:deps`
- `npm run check:architecture`

Current focused unit coverage includes:
- graph helpers
- portfolio presets/downstream state
- operations/task/mailbox derived state
- overview/observability/health derived state

### Browser Coverage

Current Playwright lanes:
- `npm run test:smoke`
- `npm run test:a11y`

Current smoke coverage includes:
- shell + overview load
- top-level tab rendering
- consolidated subview deep links
- agent -> task drilldown
- overview workflow triage -> portfolio
- agent -> observability event trace
- agent -> observability logs
- workflow workbench route sync
- observability workbench route sync
- portfolio workbench route sync
- portfolio preset + saved-view reopen
- portfolio triage -> workflow/operations pivots
- operations workbench route sync
- operations queue -> mailbox detail
- operations queue -> task detail
- operations mailbox detail -> workflow/related task
- operations task detail -> workflow/portfolio/related mailbox
- task completion refresh
- task rejection refresh
- mailbox decision refresh
- observability logs -> workflow pivot
- observability logs -> incidents pivot
- health vertical-row -> portfolio/workflow
- health diagnostics pivots

Current a11y smoke coverage includes:
- overview
- portfolio/overview
- operations/queue
- health

## Principles

1. Prefer stable operator flows over brittle DOM-detail tests.
2. Prefer one strong end-to-end assertion over many weak presence checks.
3. Keep smoke tests fast.
4. Do not use graph-canvas click paths as generic smoke coverage.
5. Put heavy Monaco/Dockview panels in targeted tests only if they are stable enough.

## Test Layers

### 1. API Contract Tests

Purpose:
- prove the backend returns the shapes the SPA depends on

Good targets:
- overview
- agents
- mailbox
- tasks
- health
- events
- logs
- incidents
- conversations
- holding
- graph
- flow

Mutation targets:
- mailbox decisions
- task claim/complete/reject
- control endpoints with visible state changes

### 2. Derived-State / Helper Tests

Purpose:
- keep dense business logic stable without browser cost

Good targets:
- urgency queues
- preset application
- workflow focus derivation
- graph focus/layout helpers
- portfolio/operations/overview aggregation

### 3. Playwright Smoke

Purpose:
- verify that the app boots, routes correctly, and the main operator pivots work

Rules:
- cover only stable, high-signal flows
- avoid brittle canvas interactions
- prefer route changes, panel activation, and detail-pane visibility

### 4. Accessibility Smoke

Purpose:
- catch missing labels, landmark issues, and obvious axe regressions

Rules:
- cover representative tabs
- do not force the heaviest workbench routes into the generic a11y lane if they are too expensive

## Coverage Matrix

### Overview

Current:
- shell load
- command-center route render
- workflow triage -> portfolio

Next:
- urgent queue -> portfolio/workflow

### Agents

Current:
- tab render
- agent expansion
- agent -> task drilldown
- agent -> observability event trace
- agent -> observability logs

Next:
- add direct agent -> workflow affordance, then cover it
- agent quick actions remain stable after dropdown section switch

### Observability

Current:
- workbench route sync
- logs -> workflow pivot
- logs -> incidents pivot

Next:
- overview hotspot -> target subview
- event detail -> workflow
- event detail -> portfolio
- incident detail -> agent

Note:
- avoid brittle `events -> logs` detail assertions unless the Dockview state becomes more deterministic

### Workflow

Current:
- workbench route sync for stable panels
- runs -> trace pivot

Next:
- issues panel stable interactions
- compare panel route activation if it becomes smoke-safe
- selected run/topology pivots that do not rely on graph-canvas clicks

Explicit non-goal for smoke:
- generic graph canvas clicking
- Monaco-heavy panel assertions unless they are proven stable

### Portfolio

Current:
- workbench route sync
- preset apply
- saved-view reopen
- triage -> workflow pivot
- triage -> operations pivot

Next:
- downstream card -> agent
- board/funnel focus persistence

### Operations

Current:
- workbench route sync
- queue -> mailbox detail
- queue -> task detail
- mailbox detail -> workflow
- mailbox detail -> related task
- task detail -> workflow/portfolio
- task detail -> related mailbox
- task completion refresh
- task rejection refresh
- mailbox decision refresh
- mailbox quick-row decision refresh

Next:
- mailbox quick-action row decisions

### Health

Current:
- diagnostics pivots
- a11y smoke
- per-vertical portfolio/workflow buttons

Next:
- workflow warnings -> workflow issues/artifacts

## Planned Execution Order

### Phase 1: Strengthen Current Smoke

1. Portfolio triage card pivots
2. Operations detail-pane pivots
3. Health per-vertical pivots
4. Overview urgent-queue pivots

### Phase 2: Broader Operator Paths

1. Observability hotspot actions
2. Agents quick-action paths
3. Workflow issues/runs stable pivots

### Phase 3: Mutation Flows

1. mailbox decision visibly updates UI
2. task complete/reject visibly updates UI
3. one control action with visible refresh

### Phase 4: Targeted Heavier Coverage

Only if stable enough:
- workflow compare panel
- workflow artifacts panel
- selected graph sidebar actions without canvas clicks

## What Not To Do

- no broad screenshot suite
- no exhaustive DOM snapshots
- no smoke tests that depend on precise graph node positions
- no “test every button on every tab” approach

## Maintenance Rules

When adding a new major pivot or workspace panel:
1. add or extend one focused unit/helper test if business logic changed
2. add one smoke test only if the interaction is high-value and stable
3. avoid widening the smoke lane with redundant presence checks

When a smoke test flakes:
1. decide whether it exposed a real bug or a bad smoke target
2. fix the product bug if real
3. narrow or remove the smoke path if it is the wrong level of test

## Current Recommendation

Best next additions:
1. one stable `Observability` detail pivot into `Portfolio`
2. mailbox quick-action rows if the table DOM becomes more smoke-safe
3. `event detail -> workflow/portfolio`
4. add direct `Agents -> Workflow` product affordance, then cover it
