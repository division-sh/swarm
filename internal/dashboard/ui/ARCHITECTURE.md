# Dashboard UI Architecture

## Current shape

- `src/main.jsx`
  - bootstrap only
- `src/app/`
  - `AppShell.jsx` for shell composition only
  - `DashboardHeader.jsx` and `DashboardModals.jsx` for shell chrome
  - `DashboardViewRouter.jsx` as a thin tab router
  - `DashboardRuntimeViews.jsx` for runtime-facing tabs
  - `DashboardOpsViews.jsx` for ops/control-facing tabs
  - `useDashboardCoordinator.js` for global composition and hook wiring
  - `useDashboardDerivedState.js` for cross-feature selectors and cleanup effects
  - `useDashboardStateBuckets.js` for grouped local state buckets
  - `dashboardTabs.js` for tab definitions and badges
- `src/api/`
  - domain request wrappers
- `src/features/graph/`
  - `GraphView.jsx` as the graph orchestrator
  - `graphLayout.js`, `graphPersistence.js`, `GraphNodes.jsx`, `GraphEdges.jsx`, `GraphToolbar.jsx`
- `src/features/holding/`
  - `HoldingView.jsx`
  - `HoldingVerticalDetail.jsx` as section composition only
  - artifact renderers and section panels
- `src/features/flow/`
  - `FlowView.jsx`
  - flow event/stage helpers
- `src/features/digest/`
  - `DigestView.jsx`
- `src/components/`
  - shared presentational primitives
- `src/hooks/`
  - feature-independent app hooks
  - split action hooks for control, navigation, and tasks
- `src/styles/`
  - ordered CSS slices concatenated at build time

## God-Submodule Watchlist

- `src/app/useDashboardCoordinator.js`
  - still the heaviest remaining coordinator module
- `src/app/DashboardRuntimeViews.jsx`
  - still the largest grouped router
- `src/features/graph/GraphPage.jsx`
  - node and edge inspector detail is still dense
- `src/features/flow/FlowView.jsx`
  - still owns both controls and inspector composition
- `src/features/agents/AgentDropdown.jsx`
  - still mixes several action affordances in one component

File-size thresholds for this repo:

- bootstrap files: under 50 lines
- shell/composer files: under 300 lines
- feature views: under 300 lines unless they are deliberate orchestrators
- hooks: under 250 lines unless they are explicit coordinators
- renderer/helper modules: under 200 lines when practical

## State management decision

Do not add Redux, Zustand, or another global client state layer yet.

## When to reconsider

Revisit a shared state library only if one of these becomes true:

- multiple extracted features need to edit the same client state directly
- runtime streams must fan out to several feature modules at once
- `useDashboardCoordinator.js` stops shrinking because several features need two-way shared state
- prop passing starts recreating cross-feature selectors in several places

Until then, prefer:

1. feature-local hooks
2. thin API modules
3. shared presentational components
4. a thin shell plus explicit coordinator and derived-state hooks
