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
  - `useDashboardCoreQueries.js`, `useDashboardRuntimeSources.js`, and `useDashboardPipelineSources.js` for app-layer data source composition
  - `useDashboardRuntimeQueries.js`, `useDashboardPortfolioQueries.js`, and `useDashboardWorkflowQueries.js` for query-backed domain data
  - `useDashboardDerivedState.js` for global badge/tab derivation only
  - `useDashboardStateBuckets.js` for grouped local state buckets
  - `dashboardTabs.js` for tab definitions and badges
- `src/api/`
  - domain request wrappers
- `src/features/**/use*Controller.js`
  - feature-owned controller hooks that shape `state` and `actions`
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

## Guardrails

- No `app` bag props.
  - App-layer routers must pass explicit feature controllers or explicit `state` / `actions` props.
  - `DashboardViewRouter.jsx`, `DashboardRuntimeViews.jsx`, and `DashboardOpsViews.jsx` must not accept a single catch-all `app` object.
- No umbrella action hook.
  - Do not recreate `useDashboardActions.js`.
  - Compose specific action hooks directly in the coordinator or in feature controllers.
- No umbrella data-source hook.
  - Do not recreate `useDashboardDataSources.js`.
  - Keep app-layer source composition split by core, runtime, and pipeline domains.
- Feature views should accept predictable contracts.
  - Default shape is `state` and `actions`.
  - If a third prop is needed, prefer `ui` and keep it narrow.
- Shared helpers belong in imports, not prop chains.
  - Import formatting helpers from `src/lib/format.js` inside the view that uses them.
  - Do not pass `fmtTime`, `relTime`, `formatDollars`, or similar down through routers.
- `useDashboardDerivedState.js` must stay global-only.
  - It should not own feature-local selection cleanup, filtering, or feature-specific selectors.
- Prefer feature controller hooks over app-layer object shaping.
  - If a feature needs nontrivial state assembly, add or extend `src/features/<feature>/use<Feature>Controller.js`.

## God-Submodule Watchlist

- `src/app/useDashboardCoordinator.js`
  - still the heaviest remaining coordinator module
- `src/features/graph/GraphPage.jsx`
  - node and edge inspector detail is still dense
- `src/features/flow/FlowView.jsx`
  - still owns both controls and inspector composition
- `src/features/control/ControlView.jsx`
  - still renders a lot of action surface in one place

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
3. feature controller hooks
4. shared presentational components
5. a thin shell plus explicit coordinator and minimal derived-state hooks
