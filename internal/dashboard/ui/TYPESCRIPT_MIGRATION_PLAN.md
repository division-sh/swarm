# TypeScript Migration Plan

## Goal

Move the dashboard UI from permissive JavaScript to incremental TypeScript without blocking feature work or forcing a flag-day rewrite.

Success means:
- the UI keeps shipping during migration
- business-logic helpers and data contracts gain type coverage first
- app-layer and feature-layer state wiring become safer over time
- React components migrate after their inputs are already typed

## Current State

Already in place:
- [tsconfig.json](./tsconfig.json)
- `npm run typecheck`
- `npm run lint`
- `npm run check:deps`
- `npm run check:architecture`
- `tsx`-based test scripts in [package.json](./package.json)
- first converted modules:
  - [src/lib/format.ts](./src/lib/format.ts)
  - [src/app/dashboardQueryKeys.ts](./src/app/dashboardQueryKeys.ts)
  - derived-state helpers under:
    - [src/features/health](./src/features/health)
    - [src/features/overview](./src/features/overview)
    - [src/features/observability](./src/features/observability)
    - [src/features/tasks](./src/features/tasks)
    - [src/features/control](./src/features/control)
    - [src/features/operations](./src/features/operations)

Current compiler posture:
- `allowJs: false`
- `strict: false`
- `noEmit: true`

That is intentional for the first phase.

Current migration checkpoint:
- app shell/coordinator path: mostly typed
- query/API layer: typed
- feature controller/helper layer: largely typed for major surfaces
- major workbench/container views: typed
- current source mix: `173` `ts/tsx` files and `0` `js/jsx` files under [src](./src)

## Migration Principles

1. Migrate leaf modules before orchestrators.
2. Type data contracts before typing JSX-heavy components.
3. Prefer small shared interfaces over massive global type files.
4. Keep JS and TS mixed until each layer is stable.
5. Raise compiler strictness gradually, not all at once.

## Phase 1: Foundation

Status: complete

Delivered:
- TypeScript compiler setup
- mixed JS/TS test execution
- first typed utility and derived-state modules
- successful `typecheck` and smoke coverage

Exit criteria:
- `npm run typecheck` stays green
- converted helper modules remain covered by tests

## Phase 2: Query And API Contracts

Status: complete

Target:
- convert API/query layers from loosely shaped JS objects to typed request/response modules

Scope:
- [src/api](./src/api)
- [src/app/useDashboardCoreQueries.js](./src/app/useDashboardCoreQueries.js)
- [src/app/useDashboardRuntimeQueries.js](./src/app/useDashboardRuntimeQueries.js)
- [src/app/useDashboardPortfolioQueries.js](./src/app/useDashboardPortfolioQueries.js)
- [src/app/useDashboardWorkflowQueries.js](./src/app/useDashboardWorkflowQueries.js)

Deliverables:
- `src/types/api.ts` or smaller domain files such as:
  - `src/types/core.ts`
  - `src/types/runtime.ts`
  - `src/types/workflow.ts`
  - `src/types/portfolio.ts`
- typed query key parameters and query return values
- typed helper functions for response normalization

Why this phase next:
- Query modules are the main data authority now
- typing them gives leverage across the rest of the app

Exit criteria:
- all dashboard query hooks compile in TS
- query return shapes are explicit
- no `any` in new API modules except documented escape hatches

## Phase 3: Feature Controllers And Derived State

Status: substantially complete

Target:
- convert state-shaping and feature-controller modules that sit between raw data and JSX

Scope:
- controller hooks in:
  - [src/features/agents](./src/features/agents)
  - [src/features/tasks](./src/features/tasks)
  - [src/features/control](./src/features/control)
  - [src/features/portfolio](./src/features/portfolio)
  - [src/features/workflow](./src/features/workflow)
- app controller/assembly hooks:
  - [src/app/useDashboardRuntimeController.js](./src/app/useDashboardRuntimeController.js)
  - [src/app/useDashboardPipelineController.js](./src/app/useDashboardPipelineController.js)
  - [src/app/useDashboardOpsController.js](./src/app/useDashboardOpsController.js)

Deliverables:
- typed `state` and `actions` surfaces for each major feature
- typed controller return contracts
- typed route/subview unions where practical

Why this phase next:
- this is where many bugs currently become wiring mistakes rather than rendering mistakes
- typing controllers reduces prop-shape drift across the app

Exit criteria:
- feature controller surfaces are explicit and reusable
- top-level views consume typed controller props

## Phase 4: App Assembly Layer

Status: substantially complete

Target:
- type the shell/coordinator/assembly layer once data and controller contracts are stable

Scope:
- [src/app/useDashboardCoordinator.js](./src/app/useDashboardCoordinator.js)
- [src/app/useDashboardRuntimeAssembly.js](./src/app/useDashboardRuntimeAssembly.js)
- [src/app/useDashboardPipelineAssembly.js](./src/app/useDashboardPipelineAssembly.js)
- [src/app/useDashboardOpsAssembly.js](./src/app/useDashboardOpsAssembly.js)
- [src/app/DashboardRuntimeViews.jsx](./src/app/DashboardRuntimeViews.jsx)
- [src/app/DashboardOpsViews.jsx](./src/app/DashboardOpsViews.jsx)

Deliverables:
- typed coordinator return shape
- typed `header`, `views`, and `modals` contracts
- typed `openView` and route helpers

Why this phase later:
- the assembly layer depends on many other surfaces
- typing it too early creates churn

Exit criteria:
- coordinator uses typed feature contracts instead of ad hoc object bags
- route helpers and tab/subview wiring are typed

## Phase 5: React Components

Status: in progress

Target:
- convert components once their inputs are typed

Priority order:
1. non-JSX-heavy components with simple props
2. shared components
3. feature components with stable controller props
4. large workbench views last

Suggested first component slice:
- [src/components/StatusDot.jsx](./src/components/StatusDot.jsx)
- [src/components/GateIndicator.jsx](./src/components/GateIndicator.jsx)
- [src/components/CopyID.jsx](./src/components/CopyID.jsx)
- [src/components/JsonBlock.jsx](./src/components/JsonBlock.jsx)

Then:
- smaller feature panels
- workbench wrappers
- finally the denser workspace/inspector components

Exit criteria:
- component props are typed at the boundaries
- no large JSX file is converted before its data/controller inputs are typed

Current checkpoint:
- all files under [src](./src) are now TypeScript or TSX
- remaining work is type-quality improvement rather than file-format migration

## Phase 6: Strictness Ramp

Target:
- move from permissive TS to meaningful enforcement

Recommended order:
1. keep `allowJs: true`
2. convert the majority of app/query/controller modules
3. enable:
   - `noImplicitAny`
   - `noUncheckedIndexedAccess`
   - `exactOptionalPropertyTypes` later if useful
4. reduce `allowJs` scope gradually
5. consider `strict: true` only after the main app/core/feature controller surfaces are typed

Exit criteria:
- typecheck catches real regressions without creating migration noise

## Recommended File Strategy

Use small domain-focused type files, not one giant global type dump.

Suggested structure:
- [src/types](./src/types)
  - `core.ts`
  - `runtime.ts`
  - `portfolio.ts`
  - `workflow.ts`
  - `operations.ts`
  - `ui.ts`

Rules:
- define shared entities once
- colocate highly local prop types with the component/hook if they are not reused
- avoid exporting huge “DashboardState” interfaces too early

## Testing Strategy During Migration

Required on every migration slice:
- `npm run typecheck`
- relevant feature tests
- `npm run test:smoke` for cross-surface routing-sensitive changes
- `go test ./internal/dashboard -count=1` when the UI change affects embedded assets or server expectations

Additions over time:
- keep converting existing `.test.js` to `.test.ts`
- prefer typing the tests for derived-state modules first

## Immediate Next Steps

1. Replace broad `Record<string, any>` types in hot paths with narrower domain interfaces.
2. Start enabling stricter compiler flags beginning with `noImplicitAny`.
3. Tighten graph/workflow/portfolio domain models where the remaining casts cluster.
4. Decide whether to type-check `scripts/` more aggressively or leave them intentionally lightweight.
5. Keep the verification baseline green as type strictness increases.

## Definition Of Done

The migration is “done enough” when:
- query/data contracts are typed
- controller surfaces are typed
- major workbench tabs consume typed feature contracts
- `strict` can be raised meaningfully
- feature work no longer requires guessing response/prop shapes by reading implementation files
