# MAS Platform v1.1.0 Phase 2 Subplan

Date: 2026-03-10
Repo: `/Users/youmew/dev/empireai`
Parent plan: `docs/architecture/mas-platform-v1_1_0-implementation-plan.md`
Prior phase: `docs/architecture/mas-platform-v1_1_0-phase1-subplan.md`

## Goal

Phase 2 exists to replace Empire-specific guard and branch interpretation in generic runtime execution with declarative expression evaluation.

The practical outcome of this phase is:

- generic runtime can evaluate MAS `guard.check` expressions
- generic runtime can evaluate MAS `rules.*.condition` expressions
- generic runtime has a stable expression context rooted in contract data rather than Empire hook code
- Empire hook-based guard logic stops being required for active MAS workflow paths

## Current Readout

### The runtime is still guard-ID driven

The current guard path in [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go) still works like this:

- resolve guard by ID from the registry
- dispatch a small set of built-in IDs in generic code
- fall back to Empire-specific `WorkflowHooks().EvaluateWorkflowGuard(...)`

That means Phase 1 made MAS-default execution real, but guard semantics are still not truly declarative.

### The contracts are ahead of the evaluator

The MAS contracts already declare inline expressions in several places:

- scoring flow `guard.check` expressions in [nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/scoring/nodes.yaml)
- validation flow `guard.check` expressions in [nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/validation/nodes.yaml)
- operating/discovery/portfolio `rules.*.condition` expressions in:
  - [operating/nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/operating/nodes.yaml)
  - [discovery/nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/discovery/nodes.yaml)
  - [nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/nodes.yaml)

The generic runtime currently does not evaluate those expressions directly.

### CEL foundations exist, but the contract dialect is wider than raw CEL

The repo now includes `github.com/google/cel-go` and generic runtime owns the first evaluator wrapper.

Current runtime facts:

- generic runtime evaluates expressions against `entity` / `payload` / `policy`
- current MAS contracts still use a small dialect on top of CEL:
  - `{{policy_placeholders}}`
  - `else`
  - unquoted enum literals like `approve` / `saas_gap`
- generic runtime now normalizes those forms before evaluation
- inline `guard.check` prefers the evaluator and falls back to migration hooks only when the expression still depends on unsupported semantics
- mailbox decision routing now executes from MAS `rules.*.condition` instead of a lifecycle compatibility no-op
- full [pipeline](/Users/youmew/dev/empireai/internal/runtime/pipeline) is green under this model

That means Phase 2 is no longer about introducing CEL. It is now about widening the declarative execution surface and shrinking the remaining hook/special-case paths.

### The likely runtime boundary

The most stable insertion points are:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
  - current guard evaluation path
  - current handler-plan/rule execution path
- [guard_action_registry.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/guard_action_registry.go)
  - current executable registry boundary
- [workflow_hooks.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/empire/workflow_hooks.go)
  - current Empire leakage point to burn down

## Main Design Constraints

### Constraint 1: Stable evaluation context first

The hardest part is not CEL syntax. It is defining a runtime context that is:

- stable
- testable
- compatible with current MAS expressions
- expressive enough for later phases

The minimum context should be:

- `entity`
- `payload`
- `policy`

Likely near-term derived fields:

- `state`
- `metadata`
- selected accumulator views for active node/flow state

### Constraint 2: Phase 2 should not require full query/filter/compute support

The MAS spec includes broader expression-bearing features, but Phase 2 should stay scoped to:

- `guard.check`
- `rules.*.condition`
- `on_complete.condition`

Query-backed expressions like `query_entities(...)` should be treated explicitly:

- either unsupported in Slice 2.1
- or supported only through tightly scoped built-in functions with deterministic tests

### Constraint 3: Preserve current shipping behavior while narrowing the hook path

Phase 2 should not try to delete all hook usage in a single step.

The safer progression is:

1. CEL evaluator exists
2. active MAS guards and rules prefer CEL
3. Empire hook path remains as explicit migration fallback
4. fallback shrinks as coverage grows

## Phase 2 Work Slices

### Slice 2.1: CEL Foundations

Objective:

- introduce CEL infrastructure and the expression context model without changing broad runtime behavior yet

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- new CEL-focused files under `internal/runtime/pipeline/`
- `go.mod`

Work:

- add CEL dependency
- create a small evaluator layer owned by generic runtime
- define the first runtime context shape:
  - `entity`
  - `payload`
  - `policy`
- add targeted unit tests for:
  - numeric comparison
  - string comparison
  - boolean logic
  - missing-field behavior
  - malformed-expression failure
  - non-boolean result rejection

Done when:

- generic runtime can evaluate simple expressions deterministically in tests
- no active runtime path depends on Empire-specific logic to prove the evaluator works

Status:

- complete

### Slice 2.2: Guard Migration

Objective:

- make `guard.check` the preferred execution path for active MAS guards

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- [workflow_hooks.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/empire/workflow_hooks.go)
- new guard-focused tests under `internal/runtime/pipeline/`

Work:

- teach `evaluateWorkflowGuard(...)` to prefer inline `guard.check`
- support both:
  - single guard form: `guard: {id, check, on_fail}`
  - multi-check form: `guard: {checks: [...] }`
- keep existing built-ins only where they are truly platform-generic
- downgrade hook-based guard evaluation to explicit migration fallback
- add parity tests for:
  - validation revision limit guards
  - operating capacity guard
  - scoring-derived prefilter guards where expressions are already contract-declared

Done when:

- active MAS guards execute from contract expressions first
- Empire-specific guard branches are no longer required for the active validation/operating paths

Status:

- in progress
- current runtime covers:
  - inline single-check guards
  - inline multi-check guards
  - MAS dialect normalization for placeholders / enum literals / `else`
  - runtime-backed query dialect for `query_entities(name == payload.opportunity_name).count`
  - explicit fallback for unsupported expressions that still need temporary hook coverage

### Slice 2.3: Rules Migration

Objective:

- move handler `rules` routing onto expression evaluation

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- [workflow_instance_projection.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_projection.go)
- rule-focused tests under `internal/runtime/pipeline/`

Work:

- replace string/alias-based rule matching with expression evaluation
- support `else` deterministically as terminal fallback
- prove behavior on current MAS rule users:
  - directive routing
  - mailbox decision routing
  - scan mode routing
  - scoring completion branch routing where feasible

Done when:

- rules evaluate by contract condition, not by handwritten event-specific branching

Status:

- started
- mailbox decision routing now uses contract rules through generic runtime matching
- scan projection no longer infers rule branches from map keys; it evaluates contract conditions directly
- structured `system.directive` now routes through portfolio-node contract rules
- shared directive helpers and product-policy gating now understand structured directives
- scan-campaign-manager now preserves and understands structured scan directives in live runtime paths while keeping the legacy free-text bridge
- root event schema coverage is aligned for new portfolio-node emissions (`budget.adjustment_requested`, `budget.alert_sent`, `directive.unhandled`, `policy.change_requested`)
- legacy `directive_text` payloads still fall back to the old scan-manager path as a migration bridge
- canned runtime E2E directive ingress still intentionally uses the legacy scan-campaign path; converting those fixtures now would change the behavior they validate

### Slice 2.4: `on_complete.condition` Migration

Objective:

- evaluate handler completion branches declaratively

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- completion-focused tests under `internal/runtime/pipeline/`

Work:

- wire CEL into `on_complete.condition`
- support ordered first-match semantics plus `else`
- rebaseline scoring completion tests around expression outcomes rather than legacy aliases

Done when:

- completion routing is expression-driven on active MAS paths

Status:

- active scoring completion now executes from MAS `on_complete.condition` semantics
- `count(tier1_dims >= 70)` is normalized and evaluated in generic runtime
- scoring fan-out and canned marginal/reject scenarios were rebaselined to MAS thresholds instead of legacy composite buckets
- declarative action-/gate-/emit-only handler plans now execute without requiring a stage advance
- `build-orchestrator` now executes from MAS contracts through the generic declarative node path, including `record_evidence`
- full [pipeline](/Users/youmew/dev/empireai/internal/runtime/pipeline) is green under this model

### Slice 2.5: Fallback Burn-Down

Objective:

- make remaining Empire hook use explicit migration debt

Primary files:

- [workflow_hooks.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/empire/workflow_hooks.go)
- [guard_action_registry.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/guard_action_registry.go)
- architecture and compliance tests

Work:

- inventory any remaining guard/rule decisions still requiring hooks
- classify each as:
  - generic built-in to promote
  - later-phase dependency
  - stale logic to remove
- add assertions so new active guard/rule paths cannot silently route back through Empire-specific code

Done when:

- Empire hook usage for guard/rule semantics is narrow, explicit, and no longer required for the active MAS workflow paths

Status:

- started
- operating `pipeline_has_capacity` no longer depends on Empire hook evaluation; the MAS stage-range count dialect is now evaluated in generic runtime
- legacy compatibility guard IDs (`signal_above_threshold`, composite/gate/approval/evidence checks) now execute generically and no longer require `WorkflowHooks()` on the active path
- legacy compatibility action IDs (`select_rubric`, validation/score outcome emits, OpCo spinup marker) now execute generically and no longer require `WorkflowHooks()` on the active path
- hookless regression coverage now proves those compatibility IDs execute without an Empire hook executor present
- Empire workflow hook executor is now a no-op and the default Empire MAS module returns `nil` for `WorkflowHooks()`; active guard/action semantics no longer execute through product-specific workflow hooks
- generic runtime no longer carries guard/action hook fallback branches in the transition engine for the MAS-default path
- dead Phase 2 fallback artifacts were removed, including the unused structured-scan parser and the deleted Empire workflow hook file
- generic runtime now prefers merged MAS policy values over root-only policy when evaluating expressions and contract policy lookups
- `vertical.derived` admission is now evaluated through the scoring-node MAS guard surface (including runtime-backed child-count and name-dedup checks) while child materialization remains on the existing background path
- overlapping operating events routed through `lifecycle-orchestrator` now delegate into declarative `build-orchestrator` execution instead of handwritten metadata shims
- `scan_campaign_manager` no longer treats MAS-structured `system.directive` as campaign ingress; structured directives now stay on the contract-owned `portfolio-node` path and the manager only keeps the legacy free-text bridge
- broader [internal/runtime](/Users/youmew/dev/empireai/internal/runtime) is green except for the known out-of-scope dashboard architecture guard
- Phase 2 completion criteria are met on the MAS-default runtime path
- remaining residuals are cleanup-only and not blockers for Phase 2 completion:
  - legacy external free-text `directive_text` ingress kept as a compatibility bridge
  - broader query/filter/compute dialect beyond the now-supported `query_entities(...).count` shape

## Recommended Execution Order

1. Slice 2.1
2. Slice 2.2
3. Slice 2.3
4. Slice 2.4
5. Slice 2.5

Reason:

- the evaluator and context model must stabilize first
- guards are the smallest high-value surface
- rules come next because multiple MAS nodes already depend on them
- `on_complete` should be last because it is the most semantics-dense path

## First Slice Recommendation

Start with `Slice 2.1 + the first half of Slice 2.2`.

That means:

- add the CEL dependency and evaluator wrapper
- define the runtime context
- route inline `guard.check` through CEL for the active validation/operating guards first

This is the smallest tranche that materially reduces product leakage without forcing a full branch-routing rewrite immediately.

## What Counts As Success

Phase 2 is successful when all of the following are true:

- generic runtime can evaluate contract expressions deterministically
- active MAS `guard.check` paths no longer require Empire-specific guard code
- active MAS `rules` paths are expression-driven
- malformed expressions and non-boolean results fail in an observable, deterministic way
- hook-based guard/rule execution is no longer the default semantic path

## What Is Explicitly Not Required For Phase 2

Phase 2 does not need to finish:

- dynamic flow instance creation
- wildcard instance routing
- timer lifecycle semantics
- full boot verification port
- broad query/filter/compute expression support beyond what active Phase 2 paths require

Those remain later phases or later slices.
