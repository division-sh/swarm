# Full Genericity Endgame Plan

## Goal
Reach a state where:
- non-test Go files contain no Empire-specific identifiers, defaults, routing targets, or policy assumptions
- runtime behavior is driven by root contracts plus injected module wiring
- system-node execution is no longer primarily handwritten product/workflow code
- node state transitions and handler behavior are inferred from contract semantics wherever those semantics are expressible in YAML
- product-specific logic lives only in:
  - contracts
  - config
  - prompts/templates
  - product module packages selected at bootstrap

In practical terms:
- no `Empire*` symbols in generic runtime code
- no `empire-coordinator` literals in non-test Go files outside product assembly/config readers
- no Empire scan taxonomy or workflow assumptions in generic runtime code
- no Empire-specific fallback loaders in generic runtime code
- no requirement that generic runtime know the Empire node behaviors in handwritten form

## Current State
- `v2.2.1` compliance: high and green
- platformization: materially advanced
- remaining gap:
  - generic runtime still carries compatibility surfaces and some product-shaped assumptions
  - node execution is still mostly handwritten Go logic, not fully contract-interpreted behavior

The remaining work is not mainly spec work anymore. It is architecture cleanup and boundary hardening.

## Phase 1: Define And Enforce The Boundary
Goal:
- turn “no Empire in generic Go code” into an enforceable rule

Tasks:
- add an architecture guard that fails if non-test Go files outside approved product packages contain:
  - `Empire`
  - `empire-`
  - `empire_`
  - `empirecoordinator`-style literals
- define the allowed product-specific directories, for example:
  - `internal/runtime/pipeline/empire`
  - `internal/runtime/productpolicy/empire`
  - explicitly named bootstrap/config bridge files if still needed
- make the guard ignore:
  - tests
  - docs
  - YAML/config files

Exit criteria:
- the repo has a hard, automated “no Empire in generic non-test Go” rule
- every remaining exception is explicit and counted

## Phase 2: Remove Empire-Named Symbols From Generic Runtime APIs
Goal:
- eliminate Empire-specific type/function names from generic code even where behavior is already injected

Tasks:
- rename remaining Empire-specific exported/internal helpers in generic packages to neutral names
- replace any `Empire*` function/type/variable names in:
  - `internal/runtime/contracts`
  - `internal/runtime/pipeline`
  - `internal/runtime/manager`
  - `internal/runtime/tools`
  - `internal/commgraph`
  - `internal/factory`
- keep the product-specific names only in product packages

Typical targets:
- contract path/loader helpers
- module defaults
- fallback bundle loaders
- policy helpers

Exit criteria:
- generic packages have neutral API names only
- product naming exists only in product packages

## Phase 3: Move Remaining Product Defaults Out Of Generic Runtime
Goal:
- generic runtime should require injected module/config behavior rather than quietly defaulting to Empire behavior

Tasks:
- remove any remaining generic fallback that implicitly selects Empire wiring
- ensure bootstrap is the only place that chooses the active product module
- move any remaining product-shaped default values into:
  - product module
  - config
  - contract-derived policy

Examples:
- workflow/module default selection
- scan mode defaults
- routing defaults
- policy defaults

Exit criteria:
- generic runtime cannot boot a product implicitly
- product selection happens only at assembly/bootstrap

## Phase 4: Eliminate Empire-Specific Policy From Shared Logic
Goal:
- no business-policy assumptions in generic runtime code

Tasks:
- audit remaining non-test Go files for:
  - mode names
  - threshold names
  - policy-specific dimensions
  - product recipients
  - product event shortcuts
- move any remaining product-specific policy into:
  - module hooks
  - productpolicy package
  - contract-derived policy readers
- prefer data-driven policy where contracts/config already expose the needed information

Exit criteria:
- generic runtime code contains only platform semantics
- business policy is injected or data-driven

## Phase 5: Remove Compatibility Buckets From Live Runtime Decisions
Goal:
- compatibility buckets may exist temporarily for restore/migration, but they must not influence live decisions

Tasks:
- finish removing live-read dependence on:
  - `pipeline-coordinator`
  - `scoring-state`
  - other migration-era buckets
- keep only contract-shaped node buckets as authoritative for:
  - workflow state
  - node state
  - transition decisions
  - timer decisions
- if compatibility buckets remain, mark them as write-only or restore-only and guard against new live reads

Exit criteria:
- live runtime behavior no longer depends on migration-era bucket shapes
- compatibility buckets are clearly secondary

## Phase 6: Make Product-Specific Restore State Contract-Shaped
Goal:
- remove the hidden need for product-specific compatibility payloads in generic restore paths

Tasks:
- identify state still recoverable only from legacy/product-shaped payload buckets
- either:
  - promote those fields into declared node `state_schema`, or
  - move them into module-owned persistence outside generic runtime state reconstruction
- keep restore logic generic:
  - contract-shaped node bucket first
  - optional module-specific enrichers second

Exit criteria:
- generic restore logic is driven by declared node state
- product-specific restore enrichment, if any, is module-owned

## Phase 7: Unify Scoring With The Same Executor Model
Goal:
- remove the last major architectural exception in the 5-node runtime model

Tasks:
- move scoring fully behind the same split executor pattern as scan/discovery/validation/lifecycle
- reduce or remove the special dedicated scoring runtime path
- keep product-specific scoring policy in injected hooks/modules, not in generic runtime

Exit criteria:
- all 5 nodes follow one architectural pattern
- scoring is no longer a special runtime path

## Phase 8: Make Runtime Routing Fully Contract/Node Driven
Goal:
- no old coordinator-era routing assumptions in generic runtime logic

Tasks:
- continue shrinking:
  - legacy source-agent labels
  - retired node names in compatibility code
  - coordinator-centric tracing assumptions
- make runtime traces, handler coverage, and architecture guards refer only to active node owners from contracts

Exit criteria:
- active runtime routing language matches the current node model everywhere
- old coordinator terminology survives only in clearly isolated migration code, if anywhere

## Phase 9: Final Genericity Audit
Goal:
- prove the codebase meets the “no Empire in non-test Go files” requirement

Tasks:
- run a repository-wide audit across non-test Go files
- fix or isolate any remaining references
- document any intentionally product-specific non-test Go packages that remain and why
- update architecture docs and handoff docs

Recommended audit commands:
```bash
rg -n "Empire|empire-|empire_|empirecoordinator" --glob '!**/*_test.go' --glob '*.go'
rg -n "pipeline-coordinator" internal/runtime --glob '!**/*_test.go'
```

Exit criteria:
- repository audit is clean for non-test Go files outside approved product packages
- architecture docs reflect the final boundary

## Phase 10: Lock The Boundary
Goal:
- prevent regression after cleanup

Tasks:
- keep architecture guards permanent
- add hotspot tests for newly genericized files if needed
- add CI checks that fail on:
  - forbidden product literals in generic Go code
  - reintroduction of product-named APIs in generic packages
  - live reads from deprecated compatibility buckets

Exit criteria:
- the generic boundary is enforced continuously

## Phase 11: Declarative Node Execution
Goal:
- finish the move from a product-shaped orchestrator runtime to a genuinely generic contract-driven node engine

Tasks:
- define the executable subset of contract semantics for system nodes, including at least:
  - `event_handlers`
  - `state_schema`
  - `entity_schema`
  - `data_accumulation`
  - transition-side effects that are representable as platform actions
- distinguish clearly between:
  - declarative behavior the generic engine can interpret
  - irreducible side effects that remain module-owned hooks
- build a generic node execution/interpreter layer that can:
  - read current node state from contract-shaped buckets
  - apply declared state mutations
  - enforce state/entity schema boundaries
  - emit declared follow-up events or platform actions through generic dispatch
- migrate common node patterns out of handwritten executor code into the generic interpreter, starting with:
  - accumulation/update handlers
  - stage-projection handlers
  - timer-driven state transitions
  - approval/revision bookkeeping
- shrink handwritten node code until it is mostly:
  - module hooks for irreducible side effects
  - product-specific heuristics that are not yet representable in contracts
- add architecture guards that fail if generic runtime reintroduces product/workflow-specific handler logic that should now be declarative

Exit criteria:
- generic runtime can execute the common system-node patterns from contracts rather than handwritten product code
- handwritten node code is minimal and hook-oriented
- adding a new product workflow requires mostly contracts + module hooks, not substantial generic-runtime surgery

## Acceptance Gates
Run after each phase:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline -count=1
go test ./internal/runtime -count=1
go test ./... -count=1
```

For final genericity audit:

```bash
rg -n "Empire|empire-|empire_|empirecoordinator" --glob '!**/*_test.go' --glob '*.go'
```

## Definition Of Done
This work is complete when:
- `v2.2.1` compliance remains green
- all active runtime behavior is contract-driven and node-owned
- non-test Go files in generic/shared packages contain no Empire references
- any remaining product-specific code is isolated to explicit product packages
- no live runtime decisions depend on migration-era compatibility buckets
- all 5 runtime nodes use one consistent execution architecture
- generic runtime executes the common node-state-machine patterns from contract semantics rather than handwritten product/workflow logic
- adding a second product requires primarily contracts, config, and module hooks rather than new generic runtime branches
