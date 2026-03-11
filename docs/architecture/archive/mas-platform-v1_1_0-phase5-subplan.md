# MAS Platform v1.1.0 Phase 5 Subplan

Date: 2026-03-11
Author: Codex implementer

## Phase 5 Goal

Make MAS conformance explicit instead of inferred.

By the end of Phase 5:

- boot verification should be checked against the MAS platform model
- event-loop behavior should be exercised against MAS semantics, not legacy expectations
- contract merge/state handling should be proven against the MAS package model
- the MAS test framework should exist as a real executable harness, not just documentation

This phase does not remove all product leakage by itself. It creates the conformance harness needed to safely do Phases 6-8.

## Starting Point

- `internal/runtime/pipeline` is green under MAS-default semantics
- full `internal/runtime` is back to the expected non-dashboard envelope
- the main remaining architectural risk is not executor correctness; it is undocumented or untested divergence from the MAS platform spec
- Slice 5.1 has started:
  - runtime startup now has an explicit failure-path test proving invalid workflow contracts abort boot before live runtime execution
  - runtime startup now also has an explicit self-check failure-path test proving boot aborts when `runtime.boot` cannot be published during bootstrap verification
- Slice 5.2 has started:
  - deferred-chain event-loop behavior is now checked explicitly in the bus tests, including interceptor re-entry, persistence, and delivery-manifest behavior for consumed versus terminal events
- Slice 5.3 has started:
  - scoped duplicate merge coverage now exists for nodes, events, agents, and tools
  - root event-catalog payload-field drift is now under test
  - package-tree max depth 99 is now enforced in loader validation
- Slice 5.5 has started:
  - `internal/runtime/masflowtest` now auto-discovers and executes the checked-in MAS doc packages
  - the harness now also covers `expected.yaml` normalization edge cases:
    - single-trigger documents with whitespace and duplicate emitted-event entries
    - sequence-driven accumulation where `entity_fields_before` arrives as string-typed values
  - the synthetic harness semantics now explicitly cover:
    - pending accumulation when expected arrivals have not been met
    - idempotent duplicate-arrival handling
    - `completion: threshold` in addition to `completion: all`
  - explicit Phase 5 conformance now also covers:
    - `on_fail: blocked`
    - sorted MAS catalog discovery
    - bus reset epoch behavior and fresh namespaced routing after reset
    - recurring/lifecycle bootstrap payload-shape checks in isolated runtime tests
- Slice 5.4 inventory outcome so far:
  - the checked-in MAS catalog currently exposes only `test-accumulate-all`
  - the platform spec catalog advertises `threshold`, `timeout`, `partial`, `idempotent`, `with-compute`, `expected-from-entity`, and crash-recovery accumulation cases, but those packages are not checked into `docs/specs/mas-platform/tests` yet
  - the live runtime still does not expose a generic MAS `accumulate` execution path; current executable conformance is limited to the `masflowtest` harness and product-specific scan accumulation paths

## Slice 5.1: Boot Sequence Conformance

### Objective

Turn MAS boot rules into explicit runtime verification.

### Target files

- `internal/runtime/runtime.go`
- `internal/runtime/wiring_verification_test.go`
- `internal/runtime/contract_compliance_test.go`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

### Work

- extract the MAS-ordered boot steps from the platform spec into an explicit verification checklist
- distinguish fatal boot violations from warning-only checks
- verify MAS-default contract source selection, runtime wiring, flow loading, and timer/bootstrap restoration against that checklist

### Acceptance

- runtime boot checks are named and traceable to the MAS platform spec
- current runtime boot/compliance tests assert MAS boot semantics directly

## Slice 5.2: Event Loop Conformance

### Objective

Prove the runtime event loop matches MAS semantics.

### Target files

- `internal/runtime/bus/eventbus_test.go`
- `internal/runtime/bus/eventbus_interceptor_tx_test.go`
- `internal/runtime/pipeline/workflow_transition_engine_test.go`
- `internal/runtime/pipeline/scheduler_test.go`

### Work

- map the MAS event-loop lifecycle to the current runtime path:
  - publish
  - persistence
  - interception
  - routing
  - handler execution
  - deferred re-entry
  - projection/state update
  - timer side effects
- add focused tests for rollback, dead-letter, wildcard routing, and deferred-event re-entry where MAS requires them

### Acceptance

- event-loop ordering and failure semantics are explicit in tests
- no core event-loop behavior is validated only by broad Empire E2E scenarios

## Slice 5.3: Contract Merger And State Model Conformance

### Objective

Prove bundle loading and state scoping against the MAS package model.

### Target files

- `internal/runtime/contracts/workflow_contracts.go`
- `internal/runtime/contracts/workflow_contracts_test.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`
- `internal/runtime/pipeline/workflow_instance_store.go`

### Work

- add direct conformance tests for path-keyed merge behavior
- prove scoped registry behavior where raw IDs collide across package/flow scope
- verify flow-scope versus entity-scope state handling against the MAS state model
- verify generated schema/event resolution against the resolved MAS bundle, not just the flat root catalog

### Acceptance

- merge behavior is tested in MAS terms, not bridge-era terms
- workflow state scope and identity are explicit and spec-backed

## Slice 5.4: Accumulation And State Progression Conformance

### Objective

Prove the runtime supports the MAS accumulation model it claims to support.

### Target files

- `internal/runtime/pipeline/workflow_transition_engine.go`
- `internal/runtime/pipeline/workflow_instance_projection.go`
- `internal/runtime/pipeline/*accumulator*`
- `docs/specs/mas-platform/tests/`

### Work

- inventory active support for `all`, `threshold`, and `timeout`
- identify which catalog cases already exist in current tests versus which are missing entirely
- either implement the missing supported accumulation behavior here or explicitly push unsupported modes to Phase 6 with clear file ownership

### Acceptance

- supported accumulation modes are documented and tested
- unsupported modes are explicit backlog, not silent gaps

## Slice 5.5: MAS Test Framework Adoption

### Objective

Start executing the spec-defined MAS test catalog.

### Target files

- `docs/specs/mas-platform/tests/`
- `internal/runtime/masflowtest/`
- `internal/runtime/contract_compliance_test.go`
- `internal/runtime/pipeline/workflow_transition_engine_test.go`

### Work

- create a minimal Go runner for MAS packages with `expected.yaml`
- reuse current MAS-default test helpers instead of inventing a second harness
- extract a canonical MAS module/bundle loader into `internal/runtime/masflowtest/`
- make the existing tiny MAS test packages executable from Go
- add edge-case coverage for `expected.yaml` parsing and normalization so the harness does not silently diverge from the spec format
- keep fixture-mocked agents as a later extension if the initial harness can start with node-only cases

### Acceptance

- at least the existing MAS doc test packages run through Go
- MAS catalog execution becomes part of the compliance path
- `internal/runtime/masflowtest/` has a clear role instead of being an ad hoc helper folder

## Phase 5 Order

1. Slice 5.3
2. Slice 5.1
3. Slice 5.2
4. Slice 5.5
5. Slice 5.4

Reason:

- merge/state conformance is the lowest-level correctness base
- boot and event-loop conformance depend on understanding the actual resolved bundle/runtime model
- the MAS harness should be built after the runtime invariants it will assert are made explicit

## Immediate Next Step

Start with Slice 5.3:

- inventory current merge/state tests
- add missing MAS path-keyed merge coverage
- wire the first conformance assertions to the resolved MAS bundle instead of the legacy flat catalog

## Exit Gate

Phase 5 is complete only if:

- the MAS boot model is reflected in explicit verification
- the MAS event-loop model is reflected in explicit tests
- bundle merge/state behavior is proven against MAS semantics
- the MAS test framework exists as an executable Go path, even if initially only for a small subset of the catalog
