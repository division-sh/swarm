# Phase 8 Subplan: Narrow Execution-Facing Handler-First Migration

## Goal

Start moving runtime execution from flat transition-first semantics toward handler-derived semantics, while preserving current behavior with explicit flat-transition fallback.

The first broad candidate-resolution attempt was too aggressive and changed real runtime behavior. Phase 8 must therefore proceed in a narrower, comparison-first way.

## Steps

### 1. Narrow the execution-facing transition index

Build a normalized internal view from handler transitions that can answer:

- candidate transitions by `(node, event)`
- target stage / `advances_to`
- guard binding
- emit binding
- data accumulation source

Done when:
- the bundle can expose a deterministic execution-facing handler-transition index
- unsupported events can be excluded explicitly rather than guessed

### 2. Add shadow candidate resolution before behavior changes

In the transition engine:

- compute a handler-derived candidate in parallel
- keep the flat candidate as the authoritative execution path
- compare where both exist

Done when:
- mismatches are visible in tests and narrow enough to classify
- production behavior is unchanged

### 3. Promote only a safe subset to handler-first lookup

In the transition engine:

- enable handler-first lookup only for a deliberate safe subset
- keep flat `workflow-schema.yaml` transitions as fallback for everything else

Candidate safe subset should likely be chosen from validation/lifecycle events where:

- the derived handler has a single clear `advances_to`
- guard binding is simple
- no observed parity mismatch exists

Done when:
- a narrow subset runs handler-first without changing observed behavior

### 4. Keep guard/action execution and timers unchanged initially

Do not rewrite execution ordering yet.

Done when:
- only candidate resolution changes
- guard/action execution still flows through the current transition engine path

### 5. Add equivalence coverage

For representative events, prove:

- shadow handler-first resolution finds the same transition as flat lookup where expected
- fallback remains active where derived semantics are not yet sufficient or not yet promoted

Done when:
- tests demonstrate parity for the promoted subset
- tests also demonstrate fallback for non-promoted events

### 6. Document remaining Phase 8 gap

Capture what still depends on flat transitions after the first cut:

- timer ownership
- some hook wiring
- multi-path or ambiguous handler-derived candidates
- any event types that must remain flat-first for now

Done when:
- the next execution-facing slice can build on a written gap list

## Acceptance Gate

```bash
go test ./internal/runtime/contracts ./internal/runtime/pipeline ./internal/runtime -count=1
go test ./... -count=1
```
