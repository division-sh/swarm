# Phase 9 Subplan: Resolve The Remaining Flat-First Mismatch Set

## Goal

Move beyond the Phase 8 hybrid boundary by dealing explicitly with the remaining events that cannot yet be promoted to handler-first candidate selection.

The target is not a big-bang rewrite. The target is:

- shrink the mismatch set where safe
- document irreducible mismatches where not safe
- prepare the engine for the later normative handler execution order

## Current Flat-First Mismatch Set

- `vertical.approved` -> `action_mismatch` (derived `emit_spinup` / `opco.spinup_requested` vs flat `emit_opco_spinup_requested`, plus human-decision semantics)
- `vertical.needs_more_data` -> `node_mismatch`

## Current Revision-Loop Alias Set

These no longer block handler-first candidate promotion because the mismatch was only semantic aliasing while execution still returns the same flat transition object:

- `spec.validation_failed` -> `revision_loop` vs flat `increment_revision_count`
- `cto.spec_revision_needed` -> `revision_loop` vs flat `increment_revision_count`

## Steps

### 1. Classify each mismatch by root cause

For every remaining flat-first event, determine whether the mismatch is due to:

- action naming / action semantics
- guard mismatch
- owning-node mismatch
- stage-target mismatch
- human/mailbox decision semantics
- missing handler-side normalization

Done when:
- each remaining flat-first event has a single explicit root-cause classification

### 2. Normalize mismatches that are only semantic aliases

If a mismatch is only due to naming or equivalent action aliases, normalize it in the derived semantic adapter instead of leaving it flat-first.

Examples to evaluate:

- revision-loop handler action vs flat `increment_revision_count`
- lifecycle emit/side-effect aliases

Done when:
- purely alias-driven mismatches no longer block promotion

### 3. Keep human/mailbox transitions flat-first unless proven safe

Do not force handler-first promotion for:

- `vertical.approved`
- any other human-decision-driven transition

unless guard/action parity can be made exact.

Done when:
- human-decision transitions are either:
  - explicitly promoted with proof, or
  - explicitly documented as flat-first by design for now

### 4. Re-evaluate the revision-loop transitions

Specifically:

- `spec.validation_failed`
- `cto.spec_revision_needed`

Determine whether the derived handler semantics can safely map to the current flat revision-loop behavior.

Done when:
- both are either:
  - promoted safely via semantic alias normalization, or
  - documented as intentionally flat-first pending handler-order work

### 5. Re-evaluate `vertical.needs_more_data`

This currently shows a node mismatch.

Determine whether:

- the flat transition should move toward current owning-node semantics, or
- the handler-first candidate should remain non-authoritative here until a later phase

Done when:
- the mismatch is either resolved or explicitly frozen as a later-phase issue

### 6. Update the parity inventory and decide the final hybrid boundary

At the end of Phase 9, explicitly list:

- promoted subset
- shadow-only match subset
- flat-first mismatch subset
- semantic-alias subset

Done when:
- the runtime’s handler-first boundary is explicit and stable

## Acceptance Gate

```bash
go test ./internal/runtime/pipeline -run 'TestFactoryPipelineCoordinator_Resolve(DerivedWorkflowTransitionByEvent|WorkflowTransitionByEvent)' -count=1
go test ./... -count=1
```

## Exit Condition

Phase 9 is complete when:

- every remaining flat-first event is either promoted or explicitly justified
- mismatch causes are classified, not vague
- the hybrid execution boundary is stable enough to start working on later execution-order migration
