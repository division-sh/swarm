# Phase 9 Closeout: Resolve The Remaining Flat-First Mismatch Set

## Status

Phase 9 is complete.

Verification:

```bash
go test ./... -count=1
```

## What Landed

The handler-first candidate path now has a stable hybrid boundary:

- promoted via derived handler-first candidate selection, while still returning the existing flat transition objects:
  - `vertical.shortlisted`
  - `research.completed`
  - `cto.spec_approved`
  - `vertical.ready_for_review`
  - `research.vertical_rejected`
  - `spec.validation_failed`
  - `cto.spec_revision_needed`
  - `opco.steady_state_reached`
  - `opco.growth_triggered`
  - `opco.growth_stabilized`
  - `build_complete`
  - `launch_ready`
  - `opco.teardown_requested`

- intentionally flat-first:
  - `vertical.approved`
  - `vertical.needs_more_data`

## Final Mismatch Classification

### Semantic alias cases that were normalized

- `spec.validation_failed`
  - derived handler action: `revision_loop`
  - flat transition action: `increment_revision_count`
  - normalized as a safe semantic alias because execution still returns the flat transition object

- `cto.spec_revision_needed`
  - derived handler action: `revision_loop`
  - flat transition action: `increment_revision_count`
  - normalized as a safe semantic alias for the same reason

### Intentionally flat-first cases

- `vertical.approved`
  - root cause: action mismatch plus human/mailbox semantics
  - derived handler action: `emit_spinup`
  - flat transition action: `emit_opco_spinup_requested`
  - additionally depends on human-decision guard semantics, so this remains flat-first for now

- `vertical.needs_more_data`
  - root cause: node mismatch
  - derived handler owner: `validation-orchestrator`
  - flat transition owner: `runtime`
  - this remains flat-first until a later execution/ownership migration phase

## Why Phase 9 Is Complete

The exit bar for Phase 9 was:

- every remaining flat-first event is either promoted or explicitly justified
- mismatch causes are classified, not vague
- the hybrid execution boundary is stable enough to start the next execution-order migration

That bar is now met.

## Handoff To Phase 10

The next phase is no longer about candidate parity classification.

It is about making the `v2.6.0` normative handler execution order real while preserving the current promoted/fallback boundary as a safety rail.
