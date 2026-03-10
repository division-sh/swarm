# Phase 8 Closeout: Narrow Execution-Facing Handler-First Migration

## Outcome

Phase 8 is complete.

The runtime now uses a mixed but explicit execution-facing model:

- **promoted handler-first candidate lookup** for a narrow safe subset
- **flat transition fallback** for all remaining events
- **shadow parity classification** for non-promoted and mismatch cases

This keeps runtime behavior stable while making handler-derived semantics part of the actual transition-resolution path.

## What Is Live

The transition engine now:

1. computes a derived handler-first candidate from node ownership + handler semantics
2. promotes only explicit allowlisted events
3. resolves those promoted events back to the existing flat transition objects
4. keeps all non-promoted or mismatching cases on the flat transition path

This means:

- execution behavior remains stable
- guards/actions still execute through the existing flat transition objects
- the runtime has a real execution-facing bridge from handler semantics to current execution

## Promoted Subset

The current promoted subset is:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `research.vertical_rejected`
- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `build_complete`
- `launch_ready`
- `opco.teardown_requested`

These events are now handler-first for candidate selection, but still execute through the existing flat transition objects after semantic parity filtering.

## Explicit Flat-First Fallback Set

The following events remain flat-first intentionally:

- `spec.validation_failed`
- `vertical.approved`
- `cto.spec_revision_needed`
- `vertical.needs_more_data`

Current classified mismatch reasons:

- `spec.validation_failed` -> `action_mismatch`
- `vertical.approved` -> mismatch
- `cto.spec_revision_needed` -> `action_mismatch`
- `vertical.needs_more_data` -> `node_mismatch`

These are not safe to promote without changing runtime behavior or redesigning the execution model around them.

## Why Phase 8 Is Complete

Phase 8 required:

- a shadow comparison lane
- explicit parity/mismatch classification
- a promoted handler-first subset
- flat fallback everywhere else
- stable full-suite behavior

All of those are now true.

This phase did **not** require:

- replacing flat transitions entirely
- making handler-first execution authoritative for all events
- rewriting execution order

Those belong to the next phase.

## Execution Gap After Phase 8

What still depends on flat transitions:

- revision-loop actions (`increment_revision_count`, revision routing)
- human-decision / mailbox-driven transitions
- some lifecycle/action-hook differences
- any event where handler semantics and flat transition semantics are not yet equivalent

So the runtime is now in a controlled hybrid state:

- safe subset: handler-first candidate selection
- mismatch set: flat-first fallback

## Acceptance Status

Green:

```bash
go test ./... -count=1
```

## Next Phase

Phase 9 should build on this boundary by deciding how to handle the remaining flat-first mismatch set:

- normalize or redesign the mismatch cases so more events can be promoted
- or keep the mismatch set explicit and begin moving execution ordering closer to the `v2.6.0` normative handler model
