# Semantic Correctness Watchlist

Canonical home for open semantic-correctness work: spec alignment, persistence/recovery truth, lifecycle proof, conformance, and fail-closed boundary semantics.

## Active Issues

- `#113` Align event-loop validation, delivery receipts, and recovery with the authoritative spec.
- `#114` Reconcile boot and loader contract vocabulary with the authoritative platform spec.
- `#115` Preserve full persisted correlation envelope during pipeline recovery replay.
- `#118` Add a reusable cross-boundary delivery lifecycle conformance suite.
- `#119` Persist canonical terminal lifecycle for template flow instances.
- `#120` Persist canonical run completion instead of inferring it from quiescence.
- `#132` Make CEL validation lifecycle-aware for entity-field availability.
- `#133` Introduce a typed canonical carrier for entity-state semantics across execution seams.
- `#149` Make handler execution order match the spec for branch selection and top-level `data_accumulation`.
- `#150` Make task conversation persistence rollout-safe across legacy and canonical storage.
- `#151` Remove preview-backed non-success assertions from conformance cases that claim runtime/store semantics.
- `#152` Fail closed on non-UUID `entity_id` at the store event boundary.

## Reserve Backlog

- `7.` Audit and prove mutation-log completeness for all `entity_state` write paths.
  - follow-on priority: issue `#162`
- `9.` Make declarative `data_accumulation` expression semantics fully explicit and CEL-capable.
  - follow-on priority: issue `#163`
- `11.` Introduce a first-class flow-instance model instead of passing identity/scope semantics through loosely-related strings.
  - combined follow-on priority: issue `#164`
- `12.` Separate semantic flow scope from concrete flow-instance path everywhere.
  - combined follow-on priority: issue `#164`
- `15.` Replace the engine helper fallback interpreter with an explicit typed expression model.
  - combined follow-on priority: issue `#165`
- `19.` Replace flat expression-context merging with explicit scoped variable semantics.
  - combined follow-on priority: issue `#165`
