# Cohort 99: lifecycle proof construction

Binding context: approved #2542/#2151 selected-authority migration; platform-spec.yaml
selected_runtime_store_construction and selected_runtime_store_projection.raw_sql_policy.
No production semantics change or new owner is introduced.

The shared runFixtureProofBackends owner serves all four lifecycle fixture proofs:
exact materialization, completion-catalog refusal, exact replay and write-free refusals.
Its PostgreSQL branch now consumes StartPostgresRuntimeStore instead of exposing a
testutil pool and passing it into AdmitPostgresRuntimeStore. The source-revision proof
uses the same original construction owner. SQLite construction is unchanged.

The canonical fixture registers selected-store close before consumer cleanup and
retains the original location, bootstrap and payload admission. Existing independent
peer/location proofs cover original writer receipts and close independence on both
stores. All five affected roots retain their clocks, source identities, candidates,
state/failure facts, replay equality, cancellation and no-write refusal assertions.

The finite codemod has two exact recipes, one shared helper and one complete root.
Its oracle permits only the constructor substitution, with hostile foreign/test/backend
substitutions rejected. Focused race proof covers the five affected roots plus both
native location controls. Debt is refreshed downward before this family is committed.

Reliability value: construction and resource-owner confinement, not measured flake
reduction. This does not close the parent or remove the remaining generic getters,
reconstructed transaction protocols or other fixture consumers. Watchlist/architecture
disposition unchanged: existing owners, no compatibility or new framework.
