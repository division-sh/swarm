# Cohort 102: persisted compute replay construction

Approved #2542/#2151 fixture-authority class and selected construction/raw_sql_policy
govern. The compute-replay fixture now consumes native PostgreSQL/SQLite construction
and the original selected run lifecycle owner on both stores. Generic pool/admission,
DatabaseForTest and dialect-specific run SQL helpers are removed from this family.

The SQLite helper delegates platform plans, schema bootstrap, payload admission and
cleanup to the shared native owner rather than hand-maintaining those requirements.
Its engine-test origin label is intentionally replaced by the canonical storetest
fixture label; no assertion or runtime behavior depended on that test-only label.
Exact run UUID, scenario origin, start clock and source artifact fact remain.

Only the setup variable combines replay persistence with RunFixtureStore; production
replay persistence does not gain a lifecycle dependency. A transaction collector
requires actual acknowledged original writes and zero active work after run admission.
No pool, context-carried transaction or reconstructed selected owner is exported.

The real both-store race proof retains initial WASM execution, persisted runtime-log
envelope, reconstructed executor replay, unrelated event/node exclusion, exact stored
output-hash divergence, typed CodeReplay/finding and no selection/commit/emit effects.
The downstream proof function is unchanged. Two finite recipes, exact constructor
and root oracles, type overlay and hostile source/owner/clock/receipt checks cover
the migration. Census must decrease without new occurrences before commit.

Value: construction/run-admission canonicalization and shared bootstrap reuse;
no measured flake-rate improvement is claimed. No production semantics/spec change,
new framework/compatibility/architecture split or additional issue. Parent closure
and remaining shared raw/context protocols remain explicitly open.
