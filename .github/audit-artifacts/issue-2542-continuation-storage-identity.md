# Native Workflow Storage Identity Cohort

Integration base `06018d2e1`, following projection family `6c6f0b311` on
`agent-b/2542-selected-authority-migration`. One family commit; no new PR or
per-family tier qualification. The broad #2151 gate and #2542's incremental
review / quiet-window delivery ruling remain binding.

## Owners And Complete Consumers

Three original roots now use the exact native SQLite/PostgreSQL workflow owner:
`TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed`,
`TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores`, and
`TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata`.
Original assertion bodies become component verifiers; original root names and
the original backend cells remain, with PostgreSQL added to the former SQLite
parent-route proof. The SQLite name is retained to preserve existing proof refs.

The existing native activity fixture supplies opaque WorkflowPersistence,
canonical MaterializeRun and the already-landed selected activation operation.
No new read/fault port, adapter, SQL callback, reconstructed coordinator or
compatibility path is introduced. Component construction is prepared storage
evidence, not public lifecycle eligibility or route-readiness proof.

The execution path is native store -> canonical source artifact and run(s) ->
acknowledged native activation -> exact selected run/route read -> joined cleanup.
Run source/context identity and both independent stored states remain exact;
singleton/template route reads must succeed while entity-ID addressing misses;
all parent route/entity fields and the canonical identity owner remain asserted.

The native transaction probe witnesses exact committed writes: source artifact
once, one/two canonical runs and one/two activations. Expected totals are 5/4/3
for isolation/exact-route/parent-route respectively; workflow-mutation writes
and active transactions are zero. The first failed probe expected 4/3/2 and
omitted the real EnsureArtifactWithData commit. Source/driver evidence identified
that operation; the probe was corrected, not disabled or loosened to a range.
The original assertion bodies passed in that failed attempt. An earlier uint64
probe-counter compilation mistake is retained separately, without execution
credit. No production defect or new ownership decision was discovered.

Missing-run validation, callback mutation, physical projection corruption,
timer/readiness and fake-runner siblings remain explicit later #2542 cohorts.
No last-caller deletion of their common helpers is claimed in this increment.

## Codemod And Guards

Three finite before/after recipes extend the existing inventory from 77 to 80.
An independent AST oracle compares every preserved workload/assertion statement,
including exact native run binding and fail-closed seed handling. Hostile changed
scope, entity/route addressing and parent-identity assertions are rejected.
The shared candidate-overlay test actually reverses all current snapshots in
memory, reapplies all 80 recipes and invokes the existing complete type preflight
once for the combined candidate; no filesystem source is modified by that test.
Read-only application on the current tree remains inert.

The unchanged full collector drives the ninth individually mandatory child of
the already-landed fresh shared census. Raw-parameter and SQL-callback adversaries
remain independent admission roots. All six venue/tier partition, membership and
timing contracts retain every earlier child and the exact two-root complement.
There is no repeated source census per family, new inventory permission, collector
change, budget raise, ignored error or raw handle.

## Proof And Debt

Focused race: all three original roots / six backend cells PASS (15.053s),
including the four singleton/template route cuts; zero failures/skips. All 80
codemod snapshots, independent controls and the real combined type overlay PASS
(22.638s). Existing 78 structural guards and planner/timing partition controls
PASS. The full downward ratchet/shared-family/hostile/registry selection PASS
(69.802s), all nine family children executed, zero skips.

Debt `14,844 -> 14,829`: 15 findings removed, ZERO added. Confirmed raw sites
`11,015 -> 11,004`: 11 removed. All 67 excluded-source uncertainties and collector
`494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5` remain
unchanged; current registry verifies 12,488 exact facts. Receipts, including both
failed preparations, are retained under
`/home/youmew/.cache/swarm-2542-local-20261006/continuation-storage-identity-*`.
These are focused working-tree receipts, not aggregate qualification.

No architecture/runtime contract changes: governing selected construction,
projection/raw-SQL and backend-neutral mutation clauses are consumed unchanged.
Existing watchlist/parent tracking suffices; no new issue or framework. Closure
is this finite native storage-identity cohort, not global #2542/#2151 elimination.
Strict zero-debt guards, capability deletion, SQLite fork deadline and final
integrated full remain the parent finish line.
