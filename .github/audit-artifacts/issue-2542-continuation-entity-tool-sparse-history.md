# Selected Sparse Entity Tool Whole-Run History

Parent #2542 / #2151. The last SQL handle/query in entity_sparse_mutation_test.go
is removed. The original selected tracked-projection owner now supplies a named
whole-run history observation, sharing its existing native owner/schema/read
validation with the exact-entity reader. No new coordinator, fake transaction
context, runtime selector or generic query interface is introduced.

The predicate is run_id ONLY, not run/entity. Every row retains entity_id, domain,
path and COALESCE(new_value,'null') bytes, ordered created_at DESC/mutation_id
DESC, unpaginated. Absent run remains an empty physical result; storage failures
return no evidence. The public fixture consumer is solely the sparse generated
tool root. Private controls prove same-run sibling inclusion and foreign-run
exclusion. The existing conformance/exact-entity consumers retain their original
unmodified query and evidence contracts through the extracted read-owner check.

The complete sparse run/source/tool workload, positive and refusal input tables,
no-write/revision checks, continuation assertions, optional-field history counts
and PostgreSQL debug-history count comparison remain unchanged. The prior finite
whole-root recipe is updated in place rather than introducing conflicting rewrites
for the same function. Its independent workload oracle adds only the exact raw
history/setup replacement and preserves every other AST statement.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Generated sparse tool history and complete public tool workload | reproduced and fixed | TestEntitySparseGeneratedToolMutation under race, both stores; all original optional/initial/immutable/equality/refusal/continuation/history assertions. |
| Complete run scope, DESC order and SQL-null bytes | execution-proven through the same corrected path | TestRunEntityMutationHistoryStoragePreservesWholeRunOrderAndNullBothStores; same-run sibling, foreign-run, null and empty-run witnesses plus one original read transaction. |
| Raw/invalid/cancelled/closed/unavailable storage | execution-proven through the same corrected path | TestRunEntityMutationHistoryStorageRefusesRawInvalidCancelledClosedAndUnavailableBothStores; no partial evidence, joined restoration of unavailable storage. |
| Shared owner exact-entity regression | execution-proven through the same corrected path | TestTrackedMutationProjectionPreservesScopeOrderAndNullBothStores, original order/null/scoping/read-commit checks. |
| Changed predicates/order/null/workload/assertions | reproduced and fixed | Complete finite recipe and independent workload equality; changed run, debug-count, receiver filter, presence-count, COALESCE and order controls. |
| Future raw sibling in completed sparse file | reproduced and fixed | Expanded native-handler completion guard and unlisted sibling/raw-parameter adversarial probes; storetest read port remains closed by the existing observation guard. |

Authority-registry disposition before refresh: new/changed resolved SQL facts are
the private tracked-projection read-owner extraction, its fixed run-history query,
the private named bookkeeping operation and their source-pinned native controls.
These retain private-runtime-adapter/private-backend ownership. No external raw
fact, exemption, raw getter permission or collector policy is added. Grouped
refresh must demonstrate zero new debt identities before pushing.

Closure: complete claimed sparse-tool source/run/history family and this file's
raw authority, NOT all tool fixtures. Parent zero-debt/global guards, remaining
harness/served/pipeline families and final integrated/fork proof remain open.
Existing watchlist/tracker mapping remains sufficient; no new issue or design
ruling is required. Spec: selected_runtime_store_projection.raw_sql_policy.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment17-sparse-history-*.

Grouped qualification: ten unique affected/native-control roots,74 passing race
records, zero failures/skips. All156 complete finite recipes, independent workload
oracles, positive actual-source overlays and the external-tool type-error negative
control pass; replay is inert. The78 structural guards and planner/timing/spec
contracts pass. Fresh downward census removes21 findings/17 raw sites, with ZERO
added/increased identities:14,564/10,794 ->14,543/10,777. Collector hash and67
excluded uncertainties are unchanged; all16 mandatory native-family children pass.
The first registry refresh correctly refused19 new/changed private facts. All19
were explicitly source-classified (three private backend facts,16 private runtime
adapter facts), with no external exemption. The independent exact registry rerun
passes12,761 facts. Exact committed-head complexity and definitive native unused
results accompany the increment before push; neither an aggregate tier nor final
parent closure is claimed.
