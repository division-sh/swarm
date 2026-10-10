# SQLite API Duplicate Counter Retirement

Parent #2542 / #2151. Four SQLite-only helper definitions are deleted outright:
countSQLiteEventsByName, countSQLiteAllEventRows, countSQLiteAllRunRows and
countSQLiteAPIIdempotencyRows. Their complete five-root caller family consumes
the already-qualified backend-neutral original-selected count owners. No SQL
adapter, renamed delegate, compatibility alias or additional query authority is
introduced. Five finite complete caller recipes preserve every request, source,
key, replay/refusal assertion and timing/synchronization cut.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| First SQLite publication, exact replay and subsequent new event | reproduced and fixed | TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock under race, same exact event/command counts and no-lock behavior. |
| Payload failure creates no rows or command completion | reproduced and fixed | TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows under race, all original zero assertions. |
| Private ordinary endpoint cannot authorize publication | reproduced and fixed | TestOperatorEventPublishSQLiteRejectsPrivateOrdinaryFlowEndpoint under race, exact original name/run/receipt checks. |
| Explicit-run followup uses the selected run | reproduced and fixed | TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun under race, whole-run/name assertions unchanged. Its separate old exact-run reader is not silently reclassified. |
| Caller entity ID cannot create persistence | reproduced and fixed | TestOperatorEventPublishSQLiteRejectsCallerEntityIDForCreateEntityBeforePersistence under race, all original zero checks. |
| Lost input/key/owner/assertion or old helper reintroduction | execution-proven through the same corrected path | Independent complete AST workload/hostile controls; raw and delegating retired definitions are rejected, including methods and build-excluded sources. Complete caller inventories now include these shared native consumers. |

Supersession: the subsequent exact-run counter continuation closes the earlier
remaining SQLite scoped reader, deletes its definition and propagates the nine
scoped consumers to original selected ownership. Earlier finite snapshots are
composed, not duplicated; unrelated delivery/receipt/direct SQL debt stays open.

Canonical ownership is unchanged: existing native selected read transactions
and fixed count observations. Previous grouped native proofs on BOTH stores
establish scope, history, cancellation, ownership refusal and no partial evidence.
This slice adds no operation and changes no production execution semantics.
The exact-run SQLite helper and other direct fixture SQL are separate remaining
predicate/consumer families in #2542, not legacy compatibility supplied by these
retired whole-store/name helpers. The five callers remain source-pinned and the
existing inventories are explicitly refined, not replaced with a second ledger.

Closure: four duplicate interpreters and complete callers retired/canonicalized,
NOT all API reader or parent closure. Existing watchlist/tracker nodes suffice;
no architecture framework or new issue is warranted. Binding spec:
selected_runtime_store_projection.raw_sql_policy,
sqlite_api_counter_retirement. Final zero-debt/strict guards/integrated proof stays
open. Before push require monotonic census/registry,78 guards +retirement controls,
complete finite overlay/hostile controls, partition/spec, exact-head complexity
and definitive unused. No PR, aggregate tier or server2 use is claimed.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment22-*.
