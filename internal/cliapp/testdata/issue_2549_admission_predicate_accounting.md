# #2549 S2-M Startup Predicate Accounting

The hosted census correctly rejected the changed native PostgreSQL consumer at
0d20fba15 and d8920c01d. S2-M adds opt-in diagnostic observations, not a startup
predicate or permission. This account refreshes the actual owners, callers and
execution witnesses before regenerating their two body fingerprints.

## Binding Rows And Preserved Owners

A19/A28 and P18/P24 retain the classifications in the
[#2286 pre-audit](https://github.com/division-sh/swarm/issues/2286#issuecomment-5970334585):

- A19: selected-store/backend/schema inspection distinguishes fresh, compatible,
  unavailable and unsupported state without creating or preparing the store.
- A28: the selected typed read roles support the required inspection; a nonnil
  pointer or successful transport is not execution authority or completeness.
- P18: execute selected-store/schema observations and prove no hidden DDL or
  process-authority acquisition. Missing state is not silently readable state.
- P24: incomplete, unavailable, cancelled or failed-cleanup observation cannot
  become successful admission. Keep the existing report reducer and caller cuts.

No census surface, row, proof family, guard or runtime predicate is removed,
reclassified or exempted. Retained PostgreSQL and SQLite settlement are still
separate native owners, not alternate implementations of this pooled consumer.

| Measured body | Actual owner and complete direct caller set | Preserved contract and execution witnesses |
| --- | --- | --- |
| `postgres.(*Backend).runTransactionOutcome` | Existing pooled native transaction owner. Direct callers remain `RunTransactionWithOptionsOutcome` for ordinary mutable attempts and `RunReadTransaction` for a standalone repeatable-read, read-only snapshot. Borrowed inspection reads reuse their enclosing transaction and do not enter a fourth settlement owner. | No callback replay or new admission. Read queries retain caller cancellation; native rollback is joined before connection disposal. Diagnostic COMMIT/ROLLBACK observations do not change return values or retry evidence. `TestInspectionSnapshotBindsReadersAndRejectsEscapes`, `TestInspectionNativeReadCancellationJoinsTransactionAndDisposes`, `TestPostgresSerializationConflictRequiresCleanNativeSettlement` and `TestOrderingNativeReadsAndPreBeginRefusalHaveNoHolder`. |
| `postgres.joinTransactionRollback` | Sole caller is the cleanup defer in `runTransactionOutcome`. It either performs the native rollback or joins that owner's existing cancellation callback. | The callback records native rollback return before publishing to its channel; the synchronous branch records after native return. Neither cleanup nor channel joining is settlement authority. Original cancellation/error/panic and disposal order remain. `TestPostgresTransactionCancellationRollbackOrders`, `TestPostgresTransactionPanicAndFailureExits` and the native inspection cancellation/disposal test. |

## Real Startup Inspection Consumers

The A19/A28 execution path remains:

1. `cliapp.inspectVerifySelectedStore` opens the selected read-only store and
   applies the existing bounded schema observation through
   `selected.AdmissionInspection.Inspect`.
2. `AdmissionInspection.Inspect` calls the selected store's `InspectSnapshot`.
   `runtimepersistence.PostgresStore.InspectSnapshot` delegates to
   `postgres.Backend.InspectSnapshot`, whose standalone `RunReadTransaction`
   calls the pooled native owner with read-only/repeatable-read options.
3. Inside that snapshot, `runtimepersistence.PostgresStore.InspectSchema`
   delegates to `schemastore.Postgres.InspectSchema`. Its `RunReadTransaction`
   borrows the same exact enclosing snapshot; standalone schema inspection
   still goes through the same pooled native owner.
4. The existing schema decision distinguishes preparation from compatibility.
   Typed retained readers consume the snapshot; cancellation and expired scope
   refuse reads. Process possession is a separate observation and is never
   acquired by schema inspection.
5. The selected observation returns through `verifyDeploymentBoundedObservation`
   and the existing `bootverify.Report.AdmissionDecision` completeness reducer.
   A cancelled/failed observation or failed cleanup remains non-success.

## Execution Proof Map

| Obligation | Existing concrete proof selected for this repair |
| --- | --- |
| P18/A19 public fresh PostgreSQL and SQLite observation | `cliapp.TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession` and `TestVerifyFreshSQLiteSchemaAccountsAbsenceWithoutClaimingPossession`: actual public verify command, zero PostgreSQL public tables / unchanged SQLite bytes and sidecars, explicit pending schema preparation, no fabricated complete readiness. |
| P18/A19 compatible, missing generated tables and unsupported drift | `schemastore.TestSchemaInspectionSharesBootDecisionWithoutPreparation`: real SQLite and PostgreSQL; inspect fresh and prepared schemas without DDL, leave missing state tables absent, and compare exact inspection/boot drift refusal. |
| A28 selected role availability and non-authority | `selected.TestAdmissionInspectionRealReadScopeBothStores`: actual typed timer, run, recovery, reset, channel and actor readers; fresh-domain reads refused; expired/cancelled snapshot cannot escape; no process-authority rows. `TestAdmissionInspectionMissingSQLiteDoesNotCreateState` preserves absent-store refusal. |
| P24 observation failure and caller deadline | `cliapp.TestVerifyDeploymentObservationPreservesFailureAndCancellation` and `TestVerifyDeploymentObservationBudgetsAndCancellation`: failed/unavailable/cancelled observations and callbacks returning nil after expiry do not become complete; caller deadline is never extended. |
| P24 missing or contradictory completeness evidence | `bootverify.TestAdmissionCompletenessReducerRequiresEvidence`, `TestAdmissionCompletenessReducerRejectsUnsupportedEvidence` and `TestAdmissionCompletenessReducerPreservesPriorityAndFindingPolicy`: missing/unknown/duplicate/failed evidence stays non-success, with unchanged precedence. |
| P18/P24 native snapshot cancellation and joining | `postgres.TestInspectionNativeReadCancellationJoinsTransactionAndDisposes`: real read-only/repeatable-read snapshot, expired SQL operation, joined socket/pool disposal and no surviving native session. Existing snapshot and transaction-exit controls cover mutation/escape/panic refusals. |
| Census itself must still detect omitted predicates/callers | `cliapp.TestVerifyBootStartupPredicateCensus` without update mode, plus all three `TestAdmissionPredicateRatchetRejects*` mutation controls. |

Regenerate only with the existing
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` mechanism. The expected delta is two
body hashes in `postgres/transaction.go`; owners, A19/A28 and P18/P24 remain
unchanged. Generator success is not execution proof. Exact-head receipts belong
in the PR proof audit; this record names the witnesses, not unrun test credit.

The repair changes evidence only. No SQL, admission, role, lock lifetime,
deadline, skip, automatic retry, compatibility path or spec change is approved.
