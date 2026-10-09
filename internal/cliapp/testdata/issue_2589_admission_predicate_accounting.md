# #2589 Startup Predicate And Describe Corpus Accounting

The qualification failure at c45e5e33e identified two stale characterization
artifacts. No runtime, admission predicate, workload, budget or assertion is
changed by this refresh.

## Native Transaction Owner

The existing census generator changes exactly one measured body:
`internal/store/internal/backend/postgres/transaction.go::*Backend.runTransactionOutcome`.
Its A19/A28 and P18/P24 classifications remain unchanged; no owner is removed,
reclassified or exempted. P1 binds the private ordering-possession scope to the
already-created exact native transaction and retires it after commit, rollback
and emergency cleanup. Binding does not acquire the ordering lock, bypass
startup possession, cache grants or change first acquisition or lock lifetime.

Consumers remain the native PostgreSQL mutation attempt and retained-scope
owners. `TestOrderingScopeReusesOnlyExactTransactionPossession`,
`TestOrderingScopeCancellationRetirementAndFreshAttempt`,
`TestOrderingScopeRefusesMissingAndBooleanAuthority`,
`TestPostgresTransactionAdmissionExits`,
`TestPostgresTransactionCancellationRollbackOrders` and
`TestPostgresTransactionPanicAndFailureExits` retain concrete ownership,
cancellation, rollback, panic and commit evidence. Existing selected-store
admission and fresh-grant controls remain required in final qualification.

The refresh uses only `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` and the existing
generator. Normal verification runs without that variable and includes the
three `TestAdmissionPredicateRatchetRejects*` negative controls. Generator
success is not execution proof.

## Compiled Describe Corpus

The approved authoritative spec adds two author-story transaction clauses:
exact transaction ordering possession and canonical active-run admission.
Consequently, compiled provenance includes two more transaction-contract
entries and moves the existing destructive-exception clause from index11 to
index13. These are the only changes across the ten JSON/graph-JSON goldens for
the five existing fixtures. All text, quiet and route goldens remain unchanged.

The existing `SWARM_UPDATE_DESCRIBE_CORPUS=1` generator retains the independent
source-artifact check, compact JSON-wire assertions and two identical public
invocations for every cell. A comparison undoing only those two added
provenance entries and the exact index shift must recover each old JSON value.
Normal `TestReadProofFactoringCompiledDescribe` verification runs with update
mode absent, together with the corpus-inventory and normalization negative
controls. No broad normalization, digest filtering or golden exclusion is added.
