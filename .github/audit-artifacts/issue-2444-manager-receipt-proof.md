# #2444 Manager Receipt Outcome Proof

Workspace: `/tmp/agent-g-2444-implementation`. Production frozen; no commit.

## Exact Scope

- `internal/runtime/manager/receipts.go`: `writeReceipt` preserves the exact acknowledged settlement snapshot alongside store, heartbeat-finish, and continuation errors. Heartbeat settlement is finished using validated acknowledgement evidence, never `err == nil`. Retry continuation retention or terminal continuation release still occurs on postcommit error. No retry or new owner was added.
- `internal/runtime/deliverylifecycle/model.go`: shared `Snapshot.MatchesSettlementClaim(claim) bool`, available for parent relay to Faraday's pipeline consumer. Validates delivery/run/event-derived ID, route identity and route, subscriber class/ID, exact claim version, and delivered/dead-letter/failed status. Only an outcome-bearing SettleSuccess/SettleFailure response establishes acknowledgement; matching arbitrary readback is not COMMIT evidence and cannot validate an opaque token. The store owns token validation.
- Tests: `internal/runtime/manager/receipt_settlement_outcome_test.go` and `internal/runtime/deliverylifecycle/settlement_snapshot_test.go`.

No edits to `runtime/pipeline/coordinator.go`, session producers, transaction
owners, or other manager production files.

## Immediate Consumer Trace

All `processEventDetailedOwned` receipt branches return on settlement error;
none issues a second settlement. `processEvent` and `safeProcessEventOwned`
forward the error. The live loop logs a nonpanic processing error and exits
that attempt; its extra `writeReceipt` path is exclusively panic handling, not
ordinary postcommit error recovery. Subsequent work remains behind exact
delivery claim admission/renewal. Existing retry eligibility and continuation
ownership, not a cleanup error, control a later retry attempt.

## Executed Proofs

`TestWriteReceiptPreservesCommittedSettlementAndContinuation`: 24 leaves,
four statuses (processed, retryable failure, dead-letter, terminal) by six cuts
(healthy, store postcommit error, continuation error, both errors, uncommitted,
foreign claim-version snapshot). The fixture delegates to real SQLite delivery
adapter SQL and COMMIT before injecting an owner return error. Tests compare
the entire returned/retained snapshot, assert exactly one terminal release or
retry retention, no postcommit final renewal, joined errors, one durable
outcome, and no second settlement when the exact settled claim is reused.
Uncommitted and foreign snapshots cannot authorize continuation handling.

`TestProcessEventDoesNotResettleAcknowledgedReceiptError`: successful and failed
handler controls preserve independent settlement/continuation errors. A second
processing invocation with the same settled claim fails before another handler
invocation or settlement.

`TestSnapshotMatchesSettlementClaim`: both agent and node subscribers accept
the three settled states, reject pending/in-progress/unknown states and eight
identity mismatches, and reject empty claims/snapshots. The first node fixture
omitted mandatory target ownership; only that fixture was corrected before the
passing repeated run.

```sh
go test -race ./internal/runtime/deliverylifecycle -run '^TestSnapshotMatchesSettlementClaim$' -count=3 -timeout=2m
```

PASS, 1.018s.

```sh
go test -race ./internal/runtime/deliverylifecycle ./internal/runtime/manager -run '^(TestSnapshotMatchesSettlementClaim|TestWriteReceiptPreservesCommittedSettlementAndContinuation|TestProcessEventDoesNotResettleAcknowledgedReceiptError)$' -count=3 -timeout=3m -v
```

PASS for both packages; manager 9.140s, 78 manager leaf executions, no skips.
`git diff --check` passes for both production and both test files.

```sh
go test -race ./internal/runtime/deliverylifecycle ./internal/runtime/manager -count=1 -timeout=5m
```

Full package regression PASS: deliverylifecycle 1.065s, manager 46.696s.

## Direct Dual-Store Extension

Added `internal/runtime/manager/receipt_selected_store_test.go` using existing
`storetest.AdmitPostgresRuntimeStore` / `storetest.StartSQLiteRuntimeStore`
factories, canonical event/delivery seeding, and exact `storetest.ClaimDelivery`.
The manager package's existing external-test bridge pattern permits the real
selected stores to invoke private `writeReceipt` without an import cycle.
`receipt_settlement_outcome_test.go` reuses its existing outcome wrapper for
either selected store. No new fixture framework or production edit.

`TestManagerReceiptOutcomeBothSelectedStores` has 16 leaves: both stores by
processed/retryable-failure/dead-letter/terminal by healthy/error. Processed and
terminal cases register an actual failing completion-candidate sink, which
independently reads the exact settled delivery before returning its error.
Both the store handoff error and independent continuation retain/release error
survive the real manager receipt consumer. Retry settlement intentionally does
not submit a completion candidate; that branch instead injects an owner-return
error after the real selected-store SettleFailure returns. It is explicitly
named `owner_return_and_continuation_errors`, not a candidate-handoff proof.

All cases assert exact returned/retained snapshots against selected-store
readback, one durable outcome, no final renewal after acknowledged settlement,
correct continuation retain/release, and refusal to resettle the same claim.
The initial run's retry branch expected a candidate that the production owner
does not request; only the test classification/injection was corrected.

```sh
go test -race ./internal/runtime/manager -run '^TestManagerReceiptOutcomeBothSelectedStores$' -count=1 -timeout=3m -v
```

PASS, 10.735s; all 16 leaves, no skips. Production remains frozen; the earlier
full-package regression receipt preceded this test-only extension.

```sh
go test -race ./internal/runtime/manager -run '^(TestManagerReceiptOutcomeBothSelectedStores|TestWriteReceiptPreservesCommittedSettlementAndContinuation|TestProcessEventDoesNotResettleAcknowledgedReceiptError)$' -count=3 -timeout=3m -v
```

Final combined PASS, 26.497s; 126 leaves across three repetitions, including 48
direct selected-store cases (24 PostgreSQL, 24 SQLite), no skips. Test-only
extension is complete; nothing remains in this slice. No commit created.

## Limits

The original manager fixture uses real SQLite delivery-adapter settlement;
the extension uses real full PostgreSQL and SQLite selected-store owners.
Failures are injected after COMMIT through candidate submission, owner return,
or continuation handling as distinguished above. These are not wire-failure,
database-driver cleanup, process-death, or restart-durability proofs. No live
providers, production credentials, or external effects were used. Parent owns
aggregate `swarm-test`; Faraday owns pipeline consumer use of the shared
matching predicate.
