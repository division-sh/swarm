# #2432 Verification: Shared SQLite Commit-Failure Blocker

Agent: agent-g. Date: 2026-09-09. This is new executable evidence, not another
submission of the approved diagnostic design. #2321/#2432 production work remains
local and uncommitted; no combined review PR or closure claim has been published.

## Finding

The required F4 commit-time fault proof fails in the existing shared SQLite
transaction owner, independently of the diagnostic implementation. Reproduced
three times on freshly fetched `origin/master@05528a92e35176b72fb9ee7ab63e963c41beefb7`
in `/tmp/agent-g-2432-commit-failure-probe`. No production file was changed there.

`internal/store/internal/backend/sqlite/transaction.go` calls `tx.Commit()`, then
`tx.Rollback()` on error. The latter returns `sql.ErrTxDone`, which the owner treats
as clean rollback. The underlying SQLite connection can still own the failed
transaction. Its uncommitted writes are visible to subsequent reads using that
pooled connection, and its next ordinary transaction fails before callback entry.

This does **not** prove that the failed transaction committed. The independent
connection control proves precisely the opposite: no durable change was visible.
The proven defect is an unrolled-back transaction returned to the connection pool,
with dirty same-connection reads and failed subsequent writes.

The generic probe uses a deferred foreign key to force failure at COMMIT rather
than during a statement. This is not a legacy-store case: authoritative
`platform-spec.yaml` defines `DEFERRABLE INITIALLY DEFERRED` constraints in the
current `event_deliveries` / `event_delivery_attempts` contract. The schema compiler
also explicitly supports them. The probe's two auxiliary tables are test-only;
no production schema change is proposed.

## Exact Reproduction

Probe source: `issue-2432-sqlite-commit-failure-probe.go.txt` alongside this artifact.
Place it at `internal/store/internal/backend/sqlite/commit_failure_probe_test.go`
in a disposable worktree of the recorded upstream head, then execute:

```sh
go test ./internal/store/internal/backend/sqlite \
  -run '^TestCommitFailureReturnsCleanConnectionProbe$' -count=3 -v -timeout=60s
```

Result in each repetition:

| Cut | Pooled read | Independent read | Next callback | Result |
|---|---:|---:|---|---|
| Error returned by transaction callback | 0 | 0 | entered | pass, clean rollback |
| Deferred constraint fails at COMMIT | 1 | 0 | never entered | fail, connection retains transaction |

Exact second-cut errors:

```text
constraint failed: FOREIGN KEY constraint failed (787)
SQL logic error: cannot start a transaction within a transaction (1)
```

Local transcript: `/tmp/agent-g-2432-master-commit-failure.log`. The first failing
integrated proof is
`TestLifecycleDiagnosticAtomicProjection/sqlite/before_commit`; its PostgreSQL
counterpart uses a deferred constraint trigger and passes. Both diagnostic writes
are visible together on the poisoned connection; a duplicate half-commit is not
being alleged.

## Scope And Requested Disposition

The shared backend transaction/connection lifetime is below the approved lifecycle
diagnostic owner. A manager mutex, local retry, error suppression, early ack, or
special diagnostic SQL path cannot repair this honestly. Do not change this shared
owner under the diagnostic gate without an explicit bounded absorb/split ruling.

Request: independently classify the confirmed shared SQLite transaction cleanup
defect and record its bounded owner/repair disposition. Keep the approved combined
#2321/#2432 PR plan and separate diagnostic commit/proof ownership. No generic
transaction framework, compatibility path, or new logging owner is proposed.
#2250 remains the architecture umbrella; no closed issue was reopened and no new
issue was created by G pending the lead's concrete tracker decision.

## Current Implementation And Evidence

Local #2432 code implements the atomic named projection, immutable typed origin,
both fork validators, and destructive-discard lifetime. Ordinary projection-failure
tests, canonical corruption tests, delayed fork projection, actual fork activation,
independent store handles/reopen, 0/1/100/101 cardinality, discard rollback/races,
and public restart were exercised on both stores. These do not erase the F4 failure.

The first full integrated `swarm-test` run failed. Several failures were corrected
fixture/guard assumptions: unpersisted accepted parent, omitted selected origin,
target-source mismatch, semantic-event counts including new reliable diagnostics,
and the exact event admission census. Targeted reruns passed. The new runtime-log
validator now uses the existing delivery adapter instead of direct delivery SQL.
Registry and diff checks pass.

That run also timed out once in the PostgreSQL golden burst workload with a due
timer still present. It is unclassified, not called fixed or a new proven defect.
Its repeat and the final suite were queued behind another user's test run; G
cancelled only those two own queued commands after the new shared-backend blocker
was confirmed. No shared capacity was bypassed or another agent's run interrupted.

Prior #2321 genuine live/restart evidence remains bound to `97a9ea04a`, with its
previous integrated green suite at `eace6c528`. It is not final diagnostic closure.
No additional Claude/Telegram calls were made and no settled delivery was replayed.

Remaining: lead disposition for this demonstrated backend failure, F4 closure,
remaining final-head proof-accounting, golden burst classification, and the full
integrated suite. Then commit/push diagnostic implementation and open one normal
combined PR with both complete proof audits. Current closure: implementation
present locally, **not review-ready and not failure-class eliminated**.
