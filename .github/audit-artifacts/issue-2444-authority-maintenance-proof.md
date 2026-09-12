# Authority maintenance C3 proof

Scope: the two `RepairAuthority` owners in
`internal/store/internal/startupownership/authority_maintenance.go`. This is the
bounded C3 correction from `issue-2444-consumer-census-review.md`, not closure of
all consumer outcome snapshots.

## Correction

- PostgreSQL uses the existing retained `RunAuthorityTransactionOutcome` on the
  acquired lease session; SQLite uses existing `RunTransactionOutcome`.
- Only acknowledged COMMIT retains the repair result. Callback success followed
  by COMMIT failure returns a zero result, without claiming rollback occurred.
- Both existing owners synchronously join possession release and preserve its
  error via the named return. Acknowledged repair evidence survives release
  failure; callback/transaction errors are joined, not replaced.
- PostgreSQL release retains its existing cancellation-independent cleanup
  policy. No new owner abstraction, transaction policy, or timeout was added.

## Named proofs

File: `internal/store/internal/startupownership/authority_repair_outcome_test.go`.
All rows invoke the actual repair owner and fresh repair callback, including
generation retirement read, active/released authority writes, and repair insert.
SQL uses sqlmock; SQLite release uses the existing construction-possession
interface; PostgreSQL release failure is injected at the advisory unlock query.

| Exact test name | Evidence |
| --- | --- |
| `TestRepairAuthorityOutcomeAndJoinedRelease/postgres/ack` | Healthy COMMIT retains valid generation-2 repair and request identity, nil error. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/postgres/ack_release_failure` | Acknowledged result survives unlock failure; owner cannot return while release is held; release error preserved. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/postgres/commit_failure` | Successful callback plus failed COMMIT returns zero result and original failure; unsafe session disposal expected. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/postgres/callback_and_release_failure` | Failed repair insert rolls back, returns zero result, joins release, preserves both errors. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/sqlite/ack` | Healthy COMMIT retains valid generation-2 repair and request identity, nil error. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/sqlite/ack_release_failure` | Acknowledged result survives possession failure; owner cannot return while release is held; release error preserved. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/sqlite/commit_failure` | Successful callback plus failed COMMIT returns zero result and original failure; unsafe connection disposal expected. |
| `TestRepairAuthorityOutcomeAndJoinedRelease/sqlite/callback_and_release_failure` | Failed repair insert rolls back, returns zero result, joins release, preserves both errors. |
| `TestSQLiteUnsupportedFilesystemInspectionOnly` | Existing real SQLite control: inspection remains available; unsupported filesystem prevents repair and leaves row counts unchanged. |

## Executed receipt

Transcribed tool results, working directory `/tmp/agent-g-2444-implementation`:

```text
go test ./internal/store/internal/startupownership -run '^TestRepairAuthorityOutcomeAndJoinedRelease$' -count=1 -timeout=30s -v
PASS
ok github.com/division-sh/swarm/internal/store/internal/startupownership 0.011s

go test -race ./internal/store/internal/startupownership -run '^(TestRepairAuthorityOutcomeAndJoinedRelease|TestSQLiteUnsupportedFilesystemInspectionOnly)$' -count=3 -timeout=60s -v
PASS
ok github.com/division-sh/swarm/internal/store/internal/startupownership 1.188s
```

All eight outcome rows and the filesystem control passed on each race repeat;
no skips. Scoped `git diff --check` passed.

## Limits

These are owner-level injected driver-settlement/release proofs, not new real
PostgreSQL durability, lost-response, or OS unlock-error qualification. The
COMMIT error deliberately does not establish whether the server committed.
No CLI exit test or full suite was run. Other consumers, startup acquisition,
and broad outcome-snapshot closure remain outside this correction. The prior
real composed monitor readiness/join proof remains separately recorded in
`issue-2444-local-fencing-proof.log`.
