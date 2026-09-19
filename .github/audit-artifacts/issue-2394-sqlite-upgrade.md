# SQLite Dependency Upgrade

The user authorized a plain `modernc.org/sqlite` v1.40.1 -> v1.59.0 upgrade
in the #2394 delivery after an isolated evaluation. This is a dependency
upgrade, not a change to platform semantics. No vendoring, third-party source
patch, statement cache, adapter, configuration flag or compatibility path is
included. `go get` selects the driver's matching libc and transitive modules;
`go mod tidy` records that graph. The Go directive remains 1.25.0.

## Isolated Decision Evidence

The same original 500-row HTTP journey, SQLite, normal build, three repetitions:

| Dependency | First submission to last durable ordinal outcome (seconds) |
| --- | --- |
| v1.40.1 | 3.773808305 / 3.802029006 / 3.782254835 |
| v1.59.0, direct queries | 3.235212460 / 3.239671652 / 3.248224950 |

All original workload and final readback assertions passed. Median elapsed time
decreased 14.35%. This is a local served integration workload with mock agents,
not a production-wide speedup claim. An experimental write-statement adapter
saved only another 2.18% and was rejected for adoption. None of its code is in
this delivery. The exact original HTTP proof is unchanged.

The isolated v1.59.0 direct-query experiment also passed native SQLite backend
transaction tests at race/count3, source/failure isolation at race/count3,
and focused restart/fork/stale-fence controls at race/count3. These were
development receipts, not final integrated-head or whole-suite qualification.

## Known Upstream Limitation

v1.59.0 rejects a second execution of the same caller-prepared `sql.Stmt`
inside one transaction while its first result cursor remains open, with
`SQLITE_MISUSE (21)`. The identical overlap probe passed on v1.40.1. No
corruption was observed. The plain direct-query equivalent passes; the current
persistence adapters use that direct-query path rather than explicit SQL
statement reuse. The upgrade does not claim to fix or fully qualify the
affected prepared-statement pattern. Any future prepared-statement optimization must
address it explicitly rather than assuming the driver's cache is transparent.

`TestDirectQueryOverlappingCursorsThroughTransactionOwner` permanently covers
the supported direct-query behavior in both read and write transactions:
interleaved identical SQL with distinct bindings, exact row/end checks,
cancelled query refusal, subsequent query reuse and the next transaction.
Its integrated race/count3 run passed (1.031s). Existing native rollback,
busy, cancellation and commit-failure tests remain unchanged and mandatory.

## Qualification Boundary

Integrated development receipt after the plain upgrade:
`TestIssue2394TwentyIntentContentionBothStores/sqlite/independent_runs`,
`-race -count=3 -timeout=180s`, passed in 167.168s. The three complete
repetitions took 57.42s, 54.31s and 54.34s, retaining all twenty intents,
500 outcomes and final histories each time. The old v1.40.1 command timed out
at the same aggregate 180s budget. This fixes that recorded qualification
failure without changing its envelope. It is not proof of unrelated matrices
or a frozen full implementation commit. Raw log:
`2394-v1590-integrated-m17-race3.log` in the retained build-artifact directory.

The complete native SQLite backend package also passes on the integrated tree
at `-race -count=3 -timeout=180s` (49.094s), including the new overlap control
and existing real busy, rollback, cancellation, panic and commit-failure cases.
Log: `2394-v1590-integrated-native-race3.log`. `go mod verify` reports all
modules verified. These receipts do not imply whole-platform qualification.

The integrated SQLite payload/entity/resource snapshot and mixed/failure
isolation selections (M15/M16) pass together at race/count3 in 66.849s,
retaining the original drain deadlines, native rollback and bisection checks.
Log: `2394-v1590-integrated-m15-m16-race3.log`.

The upgrade does not close M23: SQLite N64 race/count3 fails in 146.664s.
All three repetitions pass cap32/cap16 but exceed the unchanged 30-second
quiescence deadline at cap1. No race detector error was reported.
Log: `2394-v1590-integrated-m23-n64-race3.log`. The PostgreSQL nested-entry
five-second deadline (M29) also remains red in its diagnostic run. Neither
failure is erased or waived by the passing dependency receipts.

The dependency change does not waive any #2394 workload, operation deadline,
same-process repetition, backend, required soak or full-suite proof. The lead
subsequently authorized absorbing the distinct #2454 log-filter repair through
issue comment 5746053255; that repair retains its own proofs and is not an effect
of this dependency upgrade. Final PR proof must identify its tested commit
and actual remaining failures; this decision record alone is not readiness.
