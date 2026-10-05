# Managed Qualification Stop: Handler-Selection Contention

Source a609c110230db16711afbde5f45e473450d9092e, PR #2525 / issue #2496.
This is a qualification failure, not chosen-class closure or a rerun waiver.

## Original Failed Receipt

`go run ./cmd/swarm-test` exits1 in broad-01. Root receipts:5,033 PASS,
one FAIL, five SKIP; cells24,411 PASS/one FAIL. The failed cell is
`delivery.TestHandlerSelectionCanonicalReadConcurrentBothStores/sqlite/equal`,
4.74s, line346: `persist delivery handler rule selection: database is locked
(5) (SQLITE_BUSY)`. Its conflict sibling and both PostgreSQL cells pass.
The driver stops before13 later planned units. Test-event span
12:07:40.233051484Z--12:14:37.110728250Z, about6m56.88s; pipeline332.388s.
Raw retained receipt: /tmp/agent-e-2496-repaired-fixture-managed-default.log.

The earlier622198b0c default also exits1, solely on the closed canonical
fixture construction guard. That owned test-only fixture placement is repaired
at0819d9464; no event bytes or assertions changed. Its5,033 root passes/one
failure/five skips and24,412 passing cells remain historical RED evidence.

## Matched Diagnostics, Not Full Qualification

The original contention test and sqlite transaction implementation are byte
identical between the candidate and current mastere63f4bdb1. Other delivery
files do change in this PR; unchanged-file comparison alone is not innocence.

`go run ./cmd/swarm-test -- -json -race -count=20 -run
'^TestHandlerSelectionCanonicalReadConcurrentBothStores$'
./internal/store/internal/backend/delivery`:

- Candidatea609:20 roots/80 cells PASS, package14.384s.
- Mastere63:20 roots/80 cells PASS, package14.263s.
- Logs: /tmp/agent-e-2496-a609c1102-handler-contention.log and
  /tmp/agent-e-2496-e63f4bdb1-handler-contention.log.

These focused controls do not erase or pass the broad-run failure.

The fixture starts two raw sql.DB transactions, waits for a pre-INSERT hook,
then assumes the first writer will commit after a20ms timer but before its
native SQLite busy_timeout(1000). It does not use the production SQLite
RunTransaction authority. The hook proves call entry, not native lock waiting.
The receipt lacks transaction/lock timing, so delayed writer scheduling is a
plausible explanation, not a reconstructed schedule or proven sole cause.

A diagnostic-only test on unchanged master keeps the first writer uncommitted
until the contender completes. It gets native SQLITE_BUSY in all three
repetitions (about1.06s each), then commits and verifies exactly one canonical
fact. No sleep, timeout inflation, native error substitution or runtime edit.
The existing `TestHandlerSelectionCanonicalReadSQLiteNativeBusyRetry` control
also passes three repetitions through the canonical owner, preserving its
exact two attempts/one native busy COMMIT, post-trigger SELECTs, rollback and
single committed-row assertions. Combined small diagnostic race command6
roots PASS7.484s. This proves a real contention branch and the owner's retry
control, not exact reconstruction of the original loaded failure.

Raw: /tmp/agent-e-2496-e63f4bdb1-held-writer-diagnostic.log.
Retained diagnostic source:
issue-2496-handler-contention-held-writer-probe.go.txt.

## Requested Disposition And Boundary

Record this under the existing open #2353 test-health/SQLite contention node;
it is a different concept from construction identity/readiness. No new issue
or production retry owner is proposed. The #2536 workload-pool fix is delivered;
do not conflate this isolated scalar-table fixture with startup starvation.

Reviewer-e/lead: authorize a narrow test-only repair of the contention schedule
through existing native contention observation and transaction authority, while
retaining equal/conflicting facts, canonical post-trigger readback and explicit
physical INSERT/SELECT counts. Any additional retry attempt must be asserted,
not hidden by weakening the counts. No sleeps, deadline changes, busy-timeout
inflation, skipped cells, swallowed errors or rerun-to-green. Exact repair is
subject to the native-driver schedule proof before claiming it closes this
qualification failure. Until disposition, this test and its production owners
remain unchanged in E's branch.

The13 later unchanged digest-bound units completed separately on frozena609:
12 PASS, catalog verify RED solely on ten compiled-describe provenance hashes.
The exact C21/rebased comparisons preserve all semantic fields and35 hashes;
the accepted45-cell rerun on cleanccaab9ce3 PASS98.577s. This does not erase
the original default RED or close this contention manifestation. E's branch
is locally rebased on mastere63f4bdb1; no test/production repair or push.
C22's independent consumer ruling remains separately pending at #2496
comment5968864157; no C22 production edits are made.

## Approved Local Repair: 2026-10-03

The preceding pending statements are historical. Reviewer-e authorized both
bounded repairs in issue comment5969425820, with the independent handler ruling
cross-recorded on #2353 in comment5969435322. E owns this scalar handler fixture;
G's #2548 scatter terminal-snapshot fixture is not modified here.

The permanent repaired schedule now waits for the real SQLite INSERT BUSY and
physical rollback before releasing the first writer. Its contender consumes the
unchanged native RunTransaction owner, not a fixture retry loop. Exact attempts:
first SELECT0/INSERT1, second SELECT1/INSERT1; writer plus contender totals
SELECT2/INSERT3 before final inspection. PostgreSQL observes pg_blocking_pids,
then commits the writer; its contender has one SELECT1/INSERT1 attempt, total
SELECT2/INSERT2. Existing equality/conflict, physical rollback, canonical
post-trigger fact and COUNT1 assertions remain exact. Cleanup cancels, releases
and joins the contender on failures. No busy-timeout/deadline or production
retry policy changes. The native BUSY-at-COMMIT control is unchanged.

Permanent `TestHandlerSelectionCanonicalReadSQLiteHeldRawWriter` retains the
uncommitted writer until the real contender returns BUSY; this is the negative
schedule control, not a simulated driver error.

Managed composed command:
`go run ./cmd/swarm-test -- -json -race -count=3 -run
'^(TestSelectedConstructedActorProjectionCompleteCensus|TestSelectedConstructedActorProjectionRejects|TestSelectedConstructedAgentInputExecutesBothStores|TestSelectedNestedConstructedAgentInputExecutesBothStores|TestAdmitSelectedContractRouteHistory|TestSelectedAgentConstruction|TestSelectedReadinessRequiresCompleteConstruction|TestHandlerSelectionCanonicalRead)'
./internal/runtime/runforkreadiness ./internal/runtime/runforkadmission
./internal/runtime/runforkexecution ./internal/store/internal/backend/delivery`.

Delivery PASS20.092s: all five handler roots in all three repetitions, including
both-store equality/conflict, corruption/rollback, physical SQL counts, held
raw writer and unchanged COMMIT retry. Receipt:
`/tmp/agent-e-2496-c22-final-independent-race.log`. The composed command was RED
in readiness on an incomplete positive component header; its separate fixture
repair passes race3 in `/tmp/agent-e-2496-c22-direct-header-race-three.log`.
Neither passing subset clears the original default RED or establishes whole
candidate qualification. Native readiness survivor rebind remains separately
blocked on the exact grant/retirement disposition in comment5970534024.
