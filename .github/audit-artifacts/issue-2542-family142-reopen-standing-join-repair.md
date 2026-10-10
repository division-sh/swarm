# #2542 Family 142: Joined Reopen Observation

Binding review: issue comment `6083975125`, F1/P2. This is a test-contract
repair, not a production delivery, transaction, or timing-policy change.
The existing spec's `executable_event_delivery_obligation_rows` sections
`process_local_coordinator_lifetime` and `bounded_execution_and_atomic_renewal`
require the continuation owner to join its workers before settling its standing
lease. Transient quiescence is not that join. No authoritative spec amendment
is needed for this test-only correction.

## Repair And Exact Owners

The original native reopen proof retains pending-receipt equality, exact source
and authority, a distinct successor occurrence/store, predecessor admission and
publication refusal, both live publication replays, one claim/version and one
outcome. After joining finite execution it verifies the original runtime
occurrence still owns standing recovery, calls the existing exact
`Continuations.Retire`, verifies that occurrence has zero active leases, and
only then observes global selected-store transaction idleness. Before/after
counter snapshots are each captured once and both printed on failure.

No delegating lease, alternate store, fabricated receipt, polling, retry,
deadline increase or new cleanup owner. Existing idempotent fixture cleanup
continues to join the same coordinator on assertion failure and at teardown.

The finite source-order control rejects loss of replay, finite join, standing
join, zero-active and claim-counter checks, plus a counter sample moved before
the join. It supplements execution proof; source shape alone is not closure.

## Sibling Observation Audit

- The admission transaction-cut fixture and direct native recipe replay never
  start recovery. Their original Active=0/1/0 and final Active=0 assertions stay.
- The four other `recoverNativePipelineRetryForTest` consumer families (authored
  rule retry, changed-state selection retry, preparation failure and node retry)
  assert exact durable status/claims/outcomes, not arbitrary global Active.
- Native lookup, missing-run, mutation, storage-identity, closed-owner,
  projection and handler component observers do not start a standing recovery
  scanner. Their stronger transaction checks remain unchanged. The delivery
  fixture registers coordinator retirement after the base collector check, so
  LIFO cleanup retires the standing owner before that check.

## Fresh Bounded Proof On Vemew

Receipt prefix: `~/.cache/swarm-2542-f1-`. Go 1.26.8, GOMAXPROCS=3,
count=1, no server2 load.

| Manifestation / control | Exact proof and result |
| --- | --- |
| Reopen observation and unchanged authority/cut/evidence/foreign-claim siblings | `native-focused-race.jsonl`: five original roots, 23 passing records, both stores, no skips/failures; 53.910s. |
| Standing join omitted, otherwise identical actual native test | `omitted-join-race.jsonl` and `omitted-join/overlay.json`: both stores FAIL at the new ordering assertion, active=1; 18.603s. Setup, replay and finite join completed. This is intentional hostile evidence, not an aggregate pass. |
| Admitted read remains leased until release and exact retirement completes | `standing-read-drain-race.jsonl`: six existing channel-gated wake/synchronize x page/standing/held retirement cells PASS under race; 1.027s. Unit proof, not falsely credited as native backend execution. |
| Finite source ordering, omission/early-sample controls, affected retirement and recipes | `retirement-recipes-final.jsonl`: eight roots PASS, no skips/failures; 1.265s. |

The first local compile exposed that the pipeline holds an Occurrence interface,
not its concrete runtime inspection methods. The test now checks and uses the
actual native RuntimeOccurrence; `reopen-race.jsonl` is build-failure evidence,
not a test pass. No original runtime behavior was changed to accommodate it.

## Rebase And Remaining Obligations

The rebase onto `2fc6a13bde271492602b7ca0ed82bfe3325280f8` already completed in
`0adca5911` before this review repair. Family 142 maps from `61d9b98ed` to
`5fbbd9047`; all 185 cohort commits survived. The backup branch and full
range-diff, compact conflict dispositions, regenerated TSV/collector sidecar
and finite snapshots, fresh ratchet, changed-owner race tests and planning
contracts are recorded in `issue-2542-rebase-2596.md`. Those unchanged receipts
are reused, not repeatedly rerun. G's cache/census/timer proofs and other
lanes' assertions remain preserved.

No raw-authority migration is credited to this bounded observation repair.
The recorded residual is **11,716 findings / 8,520 raw sites**, including all
67 excluded-source uncertainties. The two strict completion guards, zero-debt
finish, SQLite fork-deadline and final integrated qualification remain open.
M09's earlier pressure signature remains unclassified in #2353; no repair or
inheritance claim is added here. No new PR or issue.

Read-only ratchet/registry, native unused and exact-head complexity receipts
are `readonly-ratchet.jsonl`, `unused.log` and `complexity/`; these are required
before the checkpoint push. No core/full/hosted CI pass is claimed.
