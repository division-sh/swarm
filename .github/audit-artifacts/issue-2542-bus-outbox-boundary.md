# Native bus outbox: implementation boundary report

Track: FULL. Approval: #2542 comment6190293197 at a525874ba4.
Boundary report: #2542 comment6190640156.

## Status

This is a signed local diagnostic checkpoint, NOT family acceptance, completion,
or review readiness. The approved class still includes all 25 original roots.
Eleven descriptor-only roots have been propagated mechanically; the fake durable
owner and the remaining semantic callers still survive. Pure recording-event
neighbors remain nonpersistent, unchanged, and unclaimed.

The named component operation delegates to the existing selected workflow state
projection, EventBus preparation, native engine mutation commit, ordered result
validation and finalization. It returns actual committed evidence, never SQL or
a transaction callback. The source is compiled and admitted, and state is
constructed through the existing lifecycle/activation fixture. No preseeded event
supplies publication evidence. The local native bus fixture owns and joins its
process/runtime occurrence and cleans pending publication claims on failure.

## Newly reproduced boundary

The native planner acquires a distinct publication claim for every intent,
including repeated event IDs inside one batch. Its second claim fails busy
before the selected transaction starts. Both ordinary and prospective engine
preparation reach this same implementation. This conflicts with the existing
platform-spec.yaml transactional_batch_handoff contract: ordered same-ID
duplicates dispatch once, all-duplicate batches dispatch zero, conflicting
same-ID facts roll back, and distinct events each dispatch once.

No production planner, claim, transaction or dispatch behavior was changed.
The bounded amendment request is on #2542; no fake claimant, bypass, early
release, outcome deduplication or compatibility repair has been introduced.
The old separate-operation replay-before-dispatch case is distinguished from
this defect: busy there correctly preserves the live publication claim.

## Receipts

Evidence directory: /home/youmew/.cache/swarm-2542-family145/.

- bus-outbox-native-claim-boundary-race.jsonl: one focused native run,
  GOMAXPROCS=2, race, p=1, count=1, wrapper exit1. Native single publication and
  distinct ordered publication pass on SQLite/PostgreSQL. The same-ID batch
  fails with second-claim busy on BOTH stores, not an infrastructure failure.
  SHA256 62e34e6b75f49eba20a25847c338b44726f60e35dce13f0c3ae795ae6dd21a1a.
- bus-outbox-mechanical-sqlite.jsonl: seven additional descriptor/refusal/readiness
  cells pass. The preexisting all-duplicate and conflicting-ID batch cells fail
  at that same claim gate. Aggregate exit1 is retained honestly.
  SHA256 fb0df42edb531b29e4e04807ee411e12307c8d890985d2ba600437bb7d30737d.
- bus-outbox-fixture-joined.log: after adding explicit owned teardown, the
  native single-publication and distinct-publication SQLite cells pass, as do
  the finite transform's oracle/replay and semantic-field refusal controls.
- bus-outbox-prototype-vet.log: affected tool, bus and pipeline vet passes with
  composites=false, retaining the existing unkeyed-literal disposition.
- Pinned cognitive-complexity scan: all new functions are below30. Whitespace
  validation passes. No exhaustive tier or per-wave history rerun was performed.

The first joined command selected no bus tests; it is NOT proof. The corrected
verbose receipt above explicitly records both SQLite cells and their parents.
The both-store race receipt predates the subsequent teardown-only fixture edit;
it is boundary evidence, not a claim of final-head family qualification.

## Accounting and next action

No census or baseline was refreshed and no removals are claimed. Accepted
remaining stays4371 findings /2555 raw, qualification-adjusted; physical prior
remaining stays4177/2400. All prior194/155 held work remains uncredited.
Standing/poison qualification and the two pre-batch history defects remain
unwaived and separate. No rebase, non-backup push, PR or hosted run is authorized
by this diagnostic checkpoint. Once the production boundary is ruled, finish
the entire caller chain and compose the one combined guard/census/real-ratchet
checkpoint. Exact-head preflight is required before any non-backup push.
