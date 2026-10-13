# Served delivery-status whole dependency: accounting hold

Part of #2542 / #2151. Signed predecessor
`5e6f4477a1a401df2c6227f617dd97eeba6449e8` (approved for local stacking).
No push, rebase, PR, tier or server2. Standing bulk gate6134544118.
This is a semantic/source-qualified candidate, **not ratchet-qualified**.

## Owners and exact class

The chosen class is the raw served event/recipient/status cardinality counter
and its entire polling/wait/no-dispatch-window chain. The canonical owner
already exists: private delivery `ReadServedDeliveryStatusCount`, its original
selected read transaction in `ReadServedDeliveryStatusCountForTest`, and the
existing storetest forwarder. Their implementations and existing hostile/native
proofs are byte-for-byte unchanged. No new query or delegation layer.

Binding context: `platform-spec.yaml:platform_tables.tables.event_deliveries`
and the issue's original physical oracle. Optional subscriber type is omitted
only when originally blank; nonblank type/ID/status values are never trimmed
or reclassified. Multiple statuses retain the original **AND**, not OR/IN.
Every matching route row is counted, not deduplicated by subscriber. These
observations are not work selectors, claim authority or lifecycle inference.
No production or platform contract changes.

Whole inventory: **28 calls / 14 declarations / four helpers**, all migrated
mechanically from the exact predecessor through the existing finite corpus.
Counter, ordinary wait, run-aware wait and blocked-window helper no longer
accept SQL handles. Run-aware callers retain their existing exact native reader
and remove only the unused DB argument; other callers consume the originally
present reader/ReceiverStateReader. Every non-reader argument, assertion,
deadline, sleep, held window, debug point, teardown and readiness cut remains.

The old counter/query construction and four DB parameters are invalid/removed.
Every owned caller is enumerated, with alias/initializer escape detection;
complete declaration pins reject changes to any assertion or lifetime cut.
All touched consumers consume the existing owner. Receipt-outcome and delivery
reason/status-row queries are separate surviving predicates, retained parent
dependency work, not a substitute for this counter. The broader raw-authority
parent remains open; this is not aggregate closure.

## Source and bookkeeping

New source manifest SHA256:
`b4284bb05b933c6ac8bb151de9355e3f5bd28a6ff363b2f926d9da5a8b0042d4`.
Newest exact inverse precedes run-scalar/request-key/debug inverses, preserving
their immutable manifests and never rewinding newer source. Read-only generators
retain 28/14, 9/6 and 39/28 inventories and their exact digests. Older seven
generators pass unchanged.

Corpus: **1475 recipes / 779 transitions**, exactly fourteen affected rows;
no historical Before or rows dropped. Transition digest:
`e8428a49dd11d6d4df7b976ddea5b9b1adc7b9386516cf989ce905c6246ab2fd`.
One existing reviewed join/timer Successor is mechanically extended: its
historical Before/After remain, and the complete prior Successor is pinned as
Before in the new manifest. E's reserved Manager eight and named
runLifecycleRootForkPolicy row are unchanged.

Exact source-composition policy fingerprint candidate:
`e6e1e0b19a6a9f9710193d07ed508d3c6a69959bbcf73b8636ec602506e41a44`.
Directional/source/sidecar controls pass; no classifier narrowing, baseline
increase, fabricated removal or broad exception has been introduced.

**Real ratchet remains RED**: stored baseline7013; typed actual6979 (34 fewer).
The actual-to-head scan reports no new sites, but seven head-to-trusted-base
identities fail because the existing served-debug comparison requires its
successor raw occurrence to remain live before normalizing the unlanded pair.
This wave removes that raw capability entirely. The baseline and sidecar are
left unchanged, with zero claimed deletion credit pending an exact ruling.

| Conserved forward pair now native-only | Multiplicity |
| --- | --- |
| TestRunServeRuntimeJoinFailureReachesAPIAndCLI | 2 |
| TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation | 3 |
| publishServedLiveAgentHoldEvent | 1 |
| runServedAgentRestartLifecycleProof | 2 |
| runServedLiveAgentEventReplayLifecycleProof | 2 |
| runServedRunControlLifecycleProof | 2 |
| startServedSessionCleanupProof | 1 |

All seven use waitServedEventPublishDeliveryStatusCountForRun: the historical
raw DB -> raw DB plus native reader pair was conserved 1->1, and is now a
native-only call with no raw parameter/query. Exact file, declaration,
operation, old/current resolved signatures, owner/family and multiplicities
are already recorded by the existing pair model. No new raw identity replaces
them. Proposed accounting repair, **not implemented**: authorize retirement
of these exact seven successors after the verified native rewrite, so the
unrefreshed head can compare to its still-old trusted base. Reject reverse,
resurrection, coexistence, duplication, foreign coordinates, missing donors
and unrelated additions; do not broaden all49 pairs or change the scanner.

## Proof receipts

Cache `/home/youmew/.cache/swarm-2542-family145/delivery-status-*`.
Go1.26.8/GOMAXPROCS=2/-p=1/-count=1; runtime/native executions use -race.

| Manifestation | Status | Actual proof |
| --- | --- | --- |
| Exact status conjunction, optional type, padded inputs, route multiplicity and original SQL-oracle equivalence | execution-proven through the same corrected path | unchanged owner roots in existing-owner-race.jsonl: two roots/36 passes, both stores, 26.755s |
| Raw/nil/cancelled/closed/unavailable owner refusal | execution-proven through the same corrected path | second root in that same receipt; errors are not successful zero |
| Join failure/refusal, active-load and exact target waits | execution-proven through the same corrected path | served-whole-race.jsonl, unchanged target/active-load cells on both stores plus both existing PostgreSQL join roots |
| Blocked paused-run interval, resumed work and delivery settlement | execution-proven through the same corrected path | same receipt's both-store RunControlLifecycle cells; original250ms windows, poll cadence and assertions preserved |
| Dynamic auto-emit, agent replay/restart/directive and cleanup/nuke writer join | execution-proven through the same corrected path | same receipt's both-store served lifecycle cells plus two original PostgreSQL cleanup roots; total13 roots/31 passes, 110.496s, no skips/failures |
| Whole rule, polling statement equality, canonical owner immutability and hostile assertions/source cuts | execution-proven through the same corrected path | composed-guards.jsonl,17 roots/1502 passes,59.536s, including finite recipe replay and older generators |
| Exact fingerprint, old conserved transfers and new source composition | execution-proven through the same corrected path | accounting-controls-final.jsonl,28 roots/1418 passes,10.919s |
| Persistence/constructor/retirement boundaries | execution-proven through the same corrected path | structural-owner-guards.jsonl,81 roots/458 passes, all four packages green |
| Complete four-package candidate typing | execution-proven through the same corrected path | frozen-candidate-overlay.jsonl, original standalone root PASS51.883s |
| Seven unlanded conservation pairs now deleted, and parent completion | split / escalated as separate class | real ratchet70.281s RED as described; exact retirement ruling required, not waived; baseline unchanged |

U1000 and focused cognitive complexity pass. Exact signed complexity evidence
is measured after the checkpoint, not attributed to a working tree. Earlier
planner/required-child receipts carry: no root/unit/child/tier changes here.
No duplicate tier, integrated runtime matrix, timeout inflation or retry loop.

Development REDs: initial generator count expected12 rather than actual14
declarations (exact census corrected before writing); expected unpinned
fingerprint probe; hostile query mutation initially hit another pre-existing
canonical call rather than this counter (repaired to require exactly one full
pinned counter and mutate that body); real ratchet remains held. No runtime
failure is concealed by those record corrections.

## Closure and continuation

Semantic closure candidate: complete named delivery-status dependency on the
original domain owner; **accounting qualification held**. Parent baseline is
still7013 findings/4770 raw sites with67 unresolved excluded occurrences.
Typed6979 is not a committed decrease or aggregate closure certificate.
No new architecture, issue or watchlist node; existing #2542/#2151 covers the
accounting decision and the remaining footprint/constructor/lifetime work.

All previous SQLite public-fork, M01/Docker, D3/M07, drain/static-fork/deadline,
finite hosted-history and integrated holds remain. The new signed predecessor
must enter exact package-keyed historical provisioning before landing.
Preflight remains required on the eventual authorized clean/rebased push.
Rebase still once after #2617 and orchestrator handoff.
