# Channel run/name count bulk wave

Part of #2542 / #2151. Standing finite bulk gate; signed predecessor
c3e48876761af8b84d8398b49c19dd1cf5002add, approved for stacking.
Local only; no push, rebase, PR, tier, server2 or landing qualification.

## Chosen Class And Owners

The complete channel-journey family of15 literal run/name event-count queries
across11 declarations/10 files now consumes existing
eventrecord.CountRunNamedEventStorageTx through its unchanged private bridge
and the original selected read-only channel inspector. No SQL, query policy,
backend switch, new transaction owner or scalar reconstruction is introduced.
The new method only exposes that existing exact data observation on the
borrowed ChannelAnchorObservation facet; it still has no Close method.

Every physical event row counts under exact issued run and event-name bytes,
including duplicates, without header/status/eligibility filters or trimming.
Each original QueryRow/Scan cut becomes one native observation at that same
statement. The finite AST transform retains the original scalar destination,
error scope, conditional/else body and subsequent destination uses; it does
not shadow the destination or replace assertions. Diagnostic standalone error
assignments retain their original scope as well. Cancellation, failed owner,
storage or snapshot settlement returns no provisional count.

Systematic consumption is exhaustive for this declared channel family: all15
queries disappear, their11 complete bodies match the committed transformation,
and every corresponding call reaches the single existing eventrecord owner.
Source/card/receipt joins, source-event lineage counts, status-only witnesses,
whole snapshots, fault/setup/openers and other caller contexts remain different
physical cuts or tracked parent debt, not alternative implementations silently
credited to this wave. No execution or writer authority crosses the facet.
Authoritative channel_run_named_event_observation records that exact boundary.

## Finite Transform And Negative Controls

migrate-channel-event-count/internal/channeleventcount, full output and immutable
preimages are committed together. Manifest
0ae312b38187870818561ce67f894777d362156c34b5181162774b817497db39;
15 calls/11 complete bodies, each independently checked against c3e488767.
Owned-source inventory requires zero old queries and exact15 current calls.
Foreign carrier, destination/arity, unknown consumer, altered read context,
run/name/count/assertion/poll/lifetime, foreign complete keys and source drift
are rejected. The outer-destination control requires assignment, not a new
shadowing declaration, and preserves the later consume(count) use.

Older anchor/decision/intent and historical generator/caller proofs compose
this newest exact inverse first. No earlier manifest/Before or reserved E
entry changes. Existing1496 recipes/800 transitions remain, with11 exact
consumer updates and no added/dropped rows. Transition digest
99b6e841e91ca0590e96320484c64139443105345231e36b1bf6a378491dd280.
The actual collector stays64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44.

## Actual Qualification

Go1.26.8/GOMAXPROCS2/-p1/count1. Receipts
~/.cache/swarm-2542-family145/channel-event-count-*; native/served use race.

- public-owner-race PASS35.301s, one root/five records, both native stores:
  two genuinely published equal-name events count2; foreign run, case/space
  variants and empty name count0; cancelled/parent-closed inspection refuses
  with count0. Borrowed facet cannot close the joined parent-owned reader.
- unchanged canonical row-owner proof PASS14.070s, one root/three records,
  both stores: duplicate rows, sibling/missing run, exact names, original
  selected read counters, foreign/invalid/cancelled/closed owner controls.
- representative-race PASS271.058s, three roots/nine records, six backend
  cells: launched acknowledgment process death, stop-supersedes temporal
  refusal and applied-after-completion uncertainty. All assertions retained.
- corpus-controls PASS91.914s,35 roots/1541 records, including complete finite
  idempotence, all historical generator inventories and prior composed source,
  admission/fork/entity-conflict/scalar controls; no failures/skips.
- complete structural/ownership/retirement/boundary guards82 roots/459 records
  across four packages pass. U1000 and all new-callable cognition below30 pass.
- real downward ratchet PASS43.165s:6841 ->6811 findings /4615 ->4585 raw
  sites,30 removals/zero additions. All67 excluded occurrences and historical
  header retained. Ordinary read-only ratchet PASS31.143s, same collector.

One development RED was meaningful: the new scope/lifetime proof initially
discarded a raw database returned by the older startChannelAnchorJourney;
the real ratchet refused the new escape before writing either baseline or
metadata (61.520s). Replacing it with existing public-only
startChannelAnchorPublicJourney fixed construction, not the classifier. Both
native proof and actual ratchet were rerun after that exact repair. No waiver,
offsetting credit, compatibility constructor, retry/skip or timeout change.
The inventory control's initial cognitive32 was reduced by extracting its
file/call counting without weakening any source or occurrence checks.

## Manifestation Coverage

For mechanically converted rows, the exact per-file/function complete-body
proof is TestFamily145ChannelEventCountWholeRuleRetainsEveryScopeAndAssertion;
the corrected execution path is CountRunNamedEventStorage on the borrowed
channel inspector, execution-proven by the actual both-store public-owner
root above, plus its unchanged duplicate/physical/counter row-owner proof.
These combined proofs preserve each named reader/run/name/destination/cut,
not an inference that a shared owner alone makes different workloads pass.

| Known manifestation | Status | Exact proof |
| --- | --- | --- |
| Acknowledgment abrupt death, two queries | reproduced and fixed | launched both-store race cells plus complete2-cut preimage/output proof |
| Draft terminal restart | execution-proven through the same corrected path | exact complete TestChannelDraftTerminalRestartPublicJourney output plus both-store physical-scope/lifetime and row-owner execution proofs |
| Delivery abrupt death | execution-proven through the same corrected path | exact complete TestChannelDeliveryAbruptProcessDeathPublicJourney output plus both-store physical-scope/lifetime and row-owner execution proofs |
| Human temporal refusal | reproduced and fixed | stop_supersedes both-store race cells plus exact complete original temporal assertions |
| Uncertain-copy completion | reproduced and fixed | applied_after_completion both-store race cells, original count1/copy/uncertainty assertions |
| Learned-object anchor producers | execution-proven through the same corrected path | exact complete TestChannelLearnedObjectRealAnchorProducersPublicJourney output plus both-store physical-scope/lifetime and row-owner execution proofs |
| Learned-object public decision | execution-proven through the same corrected path | exact complete TestChannelLearnedObjectPublicJourney output plus both-store physical-scope/lifetime and row-owner execution proofs |
| Learned-object recovery helper | execution-proven through the same corrected path | exact complete proveObjectChannelRecovery output, unchanged post-crash/recovery/copy assertions, plus both-store physical-scope/lifetime and row-owner execution proofs |
| Learned-object human temporal refusal | execution-proven through the same corrected path | exact complete TestChannelLearnedObjectHumanTemporalReceiptPublicJourney output plus both-store physical-scope/lifetime and row-owner execution proofs |
| Selected-fork input refusal | execution-proven through the same corrected path | exact complete TestChannelSelectedForkInputBoundaryMatrix output retains all unsupported-input/sibling assertions; both-store physical-scope/lifetime and row-owner execution proofs |
| Source lifecycle, four cuts | execution-proven through the same corrected path | exact four-cut TestChannelSourceLifecyclePublicJourney output retains source/parent/refusal/replay assertions; both-store physical-scope/lifetime and row-owner execution proofs |
| Invalid/cancelled/closed owner and altered context/count/namespace | execution-proven through the same corrected path | public and private both-store refusal execution, TransformRefusesForeignBindingAndKeepsOuterDestination and complete-source hostile controls |
| Legacy raw-returning setup in new proof | reproduced and fixed | actual ratchet RED before repair, public-only construction plus genuine-event/lifetime race proof and actual ratchet GREEN after repair |
| Wider SQLite public-fork/runtime reachability and parent authority tail | split / escalated as separate class | Existing #2542/#2151 holds remain; no new claim of source-fork qualification from a parametric count observation |

## Closure And Tracking

Achieved closure is the named query dependency canonicalized across all15
physical cuts, not parent failure-class elimination or full execution of every
unchanged journey. No all-journey/integrated qualification is claimed; named
representative executions plus the complete finite transform are this bulk
gate's local proof. Parent debt remains6811; setup/lifetime/fault/transaction
and other observation families remain, so its total tail estimate is unchanged.
The issue is broad, not symptom-shaped. Watchlist: existing #2542/#2151 node
and source accounting remain sufficient; no new issue, node or framework.
Architecture ROI is removing30 raw operations without another query owner or
constructor; no measured flake or performance improvement claim. Immutable
history provisioning, overlay/integrated/full qualification and all original
runtime/fork/Docker/M01/D3/M07 holds remain. Preflight is required before any
eventual clean/rebased authorized push, and rebase waits for #2617 as directed.
