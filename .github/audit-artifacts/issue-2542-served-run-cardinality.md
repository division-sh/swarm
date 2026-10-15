# Served run-cardinality dependency checkpoint

Part of #2542 / #2151; local stacking only. Signed predecessor
`c42d7ac7c3b3aa07affa1489021c1ab3e2685849`. Standing bulk gate6134544118;
request-key receipt carry and scope correction6145735720. No push, rebase,
PR, tier, or server2 run. No production runtime or platform contract changes.

## Boundary and systematic consumption

The chosen class is the complete served run-scalar counter dependency:
physical run identity, non-platform events for the exact run, and deliveries
for the exact run/event. These are physical assertions, not lifecycle status
or authority reconstruction. Binding references are
`platform-spec.yaml:platform_tables.tables.runs`, `.events`, and
`.event_deliveries`, plus the issue's selected-owner boundary and original
physical queries. `event_count` on runs is not substituted for event storage.

| Concept | Canonical owner and touched consumption |
| --- | --- |
| Physical run-row count | existing run-lifecycle owner, `CountRunIdentityStorageTx`; both pre-existing API and notify observation bridges now consume that one query, preserving their validation, read transaction, errors and signatures; served counter uses the existing storetest forwarder |
| Non-platform events by run | existing event-record owner, `CountApplicationRunEventStorageTx`; bridge stays in existing `test_event_support.go`; narrow storetest count and served reader consume it |
| Exact run/event delivery count | existing delivery owner, `CountExactRunEventDeliveryStorageTx`; original selected read transaction, narrow storetest forwarder, served reader |
| Complete helper/caller rewrite | finite `runscalar` rule and source manifest; four complete callers, the canonical counter and retirement of its redundant alias |
| Receipt/source identity | existing recipe, inverse, collector and sidecar owners; exact new inverse precedes request-key/debug inverses; no occurrence transfer or deletion payment |

The manifest exhaustively covers **nine predecessor calls / six declarations**.
Final inventory is **eight calls / one counter**, with the alias completely
absent. Follow-up's four calls, active-load's two calls, and both PostgreSQL
fault/restart calls retain every non-reader argument, assertion and temporal
cut. The two PostgreSQL witnesses use the successor's already captured reader
only after successor readiness. Their independent observers, server/proxy
faults, exact authority, one-commit and cleanup assertions remain unchanged.

The old raw counter, alias and their DB parameters are invalid/removed.
The unused `events_by_run` branch had zero callers and is deleted, not kept as
compatibility. All three used scopes retain their exact physical predicates;
no status, actor, subscriber or retention filtering is added.

Query-level sibling probe: the initialization witness still has the same
non-platform event predicate; reset's three run-row counts have the same
physical identity predicate within their reset-lifetime flow. They remain
counted parent dependency work, not different concepts. Delivery status and
fork-origin counts are genuinely different predicates. This closes the named
shared dependency, **not every run/event cardinality interpreter**. The three
retained request-key manifestations in the prior audit also remain open.

## Exact source and accounting

Manifest `family145-run-scalar-preimages.json` SHA256:
`441c152d124c3f218fe1577a3962b8b7bc153919bec0121ad89a48a904dfddca`.
Read-only original request-key generator retains its 39/28 inventory and
unchanged `cb7b37f...` digest. All seven older generator manifests are unchanged.
The new inverse never rewinds newer source and accepts only exact path,
declaration and complete body. Historical Before values and E's reserved
Successors remain untouched. Corpus: **1472 recipes / 776 transitions**, six
rows changed, none dropped; transition digest:
`3c52f0abba6d3172b7d740dc76ba331861bb87a5f783e645b8ed78dd0ae3e6f9`.

Frozen real ratchet: **7026 / 4781 -> 7013 / 4770**, **13 findings / 11 raw
sites removed, zero added**; all 67 unresolved excluded occurrences remain.
All 71 conserved served-debug occurrences and the two retained request-key
1->1 occurrences remain. No new signature relocation, classifier relaxation
or scope exception. Collector/source composition rotates exactly from the
request-key fingerprint to
`3db22e07dfc4097879c83e6f0645d8ce8d0e4349e0305c186d5be86f6fce44fc`.
Controls reject other origins/destinations, reverse transitions, changed
identity/multiplicity, additions, source/capture/assertion corruption and
missing manifests. The historical TSV header and bootstrap remain unchanged.
Baseline SHA256:
`34194e4033ef836a007c7b4365041918cee5edf05a36e49ec025e68c0c0fd36f`.

Registry refreshed through its existing owner without blanket classification.
Its completion guard stays RED on unclassified parent work; added private
canonical-port evidence is not external debt and earns no removal credit.

## Actual proof

Receipts: `/home/youmew/.cache/swarm-2542-family145/run-scalar-*`.
Go1.26.8, GOMAXPROCS=2, -p=1, -count=1; native/runtime executions use -race.

| Manifestation | Status | Exact execution proof |
| --- | --- | --- |
| All three exact predicates, cross-run/event exclusion, platform filtering, physical-oracle equality and read-only counters | execution-proven through the same corrected path | `native-cardinality-final-race.jsonl`, two roots/six passes, both stores, 14.338s; twelve read commits, zero writes, no active lease |
| Raw, incomplete, nil, cancelled and closed owner refusal | execution-proven through the same corrected path | second native root in that receipt; errors never become successful zero |
| Both existing API/notify bridge behaviors | execution-proven through the same corrected path | `shared-owner-regression-race.jsonl`, four roots/12 passes, both stores, 35.980s; exact function-body source control changes only the query delegation |
| Served follow-up run/delivery cardinality and target readback | execution-proven through the same corrected path | `served-fault-restart-race.jsonl`, SQLite follow-up and both-store target roots complete; skipped outage cells in that initial receipt are NOT evidence |
| Active held agent, durable ACK before release, exact delivery cardinality and later settlement | execution-proven through the same corrected path | `active-load-race.jsonl`, two served roots, SQLite and PostgreSQL, PASS19.815s; original held-work/release and lifecycle assertions preserved |
| PostgreSQL real connection/server loss, successor readiness and continued exact durable identity | execution-proven through the same corrected path | `postgres-real-fault-restart-race.jsonl`, both loss children, independent fsync-enabled disposable PostgreSQL16, PASS48.949s including the lost-ack root |
| PostgreSQL lost commit response, exact same-key replay and one committed publication | execution-proven through the same corrected path | same receipt's `TestServePostgresLostCommitResponseRecoversDurablePublication`; all assertions/faults retained, zero skips |
| Complete transform and old receipt composition | execution-proven through the same corrected path | `composed-guards-final.jsonl`, 13 roots/1495 passes, 58.693s; `hostile-controls-final.jsonl`, four roots, 0.702s; both read-only generators pass |
| Debt source/collector and conserved prior transfers | execution-proven through the same corrected path | `policy-accounting-final.jsonl`, 26 roots/1407 passes, 7.551s; frozen real ratchet PASS288.216s |
| Persistence/constructor/retirement boundaries | execution-proven through the same corrected path | `structural-owner-guards.jsonl`, existing 81 roots/458 passes, all four packages pass |
| Required backend children, ownership partitions and inventory | execution-proven through the same corrected path | `planner-partition-final.jsonl`, 33 roots/304 passes across planner/timing/catalog; new native roots require both backend children in their existing unit |
| Earlier gate-schema refusal control missing from fixed-size conformance inventory | reproduced and fixed | `parent-conformance-partition.jsonl` reproduces 169-vs168 at unchanged signed predecessor; final partition receipt accounts exactly for the existing reviewed root by name, preserving all other groups and assertions |
| Complete four-package candidate typing | execution-proven through the same corrected path | `frozen-candidate-overlay.jsonl`, standalone original root PASS204.536s at unchanged default timeout |
| Remaining semantic siblings and parent completion/qualification | split / escalated as separate class | retained under #2542/#2151/#2353; not waived or claimed closed |

Final U1000 is green; no new unused owner layers. Complexity measurement uses
signed revisions, with its receipt posted at the checkpoint. The sidecar
branch extraction lowers its previous cognitive score, with unchanged rules.

Disclosed development REDs: wrong native test import/type (repaired before the
native receipt); initial static SQL expectation used wrong spelling (corrected
to the exact original query); initial recipe guard had the previous count
(corrected to parsed1472); expected unpinned collector probe; inherited
conformance inventory failure above. The initial outage skips were followed by
real fault executions with TEST_POSTGRES_BIN set. No retries, timeout inflation,
coverage loss, guard waiver or blind repeated broad runtime runs.

## Parent and landing disposition

Achieved closure: **the complete named served run-scalar dependency
canonicalized**, including its two pre-existing physical run-query owners.
Parent raw authority remains open at 7013 findings/4770 raw sites. Existing
watchlist/tracker mapping remains sufficient; no new framework, issue or
untracked child tail. Removing the two duplicate queries is the minimal
architecture repair; future callers use the same existing domain owners.

SQLite public-fork, M01/Docker, D3/M07, drain/static-fork/deadline and integrated
qualification holds remain. Finite hosted historical provisioning must add
this signed predecessor to the existing package-keyed exact inputs before
landing, with depth-one RED/GREEN proof; no full unshallow. Preflight remains
required before the eventual authorized clean/rebased push, not tier credit
on this held base. Rebase still once after #2617 and orchestrator handoff.
