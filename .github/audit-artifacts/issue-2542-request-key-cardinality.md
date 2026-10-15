# Request-key cardinality whole-family checkpoint

Part of #2542 / #2151. Local stacking only; not landing or parent closure.
Signed predecessor: `80d42e2aba875393cae592e3b3f7a37f6af3d892`.
Binding accounting ruling: issue comment `6144577451`; standing bulk gate:
`6134544118`. No push, rebase, PR, tier, or server2 run in this checkpoint.

## Semantic boundary and consumption

The working class is the raw served **method + idempotency-key physical
cardinality** interpreter and its complete caller chain. It counts all actor
namespaces, without expiry deletion, actor filtering, response reconstruction,
or key normalization. This is deliberately not the actor-scoped replay lookup.
Binding context is `platform-spec.yaml:database_schema.api_idempotency`
(exact method/actor/key primary key), the issue's selected-owner migration,
and the original physical queries/assertions. No production execution,
admission, idempotency, lifetime, schema, or platform architecture rule changes.

Canonical owners after the change:

| Concept | Owner | Complete touched consumption |
| --- | --- | --- |
| API method/key physical count | existing private `apiidempotency` owner, `ReadMethodKeyCardinalityTx` | selected `ReadAPIIdempotencyCardinalityForTest` read transaction; storetest forwarder; served API counter; 22 control-counter calls; eight original direct API calls, including the lifted rollback assertions |
| Event idempotency-key physical count | existing event-record owner / `ReadEventIdempotencyCardinality` | all three served publication counter calls, through the original reader's `InspectSnapshot` |
| Source-event physical count | existing event-record owner / `CountSourceEventPublications` | the two calls in the sole replay-refusal helper; its two dynamic-auto-emit callers now pass their original native reader |
| Live native read-owner capture | existing serve composition capture hook | live-agent setup, general control setup, and each stop-rig process start; both backend results retain their exact process's reader |
| Exact debt occurrence identity | existing ratchet/source-pin/sidecar owners | precisely the two approved retained rollback-helper calls; no other identity transfer |

The owned-source inventory has **40 final direct calls / six helpers**, including
the two lifted API-zero calls. The guarded transform covers **39 predecessor
calls / 28 declarations**: 25 observation bodies and three minimal fixture
capture repairs. All definitions/callers are source-pinned; aliases,
initializers and unknown callers fail. The old three SQL counter bodies, the
raw API-counter/control/replay arguments and their DB parameters are gone.

Method-only, global, resource-ID and other physical predicates are distinct
surviving parent work. The seven other rollback queries are intentionally
retained with their original DB carrier and faults. They are not compatibility
paths for the migrated cardinality concept. No other same-predicate reader was
found in that complete owned helper-reference census. This is NOT a complete
query-level census of the predicate: reviewer-b found three retained sibling
interpreters, explicitly tracked below under the staged parent migration.

| Retained same-predicate manifestation | Binding disposition |
| --- | --- |
| `servedConversationForkRequestArtifactCounts` | migrate the complete four-count footprint, retaining refusal/no-mutation cuts |
| `root_publication_ack_loss_test.go` | preserve lost ACK, same-key replay, instance/event conservation and one commit |
| `TestServePostgresGracefulContract` | after-close independent observation; never substitute the closed serving reader |

All three remain counted in #2542/#2151. Reviewer ruling6145735720 retains them
in the approved parent stream without requiring immediate expansion of this
signed shared-helper checkpoint. Query-level residual probing supplements,
rather than replaces, the helper-reference inventory in subsequent waves.

## Accounting and immutable evidence

Exactly two `requirePublicInputRollbackNoResidue` calls transfer one way,
**1 -> 1 each, zero deletion credit**. Only the now-unused idempotency-key
parameter/argument is removed. File, declaration, operation, DB carrier,
lifecycle family, replacement-owner text, all other arguments and the seven
physical queries stay unchanged. The API-zero assertion is the immediately
following statement at both callers, before any fault teardown or cleanup.
Controls reject reverse/resurrection/coexistence, duplication, foreign fields,
missing donors, cross-caller substitutions and unrelated additions despite
lower totals. Historical collector policies and all 71 served-debug retained
occurrences remain conserved.

Manifest: `family145-request-key-preimages.json`, SHA256
`cb7b37f1c09039dff3e2c4b92e9288e25ab6001ee1bbe65225396e91773a5e1c`.
The original 28 predecessor bodies/arguments/assertions are verified against
the signed source. Historical companion/debug inverses consume only exact
new bodies, never overwrite newer source, and never change old manifests.
The final corpus has **1470 recipes / 774 transitions**, transition SHA256
`54b43fd056fb7f6531b559eb7a7985f08472697c4f26564578f1faea9954eafe`.
Only these 28 recipe rows changed; existing unrelated rows and E's reserved
Successors are retained.

Debt: **7074 / 4824 -> 7026 / 4781**: **48 findings / 43 raw-operation sites
removed, zero added**, with both retained raw calls still multiplicity one.
All **67 unresolved excluded occurrences** remain. Final collector:
`fd2d3745a56a6adaa06c3c434416d2cbb692e41f54bb0d259248e4931c2b59e8`.
Baseline SHA256: `6f30c27d422ce37a67006140fa5d05593a94700ced652adcac39c55e5c50ceae`.

The finding registry was refreshed through its existing generator, without
blanket classification or an exemption. Its completion root remains RED on
unclassified parent work. The refresh also enumerates previously unrefreshed
private-port evidence from earlier stacked waves; those rows are not 2171 new
external raw escapes and do not get closure credit. Ratchet counts above are
the bounded external authority debt, not the entire 54k internal census.

## Manifestation proof

Receipts below are in `/home/youmew/.cache/swarm-2542-family145/`.
All runtime/owner executions use Go1.26.8, `GOMAXPROCS=2`, `-p=1`, `-count=1`;
the runtime/owner receipts use `-race`. No assertion/retry/skip/timeout waiver.

| Manifestation | Status | Exact proof actually run |
| --- | --- | --- |
| Exact method/key, all actors, separate methods/keys, padded inputs, original physical count equality and read-only transactions | execution-proven through the same corrected path | `request-key-owner-final-race.jsonl`: two native roots / six pass records, both stores, 13.762s |
| Invalid/raw/incomplete/cancelled/closed owner refusal | execution-proven through the same corrected path | the second native root in that same receipt; no successful zero on error |
| Root-connected publication rollback and late replay-scope/API-completion cuts | execution-proven through the same corrected path | `request-key-rollback-bothstores-race.jsonl`: two roots / eight passes, both stores, all three existing fault stages, 40.442s |
| Publication target/active-load refusal, event-key zero and source-event replay no-mutation | execution-proven through the same corrected path | `request-key-served-publication-race.jsonl`: seven existing roots, 100.559s; SQLite follow-up plus both-store target/load/dynamic surfaces |
| Control/replay/restart/directive/ingress/mailbox/setup API counts | execution-proven through the same corrected path | `request-key-served-control-final-race.jsonl`: seven roots / 29 passes, both stores, 84.645s |
| Missing read owner in shared live-agent/control setup | reproduced and fixed | initial live-agent receipt panics before the counter; the previous row's final both-store race receipt passes; exact capture/source hostile controls |
| Missing read owner in fan-out stop/restart rig | reproduced and fixed | initial negative-admission receipt panics before the counter; `request-key-stop-emitter-native-final-race.jsonl`: three roots / 15 passes, both-store real fault and stop/restart cases, 87.647s |
| Deferral refusal counts | execution-proven through the same corrected path | the two deferral roots completed on both stores before the disclosed stop-rig panic in `request-key-served-negative-admission-race.jsonl`; their unchanged receipts carry |
| Receiver initialization conflicts via public process inspection | execution-proven through the same corrected path | `request-key-public-process-readback-race.jsonl`: actual race-built verify/serve subprocess, both stores, connected-typed-creation cases, 23 passes / 45.631s |
| Whole transform, old receipt composition, source identity and hostile assertions/cuts | execution-proven through the same corrected path | `request-key-whole-frozen-composed-guards.jsonl`: 12 roots / 22 passes, 90.546s; all seven read-only historical generators included |
| Two conserved transfers, source pins, collector direction and old policy compositions | execution-proven through the same corrected path | `request-key-complexity-repair-accounting-controls.jsonl`: 20 roots / 1362 passes, 5.288s |
| Required both-store execution selection / partition conservation | execution-proven through the same corrected path | `request-key-planner-guards.jsonl`: nine roots / 63 passes, 52.243s; two new native roots get SQLite/Postgres required children in their existing unit |
| Complete persistence/construction/retirement structural boundaries | execution-proven through the same corrected path | `request-key-whole-final-structural-guards.jsonl`: existing 81 roots / 458 passes across four packages, 53.990s |
| Complete four-package candidate typing | execution-proven through the same corrected path | `request-key-whole-final-separated-candidate-overlay.jsonl`: final standalone existing proof root, PASS49.399s |
| Parent completion guards and unresolved restart/drain/public-fork qualifications | split / escalated as separate class | retained in #2542/#2151/#2353 and prior audits; registry completion remains explicitly RED, not waived |

First valid downward scan passed93.320s. After a bounded complexity extraction,
the final independent policy scan passed54.383s with the same debt counts.
Final U1000 passes (`request-key-final-unused.log`). Signed complexity evidence
is produced separately against `80d42e2ab`; no working-tree measurement is
called exact-head evidence.

Disclosed development REDs: initial receipt test used the wrong canonical-JSON
Go-map API (corrected to actual marshaled/canonicalized wire bytes); the two
shared fixture owner omissions above; a locally chosen 5m combined guard run
interrupted the candidate overlay; and an invalid census overlapped an edit
to its Go helper/test inputs (mixed source/type snapshots failed closed).
Neither interrupted receipt is claimed green. The final candidate proof runs
standalone at its existing default timeout; all final census inputs were
frozen. No test timeout, job budget or guard permission was increased.

## Closure and remaining work

Achieved closure: **the named shared request-key dependency canonicalized**.
Every currently known caller of that dependency is migrated and source/
execution proved; this does not eliminate the entire semantic predicate class.
The symptom-shaped helper was an entry point, not the whole parent boundary.
The broad raw-authority migration remains open: 7026 findings / 4781 raw sites,
plus explicit classification and integrated qualification obligations. Tail
estimate remains the existing whole-family queue, not a new child/issue split.

Watchlist: existing selected-store/native-fixture ownership node is sufficient;
no new architecture, watchlist node or issue. The minimal reader capture repair
uses existing semantic/lifetime owners and prevents repeats in those shared
setup paths. Remaining constructor/fake-transaction/lifetime debt stays in
#2542; no replacement lifecycle framework is proposed.

Landing holds remain unchanged: SQLite public-fork qualification, M01/Docker,
D3/M07, static fork/deadline, drain/interleaving and final integrated proof.
The existing package-keyed historical provisioning must include all seven
generator predecessors plus this new signed predecessor, with finite exact
fetches and depth-one RED/GREEN proof before landing. No unshallow fallback.
Preflight is deferred until the authorized clean rebase/push; this held old
base receives no preflight or hosted-tier credit. Rebase remains once, after
#2617 and the orchestrator's handoff.
