# Durable-resource carrier and exact-receipt batch

Track: FULL. Reviewer-b6184924023 approves the complete eight-file boundary;
6184914472 releases only F1 render12/9. Base50c5c207de, metadata companion
777e1d59ff separately uncredited until this batch's composition/census.

## Complete Boundary

The existing baseline has160 findings/142 raw operations in the connected
resource family, plus metadata3/2: indicative163/144, never pre-awarded credit.
forEachDurableDataStore has22 calls; forEachDataRunLifecycleStore has19, not12.

| Caller file | Boundary |
| --- | --- |
| internal/store/durable_data_selected_store_test.go | All22 store-fixture calls, exact source/prune/version/provenance/head-history/pin witnesses. |
| internal/apiv1/operator_data_run_lifecycle_test.go | Native primary/reopen peers, nine original lifecycle consumers, raw count/retry helpers and all exact-receipt negatives. |
| internal/apiv1/operator_data_show_selected_test.go | Both original public read-family and finite-page consumers, fourteen-table physical no-mutation witness. |
| internal/apiv1/operator_data_show_corruption_test.go | Payload corruption and the original cursor/integrity refusal. |
| internal/apiv1/operator_data_test.go | Original HTTP public-surface corruption/restore and raw getter. |
| internal/apiv1/operator_finite_run_start_test.go | Four omitted finite initiation/admission/publication/replay roots. |
| internal/apiv1/operator_deployment_run_start_test.go | Both omitted deployment roots, exact feed cardinality/status, cache deletion/permanent replay, completion diagnostic and cache absence. |
| internal/apiv1/reviewer_2566_early_test.go | Omitted nested-service refusal root. |

The path-restricted2617 check finds none of these caller files. Owner-side edits
stay in existing durabledata, pipeline/fan-out, runlifecycle and API-idempotency
owners and their original runtimepersistence/storetest bridges. No nodes.yaml,
grammar, delivery ActiveSnapshots, ReadResetInventory or611-construction work.
The single integration rebase still waits for A+D.

## Concrete Cuts Frozen Before Conversion

Source JSON matrix17: result invocation, operation, outcome, bundle, schema,
declaration, expected/observed head, candidate union, head transition, delta,
delta row identity, defect page, completion; evidence defects/deleted keyed delta;
request/hash agreement. Source typed8: operation, parent run, actor, bundle,
package, event, observed revision and completion clock. The separate candidate
state/alias and malformed empty-defect-page negatives remain distinct.

Prune JSON matrix13: invocation, outcome, declaration, version, expected/observed
head, current version, payload-before/after, pin count, defects, completion and
request/hash agreement. Prune typed5: actor, package, event, version and clock.

Run-creation JSON matrix8: summary kind/count/outcome/event/clock, binding union,
binding evidence and request/hash agreement. Three typed negatives: actor,
bundle and clock. Separate pinless-status, missing rejection and retained-hash
contradictions remain. Child observed revision must restore before the subsequent
alternate-schema coordinated child/parent corruption. Child/source business-key
negatives remain separate from ordinary run-creation corruption.

Coordinated source cuts: changed exact input plus valid request hash; positional
delta/result-evidence; candidate+manifest; summary/evidence delta classes; false
validation defects; conflict candidate; admitted-schema/business-key context;
historical-head replacement. Clock rewriting keeps five writes in one native
transaction and its preceding evaluation/result and provenance reads independent.
Missing provenance/history and wrong history kind retain both read/replay
refusals. Non-mutating receipts keep their forbidden provenance/history facts.

Provenance has JSON version/ref/actor/clock and typed kind/id/actor/clock arms,
with exact restoration between arms. The full-pin refusal test retains every
pin, pagination/ordering/multiplicity assertion and exact last-evidence deletion.
Payload corruptions retain their distinct immutable-payload, public-surface and
cursor semantics. No generic table/column name, caller mutation callback or patch
object may cross the public fixture seam.

## Owners And Temporal Cuts

durabledata owns literal fixed receipt projections and closed integrity variants.
Borrow observations from the selected coordinator; do not combine independent
read cuts. Fault receipts retain complete captured rows, physical types/NULLs,
original and installed state, and exact cleanup under the original coordinator.
The shared fault lifetime arms before mutation, preserves uncertain errors,
reconciles original as a no-op and refuses changed replacements. It is not a
caller-extensible fault registry or an alternate transaction/retry protocol.

Existing fixed version JSONL/count/metadata, source invocation, run operation,
run identity/event, fan-out intent and pin count ports are reused. Feed status
belongs to pipelinepersistence; completion clocks to runlifecycle; transport
cache expiry/absence to API-idempotency. Expiry removes only the exact run's
transport cache, not permanent resource receipts; replay assertions remain.
Native independent peers prove same-store reconstruction, not process restart.

## History And Qualification

Freeze all original bodies at50c5c207de. The old durable-construction proof keeps
its historical exact constructor comparison,22-body hash and hostile mutations;
only its current endpoint advances through finite reviewed composition. Existing
recipe/transition Before/metadata/order and prior endpoints stay immutable.
The HTTP constructor, all actual API consumers and the seven added callers are
included wherever pinned. Use the existing finite reconciler, not a new driver.

Run focused affected roots and both-store/race/counterexamples per internal wave.
At one combined checkpoint run history/inverses, changed guards, quality/unused,
census and real downward ratchet once; do not pad scope or repeat whole tiers.
Queued runner-owned PostgreSQL is used, not shared5433. F2 is not a cheap
qualification addition: its missing PostgreSQL chosen, both-backend skip and
failed SQLite chosen-false cells stay held6/5 without another retry round.

Accepted starting ledger: channel174/131, channel holds76/65, prior87/70;
total held163/135, physical4646/2788, adjusted4809/2923 and cumulative accepted
1824/1499. The latest served SQLite render control's upstream5s event.publish
RED and untouched vet warnings remain disclosed. The physical36/26 channel
tail is not counted twice. Preflight waits for the authorized integration rebase.
