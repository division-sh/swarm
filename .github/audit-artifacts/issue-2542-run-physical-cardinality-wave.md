# Exact-Run Physical Cardinality Wave

Part of #2542 / #2151, under the existing equivalent-observation bulk gate.
Signed predecessor 50315ab6755c15d6137fa5cf4186c935bc2c5c24.

## Scope And Canonical Owners

This mechanical wave retires all ten standalone exact-run field/header count
calls in served and catalog receiver/lifecycle consumers: eight complete
functions, seven served files and one catalog file. It changes observation
authority, not construction, execution, delivery, fork policy or persisted state.
The issue is broader authority debt, not a newly discovered symptom bug.
Achieved closure is this finite direct-call family canonicalized; parent #2542
and #2151 remain open. Mixed query inventories and independent raw parity
oracles are not claimed closed or exempted from their debt.

No exact governing spec section defines these test-only count bridges.
Binding context is the approved #2542 equivalent-observation scope, the original
complete function bodies, and platform-spec.yaml storage_schema entity_state
and flow_instances DDL (18309 and 19212 at this predecessor). The count means
every physical row at run_id, including fieldless headers; it has no current
status, readiness, source, entity, subscription or executable-authority filter.
No authoritative runtime/spec behavior changes, so no spec amendment is needed.

Canonical SQL owners are existing pipelinepersistence.CountWorkflowFieldRowsForRun
and CountRunFlowInstanceStorageTx. The existing private
CountWorkflowRunFieldRowsForTest and CountRunFlowInstanceStorageForTest validate
the native selected owner and use readServedDeliveryObservation through that
owner's coordinator. Existing storetest forwarders remain unchanged. Two local
served adapters use the already-captured InspectSnapshot read role, one fixed
port per call, returning zero/error on failure. They grant no close, pool,
writer, selector or SQL authority. The catalog call consumes its original
h.rt.Bus.Store() directly with the original h.ctx.

The original QueryRow calls had background context. Their served replacements
keep context.Background(), including the existing polling/wait order. Counts
remain separate reads, not a reconstructed row aggregate or merged snapshot.
Every original error check, fieldless/positive count assertion, replay equality,
source-domain equality, frontier/held publication, output and teardown cut stays.

## Systematic Consumption And Residue

| Consumer/seam | Disposition |
| --- | --- |
| Selected-carrier, nested carrier and nested template lifecycle journeys | Moved all three physical field counts to the original ReceiverStateReader. |
| Fieldless gate deferral, entityless export and prospective terminal refusal | Moved all three field counts, preserving zero/nonzero and no-publication assertions. |
| Receiver composition fork, fieldless/admitted/refused geometry | Moved both field counts and the independent header count. Raw source snapshots, revision/tombstone and dynamic discard checks still survive as named parent debt. |
| Catalog root preflight foreign run | Moved its header count; the independent event-ID pair count and no-construction refusal remain unchanged. |
| Prospective publication, A2 activation and fieldless reply pipeline tests | Already consume these canonical count ports; no edits or redundant executions. |
| Selected fork execution outcome and conformance resource fork/journey | Already consume the header port, including their independent caller cuts; unchanged evidence carries. |
| Private workspace execution and selected outcome observations | Already consume the header bridge. |
| Private gate-run counts and workflow projection observations | Already consume both bridges; their raw physical parity oracles intentionally stay independent. Gate-run controls are run under race on both stores in this wave. |
| requirePublicInputRollbackNoResidue | Still bypasses in a seven-query physical/global/pattern rollback inventory in test_command_runtime_test.go; tracked in #2542, no waiver or deletion credit. |
| requireForkReceiverPostRevisionPolicyRefusal | Still bypasses in its eleven-query retained-revision/work/selected-execution inventory; same parent debt, no assertion removal. |
| Catalog tier12 fork discard inventory | Still bypasses within a mixed exact-storage counter map; remains parent debt. |
| Private construction/fork consumers and raw dialect-specific admission oracles | Remaining sibling migration/qualification obligations within #2542, not silently included in this direct served/catalog transformation. |

The complete direct-call inventory was found with case/whitespace-insensitive
SQL search, including lowercase count spellings. The three remaining plain
literals in served/catalog source are the named mixed inventories above.
Additional state/template/path/entity-filtered counts are different physical
predicates and are not substituted with an unfiltered run count.

## Finite Mechanical Rule And Counterexamples

Committed migrate-run-physical-counts applies ten exact read rewrites using
eight immutable complete declarations. Manifest digest:
d7e775886f9b93aade33f1a0ed139fef3af4a53f8337894e3c3b11c1c419c114.
It prepares every output before writing, refuses a changed predecessor/output,
keeps original source comments/formatting, and checks the exact manifest digest.
The rule and output are both committed. No caller has been inferred from a raw
pool, label or dialect. Current bodies and every other assertion are pinned.

Hostile controls change the owner, exact run, catalog context, error predicate,
count assertion and lifecycle wait; all must be refused. Foreign file/function
keys cannot receive historical correspondence. An owned-source inventory closes
all nine served calls, the two local definitions, and rejects aliases/initializers.
The standalone missing-snapshot-owner test refuses both ports without evidence.
Existing gate-run proof distinguishes positive/foreign-run counts, canceled/raw/
closed-owner refusals, one read per independent count, zero writes and unchanged
history/leases/clocks on both stores.

The historical recipe ledger retains all Before values and reserved Successors.
It now has 1511 recipes / 815 transitions, digest:
445c8e6cc0a783aab1d11b1d9d88b025eb32e723826fd3b5a29de5afd6a9c567.
Historical served-debug/lifecycle composition accepts only the exact complete
new body back to this signed predecessor; there is no generic inverse. A first
finite run correctly found the omitted served-debug current-declaration
composition. That exact-key composition was repaired, not the historical source.
The new immutable manifest is decoded once rather than on every historical
caller check; source digest validation and hostile body refusal remain.

## Manifestation Proof

| Manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Selected carriers and declaration reordering | execution-proven through the same corrected path | TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores, every original handler/choice/reordering child. |
| Nested carrier collisions, held diagnostics and sibling decisions | execution-proven through the same corrected path | TestServedCompiledTransitionNestedCarrierCollisionOnBothStores, both original normal/held-diagnostic arms. |
| Nested template recipient census | execution-proven through the same corrected path | TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores. |
| Fieldless and fieldful deferred gate companion counts | execution-proven through the same corrected path | TestServedFieldlessGateDeferralBothStores, fields=false/true, exact replay retained. |
| Entityless root export | execution-proven through the same corrected path | TestReceiverCompositionEntitylessRootExportBothStores, no-delivery and constructed/public readback retained. |
| Terminal prospective refusal preserving initial construction | execution-proven through the same corrected path | TestProspectiveTerminalReceiverRefusesWithoutMutationOrPublicationBothStores, original field/publication/dead-letter and full row assertions. |
| Fieldless/admitted/refused fork physical header/companion census | execution-proven through the same corrected path | TestReceiverCompositionForkBothStores, all original surfaces and source/fork readback. |
| Foreign missing root preflight cannot borrow an existing header | execution-proven through the same corrected path | TestReceiverMaterializationRootPreflightBothStores, exact foreign-run zero and event-pair zero retained. |
| Missing/raw/closed/canceled owner and provisional evidence | execution-proven through the same corrected path | TestServedRunPhysicalCountsRefuseMissingSnapshotOwner; TestGateRunCountsRefuseRawAndClosedOwners; TestGateRunCountsRetainPhysicalScopesAndIndependentReadsBothStores. |
| Complete transform/idempotence/source/owner drift | execution-proven through the same corrected path | TestFamily145RunPhysicalWholeChainPreservesPredicatesAssertionsAndCuts; TestFamily145RunPhysicalOwnedCallerInventoryIsFinite; TestFamily145RunPhysicalExistingOwnersRetainExactScopeAndCoordinator, including hostile alterations. |
| Mixed-query inventories and remaining private raw consumers | split / escalated as separate class | Exact residual source inventory above, tracked as unfinished broader #2542 debt; no execution or closure claimed for their migration. |

## Tracking And Qualification

No deeper production architecture or type-model decision is needed: use existing
read owners rather than another facade. The remaining shared raw carriers and
mixed inventories are already tracked by #2542/#2151 and their existing watchlist
nodes. Watchlist decision: existing mapping sufficient; this exact consumption
inventory refines the issue audit, with no new issue/node or untracked follow-up.
ROI is removal of ten executable observation escapes without another dialect,
policy or lifecycle implementation. This does not update the parent's unknown
semantic exception, integrated qualification or completion-guard tail estimate.

Evidence prefix /home/youmew/.cache/swarm-2542-family145/run-physical-.
Native focused proof: PASS 11 roots / 61 cases under race across serveapp,
cataloge2e and runtimepersistence, package times 409.010/14.407/14.835s.
Finite/source composition: PASS 16 roots / 1527 cases, 104.498s. The original
138.150s finite run was RED on the exact served-debug composition noted above.
Affected structural owners: PASS six roots across runtime, serveapp and
runtimepersistence; package times 1.047/5.876/2.218s. Canonical source owner and
both entry golden/correspondence roots: PASS three roots / 607 cases, package
times 87.920/12.496s. U1000 passes all six affected packages/commands.

Real downward ratchet: PASS 180.004s, 20 findings / 20 raw sites removed,
zero added. Independent read-only ratchet: PASS 51.692s. Residual debt is
6742 findings / 4520 raw sites / 67 excluded occurrences. Collector is unchanged:
64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44.
Only the baseline and its checksum sidecar decreased; no accounting transition,
policy, exclusion or classifier change was required. Signed complexity and the
final generator receipt are recorded with the issue checkpoint.
No push, PR, rebase, preflight, server2, tier, restart or parent-completion claim.
Unchanged receipts carry; all prior named qualification holds remain unwaived.
