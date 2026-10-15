# Native bus outbox: whole-family proof audit

Track: FULL. Binding #2542 comments6190293197 and6190684979; original
boundary/reproduction6190640156. This is a local stacking checkpoint, not
parent closure or landing qualification. The earlier diagnostic audit remains
an immutable account of the RED; its statement that production was unchanged
describes that earlier checkpoint only.

## Concept, class and governing contract

The changed concepts are native fixture authority and batch-local publication
claim ownership from preparation through atomic commit, ordered finalization,
dispatch and cleanup. The chosen class is the complete fake bus-outbox
transaction/claim dependency, including the production multiplicity defect the
native fixture exposed. The outer raw-SQL symptom belongs to #2542/#2151, which
remain open. Closure here is touched seam canonicalized, not zero-debt closure.

Authoritative contract:
platform-spec.yaml#contract_formats.event_schema.terminal_run_admission.transactional_batch_handoff,
with adjacent duplicate_operation_boundary and duplicate_recipient_preflight.
The spec now explicitly describes borrowed batch claim views, ordered outcomes,
inserted-only consequences, independent-operation exclusion and cleanup. It does
not change the previously required inserted/duplicate/conflict outcomes.

The generic RED was the second native claim in one same-ID preparation batch,
on both stores under race. Busy for a competing independent operation remains
correct. No backend-wide reentrancy, durable schema, public API, alternative
commit loop, outcome deduplication or recovery fallback was introduced.

## Owners and exhaustive touched consumption

| Canonical owner | Touched consumers and systematic consumption |
| --- | --- |
| EventBus source admission and closed publication planner | Every ordinary and prospective engine intent is admitted independently. Same-ID occurrences compare complete canonical source and projected event facts, then retain a separate prepared plan. The fan-out pre-issued member branch remains separate and uses its original exact member claim and ordinal. Direct and selected-fork publication do not borrow batch claims. |
| pipelinePublicationClaim | Only the first occurrence owns native settlement/release. Later distinct views carry identity, but cannot settle, mark decision processed, or release. Preparation error joins original and cleanup errors. Partial preparation, cancellation, rollback, finalization, shutdown and later independent-claim ownership are execution-checked. |
| Existing selected workflow mutation transaction and native event append | CommitConstructedOutboxFixture loads genuinely constructed workflow state and calls the ordinary engine MutationOwner with evaluated prospective state. Every intent invokes the selected append owner; only that owner determines inserted/duplicate/conflict. No caller supplies SQL, transaction callbacks, fake commit acknowledgments or seeded publication evidence. |
| Ordered committed-publication correspondence and pending outbox | Native receipts validate their exact planned occurrence, including separate claim-view identity. Omitted/reversed outcomes fail without losing the actual committed row. Duplicate finalization cannot repeat readiness/activation. The designated inserted operation alone dispatches; an exhausted same-ID outcome cannot fall through to recovery in the same callback. |
| Native selected-store fixture, lifecycle construction, process/runtime lifetime | All23 original outbox roots, source-preparation sibling and committed-refusal sibling use the existing native construction owners where durable behavior is asserted. The local fixture arms cleanup before consumers, resets pending operations, retires/joins the runtime and process, then closes the selected store. Pure recorder neighbors remain nonpersistent and unclaimed. |
| Existing eventrecord and delivery fault owners | Two fixed native trigger cuts are reached only through storetest -> existing test_event_support -> original selected writer. The second insert fault verifies the first insert exists inside the same transaction. Diagnostic failure is scoped to one original event. Both removals are prearmed before installation, idempotent, uncancelled and error-reporting. No new import allowance or raw public seam. |
| Existing canonical routing source owner | CopyOutboxMaterializedManifest supplies the actual compiled template/node source. Root and review/inst-1 are constructed before use. The planner still materializes before the guard and persists the exact node route. No Go-authored routing bundle survives in callers. |
| Existing private fixture observation cut | The fixed ambient-context operation owns an isolated real SQLite transaction, never the selected runtime transaction. Only detached action counts and a transaction-open observation leave it. Dispatch receives hostile ambient context only after native commit; zero action registrations and a still-open foreign transaction are required. No transaction, executor or callback is returned. |
| Finite codemod and existing source-history owners | Twelve descriptor-only roots propagate mechanically, with idempotence, preserved oracles and semantic-field refusal. Semantic routing/fault/ambient cases are separate native repairs. Last-caller retirement refuses surviving uses before deleting all three fake capabilities. Historical generators compose existing finite setup/observation/run-physical inverses; every original manifest/order/count stays unchanged. Whole-file inverses recover exact signed earlier bytes, with altered-source controls. |

Old directRecipientTransactionalStore, outboxClaimStore and
commitEnginePublicationsForTest have no live definitions/callers in the bus
family. Their historical text in the finite codemod controls is non-authoritative.
The malformed stored-route refusal remains an explicit pure observation double;
it owns no transaction, claim or commit result and is not native persistence
evidence. No generic raw getter or compatibility implementation survives here.

## Manifestation-level evidence

All receipt names are under /home/youmew/.cache/swarm-2542-family145/.
R = bus-outbox-whole-family-race.jsonl:37 roots/121 passing cases, zero skips,
both stores, race/count1/GOMAXPROCS2; package133.865s. Later helper extraction
and the two added cleanup cuts receive their own affected run, not an unrelated
duplicate tier. Immutable event images include full delivery/diagnostic evidence
and delivery clocks, not merely event/message counts.

| Manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Same-ID batch rejected at second claim | reproduced and fixed | Original bus-outbox-native-claim-boundary-race.jsonl SHA256 62e34e6b75f49eba20a25847c338b44726f60e35dce13f0c3ae795ae6dd21a1a; R/TestNativeOutboxBatchClaimOwnershipAndOrderedReceiptsBothStores/{sqlite,postgres}/{AAA,ABA}, ordinary prospective engine path |
| Ordered inserted/duplicate/duplicate and A/B/A outcomes | execution-proven through the same corrected path | R/same native root, exact native per-position outcomes and dispatch order; original TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch retained |
| All-preexisting duplicate side effects | execution-proven through the same corrected path | R/TestNativeOutboxAllDuplicateBatchPreservesDurableImageBothStores: exact event/delivery/diagnostic image, zero readiness and zero dispatch; original all-duplicate root retained |
| Changed same-ID payload/time/run/source/route | reproduced and fixed | R/TestNativeOutboxConflictingImmutableSourcesRefuseWithoutPartialEffectsBothStores, all10 backend/variation cases, zero partial writes and claim reacquisition; original conflict root retained |
| Borrowed duplicate cleanup releases live owner early | reproduced and fixed | Healthy R/AAA and ABA; bus-outbox-early-release-red.log and bus-outbox-early-release-pg-red.jsonl inject real premature release and fail competing-claim assertion on both stores |
| Independent publication/recovery exclusion and later owner preservation | execution-proven through the same corrected path | R/AAA and ABA attempt both purposes before designated dispatch; after legitimate release an independent claim succeeds and duplicate cleanup cannot consume it |
| Omitted/reordered duplicate outcomes | reproduced and fixed | R/TestNativeOutboxRefusesOmittedOrReorderedCommittedOccurrencesBothStores, actual commit followed by omit/reverse fault, no dispatch, real row preserved, claim released |
| Exhausted outcome reconstructs fresh dispatch | reproduced and fixed | R/TestNativeOutboxExhaustedBatchNeverReconstructsMissingOutcomeBothStores, extra same-ID callback entry refused after genuine outcomes |
| Partial preparation/cancellation leaks claim | execution-proven through the same corrected path | R/TestNativeOutboxPartialPreparationFailureReleasesOwnedClaimsBothStores/{guard,cancel}, both prior claims reacquired and no committed event |
| Native transaction fails after first actual event write | reproduced and fixed | R/TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack/{sqlite,postgres}/rollback: fixed native trigger proves first write, full state/clocks/revision equality, zero event/delivery escape, released claims |
| Duplicate finalization repeats readiness or loses the owning claim | execution-proven through the same corrected path | R/TestNativeOutboxDuplicateFinalizationCannotReleaseInsertedClaimBothStores, armed second-call fault, exactly one readiness call and one delivery |
| Finalization failure or shutdown loses evidence/leaks authority | execution-proven through the same corrected path | bus-outbox-native-batch-cleanup.jsonl/TestNativeOutboxBatchFailureAndShutdownPreserveCommittedEvidenceBothStores: all4 backend/cut cells; extraction control in bus-outbox-cleanup-helper-extraction.log |
| Source substitution, exact recipient refusal, immutable dispatch snapshot, native target-refusal diagnostic, malformed route refusal, ambient SQL authority, interceptor and internal-subscriber ordering | execution-proven through the same corrected path | All25 original roots preserved and passing in R; bus-outbox-real-foreign-transaction.jsonl reruns all3 changed ambient roots, proving the real foreign transaction remains open and both action lists stay empty |
| Direct/selected-fork/fan-out/gate/timer/schedule/coordinator consumers | execution-proven through the same corrected path | bus-outbox-sibling-controls.jsonl:15 roots/43 passing cases, both stores/race, bus8.204s and pipeline56.895s; bus-outbox-engine-schedule-siblings.log covers existing engine acknowledgment/cleanup and schedule lifecycle controls |
| Pre-batch standing owner-file and five historical-generator defects | reproduced and fixed | bus-outbox-owner-history.log: exact predecessor bytes, renamed/extra-byte hostility; bus-outbox-historical-generators-final.log: all7 original generators, missing-commit/source/changed-source refusal and all7 immutable manifests,70.376s |

## Accounting, quality and residuals

The actual downward refresh removes135 findings /100 raw sites, zero additions;
124 exact identities disappear (multiplicities total135). Physical4177/2400 ->
4042/2300. Accepted remaining stays4371/2555 until reviewer-b accepts this
checkpoint; proposed qualification-adjusted remaining is4236/2455. All194/155
previous holds remain uncredited and unchanged. Collector stays
b5bdd1d92df22693c570dab217a46c8a184f37085f496653d59b732c12399a4d;
trusted source26701e7b187579220dffe72e01debd2e89cd2675, excluded residual67.
No accounting exception, baseline increase, classifier relaxation or deletion
credit for private source-history composition is requested.

The real downward ratchet passes315.09s. Final read-only ratchet, exact-head
complexity and remaining affected extraction controls are recorded in the
checkpoint comment after completion. Structural event/delivery/lifecycle/source
construction guards, codemod hostility, vet and U1000 pass. The new cleanup
root's complexity37 is repaired by helper extraction; all new functions are
below30. Preparation complexity is reduced from its original37 to32.

Watchlist decision: refine the existing #2151 fixture-authority and publication
handoff node; comment6190684979 already records the newly reproduced production
manifestation. No new issue. The deeper smell is fake transaction protocols
obscuring coupled lifecycle ownership; tracked in #2542/#2151, not a proposed
new framework. Long-run direction remains canonical native fixtures and exact
typed commit/claim evidence. This bounded repair took roughly2h including native
fixtures, production diagnosis/repair, proof and history; replacing the remaining
parent families is still multi-day work, not a newly claimed complete migration.
Historical composition cost is separately disclosed rather than counted as debt
removal. Parent remaining child-tail estimate is unchanged: all residual census
families plus the final two strict guards and integrated qualification remain.

No rebase while A+D reservation is active, no PR, hosted run or duplicate local
tier. Exact-head preflight is required after integration before any non-backup
push; this held base lacks that gate, so no preflight pass is claimed here.

## Final checkpoint receipts

Code head702af7abd9e4cac916d76f158209352d261f171e is signed G. The final
read-only real ratchet passes504.25s at4042/2300, unchanged collector and zero
additions. Exact code-head swarm-complexity passes with cognitive hotspots
557/196 and cyclomatic259/59, all equal to the independently measured
a525874ba4 base. The finite codemod's newly measured complexity30 hotspot was
simplified without changing its output; all3 finite-transform/hostile tests and
its final vet pass. Final U1000 and affected owner/source controls also pass.

The final extracted-preparation race receipt has13 roots/57 passing cases,
zero failed/skipped cases, package PASS216.324s on both stores. HOWEVER its
runner aggregate is RED: after child completion, removing runner-owned
PostgreSQL container5f5fe29089279fd9d7b7a3b5d5bde9e6964591d6dfdce21015164c96835ca370
reports signal:killed, child result:nil. One read-only docker inspect afterward
reports that exact container absent. The cause of the runner cleanup error is
unclassified; this is NOT an aggregate green, pressure attribution, runtime
failure diagnosis, retry-to-green or additional debt credit. Keep this exact
runner qualification item separate under the existing #2353 Docker territory;
all native test-owned joins completed successfully within the passing child.
The original37-root/121-case whole-family wrapper, the15-root/43-case siblings,
and the separate4-cell native cleanup wrapper all exited0. Unchanged receipts
carry over the mechanical helper extraction. No assertion was waived.

Final receipt SHA256:
- whole-family race:6562cde1f84b9cb86cbe30ec6ce89655dc2a90b8e60d009e4f84d2acd322cc6d
- extracted-preparation child PASS / runner RED:2558e6f6d91f732f8dfa451a760a71cfbfc43fc351951b9f249cba0919842740
- complete historical generators:8be4fa5970533853ea846dc50a6ba68ed6026e63d709d3f21ae538b0622f2c83

Coarse end-to-end time split since the10:48 approval: about75min native
fixture/production editing and investigation,15min finite history composition,
and75min proof/build/quality/queue time (some overlapped). This is about
50 qualified proposed removals/hour over the complete wave, not accepted
credit before independent review. No extra family was added to hit a target.
