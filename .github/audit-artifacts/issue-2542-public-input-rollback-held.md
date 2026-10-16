# Public Input Rollback: Local Boundary Checkpoint

Signed predecessor: cb71e60b75385615a90e82a0463f62f737d8d0e7.
Binding owner ruling: #2542 comment6157028446. Implementation is local and
qualification-open. No parent closure or debt-removal credit is claimed yet.

The complete seven-cut witness and its two callers are migrated, not the
surviving startPublicInputRollbackRuntime construction/live-fault family.
Order is unchanged: physical run, global template instances, physical entity
field rows, global template-prefix routes, exact event, all event deliveries,
all exact-event replay scopes. Each cut independently borrows the captured
receiverProofStateReader's native snapshot; first error/zero assertion and
independent API-idempotency-zero adjacency remain. Raw DB stays solely where
existing setup/live faults still use it; no credit for those capabilities.

Five unchanged semantic owners: runlifecycle.CountRunIdentityStorageTx,
pipelinepersistence.CountTemplateInstancesTx,
entityruntime.CountWorkflowRunFieldRowsTx, eventrecord.CountTemplateEventIdentityTx,
delivery.CountTemplateEventDeliveriesTx. Two approved additions live in existing
pipelinepersistence.template_storage_observation.go:
CountTemplatePrefixRoutingRulesTx (native LIKE template+"/%", no narrowing) and
CountTemplateEventReplayScopesTx (exact event, all scopes). Each has one fixed
runtimepersistence bridge through readFanOutSemanticStorage and a detached
storetest scalar. No SQL/Tx/DB/callback escapes to the witness. The existing
subscribed-only owner is unchanged. No runtime/production admission or spec
semantics change; governing selected-runtime raw_sql_policy's bounded native
fixture/readback contract remains binding.

Native evidence prefix: /home/youmew/.cache/swarm-2542-family145/public-rollback-.
Both full original served roots PASS under race, all six backend/fault cells,
33.762s: real public publication, late delivery, replay-scope and API-completion
faults, exact original assertions and no retry/timeout/skip. New positive root
PASS on both stores: global routes across independent runs, inactive and node
routes included, nonmatching prefix excluded; direct/subscribed scopes on valid
separate events, unrelated event exclusion. Original seven read cuts remain
independent, zero writes, zero active work. Native snapshot expiry, cancellation
and foreign-owner refusal retained. Final positive plus three structural owners
PASS under race31.971s. Existing template absence/raw/canceled/closed roots pass
with the two new operations added to their existing case inventory.

The initial positive setup failed its native route foreign key on both stores.
Repaired setup uses existing commitPreparedWorkflowAggregateFixture to construct
the real header before the original route owner; no raw row insert, schema or
new fixture authority. This prepared component proof does not claim public
flow eligibility/readiness. The original served roots supply supported-surface
execution separately.

Committed finite transform candidate: three complete declarations, seven cuts,
two caller edits, manifest0806a88943bacd967a72a39fb5c92f078443af44dd4d444ac62c3745a8deceb5.
Before/current normalized hashes:

| Declaration in test_command_runtime_test.go | Before | Current |
| --- | --- | --- |
| requirePublicInputRollbackNoResidue | 204a0f50aaed0325143aed87714aef533524d1e40119717131c55a0050716680 | ed144aa595810f7300549ec28e44f856f858ebdc1b549846761522e67737538b |
| TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores | 89e32e53bd62d0e8059bada4a2a80929ab69634620ed5ea1062fb52d4aee74da | db862eff5e55f1e01179caf1b5b13ff459a7908103b3967482886873ab5e3eea |
| TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores | eea8dd9dc9c5844ac1910db7eaac14fb03e8a7edb606aa9453f8281c33307395 | 15953ed967b4bcccefe591da71113ecfc174cc9a7ecbd5567421e469e0ba769a |

Boundary question before accounting/history changes: request-key source pins
also freeze all three predecessor bodies. TestPersistenceAuthorityDebtRequestKeySourceAndCollectorPins
is RED, reporting the first changed caller; old complete-chain and caller guards
also refuse the two amended callers. Proposal is three exact source-evidence
inverses plus three finite Successors, preserving original records/order and
the existing two signature occurrences until genuinely deleted, with no new
signature/accounting transition. No policy or historical corpus edit made yet.
The older whole-file delivery-status proof also sees the two approved new
private bridge declarations; it needs exact declaration-only composition,
preserving every other byte, not a blanket pin refresh.

That source-chain sweep additionally exposes three prior independent historical
composition holes: fieldless gate deferral and the two stop consumers. Their
unrelated source is unchanged by this wave; signed-predecessor attribution is
still to be completed, and no inherited-clean-master claim is made.

Expected genuine credit remains5 findings/4 raw operations, not seven queries
plus caller double-counting. Baseline remains6678/4466 at predecessor; no
downward refresh, exclusion, allowance, fingerprint or occurrence move has
been performed for this held wave. Actual read-only ratchet is RED133.197s:
its current collector's live historical source-proof composition refuses this
unmapped successor. This is not a passing deletion or an unexplained retry.
Existing parent/integrated/fork/history
and landing holds remain. Next step: bounded exact source/history composition
decision, then affected controls and real zero-addition downward ratchet.
