# Receiver Selector Retirement Proof Map

Binding decision: division-sh/swarm#2433, comment 5606927408. This map is not
a post-implementation proof audit or a passing receipt. The final audit must
name the exact tested head and actual execution results.

The old handler selector is unsupported, not a compatibility seam. Flow instance
identity and composition resolution elect the receiver once. Handler initialization
does not derive another UUID or inject a business key into canonical state.

## Removed Selector Tests

The names below refer to the removed `select_entity_test.go` at `191b6c425`.
The old test prefix is `TestExecuteNodeContractHandler` unless stated otherwise.

| Old suffix / test | Retained obligation or explicit supersession | Current proof |
| --- | --- | --- |
| SelectEntityUpdatesTargetOwnedEntity | Write only the admitted receiver, never producer or same-key sibling | `TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores`; `TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores` executes real declared accumulation into the canonical receiver |
| TestMatchHandlerEntitiesForFlowFiltersDescendantsBeforeContractValidation | No descendant or prefix sibling can become the parent receiver; reject exact-owner contract corruption | `TestEventBusCompositionOwnerIncludesStateWithoutLifecycleOnBothStores` nested/prefix cells; `TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores/conflict` |
| SelectEntityReplayUsesSameTargetEntity | Accepted identity survives duplicates and reconstructed execution | `TestReceiverCompositionActivationReuseAndConflictBothStores`; `TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores` reconstructs the coordinator between distinct events and retains exactly one entity |
| SelectEntityMatchesTypedStatusField | Business status is not lifecycle status; business matching is no longer receiver authority | `TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores` preserves business `pending` alongside lifecycle `active`; artifact proof retains independent `committed` assertions; no positive selector matching test survives |
| SelectOrCreateEntityCreatesTargetOwnedEntity | Canonical initialization and field defaults, not a second key-derived identity | `TestDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgres`; `TestReceiverMaterializationChildToRootBothStores` |
| SelectOrCreateEntityReplayUsesSameDeclaredKey | Reuse the admitted instance; handler-key injection is superseded | `TestReceiverCompositionActivationReuseAndConflictBothStores` |
| SelectOrCreateEntityFailsClosedOnAmbiguousMatch | Same business key on different instances is lawful; duplicate exact ownership is not | `TestEventBusCompositionOwnerIncludesStateWithoutLifecycleOnBothStores/duplicate-exact-owners`; `TestClassifyDeliveryTargetOwnershipPreservesCompositionInstanceMatrix` |
| SelectOrCreateEntityFailsClosedOnDeterministicIDConflict | Alternate selector UUID removed; exact canonical entity/type contradictions still fail without mutation | `TestDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgres`; durable EventBus conflict cells |
| SelectOrCreateEntityConcurrentDuplicateCreatesOneEntity | Canonical materialization and publication concurrency must still create one receiver and one truthful history | Retained receiver materialization duplicate/race matrix; exact-head composed race receipt still required |
| SelectOrCreateEntityFeedsEntityIDToArtifactRepoCommit | `_entity.id` reaches the real local-git action; opaque URL and immutable ref identify the receiver | `TestCompositionReceiverInitializationFeedsArtifactCommitBothStores`; must pass before this row is discharged |
| SelectEntityIgnoresTerminalAndTerminatedMatches | Inactive siblings do not affect a lawful receiver; an unavailable exact receiver is not missing or replaceable | State-only matrix terminal/draining/terminated cells; classifier sibling cell; `TestTargetOwnerDedupPreservesUnavailableEvidenceInBothOrders` |
| SelectEntityFailsClosedOnNoMatch | Missing required exact ownership rejects before publication | State-only matrix `missing-with-business-key-sibling`; `TestDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutation` |
| SelectEntityFailsClosedOnMissingPayloadRef | No handler-owned payload key exists now; missing flow-key resolution belongs at the composition boundary | `TestEventBusPublish_ConnectRoutePlanFailsClosedForRenamedTemplateInstanceKeySourceGap`; `TestEventBusPublish_ConnectRoutePlanFailsClosedForTemplateInstanceKeyGaps`; strict retired-field rejection does not substitute for these execution proofs |
| SelectEntityFailsClosedOnAmbiguousMatch | Different instances may share business data; ambiguity means conflicting exact ownership evidence | State-only matrix `duplicate-exact-owners`; durable same-key sibling preservation cells |

## Additional Removed Readers

`SelectActiveWorkflowEntityStates`, `SelectActiveWorkflowInstances`, their
facade/interface entries and local selector normalization/filtering are removed.
`WorkflowEntityStateSelectionOwner` remains a live exact-scope state admission
primitive used by readiness and state hydration; it is not business-key election.

The old frozen-run selector exclusion test is replaced by
`TestForkedSourceCanonicalTargetOwnersExcludeAndPreserveReadbackBothStores`:
the real selected-store target-owner reader excludes the frozen source while the
exact historical state reader preserves it. The five rejected workflow mutations
remain in `TestForkedSourceWorkflowInstanceMutationsRefuseAndPreserveReadback`.

## Temporal Fixture Correction

The former absent-state-after-template-activation fixture contradicted canonical
activation. `TestReceiverCompositionActivationReuseAndConflictBothStores` instead
proves actual initial activation, preexisting canonical state, subsequent handler
reuse, late same-business-key siblings, reconstructed coordinator execution,
duplicates and hostile exact-state conflicts. Root first-delivery materialization
is separately proved by the real root receiver matrix. Neither substitutes a
fabricated activator or an alternate receiver key.

## Closure Requirements

The 180-cell state-only matrix spans nine scopes, ten conditions and both stores.
Strict retirement additionally covers both syntax names, empty/null/mapping
values, ingress provenance, flow modes, renamed events and multiple producers.
The shared captured-loop and E execution-authority changes require composed
G01-G31, receiver, repeated/race and full-suite receipts. No row is considered
closed solely because its old helper was removed or consumers now share an owner.
