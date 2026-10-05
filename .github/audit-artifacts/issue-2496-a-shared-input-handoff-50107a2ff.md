# A/E Shared Constructor Input Handoff: Local Qualification Status

Local production/test/spec inputs are frozen at `50107a2ff`, based on
`origin/master@0fa24140a`. Nothing is pushed; G/#2544 CI-quiet remains binding.
The [four-disposition ruling](https://github.com/division-sh/swarm/issues/2496#issuecomment-5965454522)
is implemented locally. None of its four repairs needs another model ruling.
This is not a final-review request or chosen-class elimination claim.

## Actual Qualification

- The original managed default at `a154729c7` remains RED: 66 failing roots,
  230 failing named cells, 90 skips and 13 subsequent units unexecuted. It was
  not an A-only failure; our preview handoff and constructor/test teardown
  inputs also needed correction.
- At `0a928ff7a`, the managed race/count1 repair matrix is RED: 42/44 roots
  PASS; the timer parent-fixture and template keyed-event oracle FAIL. The
  selected returned-outcome root passes on both stores, including persistent
  discard, exact-owner retry and preserved cancelled completion tombstones.
- `d7697e36f` repairs the two remaining fixtures without changing identity
  derivation, production join semantics or negative assertions. On `50107a2ff`
  inputs both roots pass race/count3: six root receipts and six timer backend
  receipts, package18.623s, no failure/skip. Eight focused Manager/frontier/
  mock-preview roots also pass on that source.
- `issue-2496-default-repair-50107a2ff.json` maps all 66 original failures to
  named receipts: 50 E-owned roots now have passing source-attributed reruns;
  the 16 shared A-owned roots below remain pending. No passing full default is
  claimed. Current-source receiver/template composition passes all five
  original roots/28 named cells race/count1,278.694s, no failures/skips, with
  restart/refusal and byte-exact post-fork source checks reached. The repaired-
  failure JSON retains the full command/source/hash. This is compiled retained
  mock lifecycle/public RPC readback, not paid-provider/public-launcher proof.

## Shared Inputs Still Pending

The [seven request-producer fixtures](https://github.com/division-sh/swarm/issues/2496#issuecomment-5966120157)
must carry their existing complete constructor/readiness `Instance`, including
the exact initialized parent, alongside the matching typed route. A's public
initial-entry signature and production collection/join semantics are unchanged.
The original no-touch-A-files instruction is respected. Please provide A's
input-only patch or explicit A/E coordination authorization; no compatibility
wrapper or inferred parent is proposed.

The fieldless paired-reply root additionally consumes the same shared
`commitA2FixtureConstruction` helper; it is not an eighth direct route producer.
The conformance fan-in fixture remains in the seven-producer census even though
the failed first default unit did not execute that later plan unit.

| Remaining Original Failing Root | A-Owned Declaration File (pipeline/) |
| --- | --- |
| TestA2StageEntryBoundReturnIsolationOnBothStores | a2_bound_reply_binding_external_test.go |
| TestA2BoundReplyCanonicalJoinOwnerRefusesCorruptedEntryOnBothStores | a2_bound_reply_binding_external_test.go |
| TestA2BoundReplyDoesNotLendAdmissionToLaterSiblingOnBothStores | a2_bound_reply_binding_external_test.go |
| TestA2BoundReplyDoesNotExpandToLaterOrdinaryConnectObserverOnBothStores | a2_bound_reply_binding_external_test.go |
| TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores | a2_fieldless_reply_preservation_external_test.go |
| TestA2KnownTargetUnarmedPublicationRetainsEarlyRefusalAfterArmAndRestartOnBothStores | a2_known_target_binding_external_test.go |
| TestA2KnownTargetWorkIssuedBeforeArmPublishesOutputBoundToActualArmOnBothStores | a2_known_target_binding_external_test.go |
| TestA2MembershipSnapshotValidationOnBothStores | a2_membership_snapshot_admission_external_test.go |
| TestA2MultiUntilIndependentRecipientEntriesAndRestartOnBothStores | a2_multi_until_binding_external_test.go |
| TestA2StageEntryPayloadDirectedOutputOnBothStores | a2_publication_binding_external_test.go |
| TestA2StageEntryMultiRecipientBindingOnBothStores | a2_publication_binding_external_test.go |
| TestA2StageEntryMissingComputedKeyRefusesWithoutReceiverMutationOnBothStores | a2_publication_binding_external_test.go |
| TestA2StageEntryFirstPublicationRacesTransitionAndRetriesOnBothStores | a2_publication_binding_external_test.go |
| TestA2StageEntryFirstPublicationRacesCloseAndRetainsLateRefusalOnBothStores | a2_publication_binding_external_test.go |
| TestWorkflowJoinDurableEventBusDeliveryClaimPreservesExactDeclarationOnBothStores | workflow_join_supported_surface_external_test.go |
| TestWorkflowJoinScheduleOccurrencePreservesExactDeclarationThroughDurableEventBusOnBothStores | workflow_join_supported_surface_external_test.go |

`WorkflowJoinAdmissionOwner` is also a named pending same-PR production consumer:
its declaration-prefix `workflowInstanceRouteForExecution` projection cannot
validate a constructed descendant below a keyed parent. The existing join
admission/header owner must consume the exact admitted coordinate and canonical
construction identity; parent context grants no join or recipient permission.
The [individual-consumer handoff](https://github.com/division-sh/swarm/issues/2496#issuecomment-5966213736)
records this separately from fixture work. It is not credited as closed by the
passing template/activity paths and is not silently deferred to another issue.

After shared input/consumer integration, rerun their deterministic both-store
proofs and the complete managed default. Source-attributed prior receipts,
current candidate qualification and hosted CI remain distinct. No push or
merge request is authorized by this status update.
