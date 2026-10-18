# #2700 Static Admission: Bounded Repair And Remaining Owner Questions

BLOCKING event under C1'', not a ready checkpoint or CI-slot release request.
The [bounded implementation acceptance](https://github.com/division-sh/swarm/issues/2700#issuecomment-6245342233)
does not qualify the joint tree. No push, dispatch, guard exception, baseline
change, collector-policy edit or business-deadline change is made here.

Source/test head: signed b24a0421fa4ffe29f8e89f31fe54720996b0d278;
tree 735548353b98d1d74b0a76057cd6df686229ce94. The following audit-only
commit does not change those runtime, test or static inputs.

## A-Owned Repair

The native-source fixture conversion at 3db6a718d3 moved all three callers
from seedSelectedExecutionTestAgent to prepareSelectedExecutionSourceAgent
but retained the unused definition. Remove that definition only. Its three
real callers and all source/session/history assertions remain unchanged.

Keep the accepted original-activity admission and split its existing work at
three semantic boundaries, without adding another admission owner:

| Existing owner | Change | Exact measured effect |
| --- | --- | --- |
| pipeline.ValidateRecordedActivityProposal | Extract the released-request comparison; retain every field and comparison order, including Input.Equal and reply context. | Cyclomatic 39->17; comparison helper 23. |
| runForkSourceStateAdmission.activityRequestOriginAtCut | Extract its inherited-parent walk. The original caller keeps the cancellation check and visiting-map lifetime through the recursive return; same transaction, source loader, binding and refusals. | Cyclomatic 31->15; inherited helper 17. Cognitive 31->15; inherited helper 16. |
| planRunForkSnapshotWithActivityOrigins | Extract the ordered per-cut origin collection, retaining its nil-source behavior, per-request fresh visiting map and error text. | Cognitive 52->47; collection helper 6. Cyclomatic 41->38; collection helper 5. |

Adds 0 test roots; existing exact-origin, approval/refusal and both-store
execution roots cover the refactored owner. No concurrency protocol changes,
so no duplicate race campaign or local tier is added.

## Measured Receipts

All new receipts are under /home/youmew/.local/state/agent-a-2700/ and
are pinned to the source/test head above. Original REDs remain retained.

| Check | Result and receipt |
| --- | --- |
| Focused execution plus affected guards | PASS: events 2.654s, contracts 44.500s, semanticview 0.011s, pipeline 0.196s, runforkexecution 8.165s, runforkpersistence 8.599s, runtimepersistence 13.042s. static-owner-repair-focused-and-guards-count1.log. Includes the runtime-container reuse matrix and genuine-approval matrix on both stores, all three native-source consumers, origin/declaration/artifact controls and the historical admission, event construction, lifecycle/fixture and live-writer guards. |
| Repair versus accepted b42048efb3 | PASS, independently measured, equal reviewed complexity policy; no hotspot-count increase. static-owner-complexity-bounded-repair-b24a0421fa/{base,head,delta}.json. This is repair attribution, not landing-tree qualification. |
| Candidate versus common B/E assembly0ee2c827e5 | PASS, independently measured, all threshold counts and maxima unchanged. Both have cognitive >=30 537, >=50 191 and cyclomatic >=30 248, >=50 55, maximum187. static-owner-complexity-common-assembly-b24a0421fa/{base,head,delta}.json. Attribution only: this assembly is NOT actual master or a qualification base. |
| Joint tree versus actual master0576 | Still RED: cognitive >=30 539->537; >=50 190->191; cyclomatic >=30 247->248, >=50 unchanged55; cyclomatic maximum186->187. static-owner-complexity-b24a0421fa/{base,head,delta}.json. No aggregate-budget waiver. |
| Existing collector-source control | RED0.12s: TestPersistenceAuthorityDebtRepair2698ExactSourceAndForwardPolicy. static-owner-collector-source-control-b24a0421fa.log. This refines the earlier real ratchet refusal; it does not replace a required final ratchet. |
| Native unused matrix | PASS, one completed go run ./cmd/swarm-unused invocation: static-owner-unused-b24a0421fa.log. Native-only default/race/issue2413 matrix, NOT Linux/Darwin-union credit. |
| Exact-head preflight | Not qualified. The prior 2a80a9ba3e invocation refused before phases because the exact candidate was not uploaded. No upload or preflight bypass is authorized by this audit. |

## Collector Ownership And Actual Consequence

The real 20-minute-limit ratchet at 2a80a9ba3e failed in391.38s at
debtValidateCollectorIdentity, before numerical comparison. The collector
source and baseline inputs are unchanged by b24a0421fa. Its first precise
source-control refusal is:

```text
native-entry custody lacks its reviewed native owner:
d2212c6e9f90807448b5ee61f2781aee485bf78c28a84a338e9b43fd2e9a142b: <nil>
```

B's debtProcessLeaseCustodyPin is
8aa1f9f4119f9a72056b3a4a0ebde68c462b6b2e8b35f381438ce656815c8560.
The digest intentionally includes the complete StartupPostgresOwner type and
methods. R04 added the bounded activity-journal role and lifecycle-candidate
handoffs; those are real source changes, not a hash nuisance to ignore.
The next existing exact native-owner pin would also refuse composition.go:
expected 1ff439f97646d8df6a4e9d333a4f3b581902278d7e8233fe92f33bb72c01ea36,
actual 9a388b421d837cd0ee7432819eff051444727ce70040cb444cfaf1250d74d021.
The assembly additions are the once-only BindLifecycleCandidates and
BindActivityJournalRole calls on both stores, not a replacement session.

Origin: B's selected-store migration009603a4c9 and policy continuations
5bcd7f8de8/8ff699d871; E's R04 startup additions include85135aafcb and
efba5b57d7. The current collector identity7898e5f7... is older than B's
current local af47497e27 policy d7f0674d...; no later B policy is silently
imported. Master's collector1a353f6c... remains the actual comparison input.

Question for B/E through reviewer-a: supply the bounded, owner-reviewed
composition accounting for these exact original-owner additions, or name
the preceding landing that supplies it. Preserve immutable source packets,
the closed custody check and all baseline ceilings. A will consume the
approved composition and rerun the real ratchet at the actual landing head;
there is no current numeric zero-addition claim.

## Remaining Complexity Ownership

All four paths below are byte-identical between the integrated R04 assembly
0ee2c827e5f1508e56713ec352f7d4f5700bb8f1 and this candidate. That pins the
input origin; it does NOT prove inheritance from master or waive a RED.

| Owner / introduction | Function and real master comparison | Required landing consequence |
| --- | --- | --- |
| B selected-store migration009603a4c9 | postgres.(*Backend).runTransactionOutcome cognitive47->51; added ObserveWriteStartContext custody retained through the transaction. | Original owner supplies a semantics-preserving simplification or its independently qualified upstream repair. A does not remove the write-start protection. |
| E recovery f999fd58d2 | completionRecoverySettlement cognitive49->52; authorizeProviderResumeTx new cyclomatic35. | E owns the takeover/resume predicates and supplies the bounded simplification; no missing precondition is accepted. |
| E original grant attribution2b56a89259 | ReadActivityExecutionGrantTx new cyclomatic34. | Preserve original native grant/source/context validation while clearing the actual admission budget. |

Question for E/B through reviewer-a: pin the owner repairs for these exact
functions, or assign the bounded structural extraction explicitly. The
planner and selected-recovery renames each replace an existing >=30
function; they are not two additional hotspots merely because names differ.

## Landing Composition, Not An Unqualified Joint-Tree Push

Current master is0576b61120. The orchestrator orders #2700 after #2614,
#2730, #2701, #2724, #2723 and #2707; those actual merged trees, not the
local joint assembly or a copied collector baseline, define the final base.
E's R04 ACK-loss/full-resume proof remains separately owned in the existing
handoff; this static cleanup does not discharge it.

1. Consume the named owner repairs and actual preceding merges, preserving
   A's activity writer/projector and E's original-session startup handoff.
2. Reconcile only the necessary A changes onto that base; retain the exact
   original source/event/request/approval checks and historical packets.
3. Run affected focused roots/guards for changed integration inputs, the
   actual debt ratchet with -timeout=20m, complexity and unused. Carry only
   input-equivalent execution receipts; never run a duplicate local tier.
4. Once staging is permitted, upload the exact candidate to the allowed
   non-PR ref and run go run ./cmd/swarm-test preflight. Its real phase
   receipt, not the present missing-upload refusal, is required.
5. Rebind the canonical selection to the actual landing SHA, then post ONE
   ready checkpoint and request the orchestrator's CI slot. No dispatch now.

```text
CI-Tier: lifecycle
Local-Tier: focused
```

No CI-Units line. The approved map remains the implementation map, not
blanket-full credit. Readiness stays BLOCKED until the owner/static and
actual-head preflight requirements above are satisfied.

## Receipt Checksums

```text
a040a6ff21f376dbc81efe718e5416a2f30d108bc49f073f1cf09ce67d436ec9 static-owner-repair-focused-and-guards-count1.log
ba9dd22cb4e0f71b268eb8b9ad58e22b5c2850e49b125d76d95a3c015e159817 static-owner-collector-source-control-b24a0421fa.log
184e70860ebf0b722f7b807ab8e477987015169ddaed3a386cbe22d9fd8fe499 static-owner-complexity-b24a0421fa/delta.json
4d1f3978f56a1c0947dc14b724484327679887c52cb11e88d8eeea90d1550bda static-owner-complexity-bounded-repair-b24a0421fa/delta.json
de2ab1f756f33daa41015980e8e6169426ae9a49c6326ef077bd19404f7fae7a static-owner-complexity-common-assembly-b24a0421fa/delta.json
0b72bac7ae4edc36cf2ca944163ec6817aa21cfcffda6252d2ad7718d7a74be5 static-owner-unused-b24a0421fa.log
```
