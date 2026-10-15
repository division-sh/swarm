# Stop Nonterminal Witness Semantic Amendment

Part of #2542 / #2151. Binding reviewer amendment:
https://github.com/division-sh/swarm/issues/2542#issuecomment-6153193834.
Signed predecessor bb42d634bc2810223a91ded22e26182a27bcea90.

## Concept, Owners And Scope

The chosen class is incorrect or duplicated interpretation of stop-quiescence
delivery cardinality. The witness includes every physical delivery row at the
exact run with persisted status pending, in_progress or failed; it excludes
delivered, dead_letter and canceled. Both canonical subscriber types contribute,
including claimed work and future retries without a session. No eligibility,
header, source, route, retry-budget or deduplication filter is added.

Binding persisted status authority is deliverylifecycle/model.go: Status and
Status.Terminal(), platform-spec.yaml event_deliveries DDL and the existing
production delivery/adapter.go terminalizeRunTx predicate. The public run.stop
contract is unchanged. platform-spec.yaml now records the fixture-observation
contract explicitly. This is an authorized semantic correction, NOT an
equivalent-query migration: processing/retrying were never persisted statuses.

The canonical projection remains delivery.ReadStopPendingDeliveryCountTx.
RunStopStorage.Pending, issue2394StopSnapshot.PendingDeliveries and
stopCommitEvidence.pending are historical field names for unfinished work, not
queued-only work. No compatibility aliases or accepted old status values exist.
The scalar port CountRunStopNonterminalDeliveriesForTest and its storetest bridge
use the original selected native read coordinator and return only a detached int.
The served adapter accepts only the original snapshot-reader capability.

The complete served snapshot also consumes fixed pipelinepersistence intent/
outcome projections, the existing physical run-event observation, the existing
runforkrevision revision counter and a separate fact-revision counter, and
runlifecycle's exact inner-join control-clock projection. These are detached
reads through existing owners, not a new aggregate or snapshot framework. Its
thirteen former raw-carrier calls now carry only the captured native reader.

## Exhaustive Consumption

| Consumer family | Final owner consumption and preserved cuts |
| --- | --- |
| Standalone stop storage | ReadRunStopStorageForTest retains its single original read transaction, left-join/empty-control summary and revision count; it consumes the corrected projection. |
| Paused-control faults | Existing standalone stop-storage observations consume the same projection; writer admission, refusal and sibling/run/revision assertions remain. |
| Bus foreground/recovery stop | Public ReadRunStopStorage bridge retains every claim, drain, mutation and required-publication assertion. |
| API stop reconciliation/replay | Public ReadRunStopStorage bridge retains receipt, no-repeat-transition and durable-state assertions. |
| Served/selected twenty-intent stop and rollback envelopes | readIssue2394StopSnapshot uses the corrected scalar through its captured ReceiverStateReader. Its ordered intent/outcome rows, three separate physical counts, INNER JOIN, missing-control refusal and CAST(updated_at AS TEXT) are untouched. No left-join summary substitution. |
| Known rejection, COMMIT uncertainty/replay and stop/chunk contention | readStopCommitEvidence propagates its original selected owner and consumes the scalar within the original unordered five-witness inventory. The first left-join summary and each other independent count cut remain; real driver COMMIT/rollback hooks are unchanged. |
| B16 independent parent and publication-group races; B18 accepted paused group | All eight additional commit-witness calls propagate the original raw selected owner, not the fan-out/group wrapper. Independent pools, real locks, no-mutation assertions and group receipts remain. |

Exactly three live impossible-status interpreters were found: the canonical
projection and the two raw witnesses. The latter predicates are removed, not
separately corrected. Production terminalization and run summaries were already
correct. Deployment pending/failure buckets and derived StateRetrying are
different concepts, not modified. The owned-source AST inventory rejects any
event_deliveries stored-status SQL containing processing or retrying.

The served snapshot and equality helper have no raw parameter, rows, query,
scan or getter left. Their complete fixed-query correspondence is checked by
TestFamily145StopSnapshotKeepsExactPhysicalQueriesAndNoRawCarrier, preserving
all seven separate cuts. The commit witness still has its different left join
and four other independent raw counts; rig setup and physical faults remain
#2542 parent debt. No generic SQL callback, selector, pool getter or
reconstructed coordinator was introduced.

## Native Proof And Finite Source

New native proofs compile real admitted source containing declared node and
agent subscriptions. Source-aware structural child construction and actual
Manager readiness admission are required. Delivery publication and claims use
the genuinely issued generation grant; no fabricated hash, lease, authority,
raw status edit or retry-time rewrite is used. Supported compiled termination
cancels an actual queued agent. This is selected/native component evidence,
not process-restart qualification.

RED before the correction: stop-nonterminal-native-before.jsonl, both stores
under race,13.279s. Real claimed node/agent and failed retry rows made the old
witness report0 instead of2, then0 instead of1 after node success. The initial
fixture compilation/admission failures were setup diagnostics, not this RED proof.

The exact fifteen-declaration semantic amendment is captured by the committed
capture-stop-witness generator and family145-stop-witness-amendment.json:
87b00ec86800b2f78e9af3947b503d9c60bee78640bef7e20d52d01966441400.
Complete immutable preimages, amended outputs, native owner propagation and all
other assertions/cuts are pinned with hostile alterations and finite replay.
The existing recipe ledger retains every historical Before and reserved
Successor;1510 recipes /814 transitions now cover the complete caller chain.
Transition digest 0c29f1adbd3d1792bd7e52b54395806fe640efeb0522156fd7245b73e6b89d70.
The finite/idempotence inventory root is explicitly included after the prior F1.

An adjacent historical bus SQL guard correctly rejected the changed predicate.
Its old source record stays immutable; exact full-declaration correspondence to
the pinned predecessor is checked separately from the authorized current
predicate and native semantic proof. Other status/control/revision predicates
and every field mapping still compare exactly. No general inverse or exemption.

## Manifestations

| Manifestation | Disposition | Exact proof |
| --- | --- | --- |
| Claimed/in_progress and failed rows omitted, including a target without pending work | reproduced and fixed | TestRunStopWitnessCountsRealClaimAndRetryBothStores, RED before / GREEN after, actual node and agent claims, retryable failure and successor claim/version, success settlement. |
| Future failed work, physical multiplicity, subscriber types and same-status foreign run | execution-proven through the same corrected path | Same native root plus stopNonterminalNativeHeldStop: hour-future retry, two physical node rows, one agent row, independent foreign claimed run. |
| Delivered/dead_letter/canceled incorrectly contributing | execution-proven through the same corrected path | TestRunStopWitnessExcludesNativeTerminalStatesBothStores: native success/dead-letter settlement and compiled queued-agent termination; one pending row remains. |
| Acknowledged stop retaining claimed or failed work | execution-proven through the same corrected path | Native held-stop helper requires acknowledged actual stop, zero count and explicit terminal snapshots of both exact prior claims; foreign run unchanged. |
| Scalar adding aggregate/header dependencies or returning provisional evidence | execution-proven through the same corrected path | TestRunStopStoragePreservesExactRunCountsAndOriginalReadBothStores: exact one additional read/no write, headerless zero, canceled/closed/invalid/foreign owner refusal with zero evidence. |
| Paused control/fault no-mutation evidence | execution-proven through the same corrected path | TestPausedRunControlFaultPreservesRunDeliveryRevisionsAndOriginalWriterBothStores. |
| Known COMMIT rejection; rollback-before-ack; committed lost-ack/replay | execution-proven through the same corrected path | TestRunStopKnownCommitRejectionBothStores and TestRunStopCommitUncertaintyPreservesRealOutcomeBothStores, unchanged real driver/constraint cuts. |
| Stop/chunk and publication-group lifecycle interleavings | execution-proven through the same corrected path | TestIssue2394StopCommitRaceBothStores; TestB16IndependentPostgresParentStopRetainedGroup; TestB16ParentStopVersusPublicationGroupBothCommitOrders; TestB16ParentStopContendsWithGroupSettlementCommit; TestB18PausedPreparedGroupRetainsAcceptedWorkBothStores. |
| Bus foreground claim and joined recovery fences | execution-proven through the same corrected path | TestRunStopPreservesForegroundClaimFenceBothStores and TestRunStopDrainsRecoveryBeforeMutationAndRequiredPublicationBothStores. |
| API committed transition reconciliation/replay | execution-proven through the same corrected path | TestOperatorRunStopDoesNotReplayCommittedTransitionAfterReconciliationFailure. |
| Served stop authentication/transport refusal, lost reply, restart replay and suffix cancellation | execution-proven through the same corrected path | TestIssue2394ServedStopTwentyBothStores and TestIssue2394SelectedStopTwentyBothStores. |
| Served invalid-capsule and cancellation-statement rollback | execution-proven through the same corrected path | TestIssue2394StopFailureEnvelopeBothStores, both original fault cuts and full rich-snapshot equality. |
| Historical source/status contract contradiction | reproduced and fixed | TestNativeBusStopStoragePreservesPhysicalPredicatesAndEveryField; exact semantic-amendment pins and unchanged historical SQL correspondence. |

## Qualification Receipts

Evidence prefix /home/youmew/.cache/swarm-2542-family145/stop-witness-.

- native-supported-final: PASS17 roots/61 passes across runtimepersistence,
  bus, apiv1 and serveapp, under race. Package times81.782/43.741/17.561/89.195s.
  After complete served helper retirement, complete-snapshot-final separately
  passes all five changed roots/17 passes, private16.608s and served65.144s;
  unaffected earlier receipts carry.
- foreign-claim-final: PASS17.004s; original native claim/retry/stop root plus
  explicit settlement of the independent foreign claim before joined cleanup.
- complete-snapshot-native-guards: PASS37.034s, both-store stop storage,
  inner-join missing-control refusal/physical-clock checks and four boundary
  guards. complete-snapshot-final includes the added all-port invalid/canceled/
  closed/refusal and headerless-empty controls.
- complete-finite-controls: PASS86.301s, complete reviewed1510-row inventory,
  814 admission transitions, historical receipts, exact stop/physical-query
  pins, hostile alterations, original bus claim/drain and SQL source controls.
- complete-ratchet-downward: PASS163.669s,37 debt findings/33 raw sites removed,
  zero additions. Residual debt6799/4573 ->6762/4540;67 excluded occurrences
  and collector64b8aced371482a1b0ef4e7e9552d39a6190f122b8ae231e8c51a898b6fe7d44
  remain exact. complete-ratchet-readonly: PASS30.151s. Global extractor totals
  54269/38516 include already-classified backend SQL and are not residual debt.

Initial scalar-only propagation correctly failed the ratchet: adding a reader
argument changed thirteen retained raw-carrier call identities. No transition,
deletion credit, ceiling increase or classifier change was made; completing the
served raw-helper retirement eliminated the escape instead. A diagnostic probe
was temporary and is deleted. The first census also timed out in go/packages
head loading at an agent-selected5m limit, with no complete census. Final runs
use the existing committed census unit's20m Go timeout; no test, job or budget
was raised, and no pressure-cause claim or timeout-only PASS is made.

Final structural, unused/complexity and signed-head receipts are reported with
the checkpoint. No duplicate local tier, server2 run, push or PR is authorized
here. Preflight remains mandatory before an eventual clean, rebased authorized
push, not claimed as run on this unre-based local checkpoint.

## Closure And Tracking

The stop-witness working class aims at entire closure across all three live
interpreters, not merely a shared-owner claim. The symptom was an observation
spot inside the broader persistence-authority class; it was an entry point,
not the boundary. #2542/#2151 remain open for the other fixture/read/fault and
construction families, strict completion guards, SQLite fork/deadline,
history/overlay and integrated landing proof. No tail obligation is waived.

Watchlist decision: refine the existing #2542/#2151 fixture-observation mapping
with this reproduced status-classification manifestation, as the binding ruling
directs. No new issue, node, POTENTIAL_ISSUES entry or framework is needed.
Architecture feedback is narrow: even a named test projection can duplicate a
persisted classification incorrectly. The existing delivery owner is the better
long-run seam; the native falsifier is necessary in addition to source ownership.
This repair is promoted now. Remaining commit/rig fixture retirement stays
tracked in the existing parent; the class tail estimate changes only by the
actual37 findings/33 raw sites removed. High ROI is avoiding false zero-quiescence proof;
no broader lifecycle abstraction or production-policy change is justified.
