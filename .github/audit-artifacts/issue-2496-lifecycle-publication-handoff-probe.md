# #2496: Lifecycle Publication Handoff Probe

Additive qualification finding, not chosen-class closure or another full audit.
Gate5916752092 remains binding. Base is d64b515c5 with the constructor projection,
API fixture and routing-pattern repairs recorded in the working diff. No
production publication handoff change has been made for this finding.

## Actual Reproduction

`TestServedParityHarnessRunControlLifecycle` now reaches an eagerly constructed
root initial gate. The first real item.received handler advances the root to
waiting and correctly supersedes that gate. Before run.pause, the probe waits
up to the existing five-second test budget for the canonical pipeline owner's
initial work to settle. Both stores remain at Replayable=1, Acknowledged=1,
TerminalNonSuccess=0, Deferred=0. The unsettled event is the real
mailbox.card_superseded occurrence, with no pipeline receipt.

Receipt: /tmp/agent-e-2496-run-control-initial-settlement.log, aggregate RED,
12.957s. This is an actual served runtime/public-control setup with selected-store
readback, not a mocked publication. No event exclusion, fabricated receipt,
relaxed handed-only assertion or production timeout change was used.

## Owner And Execution Path

Creating input -> O2 initial lifecycle/card commit -> normal claimed handler ->
prepareWorkflowLifecycleMutation -> planWorkflowGateEffect -> named atomic
CommitWorkflowEngineMutation -> FinalizeEnginePublications -> typed engine
commit result -> transferCommittedHandlerFollowUp -> post-commit dispatcher.

The inconsistency is in the existing pipelineEngineMutationOwner:

- engine_adapter.go prepares handler EmitIntents, then lifecycle.Emissions, then
  immediate activity request publications, in that exact order.
- commitPreparedEngineMutation commits and finalizes every publication, but
  selects result.EmitIntents using only len(mutation.EmitIntents). The additional
  lifecycle emissions never enter the returned follow-up.
- transferCommittedHandlerFollowUp dispatches only the returned emissions and
  exact activity requests. A handler with no authored emit therefore cannot
  transfer its independently committed gate-supersession publication.

The node's acknowledged state/claim is real; this is not permission to repeat
the handler or construction. The emitted event is durably retained, not missing
from the journal. Eager initial-entry construction exposed this older consumer
omission in a previously passing handed-only public-control proof.

## Bounded Disposition Request

Proposed repair: carry the complete declared non-activity emission list through
the existing named mutation owner and typed commit-result follow-up, validating
its exact committed publication IDs/order before dispatch. Preserve the distinct
exact activity-request mapping, commit-error authority, enclosing publication
settlement, cancellation, retained outbox ownership and existing retirement
joins. No new framework, detached dispatch, retry registry or compatibility seam.

Please confirm this bounded existing-owner repair in #2496, or explicitly split
the distinct publication class. Construction/readiness closure cannot be claimed
while the required public run-control journey fails. The three earlier authority
requests remain separate; this finding does not settle unknown Begin responses,
constructed-header read ownership or standing-generation root coordinates.

Planned proof: red-first typed commit-result regression for handler-only,
lifecycle-only, combined and activity-request mixtures; the unchanged both-store
served pause/handed-only continue/stop journey; genuine publication failure and
cleanup controls, followed by managed qualification. No production edit until
the disposition is recorded.

## Executed Red-First Result-Consumption Matrix

The parked source is
issue-2496-lifecycle-publication-handoff-probe.go.txt in this directory. Its
TestCommittedEngineTransfersDeclaredLifecyclePublications exercises the
existing commit-result consumer with handler present/absent, lifecycle emission
present/absent, activity request present/absent, and success/error/panic/cancelled
cleanup: 32 cells. Sixteen without lifecycle emissions pass; all sixteen with
lifecycle emissions fail because the acknowledged non-activity publication list
is truncated. Exact activity-request IDs, publication order, finalization count,
release disposition and genuine error causes remain asserted.

Actual receipt: /tmp/agent-e-2496-lifecycle-publication-handoff-unit-red.log,
RED0.011s. This is unit result-consumption proof, not real SQL commitment,
physical attachment, served dispatch or closure evidence. The diagnostic source
is retained as text rather than a compiled regression while the scope decision
is pending. Production engine/publication code remains unchanged.
