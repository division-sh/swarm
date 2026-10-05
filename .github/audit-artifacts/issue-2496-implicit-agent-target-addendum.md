# Focused Consumer Escalation: Implicit Local Agent Targets

The repaired stopped-root comparison is execution-proven on `3983f3a13`:
three Manager roots pass `-race -count=3` (6.375s), including nine exact/negative
root-preparation cases. The complete paired-store provider matrix now passes
startup but remains RED (66.83s), in its `agent-consumers` and `agent-replay`
text-message rows. These are not waived or attributed to baseline flakiness.

An unchanged SQLite `agent-consumers` leaf passes on clean master
`276d7723f` (5.954s). The repaired candidate's diagnostic leaf still fails
after exact typed pipeline settlement: Replayable=0, Acknowledged=2,
TerminalNonSuccess=0. All three declaration-owned agents in that exact run are
active/running with correct scoped subscriptions. Only the local node appears
in public event delivery readback. Waiting for settlement rules out the
earlier suspected premature readback; it does not repair agent selection.

## Execution And Ownership Census

Provider admission -> immutable exact standing run/service identity -> selected
run source-owner resolution -> canonical local event keys -> subscribed-agent
descriptor selection -> recipient manifest -> active-agent target projection ->
atomic publication -> post-commit dispatch/settlement -> public readback.
Construction/readiness/startup have succeeded before the failing selection.

The approved construction work correctly keeps a static declaration-owned
agent's EntityID absent rather than transferring the constructed node's entity.
The bus's `ordinaryPublicationSource` carries the exact constructed local
source route, including its entity. `resolveDeliveryRecipientManifestWithSource`
passes that route as a singular recipient target;
`deliveryTargetForDescriptor` / `routeMatchesAgentDescriptor` then compare the
source's nonempty node entity with the agent's deliberately absent entity and
exclude the agent. `resolveActiveAgentTarget` also correctly refuses an explicit
entity-bearing blueprint for an entityless agent. Restoring the old agent
entity or globally weakening that explicit-target refusal is not acceptable.

Relevant canonical owners are the existing selected-run source projection,
typed `RunScopedFlowInstance` / concrete agent identity, and closed active-agent
delivery-target projection. The local helpers above are entry points, not the
audit boundary. The remaining consumption families are:

| Seam | Disposition And Required Proof |
| --- | --- |
| Provider raw/normalized implicit local agent delivery | Same chosen class; complete both-store provider matrix, including agent replay and source/other-alias exclusions. |
| Ordinary root/static local subscribed agents | Same implicit-source consumer; exact run/flow route positives and crossed-run/instance negatives before any delivery write. |
| Explicit entity-targeted agent delivery | Different authority: requester declares an entity target. Preserve `TestExplicitAgentTargetPreservesEntitylessAndSelectedEntityOwners` and exact foreign-target refusals. |
| Explicit root/entity agreement | Different declared-target authority; preserve `TestRootAgentTargetAgreementRequiresExactRunFlowAndEntity` and root-owner contradiction refusal. |
| Connect-created and pending lifecycle recipients | Existing initialization/grant authority remains mandatory; preserve pending/materializing and speculative-owner controls. |
| Same-flow declaration absence | Existing descriptor authority, not permission to inherit a node entity; preserve `TestSameFlowDeclaredEntitylessAgentsDoNotInheritSelectedNodeOwners`. |
| Node recipients | Existing constructed-header target remains authoritative; no node target or constructor is made entityless. |

## Proposed Bounded Disposition

Before production planning changes, please confirm the exact implicit-source
projection: after consuming the admitted local source and exact current
run/flow-to-agent route identity, a declaration-owned entityless agent receives
an entityless target for that same flow instance. A source node's entity is not
a requester-authored entity target for the agent. Explicit target routes retain
their present exact entity checks; absence is not a wildcard or inferred grant.
No event-name allowlist, fabricated source, generic continuation framework,
replay expansion or compatibility path is proposed.

This remains the #2496/#2525 construction/attachment/publication class; immediate
parent #2411 and broader #2250 remain open. Tracker decision: repair the
existing consumer/gate accounting here, absorb the bounded omission in this PR
after confirmation, not a follow-up or new owner. Watchlist mapping remains
exact construction/attachment, agent topology and receiver ownership. The
deeper smell is conflating a node source's business entity with a declaration's
execution coordinate; finish typed owner consumption rather than introduce a
second routing abstraction. Estimated effort is one bounded planner/proof pass,
high ROI; no parent-tail estimate change.

Existing failure-discard and optional-header interpretation addenda remain
separate. Production implicit-agent planning is unchanged pending this precise
consumer disposition. The default 14-unit managed suite and fresh 45-cell
describe pass on `d32c35033`, but neither includes this descendant repair or
turns the supplemental RED matrix into chosen-class closure. No push, final
review request or merge claim.
