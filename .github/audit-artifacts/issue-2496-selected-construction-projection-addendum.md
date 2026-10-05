# Pre-Implementation Coverage Audit Addendum: C22

#2496 / #2525, additive to C21 ruling5967912454. Source: local
`0e3d6d99c`; no C22 production changes made. This is a newly measured remaining
interpreter of the approved construction identity, not a new routing model.

## Observed Failure And Boundary

The real loader and canonical KeylessChild/ValidateConstruction admit declaration
`templ/child`, instance `templ/one/child`, exact parent `templ/one`. The actual
selected readiness producer `Project` emits the authored-coordinate actor
`templ/child`, omits the exact `templ/one/child` actor, and returns zero readiness
states/flows for its exact fixed recipient. Independently within the same probe,
`selectedContractReadinessEntityForRoute` rejects that existing fixed header as
outside the authored scope. These are two production counterexamples, not a
claim that the component probe executed a fork or established live admission.

Small component command (three repetitions, race detector):
`go test ./internal/runtime/runforkreadiness -run '^TestSelectedKeylessActorProjectionProbe$' -race -count=3 -v`
RED on all three, package1.990s. Probe source retained in
`issue-2496-selected-construction-projection-probe.go.txt`; copy it temporarily
to `internal/runtime/runforkreadiness/zz_construction_projection_probe_test.go`
to reproduce. It is a retained diagnostic, not a skipped default regression.
Raw local receipt:
`/tmp/agent-e-2496-0e3d6d99c-projection-probe-red.log`.

Current-base confirmation: local source
`2abec8556117d30c61f892e23ebb213fe8d6b74f`, rebased onto
`origin/master e63f4bdb197e473fe35e94f65ff3c77655957123`. The same retained
component probe is RED in all three race repetitions, package3.290s:
`/tmp/agent-e-2496-2abec8556-c22-probe.log`. No production changes were made
for this probe. This confirms both omitted projection and receiver rejection
still survive the merged master; it does not prove actual normal/selected
execution. Independent bounded disposition is still requested below.

Working class remains complete constructor/header ownership of identity and
readiness, including selected preparation. Immediate parent #2411, broader
architecture parent #2250; #2497/R5.2 is not absorbed. The first failing producer
was an entry point: a sibling probe also found the fixed receiver lookup.
Current honest closure is substantial partial migration, NOT class elimination.

## Owners And Systematic Consumer Census

Canonical owners remain engine.flow_constructor, its committed header and
flowidentity.Instance/ParentRoute, the immutable declared agent record, and the
selected fixed-revision plan. Preparation cannot read mutable source rows,
invent fork headers, infer parents, or grant ancestry-based routing permission.

| Production consumer | Audited disposition / proposed repair |
| --- | --- |
| manager.staticAgentBlueprintRecords | Emits source-only globally authored coordinates. Its executable consumption must not create a keyless actor below a keyed parent without that instance's exact construction. Proposed migration to the existing construction/attachment authority, not a path heuristic. |
| manager.staticFlowRequiredAgentBlueprintRecords | Same sibling producer for required declarations; census and exact construction proof required even though the current reproducer names a declared worker. |
| manager.resolvedStaticTopologyBlueprints | Both producer families feed CompileStaticTopologyDesiredAgents (serveapp complete source-set construction), PrepareStaticTopologyForStartup, and PrepareDurableTopologySourceSetRebind individually. Each must preserve exact construction ownership and existing topology/generation semantics; no phantom executable actor. |
| manager.VerifyStaticAgents / VerifyStaticFlowRequiredAgents and their record wrappers | Definition/test-only consumers: no production callers found. Retire if redundant rather than treating their API existence as proof of executable startup coverage. resolvedStaticTopologyRecords is likewise definition-only. |
| manager.PreRunAgentMaterializationBlueprints | Non-executable provider/declaration probe is a different concept: retain preflight without creating state or granting a concrete route. Do not remove useful preflight to repair execution. |
| runforkreadiness.StaticAgentBlueprints -> Project | Actual selected producer emits the wrong coordinate. Consume exact fixed header/config and declared owner through the existing preparation/attachment primitives. No source-row lookup or fabricated fork state. |
| Project.recordState | Currently builds actors/flows only when mode=template; keyless descendants keep static mode and still need their exact fixed construction. Retire the template-only execution shortcut, not the mode distinction. |
| manager.TemplateFlowMaterialization | Its explicit template-mode requirement is correct for this existing API but cannot supply a keyless constructed projection. Reuse the existing concrete FlowInstanceActivationRequest/flowInstanceAgentMaterializationBlueprints owner; do not relabel keyless as template or weaken this API's admitted mode. |
| selectedContractTemplateAgentWorkflowState / selectedContractTemplateFlowForPlan | Non-template recipient is dropped. Generalize the receiving construction consumer without admitting a new recipient or widening the fixed frontier. |
| selectedContractReadinessEntityForRoute | Measured rejection. Consume exact fixed header/config and constructor validation instead of authored-path `Owns` equality. Activity and node callers must receive the same correction and preserve their own declaration/handler/target gates. |
| preparation and bus.SelectedInputValidation | Already consume AgentConstruction/C21; their fixed recipient equality and source-run/child-run binding stay binding. A green direct component does not cover the missing upstream producer. |
| pipeline.canonicalHandlerRoute / workflowInstanceRouteForExecution | Different non-authoritative context: production claimed execution first prepares/stamps DeliveryTargetApplication and consumes application.Route instead (coordinator.go and engine_bridge.go). The authored-string branch remains only for the definition/test-only preview API; no production PreviewContractHandlerExecution caller found. Its route is not construction, grant or permission to create missing state. Do not use this branch as a selected readiness repair. |

No production edits to these newly found producers are made before the issue/gate
record is repaired. Source declaration lookup itself, pre-run provider probing,
and routing reachability are not replaced with construction or ancestry checks.

Execution path: fixed artifact/revision -> exact frontier -> readiness projection
and provider preparation -> selected admission -> materialization -> binding ->
agent execution -> settlement/activation/readback -> owned retirement. Artifact,
frontier, grant and retirement are different already-governed concepts with their
existing refusal tests. Projection/receiver selection here is the same chosen
class; later execution is unreachable if preparation omits its actor.

## Required Proof And Tracking Decision

| Known manifestation | Planned exact proof |
| --- | --- |
| Selected projected keyless actor missing; authored phantom present | Make retained producer counterexample permanent and green; assert exact actor census for one/two keyed parents and one/two keyless levels, declared and required agents. |
| Fixed receiver lookup refuses constructed path | Exact fixed-header lookup matrix for agent, node and platform-activity consumers; missing/duplicate/foreign parent/run/source/declaration/entity facts fail before effects. |
| Normal source-set blueprint installs phantom descendant | Real normal construction/attachment and startup/restart both stores: no authored phantom; exact independently owned constructed actors; unchanged root/static controls. |
| Selected input execution beyond root/static control | Both-store actual selected nested-agent execution and activation/readback, fixed-frontier/explicit-target refusals, sibling/equal-local-name isolation, source bytes unchanged; no synthetic test counts as public-launcher qualification. |

Sibling/parent action: absorb these same-concept producers now under existing
#2496/#2525, not leave a false C21 closure. Tracker decision: additive issue/PR
audit repair and independent bounded consumer ruling required before C22 coding.
No new issue requested. Watchlist decision: canonical origin/master's
runtime-operations.yaml node shutdown_and_runtime_lifecycle already maps2496,
the widened constructor/header/attempt obligation and C21 producer handoff;
its source-set and selected consumer families require this upstream probe,
not a local-helper closure. Refine that existing node with producer and fixed-receiver bypasses; no POTENTIAL_ISSUES
entry or new broader model. #2411/#2250 remain open for their other child groups;
#2497 and A's join/B's generation ownership stay separate. Remaining same-class
tail: these two producer/selection groups, then composed qualification; moderate
confidence, subject to the normal source-set execution census. No new framework.

Existing authoritative refs: platform-spec.yaml#engine.flow_constructor
(recursive_commit, readiness), #agent_identity_model.concepts.declaration_execution_scope
(execution_agreement, producer_consumption), and selected fixed preparation/
frontier contracts. The current spec already requires exact construction;
amend the exact producer/projection wording with the implementation if needed.

Closure commitment: eliminate the chosen class entirely in this PR, not merely
make the local probe green. Fixing C21 endpoints alone leaves these live
interpreters. One bounded construction-backed consumer migration is proposed;
no new identity derivation, naming, routing permission, joins, lifecycle phases,
replay support, compatibility path or generic framework. Estimate: roughly one
focused implementation/proof pass; moderate confidence, high ROI because both
normal and selected consumers otherwise contradict the constructor.

Independent ruling requested: approve this C22 consumer extension inside
#2496/#2525, or specify a split. All already-authorized repairs and managed
verification may continue while these producer files remain unchanged. Any
solution requiring inferred ancestry permission, weakened targets or new replay
semantics must instead stop for LEAD.
