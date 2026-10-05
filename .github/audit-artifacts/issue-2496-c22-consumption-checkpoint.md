# C22 Local Consumption And Qualification Checkpoint

Issue #2496 / PR #2525. Production/test/spec batch `6dfc1ffe7`, above the
separate test-only handler-contention commit `5342a3fb5`, base `e63f4bdb1`.
Local only. This is not the final Post-Implementation Proof Audit or a claim
that the working failure class is closed. Do not use package GREEN as a waiver
for the native aggregate-refresh RED, the original default-suite RED, final
qualification or exact-head CI.

Housekeeping: active worktree moved unchanged from `/tmp` to
`~/dev/swarm/worktrees/agent-e-2496`. The two native refusal/timeout raw
receipts are preserved as `issue-2496-c22-native-*.log.gz` beside this file;
decompression reproduces the SHA256s in the structured qualification ledger.
Other receipt paths record historical scratch locations, not live dependencies.

Binding C22 ruling: issue comment5969425820. Fixed-frontier consumer census
repair: comment5969942426. Additional native grant/fence disposition requested:
comment5970534024. Split ruling5970914706 authorizes terminal ordering only;
its implemented/tested checkpoint is `issue-2496-terminal-drain-checkpoint.md`.
Survivor attempt/grant transfer remains held for user/LEAD/B. No native grant
owner was edited on assumption.
No rebase was performed merely because master advanced, and no running
qualification was interrupted. Final rebase is conflict-only, immediately
before a qualified batched push. No push occurred.

## Concept And Owner Consumption

The concept is exact constructor-owned actor/header/configuration identity,
including a keyless child under a keyed parent. An authored declaration names
`templ/child`; its recorded instance can be `templ/one/child`. Parent identity
is construction context, never delivery permission. Static remains static and
its independently admitted entityless agent remains entityless.

Canonical owners are the existing constructor and committed workflow header,
`flowidentity.Instance.ValidateConstruction`, immutable agent declarations,
the selected fixed-revision plan, and the existing readiness attempt/lifecycle
authorities. The new shared fixed-header decoder is an adapter to those facts,
not another constructor or grant issuer. Complete attachments, occurrence-bound
route proof and actual dispatch associations remain distinct.

This is the complete currently known direct production-consumption census for
the migrated owners, from exact symbol searches excluding tests. Transitive
callers and named execution proof are listed individually rather than credited
solely because they use a shared helper.

| Consumer | Disposition and actual proof boundary |
| --- | --- |
| `CompileStaticTopologyDesiredAgents` | Moved to root-only declaration execution plans. Complete Manager tests prove its desired inventory; normal native construction installs the real flow actors. It cannot manufacture an authored-path keyless actor. |
| `PrepareStaticTopologyForStartup` | Same root declaration owner, with separately construction-owned flow attachment. Native four-cell constructor/attachment/start/restart proof passes under race. |
| `PrepareDurableTopologySourceSetRebind` | Root declarations plus existing retained flow actors. Manager survivor/config/refusal controls pass; native adjacent grant refuses and aggregate retirement hangs. **Not execution-proven closed**; disposition requested in5970534024. |
| `PreRunAgentMaterializationBlueprints` -> `runtime_claude_startup` | Kept as non-executable declaration/provider preflight, including descendant declarations. It does not authorize a concrete route or lose provider census merely because executable source-only producers are root-only. Manager and selected execution package controls pass; no real-provider qualification claimed. |
| `StaticAgentMaterializationRecords` / `StaticFlowRequiredAgentMaterializationRecords` | Retained record projections consume their canonical root-only blueprint producers, without defaults or alternate nested admission. They are different representations, not another actor-identity interpreter. Manager package controls pass. |
| `runforkreadiness.StaticAgentBlueprints` -> `Project` | Root declarations only, then all known fixed constructed headers' declared/required actors. Permanent complete-census counterexample passes race3; declared/required agreement and exact equal-name siblings are checked. No recipient-only or future-instance census. |
| `Project` construction and attachment projection | Moved to `ConstructedInstances` / `FixedConstructionForRoute` and `ConstructedFlowMaterialization`. Complete parent/config persists, including two keyed parents, sibling branches, two keyless levels and fieldless/field-bearing headers. Complete-census race3 and actual nested selected execution race3 pass. |
| `AgentConstruction` -> selected preparation | Shared fixed decoder plus immutable exact declared-agent validation. Corrected complete-header/corruption matrix passes race3; selected package/provider preparation controls pass. No live-source lookup or invented fork header. |
| `AgentConstruction` -> selected-input validation | The same exact header/declaration owner, retaining fixed recipient and explicit-target checks. Actual selected root/nested input execution passes both stores race3 and normal; source snapshots and sibling isolation remain strict. |
| `selectedContractNodeWorkflowState` | Shared fixed header/config with distinct node/handler admission. Complete selected package and constructor-before-node/agent catalog journeys pass both stores. Missing receiving authority still refuses. |
| `selectedContractPlatformActivityWorkflowState` | Shared fixed header/config, preserving event/producer/entity/kind ownership. Complete selected package real activity controls pass; out-of-scope refusal fixture now supplies its source-run prerequisite and passes race3, rather than stopping at missing-run admission. |
| `installContractFrontierFlowInstanceRoutes` | Complete known exact constructed-route census, not prefix reconstruction. Actual nested selected execution and route-history controls pass. Idle attachments cannot fabricate frontier events. |
| `selectedRouteHistoryDynamicFlowInstances` | Intersects validated construction with actual fork-point/pending/frontier occurrences. A keyless authored-coordinate instance is static topology; a keyed-construction concrete coordinate uses the existing dynamic proof. Complete history refusal controls and nested selected execution pass race3. No ancestry permission or replay expansion. |
| `activateSelectedContractRunFork` recovery | Consumes `Projection.MaterializationStates`, not just dispatch states or template mode. Complete selected package recovery/config/typed-point controls pass. Attachment evidence carries no invented SourceEventID. |
| Selected-store execution materialization | Consumes the same `MaterializationStates`, validating dispatch/attachment agreement before the existing transaction writes complete readiness. Actual selected root/nested execution/readback passes both stores. No new transaction framework. |
| Selected runtime `bindRun` | Consumes `runfork.ProjectParentRoute`, preserving non-root parent coordinates and remapping only the exact source root. Clones plans before binding; actual nested selected execution and sealed-plan/parent controls pass. |
| Selected-store `projectParentRoute` | Delegates to that same existing entity-ownership projection. Actual selected nested materialization/readback passes both stores; no independent parent reconstruction. |

All other existing constructor, join/entry and generation/authority owners
remain governed by the prior audit and A/B handoffs. This additive census does
not replace P01-P60/C01-C21 or claim those historical proofs qualify this head.

## Retired Interpreters And Distinctions

- Delete the production path-prefix/template reconstruction helpers in frontier
  routing/history; fixed construction supplies declaration and parent context.
- Delete production `VerifyStaticAgents`, `VerifyStaticFlowRequiredAgents` and
  `verifyStaticAgentRecords`. Their useful assertions move to actual owner
  entry points. `resolvedStaticTopologyRecords` exists only as a test helper.
- Delete redundant selected static-record wrapper. Its test consumes canonical
  root blueprints and exact constructed-flow materialization separately.
- Rename the generalized carrier to `FlowInstanceMaterializationPlan`, without
  a compatibility alias. Keep `TemplateFlowMaterialization` template-only;
  exact constructed materialization validates the existing constructor identity.
- Remove template-only receiving/recovery shortcuts and mutable plan binding.
- Preserve non-executable declaration preflight, root/static identity,
  construction receipts, exact configured agent entities, #642 deferred-work
  refusals, source bytes, explicit targets and existing admission authority.

## Executed Evidence And Open Closure

Structured local receipts and SHA256s are recorded in
`issue-2496-c22-local-qualification.json`; raw logs remain named there. Receipt
counts include repetitions and are not a unique full-suite coverage count.
Earlier working-tree receipts are not final-head hosted CI evidence.

| Manifestation | Actual evidence / current state |
| --- | --- |
| Authored phantom and omitted exact constructed actor | Permanent `TestSelectedConstructedActorProjectionCompleteCensus`, race3 GREEN; full exact census and phantom absence, not an early-return match. |
| Fixed header/config/declaration/parent corruption | `TestSelectedAgentConstructionUsesExactFixedHeader`, corrected valid positive baseline and unchanged twelve fault oracles, race3 GREEN. First malformed positive-fixture RED is preserved. |
| Ordinary construction/startup/attachment/restart | `TestRuntimeConstructedActorCensusRestartBothStores`, race/count1, four field/backend cells GREEN46.563s. Real native constructor/lifecycle composition, not synthetic Manager persistence rows or public launcher/provider proof. |
| Actual selected root and nested execution | `TestSelectedConstructedAgentInputExecutesBothStores` and `TestSelectedNestedConstructedAgentInputExecutesBothStores`, race3 GREEN, selected package135.180s. Exact actor/config/parent, source bytes and sibling isolation. Full selected normal package GREEN125.746s after source-run fixture correction; optional credential-free Docker root was not opted in and is explicitly SKIP. |
| Receiver construction/readiness/recovery integration | Six complete catalog roots on both stores GREEN96.975s, including nested geometry, constructor-before-node/agent, selected publication frontier, flow-owned readiness and pending root input. Compiled internal mock-lifecycle/public RPC fixture boundary; not real-provider/public publication qualification. |
| Native adjacent readiness grant | `TestAgentLifecycleReadinessSourceSetRebindBothStores`, both stores RED1.804s, exact `readiness_activation_attempt_not_current`. Isolates durable admission without a live-loop cleanup hang. Pending5970534024. |
| Failed native aggregate refresh/retirement | Aggregate source-set receipt RED: SQLite refuses then waits on the retained transition fence until unchanged ten-minute test timeout. PostgreSQL aggregate cleanup is unproven; the stopped-cell store counterexample reproduces on both stores. Pending5970534024; do not label Manager fake-persistence controls as closure. |
| Handler INSERT contention | Separate test-only `5342a3fb5`: native SQLite INSERT BUSY plus acknowledged physical rollback before writer release; PostgreSQL native blocker; exact attempts/SQL counts/equality/conflict/one fact. Five handler roots GREEN race3; unchanged COMMIT BUSY branch and held raw-writer control retained. Does not clear the original default RED or G's separate scatter work. |
| Fresh compiled describe | 45 cells GREEN91.634s. Ten JSON hashes changed only through exact spec digest/source lines; 35 unchanged. `issue-2496-describe-c22-provenance.json` preserves semantic/membership comparison and initial characterization RED. No assertion normalization weakened. |
| Two previously failing served-template journeys | `TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores` and `TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores`, both stores GREEN16.734s on committed production6dfc1ffe7. Real served public mailbox/flow execution, exact historical refusal and source/sibling mutation checks; not general supported historical replay. |
| Public readiness-load budget | `TestFlowReadinessPassUsesAtMostTwoPublicLoads`, race3 GREEN. Core387 lines, below500. |

Restart and aggregate-refresh tests are separate, both permanently required
with all four backend/field cells. The stopped-cell adjacent-grant test also
requires both stores. Partition tests assert these requirements. No required
RED is skipped, filtered from its default owner, or converted into GREEN.

Pending grant/fence repair is part of the chosen class, not an approved split
to #2497. The intended closure remains complete construction/readiness owner
consumption, but **achieved closure is partial and the class remains open**.
Whole managed qualification, final conflict-only rebase, qualified batched push
and exact-head CI still remain. Parents #2411/#2250 stay open; current mapped
construction/readiness/lifecycle watchlists record the native counterexample
and requested disposition. No new issue, vendor dependency or compatibility
reader was introduced.

## Size And Adjacent Ownership

Against `e63f4bdb1`, committed production batch `6dfc1ffe7`:230 non-test Go
paths,8731 additions/8356 deletions, net+375, including moved/generated and
test-support production lines. Gross additions exceed the user's soft3000
target and are disclosed, not discounted. The purchased invariant is: **one
constructor-owned header defines the complete actor/configuration/parent
census; one selected-store attempt/phase authority governs physical attachment,
and no handler or source-only nested plan can create alternate construction.**
Native grant/retirement is still an unproven consumer of that invariant.

Complexity ratchet passes on this committed source: cognitive >=30 hotspots
573->570, >=50 193->193; cyclomatic >=30 264->260, >=50 53->55. The policy gates
>=30; disclose >=50 growth instead of claiming every metric decreases. Maximums
remain302/185. Updated baseline is measurement, not admission for a new model.

C's recorded comment5970352240 remains binding: C2c owns retirement of
`pin.Initialize` / `instance_variables` / `CompileReceiverInitialization` and
three generated fixtures after qualified creating-receipt/supplied-state
handoff. This C22 batch does not delete that chain or silently absorb C2c.
Name this retained, separately allocated authoring chain in the eventual PR;
do not treat it as the source-only runtime construction authority retired here.
