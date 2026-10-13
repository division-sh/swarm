# #2497 Startup Predicate Census Accounting

R5.2 changes agent currentness consumers, not boot ordering or provider/source
admission. The A/P identifiers below are the inherited #2286 predicate census;
they are not the R5.2 P01-P23 manifestation identifiers. Regenerating the existing
census characterizes these changed bodies and grants no execution credit.

## Owners And Callers

| Measured surface | Owner, actual consumers and preserved invariant | Execution witnesses |
| --- | --- | --- |
| `manager/activation_gateway.go::AgentManager.ProveUnpublishedActivation` (A11/A18, P09/P15) | Workspace startup still calls this observation through `runtime.observeActivationWorkspaceGateway`. It now reads `cell.view.Phase` from the complete committed agent-row cache. Exact current token, Manager occurrence, unfenced unpublished projection, owned completion and uncanceled generation remain required. The observation allocates no grant, source, route or execution. | `TestWorkspaceGatewayActivationHoldsPublicationAndSkipsCurrentReplay`, `TestWorkspaceGatewayActivationFailureCompensatesWithoutPublication` preserve real held activation, foreign token, already-published refusal and exact compensation. |
| `manager/agent_manager.go` constructor and registration/adoption helpers (A11/A23/A24, P09/P20/P21) | The selected-store composition explicitly supplies the native lifecycle-state and immutable-outcome roles. `spawnAgentInternalForSourceWithTopology` and `adoptPersistedAgentForLifecycle` consume `registerExecutionCandidate`: observe the native row and serialize publication before the factory, then revalidate the same row before adoption. Initial creation, exact hydration and non-executable retirement/replacement remain separate. Descriptor-only registration, storeless lifecycle success and session-registry currentness projection are retired. Model, source, prompt, native tool, subscription, memory ownership and mock/live admission checks remain the same owners before construction. | `TestAgentRegistrationCandidateIsBoundToObservedRow`, `TestAgentRowCacheRequiresDurableOwnerAndExactHydration`, `TestCommittedReadinessCannotConstructMissingStaticDeclaration`, `TestLifecycleOnlyAdoptionNeverStartsObsoleteAgent`, the retained authored-backend/malformed-descriptor recovery controls, and `TestExecutionProjectionRecoveryStartsPersistedRunningCell`. |
| `manager/static_topology.go` survivor publication, lifecycle-state inspection and persisted hydration (A11/A24, P09/P20/P21) | Existing startup reconcile/refresh/recovery callers read the complete cached committed view. Whole-set publication obtains each current native row and its exact last transition after member commits; partial failure publishes none of the process views. A complete source-set survivor remains generation +0 with no executable replacement. Individual replacement is +1 through the actual native grant and supplied successor projection. Native row/immutable outcome owners, not a local result-agreement decoder, authorize adoption. Desired-plan, complete census, topology/grant and exact run checks remain. | `TestPreparedDurableTopologySourceSetRebindPreservesExecutionLifecycle`, `TestPreparedDurableTopologySourceSetRebindPreservesFailedDeclaration`, `TestPreparedDurableTopologySourceSetRebindPreservesFlowReadinessAdmission`, `TestPreparedDurableTopologySourceSetRebindPublishesBindingsOnlyAfterEveryCommit`, `TestAgentRowIndividualSourceSetReplacementBothStores`, native survivor/shape controls and `TestAgentRowNativeLostAckCacheAdoptionBothStores`. |

The six session-currentness projection consumers are removed, not replaced with
another startup authority. Native `agent_sessions` status, grant, provider head,
termination, successor, history and acquire/hydration remain session-owned.
Agent/effect/session permission comes from the current native agent row in the
original transaction. The immutable outcome reader resolves response loss only;
it does not mint generation, repeat mutation or resurrect old execution.

## Preserved Boundary

Root `internal/runtime/runtime.go` and `runtime_managed_startup.go` boot-order
hunks are reserved to D/#2234 and are unchanged by R5.2. Step 10's Manager
hydration consumes the new typed view; timer arming and recovery/continuation
ordering are not edited. The similarly named `manager/runtime.go` is the existing
Manager lifecycle consumer, not the root boot-order owner. D/#2624's acquired
process capability and generation row remain the sole generation authority.
S6/#2269 dispatcher/provider-slot admission remains out of scope; no waiting
field, alternate process capability or claimed-before-slot policy is introduced.

The original source/profile/mock/live and workspace/provider refusal roots remain
binding. The changed unit fixtures explicitly register or seed the test row owner
instead of depending on removed descriptor-only acceptance; those model-only
controls are not credited as native or public-launcher qualification. Both-store
native Manager replacement, response-loss/currentness, session/provider settlement
and supported public conversation receipts retain their distinct provenance.

The normal guard run leaves `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS` unset. Its
existing update mode is used only after this owner/caller accounting and the named
witnesses are checked. The final proof audit must list the actual rerun receipts;
neither this mapping nor a generated hash supplies qualification by itself.

## Exact Characterization Delta

The native generator changes eight body hashes and no A/P labels: unpublished
activation; executable registration; persisted lifecycle adoption; source-bound
spawn; the Manager constructor; source-set preparation; source-set commit; and
`lifecycleStateMatchesCell`. These are the three owner/consumer families above.
No root Runtime boot predicate or boot-order body changes. The completed Manager
packet at pre-rebase checkpoint9b16764ff exercised all349 roots and571 child
passes without failure/skip; its source provenance is retained rather than
relabeled as post-rebase qualification.

The persistence authority generator retires73 stale findings and classifies66
replacement findings. Two typed process-local declarations are the serialized
Agent candidate callback and immutable-outcome reader. Two typed public-facade
methods forward that existing native outcome owner. The other62 findings are
private native SQL/transaction capability uses: the agent row decoder/CAS and
the effect, provider-drain/provider-head and live-session consumers delegating
to it. No test SQL port, raw public method, new transaction owner or baseline
exception is introduced. The obsolete per-backend lifecycle UPSERT/partial-cell
readers, write-shaped currentness checks and session projection declaration are
deleted. The normal registry and effective-method guards verify14842 exact
findings at the rebased source. The authority-debt ratchet is a separate complete
scan; registry success alone is not its verdict.
