# #642 Startup Predicate Accounting

Measured on signed `b9774c5ad96afebf806dc6d4079bddd9c5252e13`, based on
master `a7b8a4c8f`. Preflight's finite self-check passes after #2646; its next
RED is the stale startup census at `ProveUnpublishedActivation`. This refresh
records earlier accepted #642 implementation, not a new admission decision.
The independent body census and all refusal controls remain required.

## Owners, Callers And Proof

| Rows / Families | Exact Changed Owners | Callers / Preserved Contract | Evidence |
| --- | --- | --- | --- |
| A11,A18 / P09,P15 | `manager/activation_gateway.go`: `ProveUnpublishedActivation`, `ProveUnpublishedFlowActor`, `unpublishedActivationState`, `unpublishedActivationRevision`, `proveUnpublishedActivationActor`, `validateActivationActorReadiness`, `proveActivationActorExpectation`, `observeUnpublishedActivation` | `AgentManager.replaceExecutionTargetConfigWithTopology` observes before route publication and rechecks exact actor/configuration after gateway observation. `runtime_activation_gateway.go`, `runtime_startup_mcp.go`, selected `activation_actor_admission.go` and selected `agent_runtime_materialization.go` consume the same current unpublished token. Constructed actors additionally require the exact accepted attachment/readiness/construction and current run ownership. Original catalog actors cannot fall back to a new catalog. | `TestWorkspaceGatewayActivationHoldsPublicationAndSkipsCurrentReplay`, `TestWorkspaceGatewayActivationFailureCompensatesWithoutPublication`, `TestWorkspaceGatewayActivationRequiresCurrentActor`, `TestWorkspaceGatewayActivationRejectsConfigurationDriftBeforePublication`, `TestWorkspaceGatewayConstructedActorUsesCurrentNativeAttempt`; these are component controls, not new both-store constructor credit. `TestSelectedActivationGatewayInitializesLaterProbingWorkspace` preserves initialization through the existing owner before observation. Accepted constructed-actor/native and future-provider receipts retain their original scopes in the #642 audit. |
| A22,A24 / P03,P20,P21 | `runforkexecution/recovery.go`: `RecoverSelectedForkContexts`, `selectedRecoveryActionFor`, `selectedCancellationRecoveryAction`, `validateSelectedRecoveryOperation`, `validateSelectedRecoveryContinuation`; `recovery_inspection.go`: `InspectSelectedForkRecoveries` | Served startup classifies the complete durable child continuation under a fresh process owner. Inspection remains observation only. Exact durable invocation/cut, source artifacts, pins, topology, cancellation origins and current-generation admission precede executable activation/resume; inventory/provenance alone never grants execution. | `TestSelectedRecoveryBootActionsAreClosed`, `TestSelectedCancellationRecoveryPreservesPermanentOperationBinding`, `TestSelectedRecoveryUsesPersistedCutIdentityNotDiagnosticMetadata`, `TestSelectedFiniteFeedRecoveryRequiresExactTypedOperation`, `TestSelectedRecoveryAllTypedCutsAndActivatedAcknowledgment`. Existing accepted selected SIGKILL/reactivation, successor input-settlement, timer/join and node fan-out process receipts remain separate whole-operation evidence; this census refresh does not relabel them as newly executed. |
| A22,A24 / P20,P21 | `backend/runforkpersistence/selected_recovery.go`: `planSelectedRecoveryTx`, `planInterruptedSelectedRecoveryTx`, `planPausedUnkeyedSelectedRecoveryTx`, `loadSelectedRecoveryRecordTx`, `loadSelectedRecoveryExecutionTx`, `loadSelectedForkRecoveryOperationTx`, `recoverSelectedForkTx`, `recoverFailedSelectedForkTx`, `retireSelectedRecoveryPredecessorTx`, `settleInterruptedSelectedForkOperationTx`, `selectedForkOperationForRecoveryTx`, `selectedForkRecoveryPinsTx`, `selectedForkRecoveryTopologiesTx`, `selectedForkRecoveryEffectsSettledTx`, `selectedRecoveryContinuationDisposition`, `sameSelectedForkPointIdentity`, `failedSelectedRecovery`, `settledSelectedRecovery`; `selected_recovery_inspection.go`: `inspectSelectedRecoveryTx` | Existing native selected recovery/read-only inspection supply exact current durable facts to the runtime classifier above. The general continuation owner replaces the finite-feed-only restriction, while retaining process/fence/CAS, fixed cut, complete inventory, typed terminal/control-only states and no-mutation refusal. Pending cancellations settle through their existing effect owner before successor execution. | The runtime disposition/typed-operation controls above exercise the consumer contract. Accepted both-store native recovery and real SIGKILL/successor receipts in #642's living audit remain required; the regeneration command itself is not native execution proof. |
| A05,A08,A11,A13,A18,A23,A24,A25 / P04,P08,P09,P11,P15,P20,P21 | `runtime.go`: `newRuntime`, `Runtime.stopWithOptions` | Runtime construction uses the existing exported `RunLifecycleTerminalCatalog`; no second terminal-stage interpreter. Shutdown closes admission and joins accepted completion work before consuming its final diagnostic; an early retirement diagnostic cannot hide a later committed cleanup error. | Existing terminal-catalog controls; `TestRuntimeShutdownPreservesLateCompletionCommitDiagnostic`, `TestRuntimeShutdownReportsEarlyCompletionCommitDiagnosticOnce`, `TestExecutorCommittedErrorPreservesContinuationWithoutReplay`, `TestExecutorLateCommittedDiagnosticSurvivesJoin`, `TestExecutorCanceledJoinRetainsAcceptedWork`. |
| A01,A02,A03,A04,A10,A19,A20,A21,A22,A26,A27 / P01,P02,P03,P08,P10,P16,P17,P18,P19,P20,P21,P22 | `serveapp/main.go`: `buildRuntimeComposition` | Composition passes its existing selected owner to the real channel dispatcher, avoiding a typed-nil owner. Selected controls acquire their own current retained turn before preview/write; a selected refusal never falls through to an ordinary runtime. No new startup authority is inferred. | `TestChannelSelectedForkDecisionUsesOwnOwnerWithoutOrdinaryRuntime`, `TestChannelSelectedForkInputAcquiresOwnTurnBeforePreviewOrWrite`; actual both-store current-grant and ordered-input native receipts remain scoped separately. Approval-to-return and two-branch effects are still OPEN under C4, not credited by these component controls. |

The table covers all 36 changed/added measured bodies. Paths abbreviated in the
table are relative to `internal/runtime/`, except explicitly named native and
serveapp paths. `verify_admission_predicate_census.json` retains every exact
owner, body hash, original A row and original P family; no surface is excluded.

## Deleted Bodies

These six measured removals are replaced in the same approved continuation
owner, not omitted startup gates:

| Retired Function | Exact Replacement |
| --- | --- |
| runtime `selectedFiniteFeedRecoveryAction` | `selectedRecoveryActionFor` plus `validateSelectedRecoveryOperation` and `validateSelectedRecoveryContinuation` |
| native `selectedFiniteFeedRecoveryOperationTx` | `loadSelectedForkRecoveryOperationTx` and `selectedForkOperationForRecoveryTx` |
| native `selectedFiniteFeedRecoveryPinsTx` | `selectedForkRecoveryPinsTx` |
| native `selectedFiniteFeedRecoveryTopologiesTx` | `selectedForkRecoveryTopologiesTx` |
| native `selectedForkActivatedOperationTx` | `loadSelectedForkRecoveryOperationTx` and exact disposition validation |
| native `settleInterruptedSelectedFiniteFeedOperationTx` | `settleInterruptedSelectedForkOperationTx` |

## Qualification Boundary

Run the refreshed census without update mode and its independent negative
controls, then the named gateway/recovery/diagnostic/channel component proofs.
Only a complete clean-head `go run ./cmd/swarm-test preflight` receipt permits
declaring preflight PASS. A generated census alone supplies no runtime, native,
hosted-tier or C4-exit credit. C retains the shared historical receipt reader;
this refresh changes neither that reader nor any production code.
