# #2700 Admission And Recovery Composition

The bounded R04 composition is authorized by reviewer-a's
[landing ruling](https://github.com/division-sh/swarm/issues/2700#issuecomment-6261086538).
Its custody source is `44e60462ef8e4c687899cac3055fcbf35795d180`.
The census remains independent of the production admission owner. Updating its
exact body measurements neither waives an execution proof nor adds a predicate.

## Owners And Callers

| Owner | Consumer and preserved boundary | Existing execution witnesses |
| --- | --- | --- |
| `PreparedSourceSetGenerationRefresh.Commit` | After checking the successor's admitted state and exact source plan, install that successor's captured activity journal before topology commit. A failed install aborts the prepared topology; predecessor retirement and cancellation remain owned by the original refresh. No source, grant or bundle identity is inferred from a payload. | `TestActivityDispatchRetainsInstalledJournalAcrossReplacement`; `TestRuntimeConstructedActorCensusSourceSetRebindBothStores`; `TestRuntimeConstructedActorRetainedRefreshShutdownBothStores`; `TestRuntimeConstructedActorPartialRefreshShutdownBothStores`. |
| `Runtime.admitManagedExecution` | Normal managed startup installs the journal from its retained startup grant before actor admission. Missing, stale or foreign session evidence is an invocation failure, not a new bundle-level validation rule. | `TestRuntimeStart_RecoveryHydratesManagedNativePreflightBeforeReplayAdmission`; `TestRuntimeStart_SuccessorGrantSettlesManagedNativePreflightBeforeAdmission`; `TestRetainedActivityJournalBinderCapturesExactSession`; `TestStartupActivityJournalRequiresOneDualRole`. |
| `buildRuntimeComposition` / `buildRecoveredSelectedAPICapabilities` | The serve caller reconciles displaced HTTP attempts before selected API capability assembly and before dispatch. It uses the already-bound runtime sources and existing process recovery owner. Failure retains the activity-startup-recovery stage; it grants no delivery or provider authority. | `TestServeActivityRecoveryPrecedesBothDispatchStartupPaths`; `TestServeActivityRecoveryTargetsRequireExactRuntimeSources`; `TestServeActivityRecoveryKeepsMissingOrCanceledProcessClosed`; `TestDisplacedHTTPActivityOriginalSessionRecoveryBothStores`; `TestDisplacedHTTPActivityRecoveryRoutesSiblingBundlesBothStores`. |

## Preserved Admission Obligations

The inherited A05/A08/A11/A13/A18/A23/A24/A25 and P04/P08/P09/P11/P15/P20/P21
classifications remain attached to the same runtime surfaces. The serve
A01/A02/A03/A04/A10/A19/A20/A21/A22/A26/A27 classifications also stay unchanged.
Source/profile/model and workspace admission still precede execution; journal
installation does not replace any provider preflight, read-only store inspection,
readiness observation, source fence or shutdown drain.

The authored HTTP contract follows the user's
[effect-class deletion ruling](https://github.com/division-sh/swarm/issues/2700#issuecomment-6220888538):
the retired key is refused by the canonical contract projection, not silently
ignored by runtime. An admitted timeout is honored without a client-side cap;
platform connector classes remain owned by their registration slots. The seven
Telegram platform reads are not authored retry policy. Verification and run-start
consume that same bundle-admission owner; R04's retained-session checks are
invocation-specific and do not add a second bundle validator.

Only changed function bodies and the new serve composition helper are remeasured.
Audit/proof labels, the measurement algorithm and its omission controls remain
unchanged. Proof names identify required reaches, not receipts: their actual
results and any unstarted phases are reported separately in the landing packet.
