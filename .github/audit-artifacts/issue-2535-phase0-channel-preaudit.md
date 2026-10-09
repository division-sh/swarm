# Pre-Implementation Coverage Audit: #2535 Phase0 Channel Worker

Status: AUDIT ONLY. Independent coding re-gate requested; no production edit.
Source: master2fc6a13bde271492602b7ca0ed82bfe3325280f8,
tree206adb09324179764db0409002aef677af7f07b5. Agent-g.
Binding review: https://github.com/division-sh/swarm/issues/2535#issuecomment-6084282948.
User approval2026-10-09, issue body/thread and research/test-time/report.md
constrain the chosen slice. IMPLEMENTER_GUIDELINES and SEMANTIC_DRIFT reread.

## Class, Governing Contract And Boundary

Category: high-risk maintenance with runtime/concurrency scheduling semantics.
Observed symptom: served channel work waits for1s ordinary/5s native tickers;
public tests also wait5.5s/2.2s to observe non-redispatch. Entry point is
`serveapp/channel_delivery_worker.go:startServeChannelDelivery`, not the audit
boundary. Immediate parent: unnecessary reconciliation latency/test waiting.
Broader parent: #2535 fleet qualification latency, with #1196/#2353/#2394
retaining their separate local-runner, health and throughput obligations.

Chosen class: latency between an ACKNOWLEDGED LOCAL channel-relevant change
and the existing served worker's next durable reconciliation, plus successful
pass-bound observation of that work. This PR aims to eliminate this complete
local-handoff class, not merely speed up webhook text or change tick values.
Remote provider edits and another process's writes are different producers;
periodic repair remains required. No assertion/active-work deadline reduction,
model/store-count reduction, retry, compatibility, vendor or new scheduler.

Binding authoritative refs (source above):
`platform-spec.yaml#managed_external_effect_authority.context_authority.channel_delivery`
(40712), `.channel_native_setting` (40747), and
`managed_external_effect_authority.recovery.channel_source_projection` (40815).
Read exact destination/default/activation/generation admission, original
receipt/attempt settlement, no automatic launched/uncertain redispatch, and
foreground/startup atomic projection together. Adjacent HITL/channel-native
contracts retain card cursors, verified ingress and exact compiled publication.
They govern business authority; they do not presently define worker hints or
proof-pass counters. The proposed scheduling delta below requires approval
and promotion into authoritative platform-spec.yaml WITH implementation.

Class framing is broad enough as a first slice, not a20-minute parent closure.
Parent action: keep the separately ratified short sequence under OPEN2535.
Reporter fault window, immutable build identity/cache, plan/shard packing,
snapshot traversal and runner scheduling are different mechanisms, already
probed and separately named in6083383303/6083809604. Do not absorb pinrouting
here: its19.229s versus prior-master19.838s is not a new PR-1 body regression.
Remaining tail: four coherent PR groups, medium confidence pending matched
measurement; healthy nightly activation and fleet acceptance are additional
operational obligations, not child runtime closure.

## Full Execution Path And Gates

1. Admit selected store/source/process and current compiled channel catalogue.
   DIFFERENT authority/lifetime concepts: existing selected-store, context
   publication and Process lease owners; preserve startup/rebind/crash proofs.
2. Public command, verified provider ingress, tool/handler or maintenance owner
   admits its exact current identity and enters its OWN outer transaction.
   DIFFERENT business admission; SAME CLASS at acknowledged-change handoff.
3. Borrowed writers change notice/card/intent/default/setting/receipt facts;
   activity/revision/idempotency and candidate finalizers remain ordered.
   DIFFERENT atomic mutation concepts; SAME CLASS in returning changed facts
   rather than notifying while the enclosing transaction can still roll back.
4. Native COMMIT yields acknowledged bool separately from error. Existing
   mutation protocol can subsequently fail claim retirement/candidate handoff.
   SAME CLASS: retain acknowledged change despite later cleanup error. Unknown
   COMMIT is not rollback proof and is not acknowledged-change notification.
5. Owner-specific typed change reaches the one process-local served worker.
   SAME CLASS: coalesced ordinary/native demand, no business authority in hint.
6. Worker re-reads current owner facts, card-change cursor and bounded pages,
   then entries -> deliveries -> card actions; native settings on native demand.
   SAME CLASS scheduling; DIFFERENT eligibility/presentation/dispatch contracts.
7. Tool/effect owner authorizes, launches, settles exact delivery/native attempt;
   receipt and journal projection commit together. SAME CLASS for resulting
   local change; DIFFERENT external-effect authority and non-redispatch rule.
8. Provider-observed state/public readback and pass-bound negative assertions.
   SAME CLASS only for a SUCCESSFUL relevant pass begun after the mutation.
   Transport/qualification failures and genuine TTL windows are DIFFERENT
   proof concepts; never count an errored attempt as successful absence proof.

Before any delivery is reachable, boot/current publication, default/binding,
credentials, current source/receipt and native qualification must succeed.
No hint can bypass those gates. Each public path below reaches these gates.

## Canonical Owners And Proposed Minimal Handoff

The real semantic owners are the existing domain writers and outer native
transaction/mutation finalizers, NOT the serve callback's final `error`.
One worker owns reconciliation; `worklifetime.Process` owns its goroutine.
Runtime store composition already joins the selected domain owners. Extend
that composition with ONE process-local channel-specific hint object, exposed
through the existing channel-delivery capability, not a database registry.
No SQL hook/trigger, generic after-commit callback facility, notification row,
global mutable notifier, source metadata or transport cache is introduced.

Proposed private handoff value: closed ordinary/native reconcile demand,
actual changed/no-op disposition, and acknowledged native outcome. Domain
business return values/public RPC wire shapes remain unchanged. Where current
private writers return only error or discard changed bool, promote that
owner's internal outcome. For borrowed writes, return/accumulate ONLY typed
domain change facts into the outer business result; no notification inside
InsertTx/WithSQL and no context-value authority or guessed revision-family
interpretation. The canonical mutationprotocol itself remains unchanged.
The enclosing finalizer consumes Result.Value/Acknowledged or native
RunTransactionOutcome BEFORE projecting its public error-only return.

| Native outcome | Domain result | Notification / error contract |
| --- | --- | --- |
| Acknowledged | Actual relevant change | Publish typed demand; retain ALL cleanup/handoff errors. |
| Acknowledged | Exact replay/true no-op | No self-sustaining demand; retain replay/business result. |
| Not acknowledged | Callback abort/proven rollback | No notification, retain cause. |
| Not acknowledged | Unknown COMMIT disposition | No fabricated acknowledgment/rollback/retry; periodic/startup durable re-read remains repair. |
| Acknowledged | Change invalidated before worker scans | Hint remains advisory; currentness re-read refuses old authority. |
| Any | No active subscribed process | No detached work or send-on-closed panic; successor initial scan is required. |

Hints coalesce ordinary/native scopes; card-only demand must not force native
provider reads. Consume the pending hint BEFORE its pass, never clear hints
at pass end. A commit during actions can create work after delivery scanning;
it MUST leave one further pass pending. Subscription is exact Process-owned;
bootstrap still performs initial native and ordinary scans, periodic1s/5s
backstops stay in production, and shutdown retires and joins before release.
No-op PlanOpen/Freeze/Qualification observations must not create a hot loop.
Private test cadence changes observation frequency, never source time/TTL.

## Exact Producer And Outer Outcome Census

Paths below are under internal/store unless prefixed serveapp. PG and SQLite
implementations are separately exercised; the grouped row names the actual
shared function or BOTH owner methods, not an assumed backend parity.
O = ordinary hint; N = native plus ordinary hint. M = moved in this work;
A = already canonical owner, unchanged authority; D = different mechanism.
Every M row uses the six-outcome table above, including post-COMMIT failure.
Row-specific no-ops are named; do not infer them from err==nil or transaction
success alone. Borrowed rows carry their fact to the named outer finalizer.

| ID | Actual producer / private write | OUTERMOST finalizer and current evidence | Change / no-op and consumer |
| --- | --- | --- | --- |
| P01 M | backend/operatorchannel/owner.go:confirmBinding (connect) -> ApplyBindingTx/PlanFirstSummaryTx | postgresRunner/sqliteRunner.mutate currently discard native ack via RunTransaction | New approved bound/default/summary -> O/N; rejected, expired, credential-stale, exact terminal replay -> no relevant change. Promote private runner outcome, not service err. |
| P02 M | same confirmBinding, reconnect | Same outer finalizer | Changed binding revision, same claimant/epoch policy -> O/N; exact replay no-op; no historical individual resend. |
| P03 M | same confirmBinding, rebind | Same outer finalizer | Changed verified claimant/epoch -> O/N; replay no-op, predecessor currentness preserved. |
| P04 M | backend/operatorchannel/owner.go:unbind -> RetireBindingTx | Same outer runner/native COMMIT | Actual current-default retirement -> O/N; already-retired exact replay no-op. |
| P05 M | backend/channelonboarding/owner.go:publishActivation | PG/SQLite runner.mutate -> RunTransaction, ack discarded | New activation and any replaced sibling -> O/N; exact activation replay no-op. Durable row is not process-catalogue readiness. |
| P06 M | same advance, reboundActivation update | Same runner/native COMMIT | Only actual rebound coordinate/publication change -> O/N; phase-only/checkpoint edits are D. |
| P07 M | same retireActivation | Same runner/native COMMIT | Actual retirement -> O/N; already retired no-op; preserve CAS/reason. |
| P08 M | same retireTeardownAuthority (pending or completed identity retirement) | Same runner/native COMMIT | Retired matching operation/activation rows -> O/N; no matching affected authority no-op. |
| P09 M | same completeTeardown -> RetireStaleNativeInboxConsumersTx | Same runner/native COMMIT | Actual consumer retirement -> N/O; completed replay no-op. Reserve/get teardown alone is D. |
| P10 M | backend/channelonboarding/client_locale.go:setClientLocale | Same runner/native COMMIT | Actual locale revision -> N/O; existing language/revision replay branch no-op. |
| P11 M | runtimepersistence/channel_native_setting.go:AttachNativeInboxSetting -> AttachNativeInboxSettingTx | PG/SQLite backend.RunTransaction, ack discarded | New/changed setting/consumer/generation -> N/O; exact attach observation no-op. Inner helper must expose changed, not returned Setting alone. |
| P12 M | same RetireStaleNativeInboxConsumers | Same native COMMIT | Actual stale consumer/setting retirement -> N/O; zero affected rows no-op. |
| P13 M | same MarkNativeInboxSettingUnavailable | Same native COMMIT | Actual generation state transition -> O; exact unavailable state no-op; never auto-reinstall. |
| P14 M | same RecordNativeInboxQualification | Same native COMMIT | Eligibility/locale/readback change -> O; observed-at refresh alone is NOT N demand and cannot wake itself forever. Preserve timestamp/readback writes. |
| P15 M | internal/mailboxpersistence/sqlite.go:InsertMailboxItem -> PlanNoticeTx | SQLite backend.RunTransaction, ack discarded | Inserted relevant notice/plan -> O; absent default remains non-executable. |
| P16 M | internal/mailboxpersistence/postgres.go:InsertMailboxItem/insertMailboxItemSpec -> PlanNoticeTx | PG RunTransactionOutcome already returns committed; preserve through outer InsertMailboxItem | Same notice rule; committed plus cleanup error still O and original error. |
| P17 M | mailboxpersistence/sqlite.go/postgres.go:ExpireMailboxItems (also called by list/count) | SQLite RunTransaction; PG expireMailboxItemsSpec RunTransactionOutcome | Actual notice expiry -> O; zero expired no-op. Read entrances are not automatically read-only if they invoke this owner. |
| P18 M | mailboxpersistence/acknowledgment.go:acknowledgeMailboxNotice/channel action variant | PG/SQLite RunTransaction encloses notice + completion + applied intent | Newly acknowledged notice/intent -> O; validated api-idempotency replay no-op. No signal from acknowledgeNoticeTx. |
| P19 M | backend/decisionpersistence/decision_cards.go:CreateDecisionCard | writePostgresDecision/writeSQLiteDecision -> postgresDecisionMutation/sqliteDecisionMutation -> mutationprotocol.Run* | Actual created card/change -> O; inner InsertTx only returns a fact. Current helpers call Err and lose ack. |
| P20 M | decisionpersistence/human_task_cards.go:CreateHumanTaskCardOutcome | postgresDecisionMutation/sqliteDecisionMutation -> Result.Value | Acknowledged created card -> O, retain HumanTaskCreationResult and error. No notification from a merely returned card ID. |
| P21 M | decisionpersistence/proposed_effect_cards.go:CreateProposedEffectCard | write*Decision -> Run* result | Actual card/change -> O; same card/admission semantics. |
| P22 M | decision_cards.go:SupersedeDecisionCardsForStage; proposed_effect_cards.go:SupersedeProposedEffectsForLoopGenerations | write*Decision -> Run* result | Changed card/change-log set -> O; zero matching pending cards no-op. Preserve changed facts instead of discarded bool. |
| P23 M | decision_cards.go:ExpireDecisionCardInputDrafts | postgresDecisionMutation/sqliteDecisionMutation -> Result.Value | Actual expired drafts/change entries -> O; returned count0 no-op. Test-only Apply/Begin/Cancel facades consume their same finalizers but earn no public proof. |
| P24 M | backend/pipelinepersistence/workflow_engine_mutation_commit.go:CommitWorkflowEngineMutation -> gate lifecycle / InsertProposedEffectTx | commitWorkflowEngineMutation outer Run* -> outcome.Value, marks Committed/Lifecycle.Committed | Actual gate/proposed card mutation -> O; timer/schedule/entity-only mutations D unless they terminalize cards. Borrowed writer never signals. |
| P25 M | pipelinepersistence/flow_instance_activation_commit.go:CommitFlowInstanceActivation / borrowed CommitFlowInstanceActivationsTx -> commitWorkflowEngineLifecycle | Standalone commitOneFlowInstanceActivation Run* result OR enclosing publication finalizer in P61-P65 | Actual created/superseded gate cards -> O; existing activation without card change no-op. Borrowed construction, including recursive children, does NOT end at this file. |
| P26 M | pipelinepersistence/workflow_timer_activation.go:CommitWorkflowTimerReconciliation -> lifecycle | Own outer Run* -> acknowledged result | Actual gate changes -> O; pure timer reconciliation D. |
| P27 M | pipelinepersistence/decision_card_request.go:decisionCardRequestLease.Commit -> decision_card_mutation_commit.go:commitDecisionCardOperation | Own outer Run* -> outcome.Value, Acknowledged; includes publication, API completion, applied action/text and prompt | New decide/defer/input-begin/cancel/complete/skip changes -> O; validated replay no-op. Atomic prompt/intent must not signal from helper before publication fails. |
| P28 M | pipelinepersistence/human_task_expiry_commit.go:CommitHumanTaskExpirations | Own Run* -> outcome.Value/Acknowledged, expiry/publications atomic | Nonempty actual expiry set -> O; empty set no-op. |
| P29 M | pipelinepersistence/workflow_decision_route_commit.go:CommitProposedEffectRoute / decisionpersistence CompleteProposedEffectRoute; CommitHumanTaskOutcomeRoute/CommitHumanTaskDeferredRoute | Own Run* outcome.Value or standalone decision finalizer | Actual proposed completion/card change or constructed receiver gate via publication -> O; event-only routes with no receiver/card change are D. Borrowed publication subtree follows P61-P65. |
| P30 M | backend/runlifecycle/run_lifecycle_state.go terminal methods -> SupersedeRunTx; run_control.go/run_control_sqlite.go:StopRunControlOutcome; run_lifecycle_candidates.go:ExecuteCompletionCandidate; active_run_quiescence.go:ApplyActiveRunQuiescence/ApplyServeAbandonActiveRunQuiescence | Standalone run*LifecycleOperation OR each named control, completion-candidate and quiescence outer Run* result, OR borrowing owners P31-P33/P54 | Actual terminalization/card set -> O; MutationExactNoop/no affected cards no-op. Carry fact, do not signal from MarkTerminalTx/ForkSourceTx/CompleteRunTx/MarkRunTerminalStateTx or the borrowed run control. |
| P31 M | pipelinepersistence/standing_service.go standingRunner.mutate -> MarkTerminalTx | standingRunner PG/SQLite mutationprotocol.Run* result (currently error projection) | Actual replaced/retired generation cards -> O; exact standing no-op no-op. Suspend/run pause with no card change is D, authority unchanged. |
| P32 M | runforkpersistence/run_fork_materializer.go:MaterializeRunFork and selected-contract materialization port -> materializeRunForkDecisionCards/Proposed cards | Each existing RunPostgresWithOptions/RunSQLite -> result.Value at materialization owner | Created fork-local cards -> O only after materialization acknowledgment; selected/nonselected variants BOTH in scope. Aborted/preview path produces none. |
| P33 M | runforkpersistence/run_fork_activation.go + selected-contract activation, run_fork_source_freeze.go; fork_operation failure, selected_recovery failure, selected_stop and retained discard | EACH existing outer Run* result in those named owners, not borrowed ForkSourceTx/MarkTerminalTx | Actual source/selected-run card terminalization -> O; exact activation/terminal replay no-op. Destructive no-survivor deletion is D, not fake created work. |
| P34 M | eventpersistence/inbound_publication.go plus SQLite implementation:CommitInboundPublication -> commitOperatorChannelIntentsTx/commitPublicationTx | runPostgresEventMutationResult/runSQLiteEventMutationResult -> Result.Value; public CommitResult.Acknowledged + Record.Created | New verified action/text intent OR constructed card subtree -> O; permanent receipt replay no-op. Claim-only/bare-business publication WITHOUT constructed cards or channel intent is D. |
| P35 M | runtimepersistence/channel_delivery.go:AdmitChannelReplyAction -> AdmitReplyActionTx | PG/SQLite RunTransaction, ack discarded | Actually inserted transferred action/settled text -> O; found existing action is not automatically changed. |
| P36 M | same AdvancePartialChannelInputDraftText | PG/SQLite RunTransaction | Actual draft progress + settled text + new prompt -> O; exact already-settled readback no-op. |
| P37 M | same AdvancePartialChosenChannelInputDraftText | Same native finalizer | Actual chosen-draft progress/action/prompt -> O; no-op preserves exact choice checks. |
| P38 M | same AdvancePartialChannelInputSkip | Same native finalizer | Actual skip/progress/applied intent/prompt -> O; zero/no-op must not manufacture hint. |
| P39 M | same PlanInboxResponse | Same native finalizer | Inserted response/render/settled original entry -> O; existing deterministic plan no-op. |
| P40 M | same PlanChannelTextResponse | Same native finalizer | Inserted response/render + settled text -> O; exact plan replay no-op. |
| P41 M | same PlanChannelDraftChooser | Same native finalizer | Inserted chooser + settled intent -> O; exact replay no-op. |
| P42 M | same PlanChannelActionResponse | Same native finalizer | Inserted action response/control copy + intent -> O; exact replay no-op. |
| P43 M | same AdvanceChannelActionPage -> action.go page/index/control copy | Same native finalizer | Changed page/index/control-copy/settled navigation -> O; exact navigation replay no-op. |
| P44 M | same PlanManualChannelResend -> recovery.go PlanManualResendTx | Same native finalizer | New linked delivery + settled one-use action -> O; validated replay no-op; original uncertain attempt never retried. |
| P45 M | same PlanOpenChannelCard -> PlanOpenCardTx | Same native finalizer | Existing created bool must reach acknowledged handoff; false/no-op emits none. |
| P46 M | same PlanChangedChannelCard -> PlanChangedCardTx | Same native finalizer | Actual cursor/page/render-relevant change -> O; stale cursor/no mutation no-op. |
| P47 M | same FreezeAndPersistChannelRender -> bounds/freeze/PersistRenderTx | Same native finalizer | Actual new render/current pointer -> O; exact same render/no-op emits none. Worker performs no duplicate dispatch. |
| P48 M | same SettleUnappliedChannelAction | Same native finalizer | New settled disposition -> O; exact disposition replay no-op. |
| P49 M | same SettleUnsupportedChannelText | Same native finalizer | New unsupported/chooser settlement -> O; exact already settled no-op. |
| P50 M | same RejectUnboundChannelText | Same native finalizer | Actual rejected text -> O; exact rejected no-op. |
| P51 M | same RejectInboxEntry | Same native finalizer | Actual entry-rejected intent -> O; exact rejection no-op. |
| P52 M | effectpersistence/runtime_external_effects.go:SettleExternalAttempt -> projectChannelSourceSettlementTx | Existing outer Run* Result.Acknowledged; effectMutationError retains ack cleanup distinction | Changed channel delivery receipt/plan or native setting -> O (N only if actual new desired native work); identical terminal settlement no-op, no speculative retry. |
| P53 M | same ReconcileExternalEffectAttempts -> recoverChannelSourceSettlementTx | Existing outer Run* Result.Value; RecoverySummary | Actual original channel projection -> O; authorized abandonment/no channel projection/terminal replay no-op. Before worker starts, its initial scan consumes durable result. |
| P54 M | effectpersistence SettleCompletion/provider drain or recovery causing run terminalization | Existing completion/recovery outer Run* Result.Value and committed result | Only actual terminal card change carried from runlifecycle -> O; unrelated provider accounting/history D. No provider redispatch/change to drain semantics. |
| P55 M | serveapp/channel_onboarding.go:serveChannelActivationRefresher.PublishChannelActivation/PromoteChannelRegistration and teardown/process re-publication callbacks | Exact successful manager catalogue/ingress publication, AFTER durable ack; no SQL inferred here | O/N only after real process publication; error does not imply readiness. Preserve staged rollback/compensation. |
| P56 A | serveapp/main.go:startServeChannelDelivery; activateServeLifecycle/Process teardown/reset composition | Process.Begin lease; immediate native + ordinary scan; joined lease.Done | Initial scan covers durable work while no listener exists. No earlier autonomous launch/startup reorder. |
| P57 D | reserve/begin/expire identity ceremony, principal creation/proof bookkeeping, onboarding checkpoint without rebound | Existing owner transaction only changes ceremony/credentials, not worker input | No wake required by worker input census; existing onboarding service advances it independently. |
| P58 D | effect authorization/heartbeat/MarkLaunched/ResponseObserved, native callback ACK journal | Exact effect owners, not a new delivery/native desired-state projection | Remain admission/transport evidence. Receipt/terminal projection belongs P52/P53; ACK is not card completion. |
| P59 M | adminpersistence/destructive_reset_cleanup.go; startupownership/owner.go:postgresSession/sqliteSession.ApplyDestructiveResetCleanup; serveapp/process_runtime_reset.go:serveRuntimeReset.Complete | Standalone PG RunTransactionWithOptionsOutcome OR exact retained process transaction (PG lease.RunTransaction / SQLite RunTransaction currently discard ack); reset Complete after source reconstruction/publication | Actual cleanup/surviving notice change -> advisory O after acknowledged native outcome; completed reset publication -> O/N. Dry-run/exact cleanup replay no-op. IMPORTANT: reset joins runtime contexts, NOT the entire served Process; the channel worker survives while ready=false. Do not invent successor-worker initial scan or treat an error-only retained result as ack. Promote this owner-specific outcome and preserve existing readiness ordering. |
| P60 D | Remote provider commands/launcher/bot state, another process's writes | No in-process local COMMIT outcome to consume | Keep production1s/5s backstop, separately execution-prove readback and external-source eventual convergence. |
| P61 M | eventpersistence/event_commit.go:CommitPublication/CommitAPIEventPublication -> CommitFlowInstanceActivationsTx | Each runPostgresEventMutationResult/runSQLiteEventMutationResult -> acknowledged Value; API result also encloses exact completion/run-creation binding | Actual constructed card subtree, including nested descendants -> O; same event/construction replay/no new card no-op. No notification from CommitPublicationTx. |
| P62 M | eventpersistence/deployment_run_creation.go:CommitDeploymentRunCreation | Its own run*EventMutationResult -> outcome.Value / acknowledgeDeploymentRunCreation | Actual initial deployment construction/gate cards -> O; permanent run-creation replay no-op; source import/data-only without a card change is D. |
| P63 M | pipelinepersistence/workflow_timer_occurrence_commit.go:CommitWorkflowTimerOccurrence -> commitPublicationTx | Own mutationprotocol.Run* -> acknowledged Value | A fired publication constructing a receiver gate/card -> O; plain timer event without relevant card change is D. Existing occurrence ID/fire/settlement semantics unchanged. |
| P64 M | pipelinepersistence/generic_schedule_occurrence_commit.go:CommitGenericScheduleOccurrence -> commitPublicationTx | Own mutationprotocol.Run* -> acknowledged Value | Same constructed-card rule; schedule-only no-op/D; no timer/schedule admission or wakeup-policy rewrite. |
| P65 M | pipelinepersistence/scenario_setup.go:SetupScenarioEntities/CommitScenarioSetup -> CommitFlowInstanceActivationsTx | Each existing outer Run* -> acknowledged scenario setup result | Actual initial/recursive gate-card creation -> O; identical scenario replay/no construction no-op. Not an event-only/log writer. |
| P66 D | Schema installation/compatibility-free current schema admission, event log/diagnostic/directive append without construction | Existing schema/event owner finalizer | Not a channel business change. No schema migration or generic SQL notification hook. |

Systematic sweep: all non-test selected-store writes to decision_cards,
decision_card_changes/drafts, mailbox, channel_delivery_{defaults,plans,
renders,receipts}, operator_channel_{action,text}_intents,
channel_native_{settings,setting_consumers} and connected activations;
then all calls to card Insert/Decide/Defer/Begin/Cancel/Supersede/Expire and
all channel borrowed helpers, then outer finalizer consumers. No second
production reconciliation loop found. Generated facade forwarders delegate
to the owners above; adapters/services/CLI do not become new commit owners.
Read-only previews/lists remain A; list-triggered expiry is P17/P23, not A.
No M seam remains intentionally poll-only. Future same-concept producer
outside this map or missing acknowledged owner is a STOP/re-gate condition.

## Manifestations And Named Execution Proof

ALL rows below are planned implementation proof, not PASS claims. Every P01
throughP55, P59 andP61-P65 receives a corresponding scenario in planned
`TestChannelPostCommitProducerCoverageBothStores`: real selected owner,
explicit changed versus replay/no-op, ordinary/native hint, retained cause,
outer rollback and acknowledged cleanup injection. Existing transactiontest
and storetest construction/observation owners supply barriers and readback;
no new raw SQL test site or generic DB getter. P56/P60 have their named
process/backstop proofs below. Mechanical source/consumer census must refuse
an unclassified new writer; a delegate/owner name is not execution credit.

| ID | Actual manifestation | Exact proof / preserved oracle |
| --- | --- | --- |
| M01 | First approved connection/default selects old notice count-only summary | TestChannelDeliveryBacklogSummaryOpenInboxE2E, new P01 notification leaf; summary count/history unchanged. |
| M02 | Reconnect preserves claimant/epoch policy and releases new work promptly | TestChannelConnectTelegramFirstUserJourney, TestChannelOnboardingEarlyReconnectConfirmationRestartE2E, P02/P55. |
| M03 | Rebind/unbind retires predecessor and rejects old control | TestChannelOneBotManyConversationsPublicJourney, TestChannelSourceLifecyclePublicJourney, P03/P04/P07/P08. |
| M04 | Durable activation commits before executable process publication | TestChannelOnboardingE2E14ActivationCommitBeforeProcessPublication, E2E16ProcessPublicationBeforePromotion, E2E17AuthorityRetirementBeforeCleanup; held publication control for P05/P55. |
| M05 | Native desired setting/locale changes await5s | TestChannelNativeLocaleQualificationPublicJourney, TestChannelClientLocaleOwnsIndependentSelectedStoreRevision, P10-P14; both languages/stores and live provider reads retained. |
| M06 | Notice insertion/expiry/ack waits1s | TestChannelDeliveryNoticeFirstE2E, TestChannelNoticeAcknowledgmentE2E, TestChannelDeliveryBudgetNoticePublicJourney, TestChannelDeliveryRecoveryNoticePublicJourney, P15-P18. |
| M07 | Gate/human-task/proposed-effect creation or supersession notifies no worker | TestChannelDeliveryRealAnchorProducersPublicJourney and TestChannelLearnedObjectRealAnchorProducersPublicJourney; actual three anchor kinds, P19-P29. |
| M08 | Run/standing terminalization or loop generation changes card render/eligibility | TestRunTerminalizationAtomicallyFencesGateActivationsAndCardsOnBothStores, TestHumanTaskExpiryAndRunSupersessionParity, TestChannelDeliveryHumanTemporalReceiptPublicJourney; P22/P23/P30/P31/P54. |
| M09 | Fork-created cards/source freeze/discard share borrowed writer | TestMaterializeRunForkGateAuthoritiesSelectedStoreParity, TestMaterializeRunForkRootAuthoritiesExecuteWithForkIdentitySelectedStoreParity, TestChannelSelectedForkInputBoundaryMatrix, TestChannelSourceLifecyclePublicJourney/fork plus selected-store P32/P33 actual outer calls. Preserve selected-run public refusal; do not infer PG credit from the separate SQLite-only card fixture. |
| M10 | Verified action/text ingress becomes pending during worker wait | TestChannelDeliveryInboundDispositionE2E, TestChannelDeliveryNativeInboxE2E, TestChannelNativeEntryDispositionsPublicJourney, P34; duplicate receipt and foreign-user controls. |
| M11 | Quoted reply transfers into action intent | TestChannelDeliveryQuotedTwoDraftsE2E, TestChannelLearnedObjectPublicJourney, P35; two exact drafts/actors, no guessed latest card. |
| M12 | Ordered/partial/chosen/skip input creates next prompt after delivery scan | TestChannelDeliveryOrderedInputE2E, RequiredInputE2E, BareInputChooserE2E, SkipFinalOptionalInputE2E, CancelInputE2E, InvalidTypedInputE2E; P27/P36-P38. |
| M13 | Inbox/text/chooser/action response or navigation page created in-pass | TestChannelDeliveryPagedCardActionsE2E, ViewFullE2E, BacklogOpenInboxAcrossPublicPagesE2E, LearnedObjectInputPublicJourney; P39-P43. |
| M14 | Manual resend creates new linked work; duplicate tap must not resend | TestChannelDeliveryManualResendAfterLostResponseE2E, ManualResendAfterLostPromptEditE2E, P44; exact counts and original uncertain history unchanged. |
| M15 | Changed cursor/open-card planning/render preparation can lose wake or spin | TestChannelDeliveryChangeCursorCrossesPageBoundaryAndResumes, ResumesAfterPlanningFailure, WorkerRechecksResponsibilityBeforeRender; new no-op-pass/hot-loop control for P45-P47. |
| M16 | Rejected/unapplied/unsupported intent settlement changes worker backlog | TestChannelActionWorkerRejectsUnresolvedIntentWithoutMutation, TestChannelNativeEntryDispositionsPublicJourney, TestChannelLearnedObjectInvalidChosenAnswerPublicJourney; P48-P51. |
| M17 | Receipt/native settlement acknowledges then cleanup fails | TestChannelDeliverySettlementDiagnosticBothStores, existing channel recovery projection parity plus P52/P53 cleanup-cut leaves. Keep ack=true AND original error, no repeated operation. |
| M18 | Native install/ACK/response uncertainty never retries after hint | TestChannelDeliveryNativeInboxLostAcknowledgmentE2E, TestChannelDeliveryVerdictAcknowledgmentLossE2E, TestChannelDeliveryUncertainCopyAuthorityPublicJourney; count original attempts, explicit consent only. |
| M19 | Immediate startup/restart scans retained pending work | TestChannelDeliveryNativeInboxServedRestartE2E, TestChannelInputAuthorityRestartPublicJourney, TestChannelDeliveryOrderedInputRetainedRestartE2E, TestChannelDeliveryChooserRetainedAnswerRestartE2E. |
| M20 | Abrupt process death loses ephemeral hints, never durable work | TestChannelDeliveryAbruptProcessDeathPublicJourney, TestChannelInputAbruptProcessDeathPublicJourney, TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney, TestChannelNativeInstallAbruptProcessDeathPublicJourney. |
| M21 | Retained/source-clearing reset joins runtime contexts but retains channel worker; process shutdown joins worker | TestChannelSourceLifecyclePublicJourney/retained_reset/source_reset, P59 actual native/retained outcome cuts, planned TestChannelWorkerWakeLifecycle (both stores): ready=false during reset, no stale dispatch, actual Complete wake; partial start/stopped sink/replacement, no pending lease or post-stop effect. |
| M22 | Coalescing/concurrent before/during/after-scan commit loses demand | Planned TestChannelWorkerCoalescedWakeAndInPassCommit: held real finalizer/scan barriers, ordinary+native scopes, no sleeps/retries, -race -count=3; compare durable final state. |
| M23 | Callback/transaction finalization rolls back or COMMIT remains unknown | Planned producer matrix P01-P55 rollback/unknown cuts, existing TestFaultMatrixCancellationAndCommitAcknowledgement / MissingAcknowledgementHidesAttemptValue controls. No hint, original error retained. |
| M24 | Acknowledged COMMIT plus cleanup/handoff error still must wake | Planned producer matrix fault cuts plus TestAcknowledgedResultSurvivesCleanupFailure / TestClaimRetirementCleanupErrorStillHandsOffAcknowledgedCandidate; actual stores, not err==nil mocks. |
| M25 | Stale destination/activation/epoch or uncertain current plan seen after hint | TestChannelDeliveryEffectCurrentnessSelectedStoreParity, TestChannelDeliverySharedAudienceE2E, selected-store first-send/response/recovery tests; zero automatic launched/uncertain dispatch. |
| M26 | External provider mutation has no local producer hint | TestChannelNativeLocaleQualificationPublicJourney external command/launcher edits; new production-default backstop control at unchanged1s/5s; successful readback after seed, no local-hint credit. |
| M27 |2.2s uncertain-notice and duplicate-resend absence waits | inbound supported surface lines1445/1468; M14 actual roots, replace only after two successful relevant delivery/action passes STARTED after exact settled intent/uncertain row. Same count assertions and deadlines. |
| M28 |2.2s lost-prompt-edit absence wait | same file1551; ManualResendAfterLostPromptEditE2E with successful no-redispatch passes and exact prompt-edit count. |
| M29 |2.3/2.5/1s ACK-loss/uncertain-inbox/restart absence waits | same file869/885/893; M18/M19 exact roots; successful relevant action/delivery passes after durable cut, not elapsed wall time. |
| M30 |5.5s native restart-mismatch/install-loss/readback-error/foreign-command negatives | same file527/590; see explicit proof restriction below. Expected-failure passes do not earn success counter credit; keep original window unless independently approved stronger oracle is executable. |
| M31 | Short20/25ms convergence polls | Preserve as bounded observation polls; wake makes condition earlier but does not change timeout/assertion. Not a blanket sleep deletion. |
| M32 | Human-task/decision deadlines and measured provider-fault/TTL windows | TestChannelDeliveryHumanTemporalReceiptPublicJourney, TestChannelLearnedObjectHumanTemporalReceiptPublicJourney, TestChannelDraftTerminalRestartPublicJourney; preserve wait-to-authoritative timestamp and held-finalization controls. These windows are not ticker delays. |
| M33 | Failed/no-op scans falsely count absence or self-sustain polling | Planned TestChannelWorkerPassEvidenceRejectsFailedAndPreMutationPass: failed native/ordinary phase, page error, canceled/started-before mutation and unused stale child record cannot satisfy fence. No-op writes emit no demand. |
| M34 | Ordinary/API/deployment/timer/schedule/scenario publication creates receiver gate indirectly | Planned producer matrix P61-P65 with actual publication plus nested/recursive construction; TestChannelDeliveryRealAnchorProducersPublicJourney, source-owner activation/canonical public data-run-creation controls; later child failure rolls back cards and gives NO hint. |
| Q01 | Entire serveapp-channel command | Execute unchanged canonical selected command at final source; catalogue/onboarding/restart composition included, no credit from4-root characterization alone. |
| Q02 | Entire serveapp-channel-delivery command | Same exact full root/child selection; all notices/card/input/resend/cursor cases, both stores. |
| Q03 | Entire serveapp-channel-learned command | Same; full learned-object/paging/input/anchor/temporal cases, both stores. |
| Q04 | Entire serveapp-channel-lifecycle command | Same; source/reset/fork and selected-fork input-boundary cases, both stores. |
| Q05 | Entire serveapp-channel-native command | Same; positive locale/readback and refused/uncertain/install-death cases, both stores. |
| Q06 | Entire serveapp-channel-process-temporal command | Same; real process death/restart/drain/receipt cases, both stores, unchanged measured windows. |
| Q07 | Guard/census/inventory/registry/complexity and supported API spec | Local complete guard sweep before server2; then existing local core; no membership/TSV/baseline/guard weakening. |
| Q08 | Matched cost | Same four characterized roots pre/post on same host/toolchain/stores; six hosted commands before/after, record runner compute separately from package/queue/wall; no speed claim from shorter deadline. |
| Q09 | Final exact-head qualification and proof audit | Reviewer-ratified hosted tier, normal complete Required summary/native tier receipt; approved local core+named supplements, all rows named. Parent fleet/nightly remains open. |

### Successful Pass Evidence, Including The Native Negative Limit

Extend the existing ServeOptions test hooks and owned compiled test-child
composition with a bounded success-only progress record. No public RPC or
unowned helper process. Record independent completed entry/delivery/action/
native scopes, process identity, and pass-start sequence. A negative fence
captures its cut AFTER mutation acknowledgment/observed durable state and
waits for two relevant successful passes whose starts are after that cut.
Held/injected failure, pagination error, cancellation or a merely attempted
scan cannot advance it. A restarted child's record cannot satisfy predecessor
evidence. Negative controls must hold the actual target path and prevent the
assertion from completing until its relevant successful pass finishes.

Important existing counterexample to a blanket replacement: in
`channel_native.go:qualifyNativeInboxActivation`, `recordFailure` joins the
qualification cause with the durable record write error. Foreign commands,
uncertain installation and unavailable provider readback deliberately return
errors. There may be NO successful native pass while that condition persists.
It is dishonest to shorten M30 by counting those attempts or unrelated passes.
Recommended bounded disposition: keep M30's original5.5s negative windows and
failure/identity/count assertions in this slice; explicitly leave them as
adverse-native proof, not achieved pass-wait savings. Positive native locale
observation and successful ordinary uncertainty scans still accelerate.
No product error-to-success conversion is proposed. Reviewer-g should ratify
this restriction in the coding re-gate, or name a required stronger exact
refusal oracle before coding; an unreachable success checkpoint is a STOP.

## Proposed Authoritative Spec Delta

Add within existing managed_external_effect_authority context_authority,
without changing existing authorization/settlement text:

> Channel reconciliation remains owned by the single served Process worker.
> Named local domain owners publish coalesced ordinary/native hints only for
> actual changes from an acknowledged OUTER transaction or completed exact
> process publication. A later cleanup error does not erase acknowledgment;
> rollback or unknown commit cannot fabricate it. Borrowed writers carry
> change facts and never publish before outer finalization. Hints confer no
> business authority: current selected-store/compiled-generation/receipt
> rechecks remain mandatory. Immediate startup scan and production periodic
>1s ordinary/5s native backstops cover work with no local hint. In-pass changes
> remain pending for a subsequent pass. Work and subscription retire/join
> with the Process; no launched/uncertain automatic redispatch is introduced.
> Private test cadence affects observation only. Negative pass credit requires
> successful relevant reconciliation started after its mutation cut; errored,
> canceled, pre-cut or foreign-process attempts do not qualify.

## Baseline, Tracker, Architecture, Feasibility And Gate Request

Fresh source2fc outcome/currentness controls PASS:9 roots,3 packages, zero
fail/skip; channelonboarding4.444s, decisionpersistence0.007s,
mutationprotocol1.390s. Includes real both-store publication/callback/native
lock order plus controlled SQL-mock decision admission/expiry, ack+cleanup and
missing-ack controls. Controlled tests earn no real selected-store write proof.
Receipt: ~/.local/state/agent-g-phase0/2fc6a13bd-channel-outcome-baseline.json.
These characterize unchanged owners; no new hint/refusal/fork-wake proof.

Prior four public roots at identical-tree329 PASS230.956s/42 records/no skip;
not relabeled as a new-head qualification. Six hosted channel commands at
complete run37937174260 sum2,290 primary command-seconds;1,145-1,603 possible
saving is still an extrapolation, not acceptance. M30 is excluded from any
claimed pass-wait reduction until the independent restriction is settled.

Architecture smell: owner-specific writers throw away acknowledgment/change
evidence on the way to public error-only returns. Promote the smallest typed
private handoff NOW; do not build a generic commit framework, reinterpret
story/revision declarations, add SQL observers or leave a known local writer
silently on the backstop. Long-run shared-owner direction is the current
native outcome/mutation Result plus explicit domain result composition.
Tracking: refine existing harness_reliability_and_local_smoke under OPEN2535;
no new issue/POTENTIAL_ISSUES. #2250 remains broader composition architecture,
not authority to redesign it. Same-concept closure feasible in one bounded
PR with medium confidence; fixing the local timer alone would leave MANY
listed live producers without the owner-specific handoff and is rejected.

Tracker-state decision: update2535 current first-slice boundary/proof/tier and
existing watchlist before coding; no new child or superseded closed stream.
Intended closure: local acknowledged-change scheduling class eliminated;
successful test observation seam canonicalized; no all-waits/performance/
broader-fleet closure claim. Proof audit must give each P/M/Q actual receipt.

Tier request: provisional CI lifecycle/local core+focused both-store journey
and race controls from6084282948. This census exposes selected-fork and run-
terminal card consumers, including full-only outer-owner proof obligations.
Request reviewer-g explicitly choose whether that requires CI FULL before
coding/qualification; do not silently assume lifecycle suffices. Keep shared
native/mutationprotocol semantics unchanged; if those must change, STOP and
escalate to full as ruled. No tier run/server2 ask until this re-gate.

Other stops: missing/more-live producer or finalizer; inability to carry
changed/no-op without new framework; incomplete ack outcome; successful-pass
checkpoint unreachable; nondiscriminating negative/cost probe; changed
authority/currentness/redispatch/TTL; any wider startup/reset restructuring;
or necessary concurrent test-unit membership change. Current coding is
FROZEN pending independent recorded outcome on this complete artifact.
