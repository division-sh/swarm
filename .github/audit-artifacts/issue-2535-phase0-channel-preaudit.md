# Pre-Implementation Coverage Audit: #2535 Phase0 Channel Worker

Status: CODING APPROVED AS FIRST SLICE by6085629534, completion delta6086299760
and activity delta6087852750. The historical stops below remain recorded;
no closure claimed.
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
| P06 M | same advance, reboundActivation update AND first transition into PhaseSucceeded | Same runner/native COMMIT | Actual rebound coordinate/publication change OR successful completion releasing current delivery/native eligibility -> O/N, approved6086299760. Other phase-only/checkpoint edits are D; terminal/revision refusals are not converted to replay success. |
| P07 M | same retireActivation | Same runner/native COMMIT | Actual retirement -> O/N; already retired no-op; preserve CAS/reason. |
| P08 M | same retireTeardownAuthority (pending or completed identity retirement) | Same runner/native COMMIT | Retired matching operation/activation rows -> O/N; no matching affected authority no-op. |
| P09 D | same completeTeardown | Same runner/native COMMIT writes only the operation checkpoint | No native-consumer write is borrowed here. Actual stale consumer retirement is a separate P12 outer transaction. Completion/replay retains its checkpoint contract and emits no hint. |
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
| P54 D -> P30 M | effectpersistence SettleCompletion/provider drain or recovery requesting run completion | Existing completion/recovery outer Run* result commits the request/accounting; ExecuteCompletionCandidate later owns actual run/card terminalization | RequestCompletion and agent draining do not themselves change the rendered card. No local hint for those journal writes; actual later SupersedeRunTx change belongs to P30's acknowledged completion-candidate finalizer. Preserve provider drain and no redispatch. |
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
| P67 A | operatorchannel/owner.go:bindFromProof / BindOperatorChannelFromProof | operatorchannel.Service.Bootstrap, sole production call serveapp/main.go:1339 BEFORE worker starts at1864; own native transaction | Boot-only binding/default work is consumed by P56's mandatory initial scan. Exact proof mismatch/unbound fence/replay retained. Not proof bookkeeping, and not a missed live post-subscription producer; there is no mounted post-start Bootstrap entrance. |

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

Implementation addendum below adds M35 to the original M01-M34 matrix. The
original proof obligations and native M30 restriction are not removed.

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

## Implementation Stop Addendum: Completion Eligibility

The original final gate is approved6085629534, CI full / Local core plus
the named supplements. It supersedes the provisional tier/frozen state above.
Production edits began only after that gate. The following classification
correction triggers its stop condition; further production work is frozen
pending a focused amendment ruling, not permission to widen silently.

**Observed gap:** P06 originally called every phase-only update a checkpoint
unrelated to worker input. That is false for first transition to
`channelonboarding.PhaseSucceeded`. The existing onboarding service drives
this after successful confirmation at `internal/channelonboarding/service.go`.
Both SQLite and PostgreSQL `channeldelivery.CurrentActivationID` joins require
`onboarding.phase='succeeded'`; native admission and qualification require the
same phase in `native_setting.go` and `native_qualification.go`.

Full path: verified binding -> durable activation -> exact process snapshot
publication -> registration promotion -> confirmation effect settlement ->
`advance(...PhaseSucceeded)` native COMMIT -> selected current activation /
native eligibility -> existing served worker scan -> authorized provider
delivery/native operation. The confirmation must complete before this final
transition is reachable. Binding/publication/registration remain their named
P01/P05/P55 boundaries; the final phase transition is an additional relevant
change inside already-listed P06, not a new runtime owner or product feature.

| ID | Manifestation | Planned exact proof |
| --- | --- | --- |
| M35 / P06 completion | Earlier durable/process publication wakes may be consumed before succeeded; the last eligibility transition emits no hint if treated as a checkpoint, leaving work to the 1s/5s backstop | Existing `TestChannelDeliveryEffectCurrentnessSelectedStoreParity/(sqlite|postgres)/current`: no delivery authority before completion; exact activation becomes selected after completion; one O/N hint only after its outer native acknowledgement. Extend with ordinary/native worker held-before-finalization and failed/stale/terminal-refusal controls. Public `TestChannelConnectTelegramFirstUserJourney`, notice and native inbox journeys prove the service-driven ordered path. |

The local counterexample extends that selected-store root with a subscription
immediately before its existing final transition and a demand assertion after
the existing exact selected-activation readback. It does not alter production
phase handling, public success/refusal behavior, SQL admission, deadline or
provider actions. The callback body already uses the native outcome API in
the WIP rebound repair; it carries no completion change fact yet. Missing
notification is an intermediate implementation/audit counterexample, not a
claim of lost durable execution on master (master still polls).

Canonical owners and systematic consumption: `channelonboarding.advance`
owns this sole operation transition; the existing service drive and retained
recovery consume that owner, not a second phase writer. The actual readers
are delivery activation/currentness and native setting/qualification owners.
Other onboarding phases/checkpoints, proof bookkeeping and ceremony writers
remain D unless they perform the already-mapped rebound/retirement. P67 boot-
from-proof remains initial-scan A; P09 checkpoint-only teardown remains D;
actual stale consumer retirement remains P12. No extra registry or shared
transaction change is needed, and no other producer is granted a blanket
notify-on-success rule.

Proposed bounded correction: reset the private changed fact at callback entry;
capture the prior phase under the existing operation lock; record O/N only
for the actual successful transition (or existing rebound change); publish
only after native acknowledged COMMIT. Preserve acknowledgement plus cleanup
error. Abort/unknown commit has no notification; stale/terminal refusal has
none and retains its exact error. No retry or phase validation is changed.

Tracker action: amend this P06/M35 classification and #2535 current status,
refine the existing `harness_reliability_and_local_smoke` node, and request
reviewer-g's short independent delta gate. No new issue/parent absorption;
#2535/#2250/#1196/#2353/#2394 remain open. Existing parent sibling census,
remaining Phase0 tail, architecture disposition and no-vendoring/no-framework
conditions remain binding. Class commitment and one-PR feasibility unchanged:
close all acknowledged local worker-input changes, not only this transition.
The 67 P classifications and now35 M rows are still planned closure proof.
No qualification or measured saving is claimed; no server2/full run started.

### Completion Delta Gate And Reader Census

6086299760 approves this completion amendment in the same first slice and
permits production edits to resume. CI full / Local core plus the named
supplements, native M30 windows and all original stop conditions still bind.
The absent-hint counterexample above remains historical failing proof.

The phase's complete known worker-reader family is: activation.go (current
activation/cursor), list.go (current plan selection), resolve_text.go (verified
text/reply currentness), resolve_action.go (receipt/action currentness),
native_setting.go (exact native admission), native_qualification.go (qualified
consumer), effectpersistence/channel_delivery_authority.go and
effectpersistence/channel_native_setting_authority.go (authorization/prelaunch).
All already consume the canonical persisted onboarding phase; none is a new
phase writer or a new interpreter to refactor. M35 must execution-prove these
readers before completion, after its actual post-cut wake, and through retained
stale/foreign/terminal refusal. Hint cardinality alone earns no closure.
In list.go the explicit succeeded relation is the requested-response branch;
ordinary notice/card responsibility discovery remains possible before completion.
Its worker activation and effect-authorization gates still refuse execution.
Do not replace this distinction with an incorrect all-plan-list-empty assertion.
`advance` remains the sole completion writer and must retain the prior phase
under its existing operation lock. Exact terminal/stale requests keep existing
refusal behavior; only actual completion or rebound produces demand.

## Implementation Stop Addendum: Activity Dispatch Render Sources

A second stop condition is met: the final frozen-render dependency sweep
found an additional live domain writer, not another reconciliation worker.
Further production edits are frozen for reviewer-g's additive owner-map gate.
Approved implementation remains uncommitted in agent-g-test-time-phase0;
pushed3eab1592c remains audit-only. This is not a request for a new product
operation, framework or a global activity notifier.

### Entry Point, Exact Concept And Execution Path

`backend/channeldelivery/source_render.go:freezeCurrentCardTx` calls the
existing `decisionpersistence.ProposedEffectReadbackInTx`. Its readback reads
`activity_attempts.status,execution_mode` by the continuation's exact reserved
request ID and feeds `DispatchState` to `FreezeCard`. Consequently the first
attempt insertion and actual terminal status changes alter card render bytes
without a card-row or proposed-continuation-row mutation. P58's exclusion of
provider effect/ACK accounting does not classify these activity dispatch facts.

Full path: create proposal/card and reserved request -> decide approve ->
commit request release -> live/loop claim through the activity journal ->
acknowledge started attempt -> provider dispatch -> complete/mark uncertain ->
acknowledge terminal attempt -> exact proposed-effect readback -> frozen card
render and authorized channel edit. Card, continuation, run and execution-mode
authority must already agree before this render is reachable. Card/route
creation and request release are the existing P25/P26/P27/P29 family; activity
status is the newly named same working class below; provider effect admission
and non-redispatch remain different business concepts under their existing
owners. Channel destination, Process subscription and native acknowledgment
gates retain their original classifications and proofs. A hint grants none of
those authorities.

Chosen class remains acknowledged local channel-worker-input changes without
post-commit reconciliation demand. Immediate parent remains unnecessary
reconciliation latency/test waiting; #2535 is the fleet parent and #2250 the
separately open composition architecture parent. This is a missing rendered
source in the existing class, not evidence of data loss on polling master.
The first local helper is not the boundary: both activity backends, borrowed
fork copies, destructive deletion and all readers/outer finalizers are named.

### Canonical Owners And Systematic Consumption Delta

The activity journal owns attempt identity/status and its standalone native
finalizers. The decision/proposed-continuation owner owns the exact card/request
dependency and readback. Fork preparation owns reminting copied historical
attempt evidence; its borrowed helper does not own COMMIT. The one existing
Process-owned channel worker remains the only scheduling owner.

| Row | Producer and outer native boundary | Required consumption / proof classification |
| --- | --- | --- |
| P68 M | activityjournal.Start -> ActivityPostgresOwner/ActivitySQLiteOwner.StartActivityAttempt -> RunPostgres/RunSQLite.Value | Move to existing signal only for actual inserted attempt with exact linked card; validated insert replay/unlinked attempt stays quiet. |
| P69 M | activityjournal.Claim -> ClaimActivityAttemptForLoopGeneration -> each existing Run*.Value | Move to same handoff after real loop admission; stale generation/run refusal and exact claim replay stay quiet. |
| P70 M | activityjournal.Complete -> each CompleteActivityAttempt outer Run*.Value | Move actual started-to-terminal row-change fact plus exact linkage to acknowledged finalizer. Its public bool currently means acknowledgment, not changed; preserve that contract. |
| P71 M | activityjournal.MarkUncertain -> each MarkActivityAttemptUncertain outer Run*.Value | Move actual row change through existing finalizer; started/terminal/replay/refusal and exact mode remain fail-closed. No redispatch. |
| P72 D, execution-probed | copyRunForkActivityAttemptEvidence -> prepareRunForkSelectedContractSourceEvent -> PG LoadRunForkSelectedContractSourceEvents (ReadCommitted RunPostgresWithOptions) or SQLite counterpart (RunSQLite), each Acknowledged | Historical copied request identity is distinct from a fresh materialized pending card's reserved request. The both-store selected preparation proof below compares exact identities, absence before/after, held dispatch and immutable card/source evidence. No historical-copy hint is added; actual materialized cards are P32, and subsequent linked journal writes are P68-P71. Predecessor approval/suppression is not current-card linkage. |
| P73 D for copied evidence; P33 M retained | same borrowed preparation reached by projectRunForkReplayEvent -> applyRunForkDeliveryEventReplay -> ordinary ActivateRunFork PG/SQLite outer Run* | The both-store ordinary activation/replay matrix below proves the same identity separation, separately from P72, with no-card and real pending-card controls. Source-card terminalization still carries its actual P33 change to the activation finalizer. No notification inside borrowed copy/preparation/replay and no extra hint for copying historical evidence. |

Runtime pipeline activity_engine.go (live and replay), persistence_ports.go and
activity_journal.go already consume the canonical activity methods; generated
runtimepersistence delegates both existing owners. They gain no public
authority or schema. Eventpersistence embeds that owner, not an independent
status writer. The decision readback already consumes journal truth and must
keep its execution-mode contradiction refusal. Channel freeze/render already
consumes that readback; adding a second dispatch interpreter is forbidden.

The repo-wide production `activity_attempts` read/write sweep also found:
standing-service started/uncertain eligibility reads and fork-source-freeze
dispatch evidence reads (different business admission; actual terminal card
change already P30/P33); selected-fork discard and destructive reset deletions
(existing P33/P59, cards removed with their target, never pretend a removed
target gained a surviving card); immutable author-activity story occurrences
(different append-only historical concept). No other direct status writer was
found. No-op/rejected/unknown outcomes in all six new rows must remain quiet.

The complete FreezeCurrentSourceTx arm check names response's immutable stored
render/current-plan pointer (P39-P42/P47/P52), summary count/presentation
(existing channel plan owners), notice summary/priority/payload/notified and
source coordinates (P15-P18/P52), and card row/change revision, active input
draft, proposed continuation and activity dispatch (P19-P29/P36-P38/P45-P47
plus this delta). Exact destination bounds/page/audience remain existing
plan/default/action owners. Prompt expiry is already P23. These are explicit
reader dependencies, not claims that a helper name alone proves execution.

Old non-authoritative interpretations to remove/delegate are error==nil as
change evidence, completion's acknowledgment bool as row-change evidence, and
card/continuation change rows as an exhaustive render-input list. No old
selected-store migration, compatibility or guessed payload/effect-class link.

### Bounded Design Proposal And Spec Delta

Keep public activity return types/boolean meaning and shared mutationprotocol
semantics unchanged. Promote private actual changed/no-op facts at the
existing journal/copy owners, reset them per attempt, and let the existing
decision/proposed-continuation owner provide exact request/run/mode linkage.
Its authoritative `proposed_effect_continuations` DDL already has UNIQUE
request_event_id (platform-spec.yaml ~18107); no new table/index/registry is
needed. Only a relevant changed fact may produce ordinary demand after the
named outer native acknowledgment. Original cleanup/handoff error survives;
abort/rollback/unknown/no-op emits none. No global notify-on-any-activity-write
and no native-scope provider scan for card-only demand.

Proposed authoritative spec delta, pending gate: the channel scheduling
contract also names linked activity start/terminal dispatch facts and borrowed
fork copies as frozen-card inputs, while retaining exact dependency, no-op,
acknowledgment and no-redispatch restrictions. This does not alter the attempt
state machine, immutable fork evidence or render business meaning. P54 is
corrected above: completion request/agent drain is not actual run terminal.

### Manifestations And Exact Planned Proof

| Row | Manifestation | Required proof on both stores |
| --- | --- | --- |
| M36 | Live/loop activity first start and succeeded/failed/uncertain status changes affect exact proposed card dispatch render | Existing TestProposedEffectReadbackKeepsAuthorizationAndDispatchAxesSeparateOnBothStores gains exact post-cut demand plus its original rendered Dispatch bytes and mode-corruption refusal. Add actual valid loop Claim and dedicated MarkUncertain; run the real activity pipeline/public card edit through the corrected owner with repair ticks held, not just hint membership. |
| M37 | Exact terminal/claim retry, unlinked activity, foreign run/mode/generation and native outcome cuts must not cause another scan or dispatch | Use real journal replay/refusal and unlinked records; preserve original acknowledged bool. Existing-owner fault controls for rollback, unknown COMMIT, ack+cleanup and in-pass hints must distinguish actual row change and retain original error. Measure linked/unlinked path query/call cost; no blanket slow-path scan. |
| M38 | Fork copied activity evidence can arrive outside standalone journal completion | Execute PG/SQLite selected source preparation and ordinary replay activation separately, including exact retained repeat, linked and no-card facts, rollback and post-ack cleanup. Assert fork-local render/wake and source/sibling noninterference; where linkage is impossible prove that actual supported branch and classify D rather than infer it. |

The selected-store counterexample is execution-proven on the intermediate
WIP, with no production activity-owner edit: all eight both-store/status cells
fail only the new absent-wake assertions while original readback/render/mode
assertions pass. Race3 repeats all24 cells with42 missing-wake assertions,
87.287s, no race warning or unrelated failure. Command:
`go test ./internal/store/internal/runtimepersistence -run '^TestProposedEffectReadbackKeepsAuthorizationAndDispatchAxesSeparateOnBothStores$' -race -count=3 -v -timeout=4m`.
Evidence: channel-activity-wake-counterexample.log under agent-g's persistent
state directory, SHA256
14dca6cd8806d74888636118015d89b45e89e23a60fe8952b676c136a40d7aa8.
Dedicated Claim/MarkUncertain and conditional fork manifestations are planned,
not executed or closure-credited. No unchanged-master polling failure is claimed.

### Tracker, Promotion, Feasibility And Gate Request

Current issue must be repaired before further coding; additive audit and the
existing harness_reliability_and_local_smoke watchlist node are refined for
activity render-input coverage. No child/umbrella supersession, new issue or
POTENTIAL_ISSUES entry. Parent sibling probe above finds no second worker; the
watchlist's post-commit producer/readback family requires absorbing this live
input now rather than leaving a dishonest card-only first slice. Broader
transaction/startup/provider lifecycle work stays #2250; remaining performance
families and fleet/nightly acceptance stay #2535/#1196/#2353/#2394. Original
remaining-tail grouping/confidence is unchanged, not promoted to closure.

Architecture disposition: promote the small private fact in the existing
journal/decision/fork owners now, watchlist only for broader composition debt.
Estimated bounded correction is one implementation pass plus dual-store fault,
loop/fork and public proof; medium confidence until conditional fork linkage
is executed. ROI is complete rendered-input coverage without waking every
activity or changing provider execution. Intended closure stays complete for
the chosen class, not the parents: one PR is feasible with these six named
outer boundaries; a local card-only fix would leave this same-concept writer
live. Counts are now73 P classifications/38 M manifestations/9 Q obligations,
all new rows still planned except the explicit failing counterexample.

Request reviewer-g's short independent additive gate before any activity-owner
repair. CI full / Local core plus existing supplements remain binding, with
these focused both-store additions. M30's5.5s windows,1s/5s production repair
and all original refusal/cleanup conditions remain unchanged. Freeze again
for further unclassified live inputs or any required shared transaction,
authority/schema or new-framework change.

Current intermediate approved-boundary proof: service-driven onboarding wake,
exact Process/pass and worker controls pass race3 (39.225s/1.025s); real
card-kind create/terminal/replay controls pass race3 (33.253s); public notice
and prompt uncertain-edit roots pass13.319s with post-cut successful-pass
oracles. No matched speed saving or full manifestation closure is claimed.
The all-package census diagnostic completed RED: startup predicate owner
hash/proof ledger, existing selected-fork writer token, new root partition
assignments/catalog consumption and classified persistence registry deltas
need explicit updates. No guard is waived or weakened. The exact structural
owner-guard unit passed independently; neither receipt is local core/full
qualification. No server2/full run or review-ready PR has started.

### Activity Delta Approval

6087852750 approves P68-P71 repair and conditional P72-P73 probing/repair in
this same PR. For fork copies, first prove a live fork-local card's exact
dependency; do not manufacture a hint for unrelated historical evidence.
Further same-class persisted eligibility/render producers may be absorbed
without another per-discovery gate only after immediately numbering each
owner/manifestation, identifying its outer native acknowledgment/change fact,
and retaining both-store counterexample/fix and a supported worker path.
Consolidate the reverse reader-to-writer census at the next independent
checkpoint. Ambiguous linkage/commit ownership, second interpretation/owner,
business/schema/transaction/startup/effect changes still require a stop.
The existing CI full / Local core plus supplements and all other restrictions
remain binding. The original missing-wake receipts are retained as failing
proof, not replaced with qualification credit.

### Reader-To-Writer Checkpoint And Implementation Receipts

The reverse sweep starts at actual worker reads, not a list of presumed
source tables. Every dependency below retains selected authority; hints do
not reinterpret any gate. No additional standalone journal status writer or
channel worker was found in this delta.

| Existing reader / persisted dependency | Existing producer/finalizer consumption |
| --- | --- |
| CurrentActivationID; list/get destination currentness: default, binding/claimant/epoch, succeeded operation and connected activation | P01-P08/P55/P67, including the approved P06 final succeeded transition and exact process publication. |
| CurrentCardChangeCursor/ListDecisionCardChanges; open/deferred card lists and list-triggered expiry | P19-P31/P45-P47; actual card/temporal changes, not every list success. Time passage without a commit remains the repair tick. |
| ListCurrentPlans/GetCurrentPlan send predicate: pending notice/card or exact sent predecessor receipt | P15-P31/P39-P53/P59; accepted historical effects do not authorize redispatch. |
| AcceptedEffectPredicate and Candidate.RecoveryPending: authorized/launched/response-observed original journal responsibility | P58 different concept with named execution: reconcileDelivery returns without rendering/dispatch when RecoveryPending; original effect recovery/terminal projection is P52/P53. This visibility predicate is not a second worker recovery executor or a new demand for another provider call. |
| FreezeCurrentSourceTx response: exact immutable stored render plus plan pointer/bounds/page/audience | P39-P43/P47/P52; no reader-derived response or payload compatibility. |
| Freeze summary/notice: summary count, notice type/summary/severity/payload/notified/source coordinates | Existing plan owner/P15-P18/P52/P59; source/reset deletion does not create a new surviving target. |
| Freeze card: card row/revision, active draft/field index and proposed continuation | P19-P29/P36-P38/P45-P47; prompt expiry remains P23. |
| Freeze proposed dispatch: exact continuation request/run/mode and journal status | P68-P71 now consume actual insert/update plus indexed exact dependency before their outer acknowledgment. P72/P73 conditional fork copy is probed separately, not notified blindly. |
| ListPendingActions/ResolveAction/PreviewSkip; action current receipt/render/token/settled status | P34-P38/P42-P44/P48/P52; receipt-authority retirement and replay keep their original refusal. |
| ListPendingTexts/ResolveText/PreviewInput/choice; exact current draft/answer/disposition | P34-P38/P39-P42/P49-P51; no guessed latest card/input target. |
| Pending native inbox entries, qualified native setting/consumer, locale and exact current activation | P05-P14/P34/P39/P51-P53/P55; time-based qualification expiry and external readback still use5s repair. |
| Selected presentation/executable provider capability and effect prelaunch currentness | P55/P56 publication and P52/P53 terminal projection; immutable boot-pinned source/provider inputs are not live hot replacement. Exact effect admission remains business authority, not wake authority. |

P68-P71 preserve public results. Complete/MarkUncertain private helpers now
return their actual UPDATE row count separately; the public methods still
return true for acknowledged no-op. Decision persistence checks the existing
UNIQUE reserved request/run and decodes its canonical mode; unlinked changes
use one indexed lookup, exact mutation replay skips it. No card/destination
scan or global notify-on-activity-write. All eight journal outer finalizers
consume their acknowledged result before notification/error projection.
Private fork card accumulators reset at each existing outer attempt callback;
rolled-back facts cannot leak into a later no-op. No shared protocol change.

The expanded journal before-wiring diagnostic intentionally suppresses only
the eight scheduling hints, never the journal writes/readback/authority. On
both stores, linked Start/Claim cases and real valid loop claims fail the new
demand oracle; original dispatch/readback cells continue and also prove the
dedicated MarkUncertain omission. Unlinked and hostile header/generation cases
stay green. The candidate notifications are restored immediately afterward;
this mutation-control receipt is not an unchanged-master or qualification
claim. Log: channel-journal-before-wiring.log (package20.831s, three roots RED).

The following intermediate candidate receipts pass:
- TestChannelPostCommitActivityHintsBothStores:16 linked/unlinked start,
  claim, complete and dedicated uncertainty cells, cancellation, exact replay
  and original acknowledged-plus-synthetic-cleanup-cause controls,25.535s.
- Existing full readback/story disposition/validation/atomicity and native
  claim-reply-loss group,16.305s; original state, story and mode assertions retained.
- TestActivityAdmissionConsumesConstructedHeaderBothStores:28 real current,
  stale/foreign/missing/malformed header cells,21.738s; current linked loop
  claim/completion hints and quiet repeated claim are added without test SQL.
- Existing TestChannelDeliveryRealAnchorProducersPublicJourney: all six
  original public webhook anchor cells pass with one-hour repair ticks,26.20s.
  Its action runs inside the worker; it proves in-pass hint retention, not a
  successful scan while that same action's provider request is blocked.
- TestChannelDeliveryReconciliationActivityDispatchPublicJourney:20.258s on
  both stores. Separate real public mailbox.decide HTTP request holds the
  exact business provider call, observes started edit, drains earlier hints,
  then releases and requires succeeded edit plus a post-cut successful pass
  without a repair tick. All HTTP/client work is joined; no in-process dispatch.
- Selected preparation copied-evidence and fresh pending-proposal controls:
 9.769s on both stores. Approved terminal copies have no fork-local card;
  source attempts remain immutable, no hint is fabricated. The existing
  pending-proposal materializer reserves fresh exact authority, has no attempt
  at that request and reports held. Ordinary activation/replay P73 and a
  reachable linked-copy control remain uncredited until execution probing.

Fixture-only corrections during this work: the new public read DTO extracts
only content hash (semanticvalue must not be decoded with encoding/json);
new loop-linked proposal uses the fixture's exact admitted bundle/version.
The first blocked-worker started-edit oracle was invalid because that worker
was synchronously executing the held action; the independent RPC proof above
supplies the discriminating completion cut. Their failed diagnostics remain
recorded, not production failures or deadline changes.

Census repair is explicit, not a registry-only permission: A10/A27's exact
activation snapshot publication owner/callers still consume the same startup
checks and may emit demand only after actual generation publication; A22/A24
selected recovery retains admission/authority and carries only actual terminal
card change. Original crash boundaries E2E14/E2E16/E2E17 and selected recovery
controls remain required. Updated startup body hashes earn no execution credit
by themselves. The selected-fork writer-token guard now also requires the
exact publisher binding, alongside the unchanged mutation/acknowledgment token.

The authority registry's237 new resolved signatures are classified against
their named existing owners:141 private-backend (changed facts/native owner
calls),80 private-runtime-adapter (existing channel/native native finalizers),
10 private-domain-adapter (mailbox/admin/retained cleanup), two typed-process-local
subscription interfaces and four typed-public-facade scheduling capabilities.
187 stale signatures disappear. No new raw public carrier or classifier
exception; the independent raw-method/debt guards remain required. No collector
or debt baseline change is proposed. New worker/completion proof roots have one
owner in the existing serveapp-channel-delivery lifecycle/full unit; no old root
or assertion was removed/re-tiered. Partition/catalog and fork-writer census
controls pass22.316s/0.460s/0.324s. Full census, native fault cuts, race repetitions,
six channel units, ordinary fork/reset supplements, matched costs and clean-head
local core/CI remain required. No whole-class or performance closure yet.

Checkpoint receipts after this delta:
- Journal linked/unlinked/replay/cancel/cleanup, exact rendered mode and real
  constructed-header loop group: race3 PASS249.428s,156 leaf cells. Log
  channel-journal-after-wiring-race.log, SHA256
  15a38ecb2e721decce626b273fd07a090bb324611d36d6bfb22e38a7c9c0f15e.
- Disabling ONLY both Complete notifications, leaving all earlier producer
  hints intact: real public completion root FAILS both stores38.811s, stuck
  at Dispatch: started after the actual provider/terminal commit. Its earlier
  successful passes are drained. Log channel-public-completion-before-wiring.log,
  SHA256a3b41e9f1c34947f298864390c3d964e53e51fe98647d7386759d289c1026e82.
  Both notifications restored; race3 public root PASS170.602s, six store leaves.
  Log channel-public-completion-after-wiring-race.log, SHA256
  4bf1c27579f311eeea0c7575f57a9da4b9819bf078f860d3203ffe3969d47ab5.
  Race elapsed under shared host load is not a default-run cost/savings claim.
- Finding registry and no-raw-public-method guard PASS8.287s via swarm-test
  after the existing shared-slot8m12 wait, not bypassed admission. Full API-spec
  package PASS5.959s; direct primitive/managed owner controls PASS2.262s and
  activity schema-first refusal PASS0.006s. No native/backend protocol changes.
- Fresh all-package Census/Inventory/Registry/Guard sweep via swarm-test
  completed PASS after shared-slot3m15 admission; the earlier owned startup,
  signature, catalog/partition and fork-writer reds are corrected, not waived.
  This is a diagnostic regex sweep, not the full structural unit or a local
  tier receipt. Log channel-census-after-wiring.log.

Current approved implementation, spec, tests and owned census corrections are
preserved in one local signed implementation commit after this checkpoint.
It is not a review-ready/pushed runtime head: ordinary fork-copy replay/linkage,
native outcome cuts, complete P/M/Q execution and matched costs remain open.
Next qualification stays core plus the six named channel units/supplements,
then exact-head hosted full. No local full or server2 slot is requested.

## Ordinary Fork And Native Outcome Proof Amendment

This test-only amendment follows the independent consolidated checkpoint
6088824575. It resolves the conditional P72/P73 classification separately;
it does not add a producer, change an activity/fork contract, or infer a
worker input from the existence of an activity row. Production code remains
the reviewed e7ec69633 tree.

- P72: TestRunForkActivityTimestampRecordedReuseBothStores retains its16
  original status/policy/store cells and no-card/source-immutability/retry
  assertions. TestSelectedForkRecordedActivityCardDependencyBothStores adds
  four real selected-preparation cells with a pending fork-local proposed
  card present. Before preparation its reserved request has no attempt.
  After the native outer commit, the exact copied request is DIFFERENT;
  the pending request still has no attempt, its dispatch is held, and the
  card and predecessor journal evidence are unchanged. Preparation publishes
  no hint. This is a different historical-evidence concept, not a missing
  live-card notification or an assumed initial scan.
- P73: TestOrdinaryForkActivityReplayCardDependencyBothStores has32 cells:
  both stores x recorded/approved-effect policy x succeeded/failed/uncertain/
  started evidence x no-card/pending-card presence. Actual ordinary materialize
  and ActivateRunFork with HistoricalReplayExecutionAdmitter reconstruct one
  exact replay delivery. The copied deterministic request is absent before
  activation and present afterward, and its identity matches the admitted
  replay payload. Any pending card retains a distinct reserved request,
  absent attempt and held dispatch before/after the activation COMMIT. Source
  attempts remain unchanged. Started evidence and approved uncertain evidence
  refuse with whole execution snapshot unchanged and no hint. Actual source
  card terminalization emits the existing P33 ordinary hint; without such
  cards historical copying alone emits none. No borrowed fork notifier was
  introduced. These selected-store tests do not claim a public model/tool call.
- Current live dependency remains P68-P71: the existing linked/readback,
  constructed-header loop, and real public started-to-succeeded card-edit
  receipts above prove actual current-card notification. A fresh held card
  and historical recorded result are deliberately not interchangeable.

Native cuts use the existing completionOutcomeConnector real-driver test
owner. Its optional exact-statement predicate observes actual activity
INSERT/UPDATE success (including PostgreSQL RETURNING row consumption); the
original completion tests keep their agent-turn predicate. SQL, native
transactions, finalizers and independent journal readback are real. No new
native driver/protocol, SQL test observer port or production hook is added.

- TestChannelActivityNativeAcknowledgementCutsBothStores has48 cells: both
  stores x Start/Claim/Complete/MarkUncertain x entry cancellation, cancellation
  after the actual write, refused COMMIT, lost COMMIT acknowledgment, admitted
  COMMIT cancellation and healthy COMMIT. Each actual writer runs once (zero
  at refused entry). Cancellation/refusal rolls back, preserves predecessor
  evidence and emits no hint. Lost acknowledgment follows a real native
  COMMIT: the row is durable, but public result/ack and hint are withheld and
  the independent cause is retained. Acknowledged commits preserve their
  public result and ordinary hint, including cancellation after admission.
  The COMMIT-return fault models missing acknowledgment, not network wire loss.
  The Claim cells use the supported no-loop journal entry; the separately
  retained constructed-header test supplies valid/stale loop-generation proof.
- TestDecisionCompletionPreservesCommittedHandoffOutcome retains all four
  both-store human/proposed result/cause/candidate assertions. It now observes
  the actual finalizer after native COMMIT and the failing candidate handoff:
  proposed completion still hints and retains the cleanup cause. Exact replay
  is quiet and submits no second candidate. Human outcome-dispatched changes
  execution bookkeeping, not its frozen-card inputs; the exact card is
  unchanged and no hint is required. This is P29's existing event-only D arm,
  not a new missed producer or a swallowed cleanup failure.
- The earlier ActivityJournalCleanupPersistenceFault wrapper remains explicitly
  synthetic post-acknowledgment consumer evidence, NOT a native fault-cut
  receipt. The new native cuts and actual handoff proof do not relabel it.

Normal focused fork/handoff controls PASS18.331s (32 ordinary,4 selected,4
handoff cells); the native journal root passed all48 cells within the earlier
overall failing fixture diagnostic, not credited as qualification. The first
diagnostics correctly rejected fixture route history and a foreign authored
bundle context. The expanded pending-card fixture then rejected expired
historical cadence: it now uses a current fork cut, without extending any
deadline. The initial human-handoff hint expectation was wrong; exact frozen
inputs and native readback prove the D classification above. These fixture
counterexamples are not recorded as runtime defects or waived qualification.

The seven-root native/fork/control group then PASSED race x3 at the adcf97abb
executable source:550.585s,150 distinct leaf paths/450 passing executions,
zero skip/failure/race reports, after normal vemew admission2m31s. Roots:
TestOrdinaryForkActivityReplayCardDependencyBothStores,
TestSelectedForkRecordedActivityCardDependencyBothStores,
TestRunForkActivityTimestampRecordedReuseBothStores,
TestMaterializeRunForkProposedEffectCreatesFreshPendingAuthority,
TestChannelActivityNativeAcknowledgementCutsBothStores,
TestDecisionCompletionPreservesCommittedHandoffOutcome,
TestCompletionTransactionAcknowledgementBoundaryBothStores.
Log channel-fork-native-cuts-race.log, SHA256
b3adc48e5d9fa9c93ba3ab74905899345461d4547221a8622a8a098ea8f5d663.
The original completion driver controls remain green with their unchanged
default observed statement. The exact independent complexity comparison at
adcf97abb also passes:261/261 cyclo and559/559 cognitive hotspots at30,
no policy/baseline increase. This is focused checkpoint proof, not a tier run.

Fresh all-package Census/Inventory/Registry/Guard sweep and the COMPLETE
core-structural-owner-guards selection both PASSED through ordinary admission.
The structural run waited13m53s, then package compute0.048s/7.245s/5.368s/13.991s;
queue time is not execution or a demonstrated speed saving. Logs:
channel-structural-final.log SHA256
0427d0360abf2f953b799190d1b665e62ad4506423c0e16ded5e5158400c77d3,
channel-census-final.log SHA256
4dc33c0c5b5bf82186eee9880cfb230c0375ad4f64a8d882b522dbd3519529fc.
Full API-spec package PASS4.246s and complete base-to-head diff check passes.
These guards ran with the documentation-only 62e3c1a98 amendment; all Go,
platform spec and proof policy bytes are unchanged from the race-tested
adcf97abb executable source. No ownership/registry/baseline exception or test
selection reduction was needed for this delta.

Clean-head core plus six unchanged channel units/named supplements remain
pending. G can now request the server2 qualification window; none has been
taken yet. No PR, final audit, performance savings or whole-class closure is
claimed. This receipt update changes the audit only, not executable proof.

## First Server2 Core Attempt And Test-Authority Repair

The c29dccd99478fbb944de7994217d0e21073654c5 qualification did not pass.
Both original aggregates are retained on server2 in the disk worktree
/home/youmew/dev/swarm/worktrees/agent-g-phase0-c29dccd99/test-results/local/:
core-20261009T211744.416354140 and core-20261009T212335.288095091.
No failing aggregate, canceled sibling, or earlier focused receipt is being
relabeled Local core. No receipt carry has been requested or approved.

The first attempt refused the runner's overlong TMPDIR in
TestCapacityProbeQuotedTemporaryPath. A short writable disk TMPDIR
(/home/youmew/g-tmp) passed the unchanged control0.644s, then the same source
was retried. This is a runner-configuration error, not a product defect or
reason to relax the Unix-socket-path refusal.

The second attempt exposed seven OWNED new authority-debt sites in
TestPersistenceAuthorityDebtRatchet122.54s: the new M36 journey held a raw db
and called raw startup/card/receipt helpers; the new callback parameter also
changed three existing raw-startup call identities. The earlier
Census/Inventory/Registry/Guard regex sweep did not select this Ratchet root.
Neither an inherited-flake classification nor a baseline increase is valid.
Server2 was released immediately after joined fail-fast cleanup: no swarm-test
process and admission active=0/waiting=0. No channel supplement was started.

The bounded fixture repair consumes existing owners, with no new observation
port or classifier exception:

| Observation | Canonical owner and actual consumption |
| --- | --- |
| Exact pending run/kind/flow card, including stage/human/proposed anchors | Existing public mailbox.list paged reader; original stage helper delegates with unchanged inputs; ambiguity and cursor advance remain fail-closed |
| Exact durable sent copy and physical provider message | Existing storetest.ChannelObservation, current plan and receipt projections; exact delivery/operation/render identities and real numeric provider ID are checked |
| Approved terminal card | Public mailbox.get checks exact card identity, decided state and approve verdict; original15s deadline retained |
| Duplicate callback non-mutation | Existing typed intent observation uses the actual public channel interface and exact Telegram update ID; durable settled stale/rejected disposition required, original15s deadline retained |
| Completion cardinality after duplicate callback | Public event.list filters the exact run/name, bounded to2; exactly1 and no further cursor plus exact run/name required, rather than a loose membership assertion |
| Unchanged raw consumers outside this delta | Original five-argument startup helper retains its original raw operations and signature; no new proof consumes that carrier, and the old temporal journey returns to its original call shape |

The clean public journey composition returns only the process harness and
admitted source hash. Both new/modified supported proofs install the existing
Process observation and one-hour repair ticks BEFORE startup, retaining the
real webhook/HTTP/provider path. M36 still holds the actual provider response,
observes Dispatch: started, drains earlier successful passes, then requires
the completion-only wake and succeeded edit. The three-anchor journey retains
every original provider-call, terminal-edit, stale-action and cardinality
assertion. No deadline, M30 adverse window, business semantic or runtime code
changes in this repair.

Focused repaired-source control:
go run ./cmd/swarm-test -- ./internal/serveapp -run
'^(TestChannelDeliveryRealAnchorProducersPublicJourney|TestChannelDeliveryReconciliationActivityDispatchPublicJourney|TestChannelDraftTerminalRestartPublicJourney)$'
-count=1 -timeout=15m
PASS76.740s on both stores after ordinary admission19ms; the original8-cell
draft/restart root is included. TestScalar2556ChannelAnchorSourcePreservesGateText
also passes2.701s. These are focused fixture controls, not clean-head tier
qualification. The explicit TestPersistenceAuthorityDebtRatchet now PASSES
123.963s through the ordinary runner (admission1m5s), with the collector,
registry, raw-method classifiers and debt baseline UNCHANGED. Independent
TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority passes6.150s; full
API-spec package passes3.614s; gofmt and diff checks pass. Logs/checksums:
channel-authority-repair-journeys.log SHA256
28c2cee75fd7ca5dd6b31b3ff621bed21a74d6fc88508cf703b9a31e087b5ee6;
channel-authority-repair-debt.log SHA256
557c314458c9cb9766d90632c11d4c44d5272f96f6e6215561be534f9e07a595.
Race repetitions and fresh complete structural/census proof are still pending
in vemew's regular admission queue before the next server2 request. The
diagnostic all-package sweep expands go list's package set explicitly because
the runner correctly refuses raw ./... passthrough; this is not completion
credit, does not change root selection, and leaves the refused invocation
recorded rather than weakening the runner.

The repair is signed commit74b05a81613ca7fa72ee76a83a0eb7e0c624eac0.
Fresh complete core-structural selection PASSES through ordinary admission
12m37s: releasee2e0.069s/runtime10.085s/serveapp6.867s/runtimepersistence17.252s.
Fresh all-package Census/Inventory/Registry/Guard plus native-fixture refusal
sweep PASSES. Both run on the frozen clean74b source. Logs/checksums:
channel-structural-repair.log SHA256
3b9f156e670a735d0e258c933dc0e7d40f1266d8ef88aaa7c150ed867af7278b;
channel-census-repair-expanded.log SHA256
2768f7c076c15950f55137cbf976f5e8fc10d822a17f19893a33d43b151c97f1.

The additional two-public-root race3 aggregate FAILED317.093s and is retained
as failed, NOT a tier or all-green race receipt. Its M36 completion-only root
passes all3 repetitions/6 store leaves (37.24/31.00/39.69s), zero skip/failure.
Across the whole group23/24 terminal leaf executions passed. The third SQLite
stage-gate leaf fails in the ORIGINAL five-second event.publish HTTP helper,
before any migrated card/receipt read. Subsequent telemetry identifies
api_event_publication_finalize_failed, committed event
e9314005-62c1-5006-9100-df2d9c9ed147 and flow activation
77ba949f-6b56-4bc8-9512-c7c70ca17996, with context canceled. No failure-time
stack was captured; commit evidence alone does not establish the delay cause.
Log channel-authority-repair-race.jsonl SHA256
b38fdc03d56a43e8bacfc82800c17e1bc0c62558234236e99a67bc79eeb48b6e.

Targeted attribution controls use the exact original root/SQLite/stage-gate
selection, race3, unchanged five-second HTTP bound and assertions, through
normal vemew admission. They run serially (candidate before base due admission
arrival), not in a deliberately quiet cost-comparison window:

- Clean candidate74b05a816: PASS74.334s, all3 leaves, no skip/failure.
  channel-seed-timeout-head-race.jsonl SHA256
  3da0d56dfa316a5f722dcfe82dcb6b0b3dd4c7eda29a18c75c9ea4135cafc1e0.
- Clean integration base2fc6a13bd: PASS52.161s, all3 leaves, no skip/failure.
  channel-seed-timeout-base-race.jsonl SHA256
  1bdfbfaa91ea2ee53dd26f890f40b0c8efd203f8bc921e7c0c0ff049cabb81c3.

Classification is OBSERVED/UNCLASSIFIED/UNREPRODUCED, NOT inherited, fixed,
waived or a demonstrated performance regression. The candidate includes
additional public/pass assertions absent from base, and host load was not
controlled; those total elapsed times cannot be claimed as matched cost.
Existing #2353 owns the test-health record; no new issue, runtime repair,
deadline extension or assertion change is proposed. No production/spec/policy
bytes changed from reviewed e7ec69633 in this fixture/receipt amendment.

The raw-authority repair and fresh guard/census proof are complete; clean-head
Local core, six COMPLETE channel units/named supplements, matched cost and
hosted full still remain. Server2 is released and must be allocated again
before any fresh tier run. There is no PR or chosen-class closure claim.

## Second Server2 Core Receipt And Bounded Broad Repair

User allocated server2 again for frozen7ade404f7. Preflight verified no
swarm-test process and admission active=0/waiting=0. Go1.25.5, native PG16,
normal capacity4, short disk TMPDIR /home/youmew/g-tmp, clean detached source
7ade404f7ff4cc6f1bb4474aedbcb3cb5eb8e16a. Plan digest
70126c477a18c1112aebda4a3558622633a82649a4552d83ada0556cf214b2f5,
22 units/7424 selected roots/7407 required roots/4145 deferred roots. Receipt
directory core-20261009T220224.812098455 in the retained server2 worktree.

This aggregate FAILED, with21 passing unit receipts and broad-01 FAILED
383.322s. The pipeline package panic prevented remaining roots from running;
missing required roots are NOT credited. The original broad primary evidence
SHA256 is3e4aef94642e1cf32acfa7d48e7986f53b9e60dcb712b2194a95d3a6640e21d8.
No six-channel or other named supplement started. Server2 was released at
completion, no runner process, active=0/waiting=0. No automatic carry or
whole-core qualification is claimed; reviewer carry was requested6090227668.

The two actual failures are OWNED, unrelated to the unreproduced public
event.publish timeout. Its disposition6089996078 permits continued required
qualification, retains the failed race aggregate and requires a stop/capture
if the same timeout recurs. It did not recur in this core.

1. TestEmptyHumanTaskExpiryAcknowledgementOnBothStores/sqlite panicked in
   commitHumanTaskExpirations when the new publisher method was invoked even
   on the canonical acknowledged zero-publication result. That pure empty
   transaction test legitimately provides no publication owner. Base2fc passes
   both stores1.549s. The bounded repair skips only the proven empty/no-change
   publication outcome BEFORE touching that dependency. It still returns the
   original acknowledged result, validation and transaction/cleanup cause.
   Nonempty acknowledged results still signal through the existing owner.
   There is no nil compatibility check, ignored error or transaction/business
   change. The unchanged existing both-store control passes race3,7.494s;
   positive expiry/run-supersession and channel card/terminal hint controls
   pass7.801s. Existing native refusal and empty-commit assertions are retained.
2. TestRewrite2566EntryGoldenMatchesTypedCorpus had two stale locations for
   selectedActivityProducerSourceWithRootFields/literal-2 and literal-10.
   The exact original bodies moved into selectedActivityProducerSourceOptions
   when the fork-card fixture gained its supported root-write branch. Only
   the function/flow locators are updated. All566 entries and every independent
   entry/order/finals value are identical; no historical intent ledger,
   parser-derived expectation or fallback lookup. The full rewrite-stages
   package passes race3,79.541s, including its hostile/membership controls.

Local evidence/checksums:
channel-empty-expiry-base.log SHA256
8d38bbbb52eb32fb1096da58f1532a5b2680b30be07c216635b8857ecbc579ef;
channel-empty-expiry-repair-race.log SHA256
4cc771cb614b6189a8ca41206a9f73c3901e410fce22b26a5d16bc5e3c9fc9eb;
channel-entry-golden-repair-race.log SHA256
c1d5d5f4404afca6b361157c793ef0d9e0e65c721578703c429bb44e79a08e3f.

The four-root cost probe at clean7ade passed159.374s, same host/toolchain,
root selection/count as characterized baseline, but its package timeout was
15m versus the baseline8m; it is not the exact-command matched receipt.
The subsequent8m run overlapped the local expiry repair and is explicitly
MIXED SOURCE, uncredited, even though its tests pass164.536s. A clean frozen
exact-command comparison remains required; neither run proves fleet savings.
No measured/active deadline or M30 window has changed.

The repair is within the existing no-op finalizer and fixture-accounting gate.
No new producer, port, framework, vendoring or class split. Fresh complete
guard/census/debt proof and reviewer disposition on21-unit carry precede the
next server2 request. All six channel units and named supplements remain
unexecuted qualification, as do hosted full and the final PR proof audit.

## Completed Local Proof And Master Integration

The fresh WHOLE Local core at signed frozen61e203b7e733554c24396e55f303a1c8fad1cb99
PASSES22/22 units and7407 required roots. No carry from either failed aggregate.
Plan dd8cc24fd36b9b61a5c9b8b398002a29a074bd7aa5f7b839f4d36b191a6d4eb9;
server2 Go1.25.5/native PG16/normal capacity4/short disk TMPDIR. broad-01
PASSES349.204s, including the repaired canonical empty expiry and all566
unchanged stage-entry goldens. Fresh full guard/census/debt sweep and API spec
pass at61e. All12 required supplemental commands ALSO PASS at that same clean
source; all188 selected roots accounted for,186 required standalone roots.
The existing two subprocess entry points earn no standalone credit; one is an
expected helper skip. No unexpected skips, failed roots/children or race
reports. Worker/signal6 roots pass race3. Exact public stage-gate and M36 pass
both stores. This is Local core plus named supplements, NOT lifecycle/full.
Immutable plan, primary receipts, command logs, selected-root manifest and
validation are retained under agent-g-phase0/61e203b7e-* in local state and the
server2 disk evidence directory. Thread receipts6090508379/6090603089 bind them.
Server2 released immediately after joined cleanup, no runner, active0/waiting0.

| Complete command | Package seconds |
| --- | ---: |
| serveapp-channel |205.608|
| serveapp-channel-delivery |174.142|
| serveapp-channel-learned |137.690|
| serveapp-channel-lifecycle |74.860|
| serveapp-channel-native |239.417|
| serveapp-channel-process-temporal |202.207|
| store-channel-native-journal |60.737|
| store-fork-authority |19.803|
| store-publication-reset-terminal |19.364|
| native-transaction-protocol |0.006/0.331|
| channel-signal-worker-race |1.021/1.029|
| catalog-selected-root-regression |2.453|

Q08's frozen61e exact unchanged8m four-root command on vemew PASSes42 test
records, no skip/failure,166.809s versus baseline230.956s. SHA256
c04f5c06f725bbd823c78c122f35180069a3362691b4189679d6dde454c31986.
This single same-host sample is27.8% lower, not fleet-p90 evidence. Hosted
before source329acf46b/full run37945884736 is downloaded; all six prior unit
package/primary/job/queue times remain separate. Hosted after proof is pending.
The original race publish timeout remains red/unclassified under2353 and
disposition6089996078, NOT repaired/inherited/waived by these passing controls.

PR2599 opened at61e but conflicts with master4d129afac, hence no hosted full
could start. User requires rebase and conflict-delta guards, then push and an
explicit reviewer carry decision. Prior green receipts remain bound to61e,
not relabeled to the integrated source. This is not generated-only integration.

The signed rebase preserves master transaction-loaned callback context and
active-source/counter admission, all cancellation fields and per-turn commit
acknowledgments, and selected-fork event-counter insertion. The existing new
failure helper receives the SAME borrowed ChannelCardChanges accumulator;
supersession preserves master's lifecycle writer while returning actual change.
No new transaction/finalizer/worker, runtime authority, policy, schema or gate.
Two upstream native-admission/counter fixtures receive the same private fact
parameter without deleting their rollback, invalidation, UUID-alias or exact
physical-counter assertions. The plan retains master's complete membership
plus the approved three channel roots; the spec retains master's additions
plus the approved25-line channel scheduling contract. Generated admission
hashes and authority inventory are regenerated, never hand-merged or relaxed.
235 exact previously reviewed registry identities transfer unchanged; the
three composed SupersedeRunTx/recoverFailedSelectedForkTx private-backend
identities retain the same closed owner/disposition. No debt baseline,
collector/header/classifier exception or generic SQL port is introduced.

Pre-commit integration controls: native invalidation/unbound terminal counter,
reviewer2589 UUID snapshot, selected/ordinary fork activity dependency and
selected projection failure both-store group PASS18.939s; exact registry plus
no-raw-public-method controls PASS11.529s; regenerated admission census PASS.
These are source-tree focused controls, not an integrated-head tier receipt.
Fresh focused guards at the resulting committed head precede publication.
Reviewer-G must decide whether61e's22 core and12 supplements carry; none is
self-approved. Exact-head hosted full, final proof audit and formal closure
remain pending. Parent2535 and2250 stay open; core timing environment defect
6090327487 is the next separate item, not absorbed into this channel PR.

## Integrated Core And Subscription Stop Counterexample

Reviewer6090922606 requires fresh whole core plus all12 complete named
supplements at f431, with NO61e carry. Fresh whole core PASSES22/22 units,
7505 required/7522 selected roots, plan digest
a2f1decaca5957bd92fb40b1e6120702250bdba1777f57916802c5756ad63db9.
All22 primary receipts bind f4314748ecbda07b8d59b3956362e1420b75a3a2 and
exit0. Server2 Go1.25.5/native PG16/capacity4/short disk TMPDIR; complete
receipts retained in core-20261009T232215.019320795 and copied to local state.
No supplement began after the OWNED hosted failure was identified; server2
was released immediately, no runner and admission active0/waiting0.

The identical8m/count1 four-root comparison on vemew PASSes42 records/no
skips at each source: integration base4d129afac262.262s, frozen f431164.954s,
37.1% lower in ONE sample. This is neither fleet-p90 nor corrected-head credit.

Hosted full38003723055 at f431 FAILS in serveapp-runtime:
TestRunLocalRuntimeFreshEmptySQLiteBootsWithDevAbandon reaches ready, then
returns1 with `subscribe channel delivery worker: context canceled`.
The new synchronous subscription is reached after ready presentation. An
immediate graceful stop cancels its admitted worker context before Subscribe
completes, but the caller incorrectly reports that exact cancellation as a
runtime failure. This is the slice's OWNED M21/M33 partial-start/stop path,
not a run-lifecycle conflict defect or the earlier unclassified publish timeout.
The timing artifact is INCOMPLETE because this required root fails and
fail-fast cancels other units; it is not measured budget growth.

Two deterministic counterexamples FAIL before repair: cancel the caller or
retire the exact Process inside Subscribe AFTER its lease is admitted. The
existing TestChannelWorkerDoesNotCreditFailedPass now owns these subcases,
plus foreign cancellation on a live lease, genuine and combined independent
failure, failure during retirement and deadline expiry. Each asserts lease
settlement, no retained subscription, no worker startup/scan or pass credit.
Both worker roots PASS race50 on the repair tree. No sleep/retry, root-selection
change, weaker exit assertion, deadline extension or altered backstop.
The unchanged four fresh-boot/stop roots (SQLite dev/direct abandon and
PostgreSQL fresh schema/dev abandon) PASS race10,40 passing records with
zero failures/skips,158.067s. API-spec3.073s, Process owner1.531s and channel
owner1.080s also pass on this repair tree; final committed-head controls follow.

The bounded owner repair checks the exact context.Canceled against the
admitted lease BEFORE Done itself cancels that context, then returns Done's
cleanup result. Only that pure own stop is graceful; foreign/combined errors
and deadline expiry still return their original failure joined with cleanup.
Canonical Process admission and retirement, business authority, startup
publication ordering and every store/transaction finalizer are unchanged.
The authoritative channel scheduling contract records this same distinction.
No second worker, cancellation framework, generic filter, schema or vendor.
The independent complexity ratchet also rejects the added nested cancellation
branch as a new startup hotspot. The correction removes the duplicate loop
context check: the canonical subscription's BeginPass already rejects canceled
or replaced ownership before returning any executable pass. The existing zero-
subscription refusal remains in the worker. This consumes the same owner rather
than distributing cancellation checks or creating a helper/framework; the full
signal/worker retirement matrix and final ratchet are rerun at the resulting head.

#2595 master44c4047f0 combines cleanly: merge-tree succeeds with tree
403355abba7eee0c412ee133be68c21a163e1aa4 and GitHub reports MERGEABLE.
No rebase or unnecessary source movement is needed. Focused final-head
guards and a reviewer decision on corrected-head matrix/receipt carry still
precede further server2 qualification; f431 core is NOT relabeled automatically.
Exact-head hosted full, all12 supplements and final proof-audit addendum remain
required. Parent2535/2250 and the distinct original2353 timeout remain open.

## E2E-13 Explicit Locale Fixture Correction

Reviewer-G's bounded ruling6091539770 approves a TEST-ONLY prerequisite
correction, not a new post-commit producer. Hosted full38006790556 at b47
FAILS TestChannelOnboardingE2E13CredentialWriteBeforeCheckpoint/explicit_postgres
with registrations3/deliveries2 instead of3/3. The worker repeatedly scans at
the unchanged ordinary1s cadence, but exact native admission refuses the
unsent standing card because the recovered reconnect operation has no explicit
client-language declaration. Both provider deliveries are confirmations.

G's failure-only diagnostic at b47 reproduces that PostgreSQL failure in one
of three full both-store repetitions (aggregate61.994s). Durable operation
readback is succeeded, ClientLanguage empty, ClientLocaleRevision1; bounded
provider-effect diagnostics show two `Swarm channel connected.` messages and
no card. Other passing repetitions do not erase the counterexample: the
predecessor's card can settle before reconnect, masking the missing successor
prerequisite. This is neither a missing wake nor evidence to infer English.

The governing channel.onboarding_retry.locale_requalification contract forbids
an English default. Canonical onboarding owns the explicit declaration and
its revision; native qualification consumes it, independently of worker
scheduling. Add only `--client-language en` to the E2E-13 reconnect command.
Keep the credential-write crash cut, restart/resume identity assertions,15s
effect deadline, exact3/3 provider counts and exactly one standing card. Do
not change production qualification, backstops or recovery. The reviewer
independently proved this one-flag repair5/5 on the full both-store root.

Sibling sweep: E2E-14 already declares en and explicitly fences the predecessor
card before rebind. E2E-16 asserts two confirmations and no recovered standing
card; E2E-17 destroys the pending reconnect before checkpoint/confirmation.
Neither is credited as a positive successor native-card proof. The existing
TestChannelNativeLocaleQualificationPublicJourney independently preserves the
both-store missing-declaration refusal, explicit en/fr requalification, exact
declaration revision and no reinstall across restart. Those controls remain
unchanged; targeted repaired E2E-13 and native-locale controls precede renewed
qualification. This correction qualifies Q01's fixture, not another P owner.

Repair-tree controls on vemew/native PG16 PASS: complete E2E-13 both-store
root `-race -count=5`,15 passing records/zero failures or skips,213.192s;
unchanged complete native-locale root `-count=1`,33 passing records/zero
failures or skips,155.872s. These pre-commit focused results are not a core,
supplement-matrix or hosted-full receipt. A clean committed-head E2E-13 control
and immutable log hashes will be recorded in the PR proof-audit addendum.

## Independent Pressure Qualification Blocker

Fresh b47 whole Local core on server2 FAILS20pass/1fail/1interrupted; the
serveapp-channel command and all12 supplements never ran. The pressure root
TestIssue2394PressureReservationStarvationPostgres captures sequence3 waiting
32.056574588s behind later peers, exceeding its real30s claim before commit.
The fixture schedules sequence1 starvation but lets all peers barge; only
sequence1's stale rejection is allowed by the unchanged drain oracle. The
production owner correctly refuses the expired claim. This matches a tracked
peer-expiry shape under2353/2394, NOT proof that current PR contribution is
excluded or permission to relax the oracle. The paired FIFO control passes.

The full chronology,22 source-bound receipts and reviewer disposition request
are retained in PR comment6091479149 and2353 comment6091479475. Both fixture
blobs match integration base4d; shared publication code also changes, so causal
attribution remains open. No core/full retry, pressure fixture edit, automatic
receipt carry or class closure is authorized by the E2E-13 ruling. Server2
was released after joined cleanup with no process and admission0active0waiting.
CI full / Local core plus the12 named complete supplements remains the gate.

## Bounded Pressure Peer-Order Fixture Repair

Reviewer-G6091676660 and review5476628909 authorize a TEST-ONLY correction,
not a stale-claim relaxation or attribution waiver. The prior b47 core and
historical2542 peer-expiry receipts remain failed; identical source and shape
do not establish matched-load PR innocence. Runtime fan-out code is untouched.

The artificial reservation queue now records all four real worker tickets.
Only sequence1 may be bypassed during the negative control's finite pressure
window; every other peer is FIFO. Drain restores the complete queue order.
Each return/cancellation removes only its own ticket. The negative-control
probe reports actual peer grants (not intermediate ordered-peer denials),
while the FIFO companion still observes its first competing denial. This
changes the injected fault's scope, not candidate selection, claims or commits.
Four worker concurrency, actual30s leases,31s pressure,15s progress,90s drain,
old-waiter repeated retries, expired exact-claim cleanup/generation2 recovery
and every original effect/receipt/accounting assertion remain unchanged.

The existing starvation root adds a small `peer_order` subtest and retains its
complete native work proof under `lease_and_recovery`; no root, unit, profile,
deadline, timing budget or baseline is removed/retiered. Six controlled cells
use the actual fixture reserve method and observe its attempt before canceling
and joining: later peer ordering, oldest peer progress, intended first-waiter
starvation, drain release, drain order and the ordinary FIFO control. No sleeps,
retries, fabricated database lease or extra serving worker. A bounded four-ticket
snapshot isolates queue policy; actual durable claims remain proved by the full
native pressure roots, not credited to this small control.

Before patch, three cells FAIL deterministically: later peer4 passes queued
1/2/3, sequence1 acquires during the negative window, and draining peer3 passes
queued1/2. The before aggregate remains RED0.028s. After patch all six cells
PASS race50,400 passing records/zero failure or skip,1.194s. This is fixture-
policy proof, not elapsed-lease or qualification credit. Complete clean-head
starvation/FIFO and nearby both-store accounting/finite/startup controls with
native PG16 precede push. If any unexpected peer expiry or production error
persists, stop for the authorized paired base/head four-slot diagnostic; no
blind core/full rerun. On focused success, batch with the approved E2E-13 flag
once, then fresh core+12 supplements and hosted full; no b47 red receipt carry.
Existing2353/2394/harness watchlist records own the defect and proof refinement;
no new issue, production/spec semantics, framework, driver or vendor change.

## Bounded In-Window Pressure Observation Repair

Reviewer-G6092312347 authorizes this TEST-ONLY correction atop048ad8000,
amending the earlier requirement to reproduce literal zero samples on both
heads. The failed333.579s race aggregate remains failed evidence: synchronous
22-intent refill occupied10.256s without selecting the main-loop ticker,
despite43 actual commits and maximum commit gap859ms. This is a pre-existing
observation-scheduling flaw, not evidence that production serving stalled.
That attribution classifies the assertion, not the exact four-slot trigger.

User-selected base44c4047f0 and head048 both pass the diagnostic startup root
under actual two-co-runner overlap. PG second refill8.334s/6.039s delays the
first tick until refill returns; each reports one real observation. SQLite
reports four observations on each side before a refill longer than10s.
The planned four-slot comparison was not achieved, test-phase alignment differs,
and these instrumented diagnostics earn no qualification or receipt carry.
No candidate slowdown is established at this narrower load; absence of added
cost under heavier qualification is NOT claimed. Complete chronology/hashes:
PR6092294073. Existing2353/2394 retain the test-health/performance record.

The same pressure helper now services due ticks after each newly durable refill
item using the unchanged selected-store population read. The initial wave has
no observation channel/window. Both refill and ordinary-select paths credit a
sample only when that actual read starts and finishes before the original end.
Counter increments without readback, after-window observations and goroutine
observers are forbidden. The positive samples assertion,10s window,15s actual
commit-progress bound,22..44 durable population, real leases/worker count,
receipt/effect/final-accounting and90s drain assertions remain unchanged.
No new test root, registry/unit membership, timing budget, production behavior,
compatibility, framework or spec semantic change is introduced.

Before pushing, run the corrected complete startup root on both stores with
race and the full starvation/FIFO/accounting/finite pressure controls. Any
genuine population/progress/lease failure requires separate classification,
not another blind qualification. Then push the corrected frozen head once and
run fresh whole Local core22/22, all six complete channel units and six named
supplements (worker race x3), plus exact-head hosted full CI. No red-core or
diagnostic receipt carry; server2 allocation remains explicit after D joins.
The final-head audit and both local/hosted success still precede merge approval.

## E2E-13 Predecessor Native Settlement Gate

Hosted38015134533 at1a43 FAILS E2E-13/explicit_postgres with3 registrations and
2 deliveries despite the explicit en flag. Reviewer-G5477103531/6092686116
identifies a missed UPSTREAM fixture gate: public connect waits for its
confirmation and general readiness, not native installation or standing-card
settlement. Reconnect's credential-write crash can therefore interrupt the
predecessor's unrelated provider write. No native uncertainty retry/clearing,
cadence change, deadline waiver or production repair is authorized.

The actual hosted native refusal is uncertain/admin-recovery, not missing
language: predecessor e8d971a0 and successor3fae0e22 repeatedly refuse delivery
7fb71e7f through the entire15s bound. CI executes merge556d93a2 (parents44c and
1a43), while local focused tests used the source head; Go1.25.0 versus1.25.5
and different scheduling are recorded, not asserted to be causal. The exact
hosted pre-crash database/payload snapshot was not retained.

A disposable1a43 diagnostic uses the existing provider command-apply barrier,
existing native install/setting readback helpers, public onboarding readback
and the selected delivery observer. On BOTH stores it proves immediately
before arm AND at the credential cut: the exact predecessor install attempt
is launched, setting planned, readback absent and client_language en. After
joined shutdown the SAME operation/setting is outcome_uncertain/uncertain;
successor readback remains invalid, and both actual delivered texts are
Swarm channel connected. for chat7213. The same standing-card source persists
in rendered state with no receipt; releasing the fake provider permits one
command write but cannot manufacture acknowledgment or clear uncertainty.
The unchanged3/3 assertion then fails on both stores. This is a deterministic
real pending-write counterexample, not a blind rerun or qualification credit.
It demonstrates the fixture race; exact hosted pre-cut ordering remains
unobserved and is not reconstructed as historical fact.

The authorized fixture-only repair waits for the exact predecessor's PUBLIC
qualified native-inbox projection (which requires the installed/readback
setting) and existing exact standing-card receipt helper before barrier.Arm.
Reuse the existing selected observer connection for E2E-13/14 instead of adding
another raw constructor/query site. Keep explicit language, actual credential
crash, both-store restart/identity predicates,15s post-recovery bound, exactly
three distinct delivered effects and one card. No runtime/schema/spec-semantic,
unit/root/registry, legacy, framework, baseline or vendor change.

Focused corrected full E2E-13 under race on both stores and the complete
serveapp-channel unit precede renewed exact-head core22/all12 supplements and
hosted full. Any settled-before-crash setting that later becomes uncertain
requires STOP and a bounded native-owner runtime gate. Existing2353/harness
watchlist owns this second fixture manifestation; no new issue or closure claim.

## Revised Focused-Only Qualification Ruling

Reviewer-G6092846414 supersedes the earlier Local core22/all12 duplicate
requirement under the user-ratified2026-10-10 policy. LOCAL is focused only;
unaffected1a43 pressure, peer-order, census and structural receipts carry.
Fresh repaired-head E2E-13 both-store proof and affected guards remain required;
E2E-14 is an affected sibling control because its existing observer constructor
is shared, not a second semantic repair. No local core/lifecycle/full tier or
server2 duplicate reservation. The prior obsolete matrix requirements and red
receipts remain historical records, not current qualification instructions.

HOSTED remains CI-Tier full ONLY because the corrected common pressure helper
is consumed by the full-only900s SQLite/PostgreSQL soak units. Lifecycle plus
those named units would be sufficient, but current hosted selection has no
extra-units parser/gate. No one-off CI selector is added here;2535 tracks that
infrastructure gap. Hosted full and both long soaks must pass at the repaired
pushed head. Remove Local-Tier core from the PR body and name focused receipts
in the final audit. The present failed CI is not retried or waived.

The real pending-install negative control is retained as RED37.521s on both
stores; its source/log patch and hashes are recorded in PR6092799025. Repaired
E2E-13 on the repair tree PASSES race5/15 records/126.467s with the actual
barrier,15s/count/identity assertions unchanged. A preliminary fixture build
failure (public activation DTO exposes revision only) remains failed evidence;
use its public Operation ID for exact native qualification instead of inventing
activation fields. Fresh committed-head focused controls follow. Upon the new
qualification ruling, two queued duplicate sweeps and one begun whole-unit run
are interrupted and joined; their incomplete receipts earn no credit. No fresh
core, server2 qualification or hosted rerun has started.

The affected debt ratchet at9ed then FAILS: duplicated receipt-helper call and
an explicit new raw local variable increased site multiplicities. No baseline,
classifier or guard exception is authorized. Consolidate the actual shared
E2E-13/14 predecessor setup and receipt gate into the original observer/call
site, retaining both exact chat IDs and all boundary-specific actions. No raw
helper is renamed, moved, wrapped or hidden; one common real prerequisite
serves both cuts, and the explicit raw local declaration is removed. Rerun
both complete roots and affected guards/debt at the corrected frozen head;
9ed focused passes do not erase that failed ratchet or qualify the next head.

## Controlled Predecessor Settlement Proof And Startup Residual

At signed a6c31f0a5, the shared real prerequisite has no new raw observer
constructor/helper multiplicity: affected authority/debt controls PASS22 records,
zero skips, with the debt baseline unchanged. The clean E2E-13/14 race aggregate
is RED99.253s: initial E2E-13 SQLite harness.start times out after boot19/22,
BEFORE predecessor connect or either new gate; PostgreSQL E2E-13 and BOTH E2E-14
leaves pass. No pre-cancel stack was retained. This distinct startup failure
stays recorded under2353/PR6093083923, not called inherited, fixed or erased by
later passes. Reviewer-G6093102780 independently passes both roots normally
and under race, permits focused qualification to proceed without a tier cycle,
and requires a matched-base diagnosis if initial startup recurs in hosted CI.

The additional causal challenge6093010549 PASSES in an owned disposable a6c
checkout with the SAME existing provider apply barrier as the old-head RED.
At the first actual missing sent-receipt read, BOTH stores show exact native
attempt launched, setting planned/no readback, one registration/confirmation,
and credential barrier NOT armed. Releasing the held provider write then
permits the exact receipt and public native qualification to complete; BEFORE
arm the SAME operation is settled and SAME setting installed/readback valid.
At the actual credential crash the predecessor remains settled, not uncertain.
The complete E2E-13 root passes under race: SQLite14.35s, PostgreSQL9.85s,
package25.243s, with the original15s recovery,3/3 effects and one-card assertions
unchanged. Actual payloads are confirmation, the exact standing card, then
confirmation; exactly one native write. No runtime defect is exposed by this
controlled proof and no uncertainty clearing or automatic replay is added.

This controlled diagnostic uses test-only observations and does NOT substitute
for clean exact-head hosted execution. Raw JSON SHA256:
36f0bda9ed0963a277c4d1c26c038a9b4a7b92b339f2a60f213a9c0626d60f85;
diagnostic patch SHA256:
a42fc96dd1f36c91a2006c1cddb82168ff512d50d7095fbedca998874f8e156e.
The old-head delayed-provider RED37.521s and clean a6c startup RED remain failed
evidence. Per6092846414/6093102780 LOCAL stays focused-only, unaffected1a43
pressure/peer/census/structural proof carries, and affected guards run at the
pushed head. No Local-Tier core claim, new server2 qualification, receipt waiver,
deadline/assertion change, guard exception, baseline increase or new owner.
Hosted exact-head full/900s soak success, final proof audit and review still
precede merge;2535/2353/2394/2250 remain open.
