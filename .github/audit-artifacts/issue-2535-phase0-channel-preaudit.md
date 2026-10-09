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
handoff cells); native journal cuts separately passed all48 cells. The first
diagnostics correctly rejected fixture route history and a foreign authored
bundle context. The expanded pending-card fixture then rejected expired
historical cadence: it now uses a current fork cut, without extending any
deadline. The initial human-handoff hint expectation was wrong; exact frozen
inputs and native readback prove the D classification above. These fixture
counterexamples are not recorded as runtime defects or waived qualification.

Race repetitions, fresh all-package guards/census and clean-head core plus
six unchanged channel units/named supplements remain pending at this amendment.
No PR, final audit, performance savings or whole-class closure is claimed yet.
