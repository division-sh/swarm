package runtimepersistence

import (
	context "context"
	json "encoding/json"
	apiidempotency "github.com/division-sh/swarm/internal/apiidempotency"
	channelonboarding "github.com/division-sh/swarm/internal/channelonboarding"
	events "github.com/division-sh/swarm/internal/events"
	mailbox "github.com/division-sh/swarm/internal/mailbox"
	operatorchannel "github.com/division-sh/swarm/internal/operatorchannel"
	operatorread "github.com/division-sh/swarm/internal/operatorread"
	runtime "github.com/division-sh/swarm/internal/runtime"
	activityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	agentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	agentframe "github.com/division-sh/swarm/internal/runtime/agentframe"
	agentmemory "github.com/division-sh/swarm/internal/runtime/agentmemory"
	authoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	budgetspend "github.com/division-sh/swarm/internal/runtime/budgetspend"
	bus "github.com/division-sh/swarm/internal/runtime/bus"
	computemodule "github.com/division-sh/swarm/internal/runtime/computemodule"
	agentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	attemptgeneration "github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	identity "github.com/division-sh/swarm/internal/runtime/core/identity"
	managedcapabilities "github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	correlation "github.com/division-sh/swarm/internal/runtime/correlation"
	deadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	deliverylifecycle "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	destructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	diaglog "github.com/division-sh/swarm/internal/runtime/diaglog"
	effects "github.com/division-sh/swarm/internal/runtime/effects"
	entityquery "github.com/division-sh/swarm/internal/runtime/entityquery"
	executionmode "github.com/division-sh/swarm/internal/runtime/executionmode"
	executionposture "github.com/division-sh/swarm/internal/runtime/executionposture"
	failures "github.com/division-sh/swarm/internal/runtime/failures"
	fanoutobligation "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	genericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	inboundpublication "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	ingress "github.com/division-sh/swarm/internal/runtime/ingress"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	manager "github.com/division-sh/swarm/internal/runtime/manager"
	pipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	pipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	replycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runcontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runfork "github.com/division-sh/swarm/internal/runtime/runfork"
	runforkreadiness "github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	runlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	semanticview "github.com/division-sh/swarm/internal/runtime/semanticview"
	sessions "github.com/division-sh/swarm/internal/runtime/sessions"
	startupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	timerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	tools "github.com/division-sh/swarm/internal/runtime/tools"
	workflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	authority "github.com/division-sh/swarm/internal/sessionprovider/authority"
	sourceartifact "github.com/division-sh/swarm/internal/sourceartifact"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	time "time"
)

// Owner views promote only the selected store's existing semantic surface.
// Construction, SQL and fixture-only methods stay on the private concrete owners.
type activityOwnerMethods interface {
	AuthorActivityEventCatalogRegistered(scope authoractivity.Scope) bool
	ClaimActivityAttemptForLoopGeneration(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error)
	CompleteActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error)
	HeadAuthorActivity(ctx context.Context) (int64, error)
	ListAuthorActivity(ctx context.Context, opts authoractivity.ListOptions) (authoractivity.ListResult, error)
	LoadActivityAttempt(ctx context.Context, requestEventID string) (pipeline.ActivityAttemptRecord, bool, error)
	MarkActivityAttemptUncertain(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error)
	RegisterAuthorActivityEventCatalog(scope authoractivity.Scope, descriptors []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error)
	ResolveAuthorActivityEventDescriptor(scope authoractivity.Scope, name string) (authoractivity.EventDescriptor, bool)
	StartActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error)
}

type activityResultOwnerMethods = activityresult.Reader

type agentOwnerMethods interface {
	AdmitDirectiveExecution(ctx context.Context, req agentcontrol.DirectiveExecutionAdmissionRequest) (agentcontrol.DirectiveExecutionAdmission, error)
	FinalizeDirectiveFailure(ctx context.Context, operationID string, ownerID string, failure failures.Envelope, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, error)
	FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, error)
	ListDurableAgentLifecycleStates(ctx context.Context) ([]manager.AgentLifecycleState, error)
	ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error)
	LoadAgentLifecycleState(ctx context.Context, identity agentidentity.Identity) (manager.AgentLifecycleState, bool, error)
	LoadAgentsSpec(ctx context.Context) ([]manager.PersistedAgent, error)
	LoadDirectiveOperation(ctx context.Context, operationID string) (agentcontrol.DirectiveOperation, bool, error)
	LoadDirectiveOperationByKey(ctx context.Context, method string, actorTokenID string, idempotencyKey string) (agentcontrol.DirectiveOperation, bool, error)
	ReadAgentLifecycleOutcome(ctx context.Context, req manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error)
	ReconcileDirectiveOperation(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, bool, error)
	ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperationReconcileResult, error)
	RecordDirectiveExecuted(ctx context.Context, operationID string, ownerID string, response json.RawMessage, now time.Time) (agentcontrol.DirectiveOperation, error)
	RenewDirectiveExecutionLease(ctx context.Context, operationID string, ownerID string, now time.Time, lease time.Duration) error
	ReserveDirectiveOperation(ctx context.Context, req agentcontrol.ReserveDirectiveOperationRequest) (agentcontrol.DirectiveOperationReservation, error)
	ResolveAgentDirectiveRunTarget(ctx context.Context, identity agentidentity.Identity) (agentcontrol.RunTargetResolution, error)
	ResolveOperatorAgentIdentity(ctx context.Context, runID string, agentID string, flowInstance string) (agentidentity.Identity, error)
}

type agentSQLiteOwnerMethods interface {
	AdmitDirectiveExecution(ctx context.Context, req agentcontrol.DirectiveExecutionAdmissionRequest) (agentcontrol.DirectiveExecutionAdmission, error)
	FinalizeDirectiveFailure(ctx context.Context, operationID string, ownerID string, failure failures.Envelope, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, error)
	FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, error)
	ListDurableAgentLifecycleStates(ctx context.Context) ([]manager.AgentLifecycleState, error)
	ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error)
	LoadAgentLifecycleState(ctx context.Context, identity agentidentity.Identity) (manager.AgentLifecycleState, bool, error)
	LoadDirectiveOperation(ctx context.Context, operationID string) (agentcontrol.DirectiveOperation, bool, error)
	LoadDirectiveOperationByKey(ctx context.Context, method string, actorTokenID string, idempotencyKey string) (agentcontrol.DirectiveOperation, bool, error)
	ReadAgentLifecycleOutcome(ctx context.Context, req manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error)
	ReconcileDirectiveOperation(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperation, bool, error)
	ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (agentcontrol.DirectiveOperationReconcileResult, error)
	RecordDirectiveExecuted(ctx context.Context, operationID string, ownerID string, response json.RawMessage, now time.Time) (agentcontrol.DirectiveOperation, error)
	RenewDirectiveExecutionLease(ctx context.Context, operationID string, ownerID string, now time.Time, lease time.Duration) error
	ReserveDirectiveOperation(ctx context.Context, req agentcontrol.ReserveDirectiveOperationRequest) (agentcontrol.DirectiveOperationReservation, error)
	ResolveAgentDirectiveRunTarget(ctx context.Context, identity agentidentity.Identity) (agentcontrol.RunTargetResolution, error)
	ResolveOperatorAgentIdentity(ctx context.Context, runID string, agentID string, flowInstance string) (agentidentity.Identity, error)
}

type apiIdempotencyMethods interface {
	WithAPIIdempotency(ctx context.Context, req apiidempotency.Request, execute func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error)
}

type budgetOwnerMethods = budgetspend.Store

type channelOnboardingOwnerMethods = channelonboarding.Store

type decisionOwnerMethods interface {
	decisioncard.Store
	CompleteHumanTaskOutcome(ctx context.Context, cardID string, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error)
	CompleteProposedEffectRoute(ctx context.Context, cardID string, routeEventID string, at time.Time) (decisioncard.ProposedEffectContinuation, error)
	CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error
	CreateHumanTaskCardOutcome(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) (decisioncard.HumanTaskCreationResult, error)
	CreateProposedEffectCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.ProposedEffectContinuation) error
	ExpireDecisionCardInputDrafts(ctx context.Context, now time.Time) (int, error)
	ListDueHumanTaskExpiryEvents(ctx context.Context, now time.Time, limit int) ([]events.Event, error)
	LoadHumanTaskContinuation(ctx context.Context, cardID string) (decisioncard.HumanTaskContinuation, error)
	LoadProposedEffectContinuation(ctx context.Context, cardID string) (decisioncard.ProposedEffectContinuation, error)
	ProposedEffectReadback(ctx context.Context, cardID string) (decisioncard.ProposedEffectReadback, error)
	SupersedeProposedEffectsForLoopGenerations(ctx context.Context, runID string, entityID string, current []attemptgeneration.Generation, reason string, at time.Time) error
}

type deliveryOwnerMethods interface {
	ActivateDeliveryAuthority(ctx context.Context, authority deliverylifecycle.ExecutionAuthority) error
	BindAgentSession(ctx context.Context, claim deliverylifecycle.Claim, sessionID string) (deliverylifecycle.ClaimCommit, error)
	ClaimDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error)
	DeliverySnapshotsForEvent(ctx context.Context, eventID string) ([]deliverylifecycle.Snapshot, error)
	InspectDeliveryRecovery(ctx context.Context, source correlation.SourceArtifactFact) (deliverylifecycle.RecoveryInventory, error)
	ObserveDeliveryContinuation(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, deliveryID string) (deliverylifecycle.ContinuationObservation, error)
	ObserveDeliveryContinuations(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, deliveryIDs []string) ([]deliverylifecycle.ContinuationObservation, error)
	Outcomes(ctx context.Context, deliveryID string) ([]deliverylifecycle.Outcome, error)
	ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (deliverylifecycle.DurableHandoffProof, error)
	RecordDeadLetter(ctx context.Context, rec deadletters.Record) error
	RecordDeadLetterOutcome(ctx context.Context, rec deadletters.Record) (deadletters.RecordOutcome, error)
	RenewClaim(ctx context.Context, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error)
	ScanDeliveryContinuations(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, cursor deliverylifecycle.ContinuationCursor, limit int) (deliverylifecycle.ContinuationPage, error)
	SettleFailure(ctx context.Context, claim deliverylifecycle.Claim, settlement deliverylifecycle.Settlement) (deliverylifecycle.Snapshot, error)
	SettleSuccess(ctx context.Context, claim deliverylifecycle.Claim, sideEffects []string, duration time.Duration, selection deliverylifecycle.HandlerRuleSelectionFact) (deliverylifecycle.Snapshot, error)
	SettleWorkflowNodeSuccess(ctx context.Context, claim deliverylifecycle.Claim, sideEffects []string, duration time.Duration, selection deliverylifecycle.HandlerRuleSelectionFact) (deliverylifecycle.ClaimCommit, error)
	Snapshot(ctx context.Context, deliveryID string) (deliverylifecycle.Snapshot, error)
	SummarizeRun(ctx context.Context, runID string) (deliverylifecycle.RunSummary, error)
	TerminalizeRun(ctx context.Context, runID string, reason string) ([]deliverylifecycle.Terminalization, error)
}

type destructiveResetOwnerMethods interface {
	AcquireDestructiveReset(ctx context.Context) (destructivereset.LockLease, bool, error)
	ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error)
	ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error)
}

type destructiveResetSQLiteOwnerMethods interface {
	AcquireDestructiveReset(ctx context.Context) (destructivereset.LockLease, bool, error)
	ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error)
}

type effectOwnerMethods interface {
	AuthorizeExternalAttempt(ctx context.Context, authority effects.Authority, req effects.AuthorizeRequest) (effects.Attempt, error)
	ClaudeBackingOrigin(ctx context.Context, identity agentidentity.Identity, session string, head string) (string, error)
	CommitCanceledTurn(ctx context.Context, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error)
	ConsumeCompletionResponse(ctx context.Context, attempt effects.Attempt, successor *agentframe.ToolContinuation, terminalResult *agentframe.ToolContinuation) error
	GetExternalEffectOutcome(ctx context.Context, operationID string) (effects.OperationOutcome, bool, error)
	HeartbeatCompletionAttempt(ctx context.Context, attempt effects.Attempt, now time.Time, lease time.Duration) error
	InspectStartupRecovery(ctx context.Context, now time.Time, available bool) (effects.StartupRecoveryCounts, error)
	InstallContextCompaction(ctx context.Context, lease *sessions.Lease, result llm.ContextCompactionResult) error
	IsExternalEffectAuthorityCurrent(ctx context.Context, authority effects.Authority) (bool, error)
	IsForkChatWorkspaceAuthorityCurrent(ctx context.Context, authority effects.Authority) (bool, error)
	ListCanceledTurnRecoveries(ctx context.Context, request effects.RecoveryRequest) ([]effects.TurnExecutionResult, error)
	LoadContextCompaction(ctx context.Context, identity agentmemory.Identity, prepared llm.ContextCompactionPreparation) (llm.ContextCompactionResult, bool, error)
	LoadProviderCallResume(ctx context.Context, req effects.CompletionContinuationRequest) (effects.ProviderCallRecovery, bool, error)
	MarkExternalAttemptLaunched(ctx context.Context, attempt effects.Attempt, now time.Time) (effects.ExternalAttemptLaunch, error)
	MarkExternalAttemptResponseObserved(ctx context.Context, attempt effects.Attempt, evidence map[string]any, now time.Time) error
	ProjectCompletionConversation(ctx context.Context, attempt effects.Attempt, projection effects.CompletionConversationProjection) error
	ReconcileChannelOnboardingEffectOutcomes(ctx context.Context, onboardingOperationID string, now time.Time) ([]effects.ChannelOnboardingEffectOutcome, error)
	ReconcileExternalEffectAttempts(ctx context.Context, request effects.RecoveryRequest) (effects.RecoverySummary, error)
	RecoverCompletionContinuation(ctx context.Context, req effects.CompletionContinuationRequest) (effects.Attempt, bool, error)
	RequestTurnTimeout(ctx context.Context, attempt effects.Attempt, now time.Time) (effects.TurnCancellation, error)
	SettleCompletion(ctx context.Context, attempt effects.Attempt, settlement effects.CompletionSettlement) (effects.CompletionSettlementResult, error)
	SettleExternalAttempt(ctx context.Context, settlement effects.Settlement) error
}

type entityOwnerMethods interface {
	LoadEntityState(ctx context.Context, identity tools.EntityIdentity) (map[string]any, bool, error)
	QueryEntityStates(ctx context.Context, query tools.EntityStateQuery) ([]map[string]any, error)
}

type eventOwnerMethods interface {
	CommitAPIEventPublication(ctx context.Context, command bus.APIEventPublicationCommand) (bus.CommittedAPIEventPublication, error)
	CommitInboundPublication(ctx context.Context, command inboundpublication.CommitCommand) (inboundpublication.CommitResult, error)
	CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error)
	CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (bus.EventAppendOutcome, error)
	EventExists(ctx context.Context, eventID string) (bool, error)
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
	ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error)
	LoadComputeModuleReplayEvidenceForExecution(ctx context.Context, runID string, eventID string, nodeID string) ([]computemodule.ReplayEnvelope, error)
	LoadInboundPublicationByIdentity(ctx context.Context, identity inboundpublication.Identity) (inboundpublication.Record, bool, error)
	LoadPreparedPublishEvent(ctx context.Context, eventID string) (bus.PreparedPublishEvent, bool, error)
	LookupAPIEventPublication(ctx context.Context, request apiidempotency.Request) (apiidempotency.Completion, bool, error)
	PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtime.RuntimeLogPersistenceRecord) (bool, error)
	PersistRuntimeLog(ctx context.Context, record runtime.RuntimeLogPersistenceRecord) error
	RuntimeLogLineageParentEventID(ctx context.Context, runID string, explicitParentEventID string, subjectEventID string) (string, error)
	SetEventPayloadAdmitter(admitter bus.PayloadAdmitter)
	ValidateInboundPublicationIntegrity(ctx context.Context) error
	VerifyPreparedPublishEventIdentity(candidate events.AdmittedEvent, durable bus.PreparedPublishEvent) error
}

type eventSQLiteOwnerMethods interface {
	CommitAPIEventPublication(ctx context.Context, command bus.APIEventPublicationCommand) (bus.CommittedAPIEventPublication, error)
	CommitInboundPublication(ctx context.Context, command inboundpublication.CommitCommand) (inboundpublication.CommitResult, error)
	CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error)
	CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (bus.EventAppendOutcome, error)
	EventExists(ctx context.Context, eventID string) (bool, error)
	LoadComputeModuleReplayEvidenceForExecution(ctx context.Context, runID string, eventID string, nodeID string) ([]computemodule.ReplayEnvelope, error)
	LoadInboundPublicationByIdentity(ctx context.Context, identity inboundpublication.Identity) (inboundpublication.Record, bool, error)
	LoadPreparedPublishEvent(ctx context.Context, eventID string) (bus.PreparedPublishEvent, bool, error)
	LookupAPIEventPublication(ctx context.Context, request apiidempotency.Request) (apiidempotency.Completion, bool, error)
	PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtime.RuntimeLogPersistenceRecord) (bool, error)
	PersistRuntimeLog(ctx context.Context, record runtime.RuntimeLogPersistenceRecord) error
	RuntimeLogLineageParentEventID(ctx context.Context, runID string, explicitParentEventID string, subjectEventID string) (string, error)
	SetEventPayloadAdmitter(admitter bus.PayloadAdmitter)
	ValidateInboundPublicationIntegrity(ctx context.Context) error
	VerifyPreparedPublishEventIdentity(candidate events.AdmittedEvent, durable bus.PreparedPublishEvent) error
}

type llmMethods interface {
	Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, error)
	AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*sessions.Lease, llm.ConversationRecord, error)
	LoadActiveConversation(ctx context.Context, identity agentmemory.Identity) (llm.ConversationRecord, bool, error)
	Renew(ctx context.Context, lease *sessions.Lease) (*sessions.Lease, error)
	ResetAll(ctx context.Context, metadata sessions.ResetMetadata) (sessions.ResetSummary, error)
	Rotate(ctx context.Context, leaseInput *sessions.Lease, rotation sessions.RotationMetadata) (*sessions.Lease, error)
	UpdateLiveSessionWatchdog(ctx context.Context, lease *sessions.Lease, update llm.ConversationWatchdogUpdate) error
	UpsertConversation(ctx context.Context, lease *sessions.Lease, rec llm.ConversationRecord) error
}

type mailboxOwnerMethods interface {
	AcknowledgeMailboxNotice(ctx context.Context, req apiidempotency.Request) (apiidempotency.Completion, bool, error)
	CountMailboxItems(ctx context.Context, status string) (int, error)
	CountUnreadInformationalNotices(ctx context.Context) (int, error)
	ExpireMailboxItems(ctx context.Context, limit int) ([]tools.MailboxItem, error)
	GetMailboxItem(ctx context.Context, id string) (tools.MailboxItem, error)
	GetV1MailboxItem(ctx context.Context, id string) (mailbox.V1ItemDetail, error)
	InsertMailboxItem(ctx context.Context, item tools.MailboxItem) (string, error)
	ListMailboxItems(ctx context.Context, status string, limit int) ([]tools.MailboxItem, error)
	ListV1MailboxItems(ctx context.Context, opts mailbox.V1ListOptions) ([]mailbox.V1Item, string, error)
}

type managedCapabilityOwnerMethods = managedcapabilities.Persistence

type operatorAgentMethods interface {
	DeliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (deliverylifecycle.AgentDiagnosticCounts, error)
	DeliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query deliverylifecycle.AgentDiagnosticPageQuery) (deliverylifecycle.SnapshotPage, error)
	DeliveryLifecycleSnapshotPageForAgent(ctx context.Context, query deliverylifecycle.AgentLifecyclePageQuery) (deliverylifecycle.SnapshotPage, error)
	ListActiveAgentDescriptors(ctx context.Context, runID string) ([]bus.ActiveAgentDescriptor, error)
	ListAgentDeliveryLifecycleFacts(ctx context.Context, identities []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error)
	ListOperatorAgents(ctx context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error)
	ListPendingAgentDeliveryDetails(ctx context.Context, opts operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryPage, error)
	ListPendingAgentDeliveryFacts(ctx context.Context, identities []agentidentity.Identity, since time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error)
	LoadOperatorAgent(ctx context.Context, identity agentidentity.Identity) (operatorread.OperatorAgentDetail, error)
	LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error)
	LoadOperatorAgentDeliveryLifecycle(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleList, error)
	LoadOperatorAgentDiagnosis(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDiagnosisOptions) (operatorread.OperatorAgentDiagnosis, error)
	LoadOperatorAgentUsage(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentUsageOptions) (operatorread.OperatorAgentUsage, error)
}

type operatorAgentSQLiteMethods interface {
	DeliveryDiagnosticCountsForAgentSince(ctx context.Context, identity agentidentity.Identity, since time.Time) (deliverylifecycle.AgentDiagnosticCounts, error)
	DeliveryDiagnosticSnapshotPageForAgent(ctx context.Context, query deliverylifecycle.AgentDiagnosticPageQuery) (deliverylifecycle.SnapshotPage, error)
	DeliveryLifecycleSnapshotPageForAgent(ctx context.Context, query deliverylifecycle.AgentLifecyclePageQuery) (deliverylifecycle.SnapshotPage, error)
	ListActiveAgentDescriptors(ctx context.Context, runID string) ([]bus.ActiveAgentDescriptor, error)
	ListAgentDeliveryLifecycleFacts(ctx context.Context, identities []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error)
	ListOperatorAgents(ctx context.Context, opts operatorread.OperatorAgentListOptions) (operatorread.OperatorAgentListResult, error)
	LoadOperatorAgent(ctx context.Context, identity agentidentity.Identity) (operatorread.OperatorAgentDetail, error)
	LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error)
	LoadOperatorAgentDeliveryLifecycle(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleList, error)
	LoadOperatorAgentDiagnosis(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDiagnosisOptions) (operatorread.OperatorAgentDiagnosis, error)
	LoadOperatorAgentUsage(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentUsageOptions) (operatorread.OperatorAgentUsage, error)
}

type operatorChannelOwnerMethods interface {
	BeginChannelBinding(ctx context.Context, req operatorchannel.BeginRequest) (operatorchannel.Operation, error)
	BindOperatorChannelFromProof(ctx context.Context, req operatorchannel.BootBindRequest) (operatorchannel.Binding, error)
	CompleteProofResponsibility(ctx context.Context, operationID string, proofID string, proofRevision int64, status operatorchannel.ProofStatus, failure string, now time.Time) error
	ConfirmChannelBinding(ctx context.Context, req operatorchannel.ConfirmRequest) (operatorchannel.Operation, operatorchannel.Binding, error)
	EnsureOperatorPrincipal(ctx context.Context, now time.Time) (operatorchannel.Principal, error)
	ExpireChannelBinding(ctx context.Context, req operatorchannel.ExpireRequest) (operatorchannel.Operation, error)
	FindChannelBindingBeginReplay(ctx context.Context, req operatorchannel.BeginReplayRequest) (operatorchannel.Operation, bool, error)
	ListOperatorChannelBindings(ctx context.Context, principalID string) ([]operatorchannel.Binding, error)
	ListOperatorChannelOperations(ctx context.Context, principalID string) ([]operatorchannel.Operation, error)
	ListPendingProofResponsibilities(ctx context.Context) ([]operatorchannel.ProofResponsibility, error)
	LoadOperatorChannelClaimReceipt(ctx context.Context, id string) (operatorchannel.ClaimReceipt, bool, error)
	SettleSessionChannelClaim(ctx context.Context, fact authority.Claim) (operatorchannel.ClaimSettlement, error)
	UnbindOperatorChannel(ctx context.Context, req operatorchannel.UnbindRequest) (operatorchannel.Operation, operatorchannel.Binding, error)
}

type operatorConversationMethods interface {
	ListOperatorConversations(ctx context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error)
	LoadConversationForkSource(ctx context.Context, sessionID string) (runfork.ConversationForkSource, error)
	LoadLatestPublicConversationTurn(ctx context.Context, sessionID string) (*operatorread.OperatorPublicConversationTurn, error)
}

type operatorEntityMethods interface {
	AggregateOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error)
	ListOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error)
	LoadOperatorEntity(ctx context.Context, entityID string, runID string) (operatorread.OperatorEntityFull, error)
}

type operatorObservabilityMethods interface {
	ListOperatorEvents(ctx context.Context, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
	ListOperatorRuntimeIncidents(ctx context.Context, opts operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error)
	ListOperatorRuntimeLogs(ctx context.Context, opts operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
	LoadOperatorEvent(ctx context.Context, eventID string) (operatorread.OperatorEventFull, error)
}

type operatorRunMethods interface {
	ListRunDebugRuns(ctx context.Context, limit int) ([]operatorread.RunDebugRunSummary, error)
	ListRunHeaders(ctx context.Context, opts operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error)
	LoadLatestRunFlowInstance(ctx context.Context, runID string) (string, error)
	LoadLatestRunNonEscalationProgressAt(ctx context.Context, runID string, escalationEventName string) (time.Time, error)
	LoadRunClockSchedules(ctx context.Context, runID string) ([]genericschedule.ClockReadback, error)
	LoadRunDebugReport(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
	LoadRunDebugTrace(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, error)
	LoadRunDebugTracePage(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error)
	LoadRunHeader(ctx context.Context, runID string) (operatorread.RunHeader, error)
	LoadRunOrigin(ctx context.Context, runID string) (runlifecycle.RunOrigin, error)
	LoadRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (operatorread.RunTestQuiescence, error)
	ResolveLatestRunDebugRunID(ctx context.Context) (string, error)
}

type operatorRunSQLiteMethods interface {
	ListRunHeaders(ctx context.Context, opts operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error)
	LoadLatestRunFlowInstance(ctx context.Context, runID string) (string, error)
	LoadLatestRunNonEscalationProgressAt(ctx context.Context, runID string, escalationEventName string) (time.Time, error)
	LoadRunClockSchedules(ctx context.Context, runID string) ([]genericschedule.ClockReadback, error)
	LoadRunDebugReport(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
	LoadRunDebugTracePage(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error)
	LoadRunHeader(ctx context.Context, runID string) (operatorread.RunHeader, error)
	LoadRunOrigin(ctx context.Context, runID string) (runlifecycle.RunOrigin, error)
	LoadRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (operatorread.RunTestQuiescence, error)
}

type pipelineOwnerMethods interface {
	AbandonDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error
	AcquireDecisionCardMutation(ctx context.Context, req apiidempotency.Request, mutation pipeline.DecisionCardMutation) (pipeline.DecisionCardMutationLease, error)
	AdmitRunExecutionPosture(ctx context.Context, runID string, posture executionposture.Posture) error
	AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error)
	BeginDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error)
	CancelRunFanOut(ctx context.Context, runID string, reason string, at time.Time) error
	CommitFlowInstanceActivation(ctx context.Context, command bus.FlowInstanceActivationCommand) (pipeline.CommittedFlowInstanceActivation, error)
	CommitHumanTaskDeferredRoute(ctx context.Context, command pipeline.HumanTaskDeferredRouteCommand) (pipeline.CommittedHumanTaskRoute, error)
	CommitHumanTaskExpirations(ctx context.Context, command pipeline.HumanTaskExpiryCommand) (pipeline.CommittedHumanTaskExpiry, error)
	CommitHumanTaskOutcomeRoute(ctx context.Context, command pipeline.HumanTaskOutcomeRouteCommand) (pipeline.CommittedHumanTaskRoute, error)
	CommitProposedEffectRoute(ctx context.Context, command pipeline.ProposedEffectRouteCommand) (pipeline.CommittedProposedEffectRoute, error)
	CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error)
	CommitWorkflowEmitFeedback(ctx context.Context, candidate pipeline.WorkflowEmitFeedback) (pipeline.WorkflowEmitFeedbackCommit, error)
	CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error)
	CommitWorkflowTimerOccurrence(ctx context.Context, command pipeline.WorkflowTimerOccurrenceCommand) (pipeline.CommittedWorkflowTimerOccurrence, error)
	CommitWorkflowTimerReconciliation(ctx context.Context, command pipeline.WorkflowTimerReconciliationCommand) (pipeline.CommittedWorkflowLifecycleMutation, error)
	CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error)
	FanOutRunSummary(ctx context.Context, runID string, now time.Time) (fanoutobligation.RunSummary, error)
	InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source correlation.SourceArtifactFact) ([]pipeline.DynamicFlowRuntimeReadiness, error)
	InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error)
	ListActiveWorkflowTimerActivationsForRoute(ctx context.Context, identity flowidentity.RunScopedFlowInstance) ([]pipeline.WorkflowTimerActivation, error)
	ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error)
	ListWorkflowInstances(ctx context.Context, runID string) ([]pipeline.WorkflowInstance, error)
	ListWorkflowTimerActivations(ctx context.Context, runID string, entityID string, activeOnly bool) ([]pipeline.WorkflowTimerActivation, error)
	LoadActiveWorkflowRoute(ctx context.Context, identity flowidentity.RunScopedFlowInstance) (workflowroute.RecoveryRecord, error)
	LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error)
	LoadFlowConstructionPublication(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) (pipeline.FlowConstructionPublicationEvidence, error)
	LoadWorkflowInstance(ctx context.Context, identity flowidentity.RunScopedFlowInstance) (pipeline.WorkflowInstance, bool, error)
	LoadWorkflowTargetPersistence(ctx context.Context, identity flowidentity.RunScopedFlowInstance, entityID identity.EntityID) (pipeline.WorkflowTargetPersistenceRecord, error)
	LoadWorkflowTimerActivation(ctx context.Context, activationID string) (pipeline.WorkflowTimerActivation, bool, error)
	LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error)
	PipelineObligations() pipelineobligation.Store
	QueryWorkflowEntityCollection(ctx context.Context, owner pipeline.WorkflowEntityCollectionOwner) ([]pipeline.WorkflowEntityStatePersistenceRecord, error)
	ReadWorkflowEmitFeedback(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowEmitFeedback, bool, error)
	ReadWorkflowHandlerStageReceipts(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) ([]pipelineobligation.CommittedStageReceipt, error)
	ReadWorkflowPublicationStages(ctx context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowPublicationStageEvidence, bool, error)
	ReconcileDynamicFlowRuntimeReadinessPlans(ctx context.Context, requests []pipeline.DynamicFlowRuntimeReadinessPlanReconciliation, observedAt time.Time) ([]pipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error)
	RequireGateRouteAdmitted(ctx context.Context, runID string) error
	ResolveDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationResolution, error)
	RetireDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error
	RetireDynamicFlowRuntimeActivationAttempts(ctx context.Context, attempts []pipeline.DynamicFlowRuntimeActivationAttempt) error
	SetupScenarioEntities(ctx context.Context, req pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
	VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error
	WithFlowInstancePlanningSnapshot(ctx context.Context, scope pipeline.FlowInstanceLookupScope, prepare func(context.Context, pipeline.FlowInstanceIndexReader) error) error
}

type replyOwnerMethods interface {
	ClaimReplyContext(ctx context.Context, id string, replyEventID string) (replycontext.Record, replycontext.ClaimOutcome, error)
	CreateReplyContext(ctx context.Context, record replycontext.Record) error
	LoadReplyContext(ctx context.Context, id string) (replycontext.Record, error)
}

type runForkOwnerMethods interface {
	ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
	AdmitOperatorConversationForkChat(ctx context.Context, forkID string, posture executionposture.Posture) error
	ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (effects.Authority, error)
	CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, executionID string) (bool, error)
	CreateAPIConversationFork(ctx context.Context, req runfork.APIConversationForkCreateRequest) (runfork.ConversationForkCreateResult, error)
	CreateOperatorConversationFork(ctx context.Context, req runfork.ConversationForkCreateRequest) (runfork.OperatorConversationForkSession, error)
	DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (runfork.ConversationForkDeleteResult, error)
	DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string) error
	FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, failure json.RawMessage) (bool, error)
	FailOperatorConversationForkChat(ctx context.Context, req runfork.ConversationForkChatFailureRequest) error
	FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, failure json.RawMessage) error
	HeartbeatOperatorConversationForkChat(ctx context.Context, prepared runfork.ConversationForkChatPrepared, now time.Time) error
	HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, lease time.Duration) error
	InspectSelectedMaterializationReplay(ctx context.Context, plan runfork.RunForkPlan) ([]runfork.RunForkUnsupportedBlocker, error)
	IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
	ListOperatorConversationForks(ctx context.Context, opts runfork.ConversationForkListOptions) (runfork.ConversationForkListResult, error)
	ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]runfork.RunForkSelectedContractRouteRecovery, error)
	ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]manager.SelectedContractRouteRecoveryRecord, error)
	ListSelectedForkRecoveryEntries(ctx context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
	LoadOperatorConversationFork(ctx context.Context, forkID string) (runfork.OperatorConversationForkSession, error)
	LoadRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, bool, error)
	LoadRunForkSelectedContractRouteRecovery(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractRouteRecovery, bool, error)
	LoadRunForkSelectedContractSourceEventModes(ctx context.Context, sourceRunID string, sourceEventIDs []string) ([]executionmode.Mode, error)
	LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID string, forkRunID string, sourceEventIDs []string, original semanticview.OriginalLoopCarriage, originalRoot sourceartifact.RootAdmission) ([]runfork.RunForkSelectedContractSourceEvent, error)
	LoadRunForkSourceRunID(ctx context.Context, forkRunID string) (string, error)
	MaterializeRunFork(ctx context.Context, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
	PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	PrepareOperatorConversationForkChat(ctx context.Context, req runfork.ConversationForkChatPrepareRequest) (runfork.ConversationForkChatPrepared, error)
	QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority) error
	RecordOperatorConversationForkChat(ctx context.Context, req runfork.ConversationForkChatRecordRequest) (runfork.ConversationForkChatResult, error)
	RecordRunForkSelectedContractRouteRecovery(ctx context.Context, req runfork.RunForkSelectedContractRouteRecoveryRequest) (runfork.RunForkSelectedContractRouteRecovery, error)
	RecoverSelectedFork(ctx context.Context, req runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
	RequireMaterializedRunFork(ctx context.Context, forkRunID string, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	RequireMaterializedRunForkForSelectedContractExecution(ctx context.Context, forkRunID string, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
	RequireRunForkSelectedContractBinding(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, error)
	RequireRunForkSelectedContractExecutionSettlement(ctx context.Context, authority effects.Authority, allowedSourceEventIDs []string, source semanticview.Source) error
	RequireSelectedForkInputPublished(ctx context.Context, authority effects.Authority, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) (bool, error)
	ResolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector runfork.ConversationForkPointSelector) (runfork.ConversationForkPointDescriptor, error)
	StopSelectedFork(ctx context.Context, req runcontrol.SelectedStopRequest) (runcontrol.State, error)
}

type runLifecycleOwnerMethods interface {
	ActiveRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (string, bool, error)
	ApplyActiveRunQuiescence(ctx context.Context, req runquiescence.Request) (runquiescence.Result, error)
	ApplyDestructiveResetQuiescence(ctx context.Context, req destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error)
	ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runquiescence.Result, error)
	ClassifyLiveRunCreationFailure(ctx context.Context, scope runlifecycle.LiveRunScope, runID string, err error) error
	CompletionCandidateCoordinator() *runhandoff.CandidateCoordinator
	CreateRun(ctx context.Context, request runlifecycle.CreateRequest) (runlifecycle.MutationDisposition, error)
	ExecuteCompletionCandidate(ctx context.Context, candidate runlifecycle.Candidate, catalog runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error)
	ListCompletionCandidates(ctx context.Context, scope runlifecycle.CandidateScope, cursor runlifecycle.CandidateCursor, limit int) (runlifecycle.CandidatePage, error)
	LoadLandingRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LandingRun, bool, error)
	LoadRunLifecycleSnapshot(ctx context.Context, runID string) (bus.RunLifecycleSnapshot, error)
	MarkTerminalRun(ctx context.Context, request runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
	ReconcileFlowRun(ctx context.Context, scope runlifecycle.LiveRunScope, source correlation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error)
	RegisterCompletionCandidateSink(ctx context.Context, scope runlifecycle.CandidateScope, sink runlifecycle.CandidateSink) (runlifecycle.CandidateRegistration, error)
	RequestCompletionCandidate(ctx context.Context, request runlifecycle.CandidateRequest) (runlifecycle.CandidateRequestDisposition, error)
	RequireActiveRun(ctx context.Context, runID string) error
	RequireActiveRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error)
	RequirePresentRun(ctx context.Context, runID string) error
	RequirePresentRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error)
	ReviseRunSource(ctx context.Context, request runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
	RunDispatchBlocked(ctx context.Context, runID string) (bool, error)
	RunDispatchParked(ctx context.Context, runID string) (bool, error)
	TransitionActiveRun(ctx context.Context, request runlifecycle.ActiveTransitionRequest) (runlifecycle.MutationDisposition, error)
}

type runLifecycleSQLiteOwnerMethods interface {
	ActiveRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (string, bool, error)
	ApplyActiveRunQuiescence(ctx context.Context, req runquiescence.Request) (runquiescence.Result, error)
	ApplyDestructiveResetQuiescence(ctx context.Context, req destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error)
	ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runquiescence.Result, error)
	ClassifyLiveRunCreationFailure(ctx context.Context, scope runlifecycle.LiveRunScope, runID string, err error) error
	CompletionCandidateCoordinator() *runhandoff.CandidateCoordinator
	CreateRun(ctx context.Context, request runlifecycle.CreateRequest) (runlifecycle.MutationDisposition, error)
	DestructiveResetDeliveryQuiesced(ctx context.Context, eventID string, subscriberType string, subscriberID string) (bool, error)
	ExecuteCompletionCandidate(ctx context.Context, candidate runlifecycle.Candidate, catalog runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error)
	ListCompletionCandidates(ctx context.Context, scope runlifecycle.CandidateScope, cursor runlifecycle.CandidateCursor, limit int) (runlifecycle.CandidatePage, error)
	LoadLandingRun(ctx context.Context, scope runlifecycle.LiveRunScope) (runlifecycle.LandingRun, bool, error)
	LoadRunLifecycleSnapshot(ctx context.Context, runID string) (bus.RunLifecycleSnapshot, error)
	MarkTerminalRun(ctx context.Context, request runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
	ReconcileFlowRun(ctx context.Context, scope runlifecycle.LiveRunScope, source correlation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error)
	RegisterCompletionCandidateSink(ctx context.Context, scope runlifecycle.CandidateScope, sink runlifecycle.CandidateSink) (runlifecycle.CandidateRegistration, error)
	RequestCompletionCandidate(ctx context.Context, request runlifecycle.CandidateRequest) (runlifecycle.CandidateRequestDisposition, error)
	RequireActiveRun(ctx context.Context, runID string) error
	RequireActiveRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error)
	RequirePresentRun(ctx context.Context, runID string) error
	RequirePresentRunSource(ctx context.Context, runID string) (correlation.SourceArtifactFact, error)
	ReviseRunSource(ctx context.Context, request runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
	RunDispatchBlocked(ctx context.Context, runID string) (bool, error)
	RunDispatchParked(ctx context.Context, runID string) (bool, error)
	ServeAbandonDeliveryQuiesced(ctx context.Context, eventID string, subscriberType string, subscriberID string) (bool, error)
	TransitionActiveRun(ctx context.Context, request runlifecycle.ActiveTransitionRequest) (runlifecycle.MutationDisposition, error)
}

type runtimeIngressOwnerMethods interface {
	EnsureRuntimeIngressState(ctx context.Context, now time.Time) (ingress.State, error)
	LoadRuntimeIngressState(ctx context.Context) (ingress.State, error)
	SetRuntimeIngressTransitionEvent(ctx context.Context, target ingress.Status, eventID string, transitionAt time.Time) (bool, error)
	TransitionRuntimeIngressState(ctx context.Context, target ingress.Status, reason string, controlledBy string, now time.Time) (ingress.State, bool, error)
}

type startupOwnerMethods interface {
	AcquireProcessCapability(ctx context.Context, req startupownership.AcquireRequest) (startupownership.ProcessCapability, error)
	InspectAuthority(ctx context.Context) (startupownership.AuthorityInspection, error)
	ReleaseConstructionPossession() error
}

type timerObligationReaderMethods interface {
	timerobligation.Reader
	Read(ctx context.Context, scope timerobligation.Scope, observedAt time.Time) (timerobligation.Snapshot, error)
}
