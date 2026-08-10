package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storeeffect "github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	storeevent "github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	storeoperatorsurface "github.com/division-sh/swarm/internal/store/internal/operatorsurface"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

type execQueryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type eventReadQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type persistedEventIdentity = eventrecord.Record
type runForkActivationLineage = storerunfork.RunForkActivationLineage
type runForkActivityRequestPayload = storerunfork.RunForkActivityRequestPayload
type runForkGateActivationBinding = storerunfork.RunForkGateActivationBinding
type runForkRevisionedFact = storerunfork.RunForkRevisionedFact
type runForkRevisionEvent = storerunfork.RunForkRevisionEvent
type runForkRevisionDelivery = storerunfork.RunForkRevisionDelivery
type runForkRevisionSnapshot = storerunfork.RunForkRevisionSnapshot
type runCompletionOwnerSummaries = storerunlifecycle.RunCompletionOwnerSummaries
type terminalRunMutation = storerunlifecycle.TerminalRunMutation
type standaloneRuntimePlatformRunRecord = storerunlifecycle.StandaloneRuntimePlatformRunRecord
type conversationForkTimeValue = storerunfork.ConversationForkTimeValue
type conversationTurnRecord = storeoperatorsurface.ConversationTurnRecord
type externalEffectStorySource = storeeffect.ExternalEffectStorySource
type completionRecoveryAttempt = storeeffect.CompletionRecoveryAttempt
type completionRecoveryAuthorityEvidence = storeeffect.CompletionRecoveryAuthorityEvidence

var projectRunForkReplayEvent = storerunfork.ProjectRunForkReplayEvent
var insertRunForkReplayDelivery = storerunfork.InsertRunForkReplayDelivery
var deterministicRunForkMaterializationID = storerunfork.DeterministicRunForkMaterializationID
var deterministicRunForkReplayEventID = storerunfork.DeterministicRunForkReplayEventID
var decisionCardAuthorActivityIdentity = storedecision.DecisionCardAuthorActivityIdentity
var appendDecisionCardChangeWithStory = storedecision.AppendDecisionCardChangeWithStory
var runPostgresDecisionCardMutation = storedecision.RunPostgresDecisionCardMutation
var recordExternalEffectStory = storeeffect.RecordExternalEffectStory
var externalEffectStoryDispositions = storeeffect.ExternalEffectStoryDispositionKeys()
var jsonSemanticallyEqual = storeevent.JSONSemanticallyEqual

const runtimeLogEventName = storeevent.RuntimeLogEventName
const runForkActivityRequestEvent = storerunfork.RunForkActivityRequestEvent
const replayScopeReasonDirect = storeevent.ReplayScopeReasonDirect
const replayScopeReasonSubscribed = storeevent.ReplayScopeReasonSubscribed

var isStandaloneRuntimePlatformRunRecord = storerunlifecycle.IsStandaloneRuntimePlatformRunRecord
var decodeEventRecord = storeevent.DecodeEventRecord
var loadPostgresEventIdentity = storeevent.LoadPostgresEventIdentity
var loadSQLiteEventIdentity = storeevent.LoadSQLiteEventIdentity
var loadPostgresInboundPublicationEvent = storeevent.LoadPostgresInboundPublicationEvent
var loadSQLiteInboundPublicationEvent = storeevent.LoadSQLiteInboundPublicationEvent
var loadRunForkReplaySourceEvent = storerunfork.LoadRunForkReplaySourceEvent
var prepareRunForkSelectedContractSourceEvent = storerunfork.PrepareRunForkSelectedContractSourceEvent
var forkAttemptGenerationState = storerunfork.ForkAttemptGenerationState
var forkGateActivationState = storerunfork.ForkGateActivationState
var validateRunForkDeliveryEventReplayWorkAgainstPlan = storerunfork.ValidateRunForkDeliveryEventReplayWorkAgainstPlan
var loadRunForkPendingWorkFromRevision = storerunfork.LoadRunForkPendingWorkFromRevision
var loadRunForkSourceFactsFromRevision = storerunfork.LoadRunForkSourceFactsFromRevision
var appendRunForkRevisionFact = storerunfork.AppendRunForkRevisionFact
var stringSliceSet = storerunfork.StringSliceSet
var normalizeRunForkSelectedContractBinding = storerunfork.NormalizeRunForkSelectedContractBinding
var normalizeRunForkSelectedContractRouteRecovery = storerunfork.NormalizeRunForkSelectedContractRouteRecovery
var runForkReplayResumeBlockerFromError = storerunfork.RunForkReplayResumeBlockerFromError
var committedReplayScopeFromReasonCode = storeevent.CommittedReplayScopeFromReasonCode
var commitRunForkAuthorActivityTransaction = storerunfork.CommitRunForkAuthorActivityTransaction
var resolveExistingEventIdentity = storeevent.ResolveExistingEventIdentity
var newAgentReceiptSideEffects = storeevent.NewAgentReceiptSideEffects
var marshalAgentReceiptSideEffects = storeevent.MarshalAgentReceiptSideEffects
var decodeAgentReceiptSideEffects = storeevent.DecodeAgentReceiptSideEffects
var insertCommittedPipelineScopeTx = storepipeline.InsertCommittedPipelineScopeTx
var writePipelineDispositionTx = storepipeline.WritePipelineDispositionTx
var replayClaimLockKey = storepipeline.ReplayClaimLockKey
var expireHumanTaskCards = storedecision.ExpireHumanTaskCards
var completionRecoverySettlement = storeeffect.CompletionRecoverySettlement
var operatorConversationTurnListItemFromPublic = storeoperatorsurface.OperatorConversationTurnListItemFromPublic
var projectPublicConversationTurn = storeoperatorsurface.ProjectPublicConversationTurn
var corruptPipelineScopeDisposition = storepipeline.CorruptPipelineScopeDisposition
var projectBusRunLifecycleSnapshot = storerunlifecycle.ProjectBusRunLifecycleSnapshot
var externalEffectAuthorityCurrentPostgres = storeeffect.ExternalEffectAuthorityCurrentPostgres
var externalEffectAuthorityCurrentSQLite = storeeffect.ExternalEffectAuthorityCurrentSQLite

func postgresActiveRunSourceOwner(store *PostgresStore, tx *sql.Tx) storerunfork.ActiveRunSourceOwnerFunc {
	return func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
		return store.runLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
	}
}

var sqliteDeliveryAdapter = mustTestDeliveryAdapter(storedelivery.DialectSQLite)
var postgresDeliveryAdapter = mustTestDeliveryAdapter(storedelivery.DialectPostgres)

func mustTestDeliveryAdapter(dialect storedelivery.Dialect) *storedelivery.Adapter {
	adapter, err := storedelivery.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func loadCommittedPipelineScope(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string, postgres bool) (runtimepipelineobligation.CommittedScope, error) {
	return storepipeline.LoadCommittedScope(ctx, queryer, eventID, postgres)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func requirePostgresRunActiveQuery(ctx context.Context, queryer storerunstate.RowQueryer, runID string) error {
	return storerunstate.RequirePostgresActiveQuery(ctx, queryer, runID)
}

func requireSQLiteRunActiveQuery(ctx context.Context, queryer storerunstate.RowQueryer, runID string) error {
	return storerunstate.RequireSQLiteActiveQuery(ctx, queryer, runID)
}

func sqlitePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func requestPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, force bool) (runtimerunlifecycle.CandidateRequestResult, error) {
	if force {
		if err := runlifecyclefixture.ForcePostgresCompletionCandidateRevision(ctx, tx, runID); err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
	}
	return storerunlifecycle.RequestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt)
}

func requestSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time, force bool) (runtimerunlifecycle.CandidateRequestResult, error) {
	if force {
		if err := runlifecyclefixture.ForceSQLiteCompletionCandidateRevision(ctx, tx, runID); err != nil {
			return runtimerunlifecycle.CandidateRequestResult{}, err
		}
	}
	return storerunlifecycle.RequestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, now)
}

func postgresRunSessionNextWakeTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time) (*time.Time, error) {
	return storerunlifecycle.PostgresRunSessionNextWakeTx(ctx, tx, runID, selectedNow)
}

type eventCommitTxStore interface {
	AppendAdmittedEventTxOutcome(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
	RequirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	CommitInitialDeliveryObligationsTx(context.Context, *sql.Tx, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	CommitInitialPipelineScopeTx(context.Context, *sql.Tx, string, runtimepipelineobligation.CommittedScope) error
	CommitInitialPipelineDispositionTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	RecordDeadLetterTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, runtimedeadletters.Record, bool) error
	CreateWithinTransaction(context.Context, *sql.Tx, runtimereplycontext.Record) error
	ClaimWithinTransaction(context.Context, *sql.Tx, runtimereplycontext.ClaimCommand) error
	PrepareDynamicFlowCreationOccurrenceCommitTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error)
	CommitFlowInstanceActivationsTx(context.Context, *sql.Tx, *privateauthoractivity.Mutation, []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error)
	ReplaceFlowInstanceRouteTopologyTx(context.Context, *sql.Tx, []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error)
	MarkDynamicFlowCreationOccurrenceCommittedTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) runtimeauthoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

type sqlPublishCommitter struct {
	tx    *sql.Tx
	store eventCommitTxStore
	story runtimeauthoractivity.Mutation
}

func (c sqlPublishCommitter) commitNamedEvent(ctx context.Context, operation string, class events.EventAdmissionClass, eventType events.EventType, req runtimebus.CommitPublishRequest) (runtimebus.EventAppendOutcome, error) {
	if c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s event commit transaction is required", operation)
	}
	if err := events.ValidateNamedEvent(req.Event, class, eventType); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	outcome, err := c.store.AppendAdmittedEventTxOutcome(ctx, c.tx, c.story, req.Event)
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return outcome, err
	}
	if outcome != runtimebus.EventAppendInserted {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit returned invalid append outcome")
	}
	if err := c.commitInitialSideEffects(ctx, req, class != events.EventAdmissionDiagnosticDirect); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (c sqlPublishCommitter) commitInitialSideEffects(ctx context.Context, req runtimebus.CommitPublishRequest, requirePublicationClaim bool) error {
	proofs, err := c.commitInitialSideEffectEvidence(ctx, req, requirePublicationClaim)
	if err != nil {
		return err
	}
	if len(proofs) != 0 {
		return fmt.Errorf("named event commit must return executable delivery handoffs as typed evidence")
	}
	return nil
}

func (c sqlPublishCommitter) commitInitialSideEffectEvidence(ctx context.Context, req runtimebus.CommitPublishRequest, requirePublicationClaim bool) ([]runtimedelivery.DurableHandoffProof, error) {
	for _, record := range req.ReplyCreations {
		if err := c.store.CreateWithinTransaction(ctx, c.tx, record); err != nil {
			return nil, fmt.Errorf("commit reply context creation: %w", err)
		}
	}
	for _, claim := range req.ReplyClaims {
		if err := c.store.ClaimWithinTransaction(ctx, c.tx, claim); err != nil {
			return nil, fmt.Errorf("commit reply context claim: %w", err)
		}
	}
	if requirePublicationClaim {
		if err := c.store.RequirePipelinePublicationClaimTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim); err != nil {
			return nil, fmt.Errorf("executable event commit requires its current publication claim: %w", err)
		}
	}
	proofs, err := c.store.CommitInitialDeliveryObligationsTx(ctx, c.tx, req.Event.ID(), req.Event.Event().RunID(), req.DeliveryRoutes, req.DeliveryAuthority)
	if err != nil {
		return nil, err
	}
	if err := c.store.CommitInitialPipelineScopeTx(ctx, c.tx, req.Event.ID(), req.ReplayScope); err != nil {
		return nil, err
	}
	if req.Disposition != nil {
		if err := c.store.CommitInitialPipelineDispositionTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim, *req.Disposition); err != nil {
			return nil, err
		}
	}
	if req.DeadLetter != nil {
		if err := c.store.RecordDeadLetterTx(ctx, c.tx, c.story, *req.DeadLetter, true); err != nil {
			return nil, err
		}
	}
	return proofs, nil
}
