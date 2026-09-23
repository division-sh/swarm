package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
	"github.com/google/uuid"
)

type EventCommitOwner interface {
	CommitFanOutPublicationTx(context.Context, *mutationprotocol.Attempt, runtimebus.PublicationCommand, fanoutobligation.OrdinalEmission) (runtimebus.CommittedPublication, error)
	AppendAdmittedEventTxOutcome(context.Context, *mutationprotocol.Attempt, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	CommitPublicationTx(context.Context, *mutationprotocol.Attempt, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
}

type eventCommitTxStore interface {
	commitFanOutPublicationTx(context.Context, *mutationprotocol.Attempt, runtimebus.PublicationCommand, fanoutobligation.OrdinalEmission) (runtimebus.CommittedPublication, error)
	appendAdmittedEventTxOutcome(context.Context, *mutationprotocol.Attempt, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	RequirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	CommitInitialDeliveryObligationsTx(context.Context, *mutationprotocol.Attempt, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	CommitInitialPipelineScopeTx(context.Context, *mutationprotocol.Attempt, string, runtimepipelineobligation.CommittedScope) error
	CommitInitialPipelineDispositionTx(context.Context, *mutationprotocol.Attempt, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	RecordDeadLetterTx(context.Context, *mutationprotocol.Attempt, runtimedeadletters.Record, bool) error
	createReplyContextTx(context.Context, *mutationprotocol.Attempt, runtimereplycontext.Record) error
	claimReplyContextTx(context.Context, *mutationprotocol.Attempt, runtimereplycontext.ClaimCommand) error
	CommitFlowInstanceActivationsTx(context.Context, *mutationprotocol.Attempt, []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error)
	workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner
	genericScheduleTxOwner() GenericScheduleTxOwner
	commitPublicationTx(context.Context, *mutationprotocol.Attempt, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
	SettleWorkflowNodeSuccessTx(context.Context, *mutationprotocol.Attempt, runtimedelivery.Claim, []string, time.Duration, runtimedelivery.HandlerRuleSelectionFact) (runtimedelivery.Snapshot, error)
}

type GenericScheduleTxOwner interface {
	AdmitTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error)
	CancelAdmissionTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.AdmissionCommand, string, time.Time) (runtimegenericschedule.CancelResult, error)
	LoadActivationTx(context.Context, *mutationprotocol.Attempt, string) (runtimegenericschedule.Activation, bool, error)
	CancelActivationTx(context.Context, *mutationprotocol.Attempt, runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error)
}

type activeRunSourceOwnerFunc func(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)

func (fn activeRunSourceOwnerFunc) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return fn(ctx, runID)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func jsonRawMessageValue(raw any) json.RawMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), value...)
	case []byte:
		return json.RawMessage(append([]byte(nil), value...))
	case string:
		return json.RawMessage(value)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return encoded
	}
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

type PipelinePostgresOwner struct {
	fanOutReadiness FanOutReadiness
	apiIdempotency  *storeapiidempotency.PostgresOwner
	*storerunlifecycle.RunLifecyclePostgresOwner
	*storedecision.DecisionPostgresOwner
	*storedelivery.DeliveryPostgresOwner
	*storereplycontext.ReplyPostgresOwner

	backend                *postgresbackend.Backend
	requireCurrent         func() error
	candidateRequests      mutationprotocol.CandidateWriter
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	workflowEntityQueries  *storeworkflowentityquery.Postgres
	workflowRoutes         *storeworkflowroute.Postgres
	events                 EventCommitOwner
	selectedFork           SelectedForkCommitTxOwner
	genericSchedules       GenericScheduleTxOwner
}

type PipelineSQLiteOwner struct {
	activeFlowDescriptors   sqlitebackend.FixedReadStatement
	selectedRunTargetOwners sqlitebackend.FixedReadStatement
	fanOutReadiness         FanOutReadiness
	apiIdempotency          *storeapiidempotency.SQLiteOwner
	*storerunlifecycle.RunLifecycleSQLiteOwner
	*storedecision.DecisionSQLiteOwner
	*storedelivery.DeliverySQLiteOwner
	*storereplycontext.ReplySQLiteOwner

	backend                *sqlitebackend.Backend
	requireCurrent         func() error
	candidateRequests      mutationprotocol.CandidateWriter
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	workflowEntityQueries  *storeworkflowentityquery.SQLite
	workflowRoutes         *storeworkflowroute.SQLite
	events                 EventCommitOwner
	selectedFork           SelectedForkCommitTxOwner
	genericSchedules       GenericScheduleTxOwner
	nowFn                  func() time.Time
	mutationMu             sync.Mutex
	pipelineClaimMu        sync.Mutex
	recoveryTransitions    pipelineRecoveryTransitions
	pipelineClaimIssuer    *runtimepipelineobligation.ClaimIssuer
	pipelineClaims         map[string]*pipelineClaimState
	pipelineScanIssuer     *runtimepipelineobligation.ScanIssuer
	pipelineScans          map[string]*pipelineScanState
	testPipelineReleaseErr func() error
}

func (s *PipelinePostgresOwner) BindGenericScheduleTxOwner(owner GenericScheduleTxOwner) error {
	if s == nil || owner == nil {
		return errors.New("pipeline PostgreSQL generic schedule transaction owner is required")
	}
	if s.genericSchedules != nil {
		return errors.New("pipeline PostgreSQL generic schedule transaction owner is already bound")
	}
	s.genericSchedules = owner
	return nil
}

func (s *PipelineSQLiteOwner) BindGenericScheduleTxOwner(owner GenericScheduleTxOwner) error {
	if s == nil || owner == nil {
		return errors.New("pipeline SQLite generic schedule transaction owner is required")
	}
	if s.genericSchedules != nil {
		return errors.New("pipeline SQLite generic schedule transaction owner is already bound")
	}
	s.genericSchedules = owner
	return nil
}

func (s *PipelinePostgresOwner) genericScheduleTxOwner() GenericScheduleTxOwner {
	if s == nil {
		return nil
	}
	return s.genericSchedules
}

func (s *PipelineSQLiteOwner) genericScheduleTxOwner() GenericScheduleTxOwner {
	if s == nil {
		return nil
	}
	return s.genericSchedules
}

type SelectedForkCommitTxOwner interface {
	CommitSelectedForkTx(context.Context, *mutationprotocol.Attempt, runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error)
}

func (s *PipelinePostgresOwner) BindSelectedForkWriter(owner SelectedForkCommitTxOwner) error {
	if s == nil || owner == nil {
		return errors.New("pipeline PostgreSQL selected-fork writer is required")
	}
	if s.selectedFork != nil {
		return errors.New("pipeline PostgreSQL selected-fork writer is already bound")
	}
	s.selectedFork = owner
	return nil
}

func (s *PipelineSQLiteOwner) BindSelectedForkWriter(owner SelectedForkCommitTxOwner) error {
	if s == nil || owner == nil {
		return errors.New("pipeline SQLite selected-fork writer is required")
	}
	if s.selectedFork != nil {
		return errors.New("pipeline SQLite selected-fork writer is already bound")
	}
	s.selectedFork = owner
	return nil
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, lifecycle *storerunlifecycle.RunLifecyclePostgresOwner, candidates *runhandoff.CandidateCoordinator, decision *storedecision.DecisionPostgresOwner, delivery *storedelivery.DeliveryPostgresOwner, reply *storereplycontext.ReplyPostgresOwner, entityQueries *storeworkflowentityquery.Postgres, routes *storeworkflowroute.Postgres, events EventCommitOwner, idempotency *storeapiidempotency.PostgresOwner) (*PipelinePostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil || decision == nil || delivery == nil || reply == nil || entityQueries == nil || routes == nil || events == nil || idempotency == nil {
		return nil, errors.New("pipeline PostgreSQL owner dependencies are required")
	}
	return &PipelinePostgresOwner{apiIdempotency: idempotency, RunLifecyclePostgresOwner: lifecycle, DecisionPostgresOwner: decision, DeliveryPostgresOwner: delivery, ReplyPostgresOwner: reply, backend: backend, requireCurrent: requireCurrent, candidateRequests: lifecycle, runLifecycleCandidates: candidates, workflowEntityQueries: entityQueries, workflowRoutes: routes, events: events}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, lifecycle *storerunlifecycle.RunLifecycleSQLiteOwner, candidates *runhandoff.CandidateCoordinator, decision *storedecision.DecisionSQLiteOwner, delivery *storedelivery.DeliverySQLiteOwner, reply *storereplycontext.ReplySQLiteOwner, entityQueries *storeworkflowentityquery.SQLite, routes *storeworkflowroute.SQLite, events EventCommitOwner, idempotency *storeapiidempotency.SQLiteOwner, now func() time.Time) (*PipelineSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil || decision == nil || delivery == nil || reply == nil || entityQueries == nil || routes == nil || events == nil || idempotency == nil {
		return nil, errors.New("pipeline SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &PipelineSQLiteOwner{
		apiIdempotency:          idempotency,
		RunLifecycleSQLiteOwner: lifecycle, DecisionSQLiteOwner: decision, DeliverySQLiteOwner: delivery, ReplySQLiteOwner: reply,
		backend: backend, requireCurrent: requireCurrent, candidateRequests: lifecycle, runLifecycleCandidates: candidates, workflowEntityQueries: entityQueries, workflowRoutes: routes, events: events, nowFn: now,
		pipelineClaimIssuer: runtimepipelineobligation.NewClaimIssuer(), pipelineClaims: map[string]*pipelineClaimState{},
		pipelineScanIssuer: runtimepipelineobligation.NewScanIssuer(), pipelineScans: map[string]*pipelineScanState{},
	}, nil
}

func (s *PipelinePostgresOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("pipeline PostgreSQL owner is required")
	}
	return s.requireCurrent()
}

func (s *PipelineSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("pipeline SQLite owner is required")
	}
	return s.requireCurrent()
}

func (s *PipelineSQLiteOwner) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}

func (s *PipelinePostgresOwner) workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner {
	return s.DecisionPostgresOwner
}

func (s *PipelineSQLiteOwner) workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner {
	return s.DecisionSQLiteOwner
}

func (s *PipelinePostgresOwner) appendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.events.AppendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *PipelineSQLiteOwner) appendAdmittedEventTxOutcome(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.events.AppendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *PipelinePostgresOwner) commitPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return s.events.CommitPublicationTx(ctx, attempt, command)
}

func (s *PipelineSQLiteOwner) commitPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return s.events.CommitPublicationTx(ctx, attempt, command)
}

func (s *PipelinePostgresOwner) commitFanOutPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission) (runtimebus.CommittedPublication, error) {
	return s.events.CommitFanOutPublicationTx(ctx, attempt, command, projection)
}

func (s *PipelineSQLiteOwner) commitFanOutPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission) (runtimebus.CommittedPublication, error) {
	return s.events.CommitFanOutPublicationTx(ctx, attempt, command, projection)
}

func (s *PipelinePostgresOwner) createReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.ReplyPostgresOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *PipelineSQLiteOwner) createReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.ReplySQLiteOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *PipelinePostgresOwner) claimReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.ReplyPostgresOwner.ClaimWithinTransaction(ctx, attempt, command)
}

func (s *PipelineSQLiteOwner) claimReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.ReplySQLiteOwner.ClaimWithinTransaction(ctx, attempt, command)
}
