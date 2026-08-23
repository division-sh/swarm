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
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedecision "github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	storeworkflowentityquery "github.com/division-sh/swarm/internal/store/internal/workflowentityquery"
	storeworkflowroute "github.com/division-sh/swarm/internal/store/internal/workflowroute"
	"github.com/google/uuid"
)

type CompletionCandidateRequester interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type EventCommitOwner interface {
	AppendAdmittedEventTxOutcome(context.Context, *sql.Tx, authoractivity.Mutation, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	CommitPublicationTx(context.Context, *sql.Tx, *privateauthoractivity.Mutation, runtimebus.PublicationCommand, *runhandoff.CandidateHandoff) (runtimebus.CommittedPublication, error)
}

type eventCommitTxStore interface {
	appendAdmittedEventTxOutcome(context.Context, *sql.Tx, authoractivity.Mutation, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	RequirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	CommitInitialDeliveryObligationsTx(context.Context, *sql.Tx, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	CommitInitialPipelineScopeTx(context.Context, *sql.Tx, string, runtimepipelineobligation.CommittedScope) error
	CommitInitialPipelineDispositionTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	RecordDeadLetterTx(context.Context, *sql.Tx, authoractivity.Mutation, runtimedeadletters.Record, bool) error
	createReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.Record) error
	claimReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.ClaimCommand) error
	workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner
	genericScheduleTxOwner() GenericScheduleTxOwner
	commitPublicationTx(context.Context, *sql.Tx, *privateauthoractivity.Mutation, runtimebus.PublicationCommand, *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error)
	SettleWorkflowNodeSuccessTx(context.Context, *sql.Tx, authoractivity.Mutation, runtimedelivery.Claim, []string, time.Duration) (runtimedelivery.Snapshot, error)
}

type GenericScheduleTxOwner interface {
	AdmitTx(context.Context, *sql.Tx, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error)
	CancelAdmissionTx(context.Context, *sql.Tx, runtimegenericschedule.AdmissionCommand, string, time.Time) (runtimegenericschedule.CancelResult, error)
}

type runLifecycleCandidateHandoffReservation = runhandoff.CandidateHandoff
type activeRunSourceOwnerFunc func(context.Context, string) (runtimecorrelation.BundleSourceFact, error)

func (fn activeRunSourceOwnerFunc) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return fn(ctx, runID)
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) authoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func withRunLifecycleCandidateHandoff(ctx context.Context, operation func(*runLifecycleCandidateHandoffReservation) error) error {
	return runhandoff.WithCandidateHandoff(ctx, operation)
}

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return runhandoff.ReserveCandidateHandoff(ctx)
}

func requestPostgresCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, _ bool) (runtimerunlifecycle.CandidateRequestResult, error) {
	return storerunlifecycle.RequestPostgresCompletionCandidateTx(ctx, tx, runID, dueAt)
}

func requestSQLiteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, now time.Time, _ bool) (runtimerunlifecycle.CandidateRequestResult, error) {
	return storerunlifecycle.RequestSQLiteCompletionCandidateTx(ctx, tx, runID, dueAt, now)
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

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any { return sqliteNullString(raw) }

type PipelinePostgresOwner struct {
	*storerunlifecycle.RunLifecyclePostgresOwner
	*storedecision.DecisionPostgresOwner
	*storedelivery.DeliveryPostgresOwner
	*storereplycontext.ReplyPostgresOwner

	backend                *postgresbackend.Backend
	requireCurrent         func() error
	candidateRequests      CompletionCandidateRequester
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	workflowEntityQueries  *storeworkflowentityquery.Postgres
	workflowRoutes         *storeworkflowroute.Postgres
	events                 EventCommitOwner
	selectedFork           SelectedForkCommitTxOwner
	genericSchedules       GenericScheduleTxOwner
}

type PipelineSQLiteOwner struct {
	*storerunlifecycle.RunLifecycleSQLiteOwner
	*storedecision.DecisionSQLiteOwner
	*storedelivery.DeliverySQLiteOwner
	*storereplycontext.ReplySQLiteOwner

	backend                *sqlitebackend.Backend
	requireCurrent         func() error
	candidateRequests      CompletionCandidateRequester
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	workflowEntityQueries  *storeworkflowentityquery.SQLite
	workflowRoutes         *storeworkflowroute.SQLite
	events                 EventCommitOwner
	selectedFork           SelectedForkCommitTxOwner
	genericSchedules       GenericScheduleTxOwner
	nowFn                  func() time.Time
	mutationMu             sync.Mutex
	pipelineClaimMu        sync.Mutex
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
	CommitSelectedForkTx(context.Context, *sql.Tx, authoractivity.Mutation, runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error)
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

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, lifecycle *storerunlifecycle.RunLifecyclePostgresOwner, candidates *runhandoff.CandidateCoordinator, decision *storedecision.DecisionPostgresOwner, delivery *storedelivery.DeliveryPostgresOwner, reply *storereplycontext.ReplyPostgresOwner, entityQueries *storeworkflowentityquery.Postgres, routes *storeworkflowroute.Postgres, events EventCommitOwner) (*PipelinePostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil || decision == nil || delivery == nil || reply == nil || entityQueries == nil || routes == nil || events == nil {
		return nil, errors.New("pipeline PostgreSQL owner dependencies are required")
	}
	return &PipelinePostgresOwner{RunLifecyclePostgresOwner: lifecycle, DecisionPostgresOwner: decision, DeliveryPostgresOwner: delivery, ReplyPostgresOwner: reply, backend: backend, requireCurrent: requireCurrent, candidateRequests: lifecycle, runLifecycleCandidates: candidates, workflowEntityQueries: entityQueries, workflowRoutes: routes, events: events}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, lifecycle *storerunlifecycle.RunLifecycleSQLiteOwner, candidates *runhandoff.CandidateCoordinator, decision *storedecision.DecisionSQLiteOwner, delivery *storedelivery.DeliverySQLiteOwner, reply *storereplycontext.ReplySQLiteOwner, entityQueries *storeworkflowentityquery.SQLite, routes *storeworkflowroute.SQLite, events EventCommitOwner, now func() time.Time) (*PipelineSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil || decision == nil || delivery == nil || reply == nil || entityQueries == nil || routes == nil || events == nil {
		return nil, errors.New("pipeline SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &PipelineSQLiteOwner{
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

func (s *PipelineSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func (s *PipelinePostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.CaptureCurrentTransaction(txctx, tx); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *PipelineSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *PipelinePostgresOwner) workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner {
	return s.DecisionPostgresOwner
}

func (s *PipelineSQLiteOwner) workflowDecisionLifecycleOwner() workflowDecisionLifecycleTxOwner {
	return s.DecisionSQLiteOwner
}

func (s *PipelinePostgresOwner) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story authoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.events.AppendAdmittedEventTxOutcome(ctx, tx, story, admitted, settlement)
}

func (s *PipelineSQLiteOwner) appendAdmittedEventTxOutcome(ctx context.Context, tx *sql.Tx, story authoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) (runtimebus.EventAppendOutcome, error) {
	return s.events.AppendAdmittedEventTxOutcome(ctx, tx, story, admitted, settlement)
}

func (s *PipelinePostgresOwner) commitPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, command runtimebus.PublicationCommand, handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
	return s.events.CommitPublicationTx(ctx, tx, story, command, handoff)
}

func (s *PipelineSQLiteOwner) commitPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, command runtimebus.PublicationCommand, handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
	return s.events.CommitPublicationTx(ctx, tx, story, command, handoff)
}

func (s *PipelinePostgresOwner) createReplyContextTx(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.ReplyPostgresOwner.CreateWithinTransaction(ctx, tx, record)
}

func (s *PipelineSQLiteOwner) createReplyContextTx(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.ReplySQLiteOwner.CreateWithinTransaction(ctx, tx, record)
}

func (s *PipelinePostgresOwner) claimReplyContextTx(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.ReplyPostgresOwner.ClaimWithinTransaction(ctx, tx, command)
}

func (s *PipelineSQLiteOwner) claimReplyContextTx(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.ReplySQLiteOwner.ClaimWithinTransaction(ctx, tx, command)
}
