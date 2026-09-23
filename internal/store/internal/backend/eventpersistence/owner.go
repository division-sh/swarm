package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

type selectedForkLineageOwner interface {
	ValidateLifecycleDiagnosticOriginTx(context.Context, *sql.Tx, runtimemanager.AgentLifecycleTransitionResult, bool) (string, error)
	InsertSelectedForkExecutionLineageTx(context.Context, *sql.Tx, runtimerunfork.RunForkSelectedContractExecutionLineage) error
}

type operatorChannelClaimPostgresOwner interface {
	SettleInboundClaimTx(context.Context, *sql.Tx, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
}

type operatorChannelClaimSQLiteOwner interface {
	SettleInboundClaimTx(context.Context, *sql.Tx, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
}

type EventPostgresOwner struct {
	*storeactivityjournal.ActivityPostgresOwner
	*storerunlifecycle.RunLifecyclePostgresOwner
	*storedelivery.DeliveryPostgresOwner
	*storepipeline.PipelinePostgresOwner
	*storereplycontext.ReplyPostgresOwner

	backend               *postgresbackend.Backend
	requireCurrent        func() error
	payloadAdmitterMu     sync.RWMutex
	payloadAdmitter       runtimebus.PayloadAdmitter
	runFork               selectedForkLineageOwner
	apiIdempotency        *storeapiidempotency.PostgresOwner
	operatorChannelClaims operatorChannelClaimPostgresOwner
	durableData           *storedurabledata.Owner
}

type EventSQLiteOwner struct {
	preparedPublishEvent eventrecordsqlite.SingleEventReader
	*storeactivityjournal.ActivitySQLiteOwner
	*storerunlifecycle.RunLifecycleSQLiteOwner
	*storedelivery.DeliverySQLiteOwner
	*storepipeline.PipelineSQLiteOwner
	*storereplycontext.ReplySQLiteOwner

	backend               *sqlitebackend.Backend
	requireCurrent        func() error
	nowFn                 func() time.Time
	payloadAdmitterMu     sync.RWMutex
	payloadAdmitter       runtimebus.PayloadAdmitter
	runFork               selectedForkLineageOwner
	apiIdempotency        *storeapiidempotency.SQLiteOwner
	operatorChannelClaims operatorChannelClaimSQLiteOwner
	durableData           *storedurabledata.Owner
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, activity *storeactivityjournal.ActivityPostgresOwner, lifecycle *storerunlifecycle.RunLifecyclePostgresOwner, delivery *storedelivery.DeliveryPostgresOwner, reply *storereplycontext.ReplyPostgresOwner, apiIdempotency *storeapiidempotency.PostgresOwner, durableData *storedurabledata.Owner) (*EventPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || activity == nil || lifecycle == nil || delivery == nil || reply == nil || apiIdempotency == nil || durableData == nil {
		return nil, errors.New("event PostgreSQL owner dependencies are required")
	}
	return &EventPostgresOwner{ActivityPostgresOwner: activity, RunLifecyclePostgresOwner: lifecycle, DeliveryPostgresOwner: delivery, ReplyPostgresOwner: reply, backend: backend, requireCurrent: requireCurrent, apiIdempotency: apiIdempotency, durableData: durableData}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, activity *storeactivityjournal.ActivitySQLiteOwner, lifecycle *storerunlifecycle.RunLifecycleSQLiteOwner, delivery *storedelivery.DeliverySQLiteOwner, reply *storereplycontext.ReplySQLiteOwner, apiIdempotency *storeapiidempotency.SQLiteOwner, durableData *storedurabledata.Owner, now func() time.Time) (*EventSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || activity == nil || lifecycle == nil || delivery == nil || reply == nil || apiIdempotency == nil || durableData == nil {
		return nil, errors.New("event SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &EventSQLiteOwner{ActivitySQLiteOwner: activity, RunLifecycleSQLiteOwner: lifecycle, DeliverySQLiteOwner: delivery, ReplySQLiteOwner: reply, backend: backend, requireCurrent: requireCurrent, apiIdempotency: apiIdempotency, durableData: durableData, nowFn: now}, nil
}

func (s *EventPostgresOwner) BindPipeline(owner *storepipeline.PipelinePostgresOwner) error {
	if s == nil || owner == nil {
		return errors.New("event PostgreSQL pipeline owner is required")
	}
	if s.PipelinePostgresOwner != nil {
		return errors.New("event PostgreSQL pipeline owner is already bound")
	}
	s.PipelinePostgresOwner = owner
	return nil
}

func (s *EventSQLiteOwner) BindPipeline(owner *storepipeline.PipelineSQLiteOwner) error {
	if s == nil || owner == nil {
		return errors.New("event SQLite pipeline owner is required")
	}
	if s.PipelineSQLiteOwner != nil {
		return errors.New("event SQLite pipeline owner is already bound")
	}
	s.PipelineSQLiteOwner = owner
	return nil
}

func (s *EventPostgresOwner) BindOperatorChannelClaims(owner operatorChannelClaimPostgresOwner) error {
	if s == nil || owner == nil || s.operatorChannelClaims != nil {
		return errors.New("event PostgreSQL operator channel claim owner must be bound exactly once")
	}
	s.operatorChannelClaims = owner
	return nil
}

func (s *EventSQLiteOwner) BindOperatorChannelClaims(owner operatorChannelClaimSQLiteOwner) error {
	if s == nil || owner == nil || s.operatorChannelClaims != nil {
		return errors.New("event SQLite operator channel claim owner must be bound exactly once")
	}
	s.operatorChannelClaims = owner
	return nil
}

func (s *EventPostgresOwner) requireCurrentSchema() error { return s.requireCurrent() }
func (s *EventSQLiteOwner) requireCurrentSchema() error   { return s.requireCurrent() }

func runPostgresEventMutation[T any](ctx context.Context, s *EventPostgresOwner, candidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) (T, error) {
	result := runPostgresEventMutationResult(ctx, s, candidates, write)
	value, acknowledged := result.Value()
	if !acknowledged {
		var zero T
		return zero, result.Err()
	}
	return value, result.Err()
}

func runPostgresEventMutationResult[T any](ctx context.Context, s *EventPostgresOwner, candidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) mutationprotocol.Result[T] {
	if err := s.requireCurrentSchema(); err != nil {
		return mutationprotocol.Reject[T](err)
	}
	var coordinator *storerunhandoff.CandidateCoordinator
	if candidates {
		coordinator = s.RunLifecyclePostgresOwner.CompletionCandidateCoordinator()
	}
	return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, coordinator, write)
}

func runSQLiteEventMutation[T any](ctx context.Context, s *EventSQLiteOwner, label string, candidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) (T, error) {
	result := runSQLiteEventMutationResult(ctx, s, label, candidates, write)
	value, acknowledged := result.Value()
	if !acknowledged {
		var zero T
		return zero, result.Err()
	}
	return value, result.Err()
}

func runSQLiteEventMutationResult[T any](ctx context.Context, s *EventSQLiteOwner, label string, candidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) mutationprotocol.Result[T] {
	if err := s.requireCurrentSchema(); err != nil {
		return mutationprotocol.Reject[T](err)
	}
	var coordinator *storerunhandoff.CandidateCoordinator
	if candidates {
		coordinator = s.RunLifecycleSQLiteOwner.CompletionCandidateCoordinator()
	}
	return mutationprotocol.RunSQLite(ctx, s.backend, label, mutationprotocol.Story, mutationprotocol.Ordinary, nil, coordinator, write)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func loadCommittedPipelineScope(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string, postgres bool) (runtimepipelineobligation.CommittedScope, error) {
	return storepipeline.LoadCommittedScope(ctx, queryer, eventID, postgres)
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

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05"}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, lastErr
}

var postgresDeliveryAdapter = mustDeliveryAdapter(storedelivery.DialectPostgres)

func mustDeliveryAdapter(dialect storedelivery.Dialect) *storedelivery.Adapter {
	adapter, err := storedelivery.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func (s *EventPostgresOwner) BindRunFork(owner selectedForkLineageOwner) error {
	if s == nil || owner == nil {
		return errors.New("event PostgreSQL run-fork owner is required")
	}
	if s.runFork != nil {
		return errors.New("event PostgreSQL run-fork owner is already bound")
	}
	s.runFork = owner
	return nil
}

func (s *EventSQLiteOwner) BindRunFork(owner selectedForkLineageOwner) error {
	if s == nil || owner == nil {
		return errors.New("event SQLite run-fork owner is required")
	}
	if s.runFork != nil {
		return errors.New("event SQLite run-fork owner is already bound")
	}
	s.runFork = owner
	return nil
}

func (s *EventPostgresOwner) createReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.ReplyPostgresOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *EventSQLiteOwner) createReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, record runtimereplycontext.Record) error {
	return s.ReplySQLiteOwner.CreateWithinTransaction(ctx, attempt, record)
}

func (s *EventPostgresOwner) claimReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.ReplyPostgresOwner.ClaimWithinTransaction(ctx, attempt, command)
}

func (s *EventSQLiteOwner) claimReplyContextTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimereplycontext.ClaimCommand) error {
	return s.ReplySQLiteOwner.ClaimWithinTransaction(ctx, attempt, command)
}
