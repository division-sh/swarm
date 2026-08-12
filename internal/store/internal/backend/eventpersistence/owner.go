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

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storereplycontext "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

type selectedForkLineageOwner interface {
	InsertSelectedForkExecutionLineageTx(context.Context, *sql.Tx, runtimerunfork.RunForkSelectedContractExecutionLineage) error
}

type EventPostgresOwner struct {
	*storeactivityjournal.ActivityPostgresOwner
	*storerunlifecycle.RunLifecyclePostgresOwner
	*storedelivery.DeliveryPostgresOwner
	*storepipeline.PipelinePostgresOwner
	*storereplycontext.ReplyPostgresOwner

	backend        *postgresbackend.Backend
	requireCurrent func() error
	validatorMu    sync.RWMutex
	validator      func(context.Context, string, []byte) error
	runFork        selectedForkLineageOwner
	apiIdempotency *storeapiidempotency.PostgresOwner
}

type EventSQLiteOwner struct {
	*storeactivityjournal.ActivitySQLiteOwner
	*storerunlifecycle.RunLifecycleSQLiteOwner
	*storedelivery.DeliverySQLiteOwner
	*storepipeline.PipelineSQLiteOwner
	*storereplycontext.ReplySQLiteOwner

	backend        *sqlitebackend.Backend
	requireCurrent func() error
	nowFn          func() time.Time
	validatorMu    sync.RWMutex
	validator      func(context.Context, string, []byte) error
	runFork        selectedForkLineageOwner
	apiIdempotency *storeapiidempotency.SQLiteOwner
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, activity *storeactivityjournal.ActivityPostgresOwner, lifecycle *storerunlifecycle.RunLifecyclePostgresOwner, delivery *storedelivery.DeliveryPostgresOwner, reply *storereplycontext.ReplyPostgresOwner, apiIdempotency *storeapiidempotency.PostgresOwner) (*EventPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || activity == nil || lifecycle == nil || delivery == nil || reply == nil || apiIdempotency == nil {
		return nil, errors.New("event PostgreSQL owner dependencies are required")
	}
	return &EventPostgresOwner{ActivityPostgresOwner: activity, RunLifecyclePostgresOwner: lifecycle, DeliveryPostgresOwner: delivery, ReplyPostgresOwner: reply, backend: backend, requireCurrent: requireCurrent, apiIdempotency: apiIdempotency}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, activity *storeactivityjournal.ActivitySQLiteOwner, lifecycle *storerunlifecycle.RunLifecycleSQLiteOwner, delivery *storedelivery.DeliverySQLiteOwner, reply *storereplycontext.ReplySQLiteOwner, apiIdempotency *storeapiidempotency.SQLiteOwner, now func() time.Time) (*EventSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || activity == nil || lifecycle == nil || delivery == nil || reply == nil || apiIdempotency == nil {
		return nil, errors.New("event SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &EventSQLiteOwner{ActivitySQLiteOwner: activity, RunLifecycleSQLiteOwner: lifecycle, DeliverySQLiteOwner: delivery, ReplySQLiteOwner: reply, backend: backend, requireCurrent: requireCurrent, apiIdempotency: apiIdempotency, nowFn: now}, nil
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

func (s *EventPostgresOwner) requireCurrentSchema() error { return s.requireCurrent() }
func (s *EventSQLiteOwner) requireCurrentSchema() error   { return s.requireCurrent() }
func (s *EventSQLiteOwner) now() time.Time                { return s.nowFn().UTC() }

func (s *EventPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
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

func (s *EventSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func (s *EventSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
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

type runLifecycleCandidateHandoffReservation = storerunhandoff.CandidateHandoff

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return storerunhandoff.ReserveCandidateHandoff(ctx)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
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

func replayAdmissionFailure(reasonCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "persisted_replay_run_identity_invalid", "event-store", "load_replay", map[string]any{"reason_code": strings.TrimSpace(reasonCode)}), "event-store", "load_replay")
	return &failure
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
var sqliteDeliveryAdapter = mustDeliveryAdapter(storedelivery.DialectSQLite)

func mustDeliveryAdapter(dialect storedelivery.Dialect) *storedelivery.Adapter {
	adapter, err := storedelivery.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

func withRunLifecycleCandidateHandoffResult[T any](ctx context.Context, operation func(*runLifecycleCandidateHandoffReservation) (T, error)) (T, error) {
	return storerunhandoff.WithCandidateHandoffResult(ctx, operation)
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

func (s *EventPostgresOwner) createReplyContextTx(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.ReplyPostgresOwner.CreateWithinTransaction(ctx, tx, record)
}

func (s *EventSQLiteOwner) createReplyContextTx(ctx context.Context, tx *sql.Tx, record runtimereplycontext.Record) error {
	return s.ReplySQLiteOwner.CreateWithinTransaction(ctx, tx, record)
}

func (s *EventPostgresOwner) claimReplyContextTx(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.ReplyPostgresOwner.ClaimWithinTransaction(ctx, tx, command)
}

func (s *EventSQLiteOwner) claimReplyContextTx(ctx context.Context, tx *sql.Tx, command runtimereplycontext.ClaimCommand) error {
	return s.ReplySQLiteOwner.ClaimWithinTransaction(ctx, tx, command)
}
