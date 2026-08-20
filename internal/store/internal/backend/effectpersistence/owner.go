package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storellm "github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type completionCandidateOwner interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *storerunhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type schemaQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type providerDrainDeliveryOwner interface {
	ValidateProviderOriginTx(context.Context, *sql.Tx, runtimedelivery.Claim) error
	SettleProviderOriginSuccessTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, runtimedelivery.Claim, []string, time.Duration) error
	SettleProviderOriginFailureTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, runtimedelivery.Claim, runtimedelivery.Settlement) error
}

type EffectPostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent func() error
	lifecycle      completionCandidateOwner
	llm            *storellm.LLMPostgresOwner
	delivery       providerDrainDeliveryOwner
}

type EffectSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	requireCurrent func() error
	lifecycle      completionCandidateOwner
	llm            *storellm.LLMSQLiteOwner
	delivery       providerDrainDeliveryOwner
}

func (s *EffectPostgresOwner) BindProviderDrainDelivery(owner providerDrainDeliveryOwner) error {
	if s == nil || owner == nil {
		return errors.New("provider-drain PostgreSQL delivery owner is required")
	}
	if s.delivery != nil {
		return errors.New("provider-drain PostgreSQL delivery owner is already bound")
	}
	s.delivery = owner
	return nil
}

func (s *EffectSQLiteOwner) BindProviderDrainDelivery(owner providerDrainDeliveryOwner) error {
	if s == nil || owner == nil {
		return errors.New("provider-drain SQLite delivery owner is required")
	}
	if s.delivery != nil {
		return errors.New("provider-drain SQLite delivery owner is already bound")
	}
	s.delivery = owner
	return nil
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, lifecycle completionCandidateOwner, llm *storellm.LLMPostgresOwner) (*EffectPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || llm == nil {
		return nil, errors.New("external-effect PostgreSQL owner dependencies are required")
	}
	return &EffectPostgresOwner{backend: backend, requireCurrent: requireCurrent, lifecycle: lifecycle, llm: llm}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, lifecycle completionCandidateOwner, llm *storellm.LLMSQLiteOwner) (*EffectSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || llm == nil {
		return nil, errors.New("external-effect SQLite owner dependencies are required")
	}
	return &EffectSQLiteOwner{backend: backend, requireCurrent: requireCurrent, lifecycle: lifecycle, llm: llm}, nil
}

func (s *EffectPostgresOwner) runRuntimeMutation(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, operation)
}

func (s *EffectSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func (s *EffectPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
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

func (s *EffectSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
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

func (s *EffectPostgresOwner) requestCompletionCandidate(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, handoff *storerunhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error) {
	return s.lifecycle.RequestCompletionCandidateTx(ctx, tx, runID, dueAt, handoff)
}

func (s *EffectSQLiteOwner) requestCompletionCandidate(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, handoff *storerunhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error) {
	return s.lifecycle.RequestCompletionCandidateTx(ctx, tx, runID, dueAt, handoff)
}

const runLifecycleActiveStateSQLValues = storerunstate.ActiveStateSQLValues
const sqliteCurrentLeaseSQL = "datetime(substr(replace(CAST(lease_expires_at AS TEXT),'T',' '),1,19))>CURRENT_TIMESTAMP"
const conversationForkChatExecutionLease = 2 * time.Minute

var agentIdentityFields = storeagent.IdentityFields

type runLifecycleCandidateHandoffReservation = storerunhandoff.CandidateHandoff

func withRunLifecycleCandidateHandoff(ctx context.Context, operation func(*runLifecycleCandidateHandoffReservation) error) error {
	return storerunhandoff.WithCandidateHandoff(ctx, operation)
}

func nullUUIDString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullString(raw string) any { return nullUUIDString(raw) }

func coalesce(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
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

type conversationForkTimeValue struct {
	Time  time.Time
	Valid bool
}

func (v *conversationForkTimeValue) Scan(src any) error {
	parsed, valid, err := sqliteTimeValue(src)
	if err != nil {
		return err
	}
	v.Time = parsed
	v.Valid = valid
	return nil
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
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
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

func firstNonNilError(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
