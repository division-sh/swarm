package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type completionCandidateOwner interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type LLMPostgresOwner struct {
	backend                *postgresbackend.Backend
	requireCurrent         func() error
	lifecycle              completionCandidateOwner
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	sessionLockTTL         time.Duration
}

type LLMSQLiteOwner struct {
	backend                *sqlitebackend.Backend
	requireCurrent         func() error
	nowFn                  func() time.Time
	lifecycle              completionCandidateOwner
	runLifecycleCandidates *runhandoff.CandidateCoordinator
	sessionMu              sync.Mutex
	sessionLockTTL         time.Duration
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, lifecycle completionCandidateOwner, candidates *runhandoff.CandidateCoordinator) (*LLMPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil {
		return nil, errors.New("LLM PostgreSQL owner dependencies are required")
	}
	return &LLMPostgresOwner{backend: backend, requireCurrent: requireCurrent, lifecycle: lifecycle, runLifecycleCandidates: candidates, sessionLockTTL: 120 * time.Second}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, lifecycle completionCandidateOwner, candidates *runhandoff.CandidateCoordinator, now func() time.Time) (*LLMSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || lifecycle == nil || candidates == nil {
		return nil, errors.New("LLM SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &LLMSQLiteOwner{backend: backend, requireCurrent: requireCurrent, lifecycle: lifecycle, runLifecycleCandidates: candidates, nowFn: now, sessionLockTTL: 120 * time.Second}, nil
}

func (s *LLMPostgresOwner) requireCurrentSchema() error { return s.requireCurrent() }
func (s *LLMSQLiteOwner) requireCurrentSchema() error   { return s.requireCurrent() }
func (s *LLMSQLiteOwner) now() time.Time                { return s.nowFn().UTC() }

func (s *LLMPostgresOwner) runPostgresRuntimeMutation(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, fn)
}

func (s *LLMPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.CaptureCurrentTransaction(txctx, tx); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *LLMSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, fn)
}

func (s *LLMSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := fn(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any { return sqliteNullString(raw) }

func sqliteJSONRawMessage(raw any) json.RawMessage {
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
		return time.Time{}, false, errors.New("unsupported SQLite time value")
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

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *LLMPostgresOwner) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.sessionLockTTL = ttl
}

func (s *LLMSQLiteOwner) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.sessionLockTTL = ttl
}

func commitPostgresRunForkRevisionTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := privaterunforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}
