package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	deliverystore "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storerunhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type RunLifecyclePostgresOwner struct {
	backend                *postgresbackend.Backend
	requireCurrent         func() error
	runLifecycleCandidates *storerunhandoff.CandidateCoordinator
	delivery               *deliverystore.DeliveryPostgresOwner
	pipeline               pipelineTerminalizer
	decisionCards          decisionCardTerminalizer
}

type RunLifecycleSQLiteOwner struct {
	backend                *sqlitebackend.Backend
	requireCurrent         func() error
	runLifecycleCandidates *storerunhandoff.CandidateCoordinator
	delivery               *deliverystore.DeliverySQLiteOwner
	pipeline               pipelineTerminalizer
	decisionCards          decisionCardTerminalizer
	nowFn                  func() time.Time
}

type pipelineTerminalizer interface {
	TerminalizeRunTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Disposition, time.Time) (int, error)
	SummarizeRunTx(context.Context, *sql.Tx, string) (runtimepipelineobligation.RunSummary, error)
}

type decisionCardTerminalizer interface {
	SupersedeRunTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, string, string, time.Time, bool) error
}

func (s *RunLifecyclePostgresOwner) BindPipeline(owner pipelineTerminalizer) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle PostgreSQL pipeline owner is required")
	}
	if s.pipeline != nil {
		return errors.New("run lifecycle PostgreSQL pipeline owner is already bound")
	}
	s.pipeline = owner
	return nil
}

func (s *RunLifecycleSQLiteOwner) BindPipeline(owner pipelineTerminalizer) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle SQLite pipeline owner is required")
	}
	if s.pipeline != nil {
		return errors.New("run lifecycle SQLite pipeline owner is already bound")
	}
	s.pipeline = owner
	return nil
}

func (s *RunLifecyclePostgresOwner) BindDecisionCards(owner decisionCardTerminalizer) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle PostgreSQL decision-card owner is required")
	}
	if s.decisionCards != nil {
		return errors.New("run lifecycle PostgreSQL decision-card owner is already bound")
	}
	s.decisionCards = owner
	return nil
}

func (s *RunLifecycleSQLiteOwner) BindDecisionCards(owner decisionCardTerminalizer) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle SQLite decision-card owner is required")
	}
	if s.decisionCards != nil {
		return errors.New("run lifecycle SQLite decision-card owner is already bound")
	}
	s.decisionCards = owner
	return nil
}

func (s *RunLifecyclePostgresOwner) BindDelivery(owner *deliverystore.DeliveryPostgresOwner) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle PostgreSQL delivery owner is required")
	}
	if s.delivery != nil {
		return errors.New("run lifecycle PostgreSQL delivery owner is already bound")
	}
	s.delivery = owner
	return nil
}

func (s *RunLifecycleSQLiteOwner) BindDelivery(owner *deliverystore.DeliverySQLiteOwner) error {
	if s == nil || owner == nil {
		return errors.New("run lifecycle SQLite delivery owner is required")
	}
	if s.delivery != nil {
		return errors.New("run lifecycle SQLite delivery owner is already bound")
	}
	s.delivery = owner
	return nil
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, candidates *storerunhandoff.CandidateCoordinator) (*RunLifecyclePostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("run lifecycle PostgreSQL backend is required")
	}
	if requireCurrent == nil || candidates == nil {
		return nil, errors.New("run lifecycle PostgreSQL dependencies are required")
	}
	return &RunLifecyclePostgresOwner{backend: backend, requireCurrent: requireCurrent, runLifecycleCandidates: candidates}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, candidates *storerunhandoff.CandidateCoordinator, now func() time.Time) (*RunLifecycleSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("run lifecycle SQLite backend is required")
	}
	if requireCurrent == nil || candidates == nil {
		return nil, errors.New("run lifecycle SQLite dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &RunLifecycleSQLiteOwner{backend: backend, requireCurrent: requireCurrent, runLifecycleCandidates: candidates, nowFn: now}, nil
}

func (s *RunLifecyclePostgresOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("run lifecycle PostgreSQL owner is required")
	}
	return s.requireCurrent()
}

func (s *RunLifecycleSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("run lifecycle SQLite owner is required")
	}
	return s.requireCurrent()
}

func (s *RunLifecycleSQLiteOwner) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}

func (s *RunLifecyclePostgresOwner) runPostgresRuntimeMutation(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, operation)
}

func (s *RunLifecycleSQLiteOwner) runRuntimeMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, operation)
}

func (s *RunLifecyclePostgresOwner) runPrivateAuthorActivityMutation(
	ctx context.Context,
	effects *privaterunforkrevision.Effects,
	operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
	return s.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizePostgres(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *RunLifecycleSQLiteOwner) runPrivateAuthorActivityMutation(
	ctx context.Context,
	label string,
	effects *privaterunforkrevision.Effects,
	operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
	return s.runRuntimeMutation(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func runTerminationRevisionEffects(runIDs ...string) (*privaterunforkrevision.Effects, error) {
	effects := privaterunforkrevision.NewEffects()
	for _, runID := range runIDs {
		if err := effects.Add(runID,
			privaterunforkrevision.FamilyEventDeliveries,
			privaterunforkrevision.FamilyEventReceipts,
			privaterunforkrevision.FamilyAgentSessions,
			privaterunforkrevision.FamilyTimers,
		); err != nil {
			return nil, err
		}
	}
	return effects, nil
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) runtimeauthoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return raw
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
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid SQLite time %q", raw)
}
