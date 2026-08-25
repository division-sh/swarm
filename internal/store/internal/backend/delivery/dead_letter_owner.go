package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type DeadLetterPostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent func() error
}

type DeadLetterSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	requireCurrent func() error
}

func NewDeadLetterPostgresOwner(backend *postgresbackend.Backend, requireCurrent func() error) (*DeadLetterPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("dead-letter PostgreSQL backend is required")
	}
	if requireCurrent == nil {
		return nil, errors.New("dead-letter PostgreSQL schema owner is required")
	}
	return &DeadLetterPostgresOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func NewDeadLetterSQLiteOwner(backend *sqlitebackend.Backend, requireCurrent func() error) (*DeadLetterSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("dead-letter SQLite backend is required")
	}
	if requireCurrent == nil {
		return nil, errors.New("dead-letter SQLite schema owner is required")
	}
	return &DeadLetterSQLiteOwner{backend: backend, requireCurrent: requireCurrent}, nil
}

func (s *DeadLetterPostgresOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("dead-letter PostgreSQL owner is required")
	}
	return s.requireCurrent()
}

func (s *DeadLetterSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.requireCurrent == nil {
		return errors.New("dead-letter SQLite owner is required")
	}
	return s.requireCurrent()
}

func (s *DeadLetterPostgresOwner) runPrivateAuthorActivityMutation(
	ctx context.Context,
	effects *privaterunforkrevision.Effects,
	operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
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
		if _, err := privaterunforkrevision.FinalizePostgres(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *DeadLetterSQLiteOwner) runPrivateAuthorActivityMutation(
	ctx context.Context,
	label string,
	effects *privaterunforkrevision.Effects,
	operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error,
) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
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

func requireActiveRunForEvent(ctx context.Context, tx *sql.Tx, eventID string, postgres bool) error {
	eventID = strings.TrimSpace(eventID)
	if tx == nil || eventID == "" {
		return errors.New("dead-letter active event lookup requires transaction and event_id")
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, eventID).Scan(&runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("require active event run: event %s not found", eventID)
		}
		return fmt.Errorf("require active event run: %w", err)
	}
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if postgres {
		return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
	}
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
}

func sqliteNullUUID(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func rowsAffected(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}
	return rows > 0, nil
}
