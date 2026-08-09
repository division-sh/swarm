package gateroute

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func RequirePostgres(ctx context.Context, db QueryRower, runID string) error {
	return require(ctx, db, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID)
}

func RequireSQLite(ctx context.Context, db QueryRower, runID string) error {
	return require(ctx, db, `SELECT status FROM runs WHERE run_id = ?`, runID)
}

func require(ctx context.Context, db QueryRower, query, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("gate route run id is required")
	}
	if db == nil {
		return fmt.Errorf("gate route admission reader is required")
	}
	var status string
	if err := db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("gate route run %s is unavailable", runID)
		}
		return fmt.Errorf("load gate route run %s: %w", runID, err)
	}
	state, err := runtimerunlifecycle.ParseState(status)
	if err != nil || state != runtimerunlifecycle.StateRunning {
		return fmt.Errorf("gate route run %s is not routable in status %s", runID, strings.TrimSpace(status))
	}
	return nil
}
