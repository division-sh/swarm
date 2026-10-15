package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Historical clocks are fixture facts, not a new live scope decision. The
// existing owner rejects contradictory scope before the exact clock refresh.
func StampHistoricalReplayScopeFixtureTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventID string, scope pipelineobligation.CommittedScope, at time.Time) error {
	if at.IsZero() {
		return fmt.Errorf("historical replay scope requires its original timestamp")
	}
	if err := persistCommittedPipelineScopeTx(ctx, tx, eventID, scope, postgres, at); err != nil {
		return err
	}
	query := `UPDATE committed_replay_scopes SET created_at=?, updated_at=? WHERE event_id=? AND run_id=? AND scope=?`
	if postgres {
		query = `UPDATE committed_replay_scopes SET created_at=$1, updated_at=$2 WHERE event_id=$3::uuid AND run_id=$4::uuid AND scope=$5`
	}
	result, err := tx.ExecContext(ctx, query, at, at, eventID, runID, string(scope))
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("historical replay scope refreshed %d rows, want the exact event/run/scope", changed)
	}
	return nil
}

type HistoricalReplayScopeStorage struct {
	RunID, Scope         string
	CreatedAt, UpdatedAt time.Time
}

func ReadHistoricalReplayScopeStorageTx(ctx context.Context, tx *sql.Tx, eventID string) (HistoricalReplayScopeStorage, error) {
	var out HistoricalReplayScopeStorage
	var created, updated any
	err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT), scope, created_at, updated_at FROM committed_replay_scopes WHERE event_id=$1`, eventID).Scan(&out.RunID, &out.Scope, &created, &updated)
	if err != nil {
		return HistoricalReplayScopeStorage{}, err
	}
	var present bool
	out.CreatedAt, present, err = sqliteTimeValue(created)
	if err != nil || !present {
		return HistoricalReplayScopeStorage{}, fmt.Errorf("historical replay scope created clock missing or invalid: %v", err)
	}
	out.UpdatedAt, present, err = sqliteTimeValue(updated)
	if err != nil || !present {
		return HistoricalReplayScopeStorage{}, fmt.Errorf("historical replay scope updated clock missing or invalid: %v", err)
	}
	return out, nil
}

func CountCopiedSourceReplayScopeStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM committed_replay_scopes WHERE run_id=$1 AND event_id=$2`, runID, sourceEventID).Scan(&count)
	return count, err
}

func CountForkLocalReplayScopeStorageTx(ctx context.Context, tx *sql.Tx, runID, sourceEventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM committed_replay_scopes WHERE run_id=$1 AND event_id<>$2`, runID, sourceEventID).Scan(&count)
	return count, err
}
