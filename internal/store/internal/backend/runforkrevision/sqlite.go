package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type sqliteAdapter struct{ tx *sql.Tx }

func FinalizeSQLite(ctx context.Context, tx *sql.Tx, effects *Effects) (map[string]Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision finalization requires an existing SQLite transaction")
	}
	return finalize(ctx, &sqliteAdapter{tx: tx}, effects)
}

func ValidateCompleteSQLite(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return fmt.Errorf("run fork revision validation requires an existing SQLite transaction")
	}
	return validateComplete(ctx, &sqliteAdapter{tx: tx}, runID)
}

func (a *sqliteAdapter) projectionQueryer() queryer { return a.tx }

func (a *sqliteAdapter) lockParents(ctx context.Context, runIDs []string) error {
	for _, runID := range runIDs {
		var locked string
		err := a.tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM runs WHERE run_id=$1`, runID).Scan(&locked)
		if err == sql.ErrNoRows {
			return fmt.Errorf("lock run fork revision parent run %s: run does not exist", runID)
		}
		if err != nil {
			return fmt.Errorf("lock run fork revision parent run %s: %w", runID, err)
		}
	}
	return nil
}

func (a *sqliteAdapter) lockRevisionState(ctx context.Context, runIDs []string) error {
	now := time.Now().UTC()
	for _, runID := range runIDs {
		if _, err := a.tx.ExecContext(ctx, `INSERT INTO run_fork_revision_heads (run_id,updated_at) VALUES ($1,$2) ON CONFLICT (run_id) DO NOTHING`, runID, now); err != nil {
			return fmt.Errorf("ensure run fork revision head for %s: %w", runID, err)
		}
	}
	return nil
}

func (a *sqliteAdapter) latestRevision(ctx context.Context, runID string) (int64, bool, error) {
	var revision int64
	err := a.tx.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&revision)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read latest run fork revision: %w", err)
	}
	return revision, true, nil
}

func (a *sqliteAdapter) latestFacts(ctx context.Context, runID string) (ledgerFactsByFamily, error) {
	rows, err := a.tx.QueryContext(ctx, `
		SELECT r.family, r.fact_key, r.fact, r.present
		FROM run_fork_fact_revisions r
		WHERE r.run_id=$1
		  AND NOT EXISTS (
			SELECT 1 FROM run_fork_fact_revisions newer
			WHERE newer.run_id=r.run_id AND newer.family=r.family AND newer.fact_key=r.fact_key AND newer.revision>r.revision
		  )
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	facts := ledgerFactsByFamily{}
	for rows.Next() {
		var family Family
		var key string
		var fact []byte
		var present bool
		if err := rows.Scan(&family, &key, &fact, &present); err != nil {
			return nil, err
		}
		if !ValidFamily(family) {
			return nil, fmt.Errorf("decode unsupported run fork revision fact family %q", family)
		}
		if facts[family] == nil {
			facts[family] = map[string]ledgerFact{}
		}
		facts[family][key] = ledgerFact{fact: append([]byte(nil), fact...), present: present}
	}
	return facts, rows.Err()
}

func (a *sqliteAdapter) allocate(ctx context.Context, runID string) (int64, error) {
	now := time.Now().UTC()
	if _, err := a.tx.ExecContext(ctx, `UPDATE run_fork_revision_heads SET last_revision=last_revision+1, updated_at=$2 WHERE run_id=$1`, runID, now); err != nil {
		return 0, fmt.Errorf("allocate run fork revision: %w", err)
	}
	var revision int64
	if err := a.tx.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read allocated run fork revision: %w", err)
	}
	if _, err := a.tx.ExecContext(ctx, `INSERT INTO run_fork_revisions (run_id,revision,recorded_at) VALUES ($1,$2,$3)`, runID, revision, now); err != nil {
		return 0, fmt.Errorf("record run fork revision: %w", err)
	}
	return revision, nil
}

func (a *sqliteAdapter) insertFact(ctx context.Context, runID string, revision int64, family Family, key string, fact []byte, present bool) error {
	if _, err := a.tx.ExecContext(ctx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,$2,$3,$4,$5,$6)`, runID, revision, family, key, string(fact), present); err != nil {
		return fmt.Errorf("record run fork %s fact %s at revision %d: %w", family, key, revision, err)
	}
	return nil
}
