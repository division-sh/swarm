package runforkrevision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

type postgresAdapter struct {
	tx       revisionSQL
	nativeTx *sql.Tx
}

func FinalizePostgres(ctx context.Context, tx *sql.Tx, effects *Effects) (map[string]Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision finalization requires an existing PostgreSQL transaction")
	}
	return finalize(ctx, &postgresAdapter{tx: revisionQueryOwner(ctx, tx), nativeTx: tx}, effects)
}

func (a *postgresAdapter) applyEventCounts(ctx context.Context, deltas []counterprojection.Delta) error {
	return counterprojection.ApplyAll(ctx, a.nativeTx, authoractivity.DialectPostgres, deltas)
}

func ValidateCompletePostgres(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return fmt.Errorf("run fork revision validation requires an existing PostgreSQL transaction")
	}
	return validateComplete(ctx, &postgresAdapter{tx: tx}, runID)
}

func (a *postgresAdapter) projectionQueryer() queryer { return a.tx }

func (a *postgresAdapter) lockParents(ctx context.Context, runIDs []string) error {
	for _, runID := range runIDs {
		var locked string
		err := a.tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM runs WHERE run_id=$1 FOR KEY SHARE`, runID).Scan(&locked)
		if err == sql.ErrNoRows {
			return fmt.Errorf("lock run fork revision parent run %s: run does not exist", runID)
		}
		if err != nil {
			return fmt.Errorf("lock run fork revision parent run %s: %w", runID, err)
		}
	}
	return nil
}

func (a *postgresAdapter) lockRevisionState(ctx context.Context, runIDs []string) error {
	for _, runID := range runIDs {
		if _, err := a.tx.ExecContext(ctx, `INSERT INTO run_fork_revision_heads (run_id) VALUES ($1) ON CONFLICT (run_id) DO NOTHING`, runID); err != nil {
			return fmt.Errorf("ensure run fork revision head for %s: %w", runID, err)
		}
		var revision int64
		if err := a.tx.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1 FOR UPDATE`, runID).Scan(&revision); err != nil {
			return fmt.Errorf("lock run fork revision state for %s: %w", runID, err)
		}
	}
	return nil
}

func (a *postgresAdapter) latestRevision(ctx context.Context, runID string) (int64, bool, error) {
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

func (a *postgresAdapter) latestFacts(ctx context.Context, runID string, families []Family) (ledgerFactsByFamily, error) {
	return readLatestFacts(ctx, a.tx, runID, families)
}

func (a *postgresAdapter) allocate(ctx context.Context, runID string) (int64, error) {
	var revision int64
	if err := a.tx.QueryRowContext(ctx, `UPDATE run_fork_revision_heads SET last_revision=last_revision+1, updated_at=NOW() WHERE run_id=$1 RETURNING last_revision`, runID).Scan(&revision); err != nil {
		return 0, fmt.Errorf("allocate run fork revision: %w", err)
	}
	if _, err := a.tx.ExecContext(ctx, `INSERT INTO run_fork_revisions (run_id, revision) VALUES ($1,$2)`, runID, revision); err != nil {
		return 0, fmt.Errorf("record run fork revision: %w", err)
	}
	return revision, nil
}

func (a *postgresAdapter) insertFacts(ctx context.Context, runID string, revision int64, facts []revisionFactInsert) error {
	return insertRevisionFacts(ctx, a.tx, true, runID, revision, facts)
}

// Physical cardinality is not historical payload admission. This portable read
// uses the caller's original selected transaction on either native backend.
func CountNotifyFanOutRevisionStorageForTest(ctx context.Context, tx *sql.Tx, runID string) (revisions, facts int64, err error) {
	err = tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1)`, runID).Scan(&revisions, &facts)
	if err != nil {
		return 0, 0, err
	}
	return revisions, facts, nil
}

// Journal component evidence shares the original native read snapshot; it
// does not admit historical ledger payloads or create another revision owner.
func CountActivityJournalRevisionsForTest(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func CountRunFactRevisionRowsForTest(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLatestRunRevisionStorageForTestTx(ctx context.Context, tx *sql.Tx, runID string) (int64, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&revision)
	return revision, err
}

func CountServingLifetimeHistoryStorageTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'`, runID).Scan(&count)
	return count, err
}

type FanOutObligationHistoryStorage struct {
	Key  string
	Fact []byte
}

type RunRevisionFactStorage struct {
	Family, Key string
	Fact        []byte
	Present     bool
}

func ReadLatestFanOutObligationFactStorageTx(ctx context.Context, tx *sql.Tx, runID string) (FanOutObligationHistoryStorage, error) {
	var out FanOutObligationHistoryStorage
	err := tx.QueryRowContext(ctx, `SELECT fact_key,fact
		FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'
		ORDER BY revision DESC LIMIT 1`, runID).Scan(&out.Key, &out.Fact)
	return out, err
}

func ReadRunRevisionFactStorageTx(ctx context.Context, tx *sql.Tx, runID string, revision int64) (RunRevisionFactStorage, error) {
	var out RunRevisionFactStorage
	err := tx.QueryRowContext(ctx, `SELECT family,fact_key,fact,present
		FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2`, runID, revision).Scan(&out.Family, &out.Key, &out.Fact, &out.Present)
	return out, err
}

// Preserve the complete physical revision witness. These bytes are not
// historical payload admission or authority to restore executable work.
func ReadFanOutObligationHistoryStorageForTestTx(ctx context.Context, tx *sql.Tx, runID string) ([]FanOutObligationHistoryStorage, error) {
	return readFanOutObligationHistoryStorageTx(ctx, tx, runID, `SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`)
}

func ReadSupplementalFanOutObligationHistoryTx(ctx context.Context, tx *sql.Tx, runID string) ([]FanOutObligationHistoryStorage, error) {
	return readFanOutObligationHistoryStorageTx(ctx, tx, runID, `SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY fact_key,revision`)
}

func readFanOutObligationHistoryStorageTx(ctx context.Context, tx *sql.Tx, runID, query string) ([]FanOutObligationHistoryStorage, error) {
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FanOutObligationHistoryStorage
	for rows.Next() {
		var row FanOutObligationHistoryStorage
		if err := rows.Scan(&row.Key, &row.Fact); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Fact = append([]byte(nil), row.Fact...)
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
