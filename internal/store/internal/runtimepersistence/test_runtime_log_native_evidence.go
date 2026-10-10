package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func ReadRuntimeLogRunSnapshotForTest(ctx context.Context, selected any, runID string) (runlifecycle.Snapshot, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return runlifecycle.Snapshot{}, err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return runlifecycle.Snapshot{}, err
	}
	var snapshot runlifecycle.Snapshot
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			snapshot, err = owner.runLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, false)
		case *SQLiteRuntimeStore:
			snapshot, err = owner.runLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		}
		return err
	})
	if err != nil {
		return runlifecycle.Snapshot{}, err
	}
	return snapshot, nil
}

func RemoveRuntimeLogFixtureSourceArtifactForTest(ctx context.Context, selected any, hash string) (int64, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(hash); err != nil {
		return 0, err
	}
	var removed int64
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM source_artifacts WHERE bundle_hash=$1`, hash)
		if err != nil {
			return err
		}
		removed, err = result.RowsAffected()
		return err
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

// The fixed AFTER INSERT cut aborts the original diagnostic transaction after
// its event append. It never replaces the selected writer or grants SQL access.
func SetRuntimeLogFixtureAppendFaultForTest(ctx context.Context, selected any, runID string, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return err
	}
	_, postgres := selected.(*PostgresStore)
	literal := "'" + strings.ReplaceAll(runID, "'", "''") + "'"
	statements := []string{`DROP TRIGGER runtime_log_exact_append_cut`}
	if postgres {
		statements = []string{`DROP TRIGGER runtime_log_exact_append_cut ON events`, `DROP FUNCTION runtime_log_exact_append_cut()`}
	}
	if enabled {
		statements = []string{`CREATE TRIGGER runtime_log_exact_append_cut AFTER INSERT ON events WHEN NEW.event_name='platform.runtime_log' AND NEW.run_id=` + literal + ` BEGIN SELECT RAISE(ABORT,'runtime_log_exact_append_cut'); END`}
		if postgres {
			statements = []string{
				`CREATE FUNCTION runtime_log_exact_append_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='platform.runtime_log' AND NEW.run_id=` + literal + `::uuid THEN RAISE EXCEPTION 'runtime_log_exact_append_cut'; END IF; RETURN NEW; END $$`,
				`CREATE TRIGGER runtime_log_exact_append_cut AFTER INSERT ON events FOR EACH ROW EXECUTE FUNCTION runtime_log_exact_append_cut()`,
			}
		}
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}
