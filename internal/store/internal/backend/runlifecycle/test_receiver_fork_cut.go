package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// The history owner has validated the original cut and absence of its replacement.
func CorruptReceiverForkRevisionForTest(ctx context.Context, tx *sql.Tx, expected runtimerunlifecycle.Snapshot, missingRevision int64) error {
	if tx == nil || expected.Validate() != nil || expected.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization || missingRevision <= 0 || missingRevision == math.MaxInt64 || missingRevision == expected.Origin.ForkRevision() {
		return fmt.Errorf("receiver cut fault requires exact fork coordinates and a distinct bounded positive revision")
	}
	result, err := tx.ExecContext(ctx, `UPDATE runs SET forked_from_revision=$1
		WHERE run_id=$2 AND origin_kind=$3 AND forked_from_run_id=$4
		AND forked_from_point_kind=$5 AND forked_from_revision=$6
		AND COALESCE(CAST(forked_from_event_id AS TEXT),'')=$7`, missingRevision,
		expected.RunID, string(expected.Origin.Kind()), expected.Origin.SourceRunID(),
		string(expected.Origin.ForkPointKind()), expected.Origin.ForkRevision(), expected.Origin.SourceEventID())
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("receiver cut fault requires unchanged original origin: rows=%d err=%v", count, err)
	}
	return nil
}
