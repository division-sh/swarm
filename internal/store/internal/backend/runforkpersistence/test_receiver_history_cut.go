package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

// Corrupt only the persisted cut reference, preserving all source history.
func CorruptReceiverConstructionHistoryCutForTest(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance, missingRevision int64) error {
	_, historical, err := readHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, header)
	if err != nil {
		return err
	}
	if !historical {
		return fmt.Errorf("history-cut fault requires an exact captured receiver")
	}
	if missingRevision <= 0 || missingRevision == math.MaxInt64 || missingRevision == run.Origin.ForkRevision() {
		return fmt.Errorf("history-cut fault requires a distinct bounded positive revision")
	}
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_fork_revisions WHERE run_id=$1 AND revision=$2)`, run.Origin.SourceRunID(), missingRevision).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("history-cut fault refuses an existing source revision")
	}
	return storerunlifecycle.CorruptReceiverForkRevisionForTest(ctx, tx, run, missingRevision)
}
