package runtimepersistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// The receipt is the exact finalizer result from the acknowledged transaction,
// not a subsequent head query that could observe another writer's revision.
func CaptureSelectedRunForkRevisionForTest(ctx context.Context, selected any, runID string, families ...runforkrevision.Family) (int64, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return 0, fmt.Errorf("run-fork snapshot fixture requires a run identity: %w", err)
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.runForkPostgresOwner.CaptureSelectedRunForkRevisionForTest(ctx, runID, families...)
	case *SQLiteRuntimeStore:
		return owner.runForkSQLiteOwner.CaptureSelectedRunForkRevisionForTest(ctx, runID, families...)
	default:
		return 0, fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

// CaptureRunForkSnapshotForTest captures all canonical historical families on
// the selected coordinator without lending revision or transaction authority.
func CaptureRunForkSnapshotForTest(ctx context.Context, selected any, runID string) error {
	_, err := CaptureSelectedRunForkRevisionForTest(ctx, selected, runID)
	return err
}

// The receipt is the exact finalizer result from the acknowledged transaction,
// not a subsequent head query that could observe another writer's revision.
