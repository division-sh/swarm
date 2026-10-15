package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/google/uuid"
)

// CaptureRunForkSnapshotForTest captures all canonical historical families on
// the selected coordinator without lending revision or transaction authority.
func CaptureRunForkSnapshotForTest(ctx context.Context, selected any, runID string) error {
	_, err := CaptureSelectedRunForkRevisionForTest(ctx, selected, runID)
	return err
}

// The receipt is the exact finalizer result from the acknowledged transaction,
// not a subsequent head query that could observe another writer's revision.
func CaptureSelectedRunForkRevisionForTest(ctx context.Context, selected any, runID string, families ...runforkrevision.Family) (int64, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return 0, fmt.Errorf("run-fork snapshot fixture requires a run identity: %w", err)
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return 0, err
	}
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	var revision int64
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if dialect == authoractivityfixture.DialectPostgres {
				revision, err = runforkrevisionfixture.Capture(ctx, tx, runID, families...)
			} else {
				revision, err = runforkrevisionfixture.CaptureSQLite(ctx, tx, runID, families...)
			}
			return err
		})
	})
	if err != nil {
		return 0, err
	}
	return revision, nil
}
