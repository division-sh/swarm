package store

import (
	"context"
	"database/sql"
	"testing"

	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
)

func captureRunForkTestRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin run fork test revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	revision, err := runforkrevision.Capture(context.Background(), tx, runID, families...)
	if err != nil {
		t.Fatalf("capture run fork test revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run fork test revision: %v", err)
	}
	return revision
}
