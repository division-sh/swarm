package runforkrevision

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestRevisionObservationCountsActualSQLAndFailedFinalizer(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "observation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var slot transactiontest.Slot
	collector, restore, err := slot.Install(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	a := slot.Begin(false, false)
	a.Begun()
	ctx := transactiontest.WithAttempt(context.Background(), a)
	q := revisionQueryOwner(ctx, tx)
	if _, err := q.ExecContext(ctx, `CREATE TABLE runs (run_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	rows, err := q.QueryContext(ctx, `SELECT run_id FROM runs`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if _, err := FinalizeSQLite(ctx, tx, NewEffects()); err != nil {
		t.Fatal(err)
	}
	effects := NewEffects()
	if err := effects.Add("11111111-1111-4111-8111-111111111111", FamilyEventReceipts); err != nil {
		t.Fatal(err)
	}
	_, finalErr := FinalizeSQLite(ctx, tx, effects)
	if finalErr == nil {
		t.Fatal("missing parent accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	a.RollbackAttempted()
	a.Finish(finalErr)
	r := collector.Snapshot().Total.Revision
	if r.ExecCalls != 1 || r.QueryCalls != 1 || r.QueryRowCalls != 2 || r.Finalizations != 2 || r.LockPhases != 1 || r.Duration <= 0 || r.LockDuration <= 0 || r.Duration < r.LockDuration {
		t.Fatalf("actual SQL/finalizer counts = %+v", r)
	}
	if revisionQueryOwner(context.Background(), tx) != tx {
		t.Fatal("disabled observer wrapped SQL")
	}
}
