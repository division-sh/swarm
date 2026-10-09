package runforkrevision

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

func TestRevisionCountersFollowCanonicalLocksBeforeProjection(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "sqlite"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			const first = "00000000-0000-4000-8000-000000000001"
			const second = "ffffffff-ffff-4fff-8fff-ffffffffffff"
			effects, err := ForRun(second, FamilyEvents)
			if err != nil {
				t.Fatal(err)
			}
			if err := effects.Add(first, FamilyEvents); err != nil {
				t.Fatal(err)
			}
			parentQuery := `SELECT CAST\(run_id AS TEXT\) FROM runs WHERE run_id=\$1`
			if postgres {
				parentQuery += " FOR KEY SHARE"
			}
			for _, runID := range []string{first, second} {
				mock.ExpectQuery(parentQuery).WithArgs(runID).WillReturnRows(sqlmock.NewRows([]string{"run_id"}).AddRow(runID))
			}
			for _, runID := range []string{first, second} {
				insert := mock.ExpectExec(`INSERT INTO run_fork_revision_heads`)
				if postgres {
					insert.WithArgs(runID)
				} else {
					insert.WithArgs(runID, sqlmock.AnyArg())
				}
				insert.WillReturnResult(sqlmock.NewResult(0, 0))
				if postgres {
					mock.ExpectQuery(`SELECT last_revision FROM run_fork_revision_heads WHERE run_id=\$1 FOR UPDATE`).WithArgs(runID).WillReturnRows(sqlmock.NewRows([]string{"last_revision"}).AddRow(1))
				}
			}
			// Opposite contributions retain their order, but cannot lock runs
			// until the pre-existing canonical revision locks have been acquired.
			deltas := []counterprojection.Delta{{RunID: second, Amount: 2}, {RunID: first, Amount: 1}}
			effects.SetPendingEventCounts(deltas)
			for _, delta := range deltas {
				update := mock.ExpectExec(`UPDATE runs SET event_count = event_count \+`)
				if postgres {
					update.WithArgs(delta.Amount, delta.RunID)
				} else {
					update.WithArgs(delta.Amount, delta.RunID, delta.Amount)
				}
				update.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			projectionErr := errors.New("projection after locked counter updates")
			mock.ExpectQuery(`SELECT .*run_fork_fact_revisions`).WillReturnError(projectionErr)
			if postgres {
				_, err = FinalizePostgres(context.Background(), tx, effects)
			} else {
				_, err = FinalizeSQLite(context.Background(), tx, effects)
			}
			if !errors.Is(err, projectionErr) {
				t.Fatalf("finalization order: %v", err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
