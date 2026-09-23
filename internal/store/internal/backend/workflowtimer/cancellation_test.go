package workflowtimer

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestCancelRunsSQLExactFactsAndReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "workflow-timer.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			isPostgres := backend == "postgres"
			schema := `CREATE TABLE timers (timer_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, timer_name TEXT NOT NULL, fire_at TIMESTAMP NOT NULL, task_type TEXT NOT NULL, status TEXT NOT NULL)`
			if isPostgres {
				schema = `CREATE TABLE timers (timer_id UUID PRIMARY KEY, run_id UUID NOT NULL, timer_name TEXT NOT NULL, fire_at TIMESTAMP NOT NULL, task_type TEXT NOT NULL, status TEXT NOT NULL)`
			}
			if _, err := db.Exec(schema); err != nil {
				t.Fatal(err)
			}
			runID, otherRunID := uuid.NewString(), uuid.NewString()
			first, second, other := uuid.NewString(), uuid.NewString(), uuid.NewString()
			at := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
			for _, row := range []struct{ timerID, owner string }{{first, runID}, {second, runID}, {other, otherRunID}} {
				if _, err := db.Exec(`INSERT INTO timers (timer_id, run_id, timer_name, fire_at, task_type, status) VALUES ($1,$2,'waiting.timeout',$3,'workflow_timer','active')`, row.timerID, row.owner, at); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			facts := runforkrevision.NewEffects()
			refs, err := cancelRunsSQL(ctx, tx, isPostgres, facts, []string{runID, runID})
			if err != nil || len(refs) != 2 {
				t.Fatalf("cancel exact run: refs=%+v err=%v", refs, err)
			}
			seen := map[string]bool{}
			for _, ref := range refs {
				seen[ref.ActivationID] = true
				if ref.RunID != runID || !ref.DueAt.Equal(at) {
					t.Fatalf("wrong cancellation coordinate: %+v", ref)
				}
			}
			if !seen[first] || !seen[second] || seen[other] {
				t.Fatalf("wrong cancellation set: %+v", refs)
			}
			want := runforkrevision.NewEffects()
			for _, id := range []string{first, second} {
				if err := want.AddFact(runID, runforkrevision.FamilyTimers, id); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(facts, want) {
				t.Fatalf("cancellation facts differ from exact timer IDs")
			}
			noop := runforkrevision.NewEffects()
			refs, err = cancelRunsSQL(ctx, tx, isPostgres, noop, []string{runID})
			if err != nil || len(refs) != 0 || !reflect.DeepEqual(noop, runforkrevision.NewEffects()) {
				t.Fatalf("replay changed timers or facts: refs=%+v err=%v", refs, err)
			}
			var status string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id=$1`, other).Scan(&status); err != nil || status != "active" {
				t.Fatalf("other run timer changed: status=%q err=%v", status, err)
			}
		})
	}
}
