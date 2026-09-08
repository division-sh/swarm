package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func historicalSnapshotDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	jsonType := "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		jsonType = "JSONB"
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "history.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE run_fork_fact_revisions (
		run_id TEXT NOT NULL, family TEXT NOT NULL, fact_key TEXT NOT NULL,
		revision BIGINT NOT NULL, fact %s NOT NULL, present BOOLEAN NOT NULL,
		PRIMARY KEY(run_id,family,fact_key,revision))`, jsonType)); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRunForkHistoricalEventCursorContextBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := historicalSnapshotDatabase(t, backend)
			runID, eventID := uuid.NewString(), uuid.NewString()
			body := `{"event_id":"` + eventID + `","event_name":"at-R","payload_base64":"e30="}`
			const insert = `INSERT INTO run_fork_fact_revisions VALUES($1,'events',$2,$3,$4,true)`
			if _, err := db.Exec(insert, runID, eventID, 1, body); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(insert, runID, eventID, 2, `{"event_id":"`+eventID+`","event_name":"later","payload_base64":"e30="}`); err != nil {
				t.Fatal(err)
			}
			resolve := func(at string) (runForkEventCursor, error) {
				t.Helper()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if backend == "sqlite" {
					return resolveSQLiteRunForkRevisionPoint(context.Background(), tx, runID, at)
				}
				return resolveRunForkRevisionPoint(context.Background(), tx, runID, at)
			}
			for _, selector := range []string{"", eventID} {
				got, err := resolve(selector)
				if err != nil || got.EventID != eventID || got.Revision != 1 || got.EventName != "at-R" {
					t.Fatalf("first-revision cursor=%#v err=%v", got, err)
				}
			}
			for _, bad := range []string{`{"event_id":"` + uuid.NewString() + `","payload_base64":"e30="}`, `{"payload_base64":"e30="}`, `{"event_id":"invalid","payload_base64":"e30="}`} {
				if _, err := db.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=1`, bad, runID); err != nil {
					t.Fatal(err)
				}
				for _, selector := range []string{"", eventID} {
					got, err := resolve(selector)
					if err == nil || got != (runForkEventCursor{}) {
						t.Fatalf("corrupt cursor=%#v err=%v", got, err)
					}
				}
			}
			if _, err := db.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=1`, body, runID); err != nil {
				t.Fatal(err)
			}
			if got, err := resolve(eventID); err != nil || got.Revision != 1 {
				t.Fatalf("restored cursor=%#v err=%v", got, err)
			}
		})
	}
}

// This is the SQL snapshot boundary, not a served writer fixture. The separate
// thirteen-family matrix covers canonical publication and the activation tests
// cover the current-source safety gate. No production scheduling hook is needed.
func TestRunForkHistoricalSnapshotCommitBarrierBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := historicalSnapshotDatabase(t, backend)
			runID, eventID, laterID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			body := func(id, name string) string {
				return `{"event_id":"` + id + `","event_name":"` + name + `","payload_base64":"eyJ2YWx1ZSI6MS4wfQ=="}`
			}
			const insert = `INSERT INTO run_fork_fact_revisions VALUES($1,'events',$2,$3,$4,true)`
			if _, err := db.Exec(insert, runID, eventID, 1, body(eventID, "before")); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			options := &sql.TxOptions{ReadOnly: true}
			if backend == "postgres" {
				options.Isolation = sql.LevelRepeatableRead
			}
			reader, err := db.BeginTx(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback()
			before, err := loadRunForkRevisionSnapshot(ctx, reader, runID, 1)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Rollback()
			if _, err := writer.ExecContext(ctx, insert, runID, laterID, 2, body(laterID, "after")); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.ExecContext(ctx, insert, runID, eventID, 2, body(eventID, "updated")); err != nil {
				t.Fatal(err)
			}
			assertOld := func() {
				t.Helper()
				got, err := loadRunForkRevisionSnapshot(ctx, reader, runID, 2)
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Events) != 1 || !reflect.DeepEqual(got.Events, before.Events) {
					t.Fatalf("reader mixed transaction generations: before=%#v after=%#v", before.Events, got.Events)
				}
				cursor, err := resolveRunForkRevisionPoint(ctx, reader, runID, "")
				if err != nil || cursor.EventID != eventID || cursor.Revision != 1 {
					t.Fatalf("snapshot cursor=%#v err=%v", cursor, err)
				}
			}
			assertOld() // The new facts exist in the writer but are not committed.
			committed := make(chan error, 1)
			go func() { committed <- writer.Commit() }()
			select {
			case err := <-committed:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("writer did not cross commit barrier: ", ctx.Err())
			}
			assertOld() // The reader established its snapshot before the commit barrier.
			if err := reader.Rollback(); err != nil {
				t.Fatal(err)
			}
			fresh, err := db.BeginTx(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Rollback()
			fixed, err := loadRunForkRevisionSnapshot(ctx, fresh, runID, 1)
			if err != nil || !reflect.DeepEqual(fixed.Events, before.Events) {
				t.Fatalf("fixed cut moved after commit: %#v %v", fixed, err)
			}
			latest, err := loadRunForkRevisionSnapshot(ctx, fresh, runID, 2)
			if err != nil || len(latest.Events) != 2 || latest.Events[0].EventName != "updated" {
				t.Fatalf("fresh R2=%#v err=%v", latest, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := loadRunForkRevisionSnapshot(cancelled, fresh, runID, 1); err == nil || got != nil {
				t.Fatalf("cancelled snapshot=%#v err=%v", got, err)
			}
		})
	}
}
