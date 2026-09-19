package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestRunForkExactFactsSQLiteBusyRetryGeneratedIDs(t *testing.T) {
	for _, phase := range []string{"callback", "commit", "stale_callback_control"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "retry.db")
			store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
			db := store.backend.ConstructionHandle()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			s := exactFactStore{db: db, selected: store}
			f := newExactFactFixture(t, s)
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at, false)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,name,current_state,created_at,updated_at) VALUES ($1,$2,'retry','retry','Before Retry','ready',$3,$3)`, f.runID, f.entityID, f.at)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID), exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID)), 1, true)
			})
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
			defer cancel()
			if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout=1`); err != nil {
				t.Fatal(err)
			}
			var blocker *sql.Tx
			if phase == "commit" {
				// A real shared reader lock blocks physical COMMIT, not callback SQL.
				var mode string
				if err := db.QueryRowContext(ctx, `PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil || mode != "delete" {
					t.Fatalf("rollback journal mode=%s err=%v", mode, err)
				}
				other, err := sql.Open("sqlite", "file:"+path)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				blocker, err = other.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				var count int
				if err := blocker.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
					t.Fatal(err)
				}
			} else {
				// A separate attached database permits real main-database writes and
				// generated history first, then forces native BUSY in callback SQL.
				lockPath := filepath.Join(t.TempDir(), "lock.db")
				other, err := sql.Open("sqlite", "file:"+lockPath)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				if _, err := other.ExecContext(ctx, `CREATE TABLE retry_lock (id INTEGER PRIMARY KEY,value INTEGER NOT NULL)`); err != nil {
					t.Fatal(err)
				}
				if _, err := other.ExecContext(ctx, `INSERT INTO retry_lock VALUES (1,0)`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `ATTACH DATABASE $1 AS retry_probe`, lockPath); err != nil {
					t.Fatal(err)
				}
				blocker, err = other.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := blocker.ExecContext(ctx, `UPDATE retry_lock SET value=1 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
			}
			defer blocker.Rollback()
			effects, err := runforkrevision.ForRun(f.runID, runforkrevision.FamilyEntityMetadata)
			if err != nil {
				t.Fatal(err)
			}
			resetAttempt := effects.AttemptReset()
			probe, restore, err := store.backend.InstallTransactionProbeForTest(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var generated []string
			var callbackBusy bool
			committed, err := store.backend.RunTransactionOutcome(ctx, "exact generated-ID busy retry", func(ctx context.Context, tx *sql.Tx) error {
				if len(generated) == 1 {
					if err := blocker.Rollback(); err != nil {
						return err
					}
				}
				if phase != "stale_callback_control" {
					resetAttempt()
				}
				id := uuid.NewString()
				generated = append(generated, id)
				seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, id, f.at.Add(time.Second), false)
				if err := effects.AddFacts(f.runID, exactFactRef(t, runforkrevision.FamilyEvents, id)); err != nil {
					return err
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_state SET name='After Retry' WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID)
				if _, err := runforkrevision.FinalizeSQLite(ctx, tx, effects); err != nil {
					return err
				}
				if phase != "commit" {
					_, err := tx.ExecContext(ctx, `UPDATE retry_probe.retry_lock SET value=2 WHERE id=1`)
					if len(generated) == 1 {
						var coded interface{ Code() int }
						callbackBusy = errors.As(err, &coded) && coded.Code()&255 == 5
						if !callbackBusy {
							return fmt.Errorf("expected native callback SQLITE_BUSY, got %v", err)
						}
					}
					return err
				}
				return nil
			})
			if len(generated) != 2 || generated[0] == generated[1] {
				t.Fatalf("generated attempts=%v result=%v", generated, err)
			}
			counts := probe.Snapshot()
			if phase == "stale_callback_control" {
				if err == nil || committed || !strings.Contains(err.Error(), "not a deletion") || !strings.Contains(err.Error(), generated[0]) {
					t.Fatalf("stale exact-ID refusal committed=%v err=%v", committed, err)
				}
			} else {
				if err != nil || !committed {
					t.Fatalf("scoped busy recovery committed=%v err=%v", committed, err)
				}
				wantCommits, wantFailures := uint64(1), uint64(0)
				if phase == "commit" {
					wantCommits, wantFailures = 2, 1
				}
				if counts.Total.Begun != 2 || counts.Total.WriteCommits != 1 || counts.Total.CommitAttempts != wantCommits || counts.Total.CommitFailures != wantFailures || counts.Total.RollbackAttempts != 1 || counts.Total.Revision.Finalizations != 2 || counts.Active != 0 {
					t.Fatalf("actual retry accounting=%+v", counts)
				}
			}
			if phase != "commit" && !callbackBusy {
				t.Fatal("no actual native callback busy observed")
			}
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				if err := validateRunForkRevisionMatrix(ctx, tx, false, f.runID); err != nil {
					t.Fatalf("retry lost immutable whole baseline: %v", err)
				}
				for i, id := range generated {
					want := 0
					if i == 1 && committed {
						want = 1
					}
					for _, query := range []string{`SELECT COUNT(*) FROM events WHERE event_id=$1`, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE family='events' AND fact_key=$1`} {
						var got int
						if err := tx.QueryRowContext(ctx, query, id).Scan(&got); err != nil {
							t.Fatal(err)
						}
						if got != want {
							t.Fatalf("generated ID %s durable count=%d want=%d", id, got, want)
						}
					}
				}
				var name string
				if err := tx.QueryRowContext(ctx, `SELECT name FROM entity_state WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&name); err != nil {
					t.Fatal(err)
				}
				wantName := "Before Retry"
				if committed {
					wantName = "After Retry"
				}
				if name != wantName {
					t.Fatalf("retry metadata=%q want=%q", name, wantName)
				}
			})
		})
	}
}
