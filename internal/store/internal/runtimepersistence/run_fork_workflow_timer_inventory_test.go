package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// Inventory is all-or-nothing even when a native read fails after an earlier
// activation was hydrated. The existing connector only fails real SQL reads.
func TestRunForkWorkflowTimerInventoryRefusesIncompleteReadbackBothDialects(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"query_error", "partial_read", "cancelled_read"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				probe := &groupProofConnector{}
				if backend == "postgres" {
					probe.dsn, _, _ = testutil.StartPostgres(t)
					probe.driver = &pq.Driver{}
				} else {
					probe.dsn = "file:" + filepath.Join(t.TempDir(), "timer-inventory.db") + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
					probe.driver = &sqlite.Driver{}
				}
				db := sql.OpenDB(probe)
				t.Cleanup(func() {
					probe.set(nil)
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				selected := newStopCommitStoreOwner(t, backend, db)
				if native, ok := selected.(*SQLiteRuntimeStore); ok {
					if err := native.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
						t.Fatal(err)
					}
				}
				ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), selected)
				sourceRun, childRun := uuid.NewString(), uuid.NewString()
				for _, id := range []string{sourceRun, childRun} {
					ctx = originalStoreFixtureContext(t, ctx, selected)
					requireRunningRunForTest(t, ctx, selected, id, time.Now().UTC())
				}
				for i := 0; i < 2; i++ {
					source := newWorkflowTimerDDLProofRow(sourceRun)
					if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, source); err != nil {
						t.Fatal(err)
					}
					row := newWorkflowTimerDDLProofRow(childRun)
					if err := insertWorkflowTimerDDLProofRow(ctx, db, selected, row); err != nil {
						t.Fatal(err)
					}
					if _, err := db.ExecContext(ctx, `UPDATE timers SET source_timer_id=$1, forked_from_run_id=$2,
						forked_from_point_kind=$3, forked_from_point_revision=1, source_armed_at=$4,
						reconstruction_owner='selected-cut' WHERE timer_id=$5`, source.timerID, sourceRun, string(forkpoint.RunStart), source.createdAt, row.timerID); err != nil {
						t.Fatal(err)
					}
				}
				ctx = correlation.WithRunID(ctx, childRun)
				read := func(ctx context.Context) ([]pipeline.WorkflowTimerActivation, error) {
					var inventory []pipeline.WorkflowTimerActivation
					err := runSelectedFixtureMutation(ctx, selected, "timer inventory readback", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						var err error
						switch native := selected.(type) {
						case *PostgresStore:
							inventory, err = native.pipelinePostgresOwner.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, childRun)
						case *SQLiteRuntimeStore:
							inventory, err = native.pipelineSQLiteOwner.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, childRun)
						}
						return err
					})
					return inventory, err
				}
				if got, err := read(ctx); err != nil || len(got) != 2 {
					t.Fatalf("native timer inventory control: got=%+v err=%v", got, err)
				}
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				injected := errors.New("native timer inventory read failed")
				hydrations := 0
				probe.set(func(phase, query string) error {
					if phase != "before_query" {
						return nil
					}
					if cut == "query_error" && strings.Contains(query, "SELECT CAST(timer_id AS TEXT), timer_name FROM timers") {
						return injected
					}
					if strings.Contains(query, "SELECT timer_name,") && strings.Contains(query, "FROM timers WHERE timer_id") {
						hydrations++
						if hydrations == 2 {
							if cut == "cancelled_read" {
								cancel()
								return context.Canceled
							}
							return injected
						}
					}
					return nil
				})
				got, err := read(ctx)
				probe.set(nil)
				if got != nil || err == nil {
					t.Fatalf("incomplete readback leaked partial inventory: got=%+v err=%v", got, err)
				}
				if cut != "query_error" && hydrations != 2 {
					t.Fatalf("read did not reach the failure after a complete activation: hydrations=%d err=%v", hydrations, err)
				}
				if cut == "cancelled_read" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled native read lost cancellation: %v", err)
				}
			})
		}
	}
}
