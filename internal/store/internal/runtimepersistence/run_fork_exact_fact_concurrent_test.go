package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestRunForkExactFactsConcurrentBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		for _, runCount := range []int{1, 2} {
			t.Run(fmt.Sprintf("runs_%d_opposite_declaration_order", runCount), func(t *testing.T) {
				fixtures := make([]runForkRevisionMatrixFixture, runCount)
				for i := range fixtures {
					fixtures[i] = newExactFactFixture(t, s)
					seedExactEvent(t, s, fixtures[i])
				}
				ids := [2][]string{make([]string, runCount), make([]string, runCount)}
				refs := [2][]runforkrevision.FactRef{make([]runforkrevision.FactRef, runCount), make([]runforkrevision.FactRef, runCount)}
				for worker := range ids {
					for i := range ids[worker] {
						ids[worker][i] = uuid.NewString()
						refs[worker][i] = exactFactRef(t, runforkrevision.FamilyEvents, ids[worker][i])
					}
				}
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
				defer cancel()
				start, finalize := make(chan struct{}), make(chan struct{})
				started, written := make(chan int, 2), make(chan int, 2)
				type completion struct {
					worker    int
					result    map[string]runforkrevision.Result
					committed bool
					err       error
				}
				done := make(chan completion, 2)
				for worker := range ids {
					go func(worker int) {
						finished := completion{worker: worker}
						defer func() { done <- finished }()
						started <- worker
						select {
						case <-start:
						case <-ctx.Done():
							finished.err = ctx.Err()
							return
						}
						effects := runforkrevision.NewEffects()
						reset := effects.AttemptReset()
						operation := func(ctx context.Context, tx *sql.Tx) error {
							reset()
							for order := range fixtures {
								i := order
								if worker == 1 {
									i = runCount - 1 - order
								}
								f := fixtures[i]
								dialect := authoractivityfixture.DialectSQLite
								if s.postgres {
									dialect = authoractivityfixture.DialectPostgres
								}
								event := eventtest.ExistingRunRootIngress(ids[worker][i], events.EventType("matrix.event"), "exact-concurrent", "", json.RawMessage(`{"matrix":true}`), 0, f.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, f.at.Add(time.Second))
								if err := eventfixture.Insert(ctx, tx, dialect, event); err != nil {
									return err
								}
								if err := effects.AddFacts(f.runID, refs[worker][i]); err != nil {
									return err
								}
							}
							select {
							case written <- worker:
							case <-ctx.Done():
								return ctx.Err()
							}
							select {
							case <-finalize:
							case <-ctx.Done():
								return ctx.Err()
							}
							var err error
							finished.result, err = finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects)
							return err
						}
						switch selected := s.selected.(type) {
						case *PostgresStore:
							finished.committed, finished.err = selected.backend.RunTransactionOutcome(ctx, operation)
						case *SQLiteRuntimeStore:
							finished.committed, finished.err = selected.backend.RunTransactionOutcome(ctx, "concurrent exact facts", operation)
						default:
							finished.err = fmt.Errorf("unsupported selected store %T", selected)
						}
					}(worker)
				}
				for range 2 {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				close(start)
				// PostgreSQL must have both writers' domain mutations in flight before
				// either finalizes. SQLite's existing mutation owner serializes them.
				inFlight := 1
				if s.postgres {
					inFlight = 2
				}
				for range inFlight {
					select {
					case <-written:
					case result := <-done:
						t.Fatalf("writer ended before finalization rendezvous: %+v", result)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				close(finalize)
				results := make([]completion, 0, 2)
				for range 2 {
					select {
					case result := <-done:
						if result.err != nil || !result.committed {
							t.Fatalf("concurrent exact writer %d committed=%v err=%v", result.worker, result.committed, result.err)
						}
						results = append(results, result)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				for _, f := range fixtures {
					seen := map[int64]bool{}
					for _, result := range results {
						capture := result.result[f.runID]
						if !capture.Changed || (capture.Revision != 2 && capture.Revision != 3) || seen[capture.Revision] {
							t.Fatalf("concurrent revisions lost/duplicated: %#v", results)
						}
						seen[capture.Revision] = true
						if capture.Revision != result.result[fixtures[0].runID].Revision {
							t.Fatal("multi-run transaction split its revision ordering")
						}
					}
				}
				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					for i, f := range fixtures {
						ledger := exactLedger(t, ctx, tx, f.runID)
						if len(ledger) != 3 {
							t.Fatalf("concurrent ledger rows=%#v", ledger)
						}
						want := map[string]bool{f.eventID: true, ids[0][i]: true, ids[1][i]: true}
						for _, row := range ledger {
							if row.Family != string(runforkrevision.FamilyEvents) || !row.Present || !want[row.Key] {
								t.Fatalf("phantom/lost concurrent fact=%#v", row)
							}
							delete(want, row.Key)
						}
						if len(want) != 0 {
							t.Fatalf("missing concurrent facts=%v", want)
						}
						compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, refs[0][i], refs[1][i]), 3, false)
					}
				})
			})
		}
	})
}
