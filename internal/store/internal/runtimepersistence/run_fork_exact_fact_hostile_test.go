package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func seedExactEvent(t *testing.T, s exactFactStore, f runForkRevisionMatrixFixture) {
	t.Helper()
	exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
		seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at, s.postgres)
		compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID,
			exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)), 1, true)
	})
}

func exactRollbackTransaction(t *testing.T, s exactFactStore, fn func(context.Context, *sql.Tx)) {
	t.Helper()
	ctx := testAuthorActivityContext()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	fn(ctx, tx)
}

func TestRunForkExactFactsHostileBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f, foreign := newExactFactFixture(t, s), newExactFactFixture(t, s)
		seedExactEvent(t, s, f)
		seedExactEvent(t, s, foreign)
		t.Run("malformed_and_zero_coordinates", func(t *testing.T) {
			for _, key := range []string{"", "not-a-uuid", f.eventID + "|receipt"} {
				if _, err := runforkrevision.NewFactRef(runforkrevision.FamilyEvents, key); err == nil {
					t.Fatalf("accepted malformed key %q", key)
				}
			}
			if _, err := runforkrevision.NewFactRef(runforkrevision.FamilyFanOutObligations, "intent|"+f.deliveryID+"|root|fan_out|x"); err == nil {
				t.Fatal("accepted serialized fan-out key")
			}
			if err := runforkrevision.NewEffects().AddFacts(f.runID, runforkrevision.FactRef{}); err == nil {
				t.Fatal("accepted zero ref")
			}
			ref, err := runforkrevision.FanOutIntentFact(exactMatrixIntentKey(f))
			if err != nil {
				t.Fatal(err)
			}
			if err := runforkrevision.NewEffects().AddFacts(foreign.runID, ref); err == nil {
				t.Fatal("accepted foreign structured run")
			}
			if _, err := runforkrevision.FanOutOutcomeFact(exactMatrixIntentKey(f), -1); err == nil {
				t.Fatal("accepted negative ordinal")
			}
		})
		for _, probe := range []struct{ name, key string }{
			{"missing_is_not_deletion", uuid.NewString()},
			{"foreign_is_not_deletion", foreign.eventID},
		} {
			t.Run(probe.name, func(t *testing.T) {
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					before := exactLedger(t, ctx, tx, f.runID)
					_, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, probe.key)))
					if err == nil || !strings.Contains(err.Error(), "not a deletion") {
						t.Fatalf("missing/foreign coordinate error=%v", err)
					}
					if !reflect.DeepEqual(before, exactLedger(t, ctx, tx, f.runID)) {
						t.Fatal("refusal wrote phantom history")
					}
				})
			})
		}
		for _, field := range []string{"event_id", "run_id"} {
			t.Run("affected_ledger_"+field, func(t *testing.T) {
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					var raw []byte
					if err := tx.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2`, f.runID, f.eventID).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var body map[string]any
					if err := json.Unmarshal(raw, &body); err != nil {
						t.Fatal(err)
					}
					body[field] = uuid.NewString()
					encoded, err := json.Marshal(body)
					if err != nil {
						t.Fatal(err)
					}
					mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='events' AND fact_key=$3`, string(encoded), f.runID, f.eventID)
					_, err = finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)))
					if err == nil {
						t.Fatalf("accepted affected contradictory %s", field)
					}
				})
			})
		}
		t.Run("moved_current_row_is_not_old_run_deletion", func(t *testing.T) {
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE events SET run_id=$1 WHERE event_id=$2`, foreign.runID, f.eventID)
				_, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)))
				if err == nil {
					t.Fatal("accepted old-run tombstone although globally keyed current event still exists in foreign run")
				}
			})
		})
		t.Run("run_scoped_entity_same_id_delete", func(t *testing.T) {
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				ref := exactFactRef(t, runforkrevision.FamilyEntityMetadata, f.entityID)
				for _, runID := range []string{f.runID, foreign.runID} {
					mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,name,current_state,created_at,updated_at) VALUES ($1,$2,'matrix-flow','matrix-type',$3,'ready',$4,$4)`, runID, f.entityID, "entity-"+runID, f.at)
					compareExactWithWhole(t, ctx, tx, s, runID, exactEffects(t, runID, ref), 2, true)
				}
				foreignBefore := exactLedger(t, ctx, tx, foreign.runID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, ref), 3, true)
				if !reflect.DeepEqual(foreignBefore, exactLedger(t, ctx, tx, foreign.runID)) {
					t.Fatal("run-scoped deletion changed foreign history")
				}
				var name string
				if err := tx.QueryRowContext(ctx, `SELECT name FROM entity_state WHERE run_id=$1 AND entity_id=$2`, foreign.runID, f.entityID).Scan(&name); err != nil || name != "entity-"+foreign.runID {
					t.Fatalf("foreign same-ID metadata changed: name=%q err=%v", name, err)
				}
				if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, foreign.runID); err != nil {
					t.Fatalf("foreign complete history: %v", err)
				}
			})
		})
		t.Run("duplicate_join_projection_rows", func(t *testing.T) {
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,outcome,processed_at) VALUES ($1,$2,'platform','duplicate-projection','success',$3)`, f.receiptID, f.eventID, f.at)
				// Shadow only this test transaction's read relation; do not weaken the
				// real table's primary key or change production query construction.
				schema := "main"
				if s.postgres {
					if err := tx.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
						t.Fatal(err)
					}
				}
				qualified := `"` + strings.ReplaceAll(schema, `"`, `""`) + `".event_receipts`
				mustExecRunForkRevisionMatrix(t, ctx, tx, fmt.Sprintf(`CREATE TEMP VIEW event_receipts AS SELECT * FROM %s UNION ALL SELECT * FROM %s`, qualified, qualified))
				_, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEventReceipts, f.receiptID)))
				if err == nil || !strings.Contains(err.Error(), "duplicate") {
					t.Fatalf("duplicate projection error=%v", err)
				}
			})
		})
		t.Run("duplicate_latest_ledger_rows", func(t *testing.T) {
			exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				schema := "main"
				if s.postgres {
					if err := tx.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
						t.Fatal(err)
					}
				}
				qualified := `"` + strings.ReplaceAll(schema, `"`, `""`) + `".run_fork_fact_revisions`
				mustExecRunForkRevisionMatrix(t, ctx, tx, fmt.Sprintf(`CREATE TEMP VIEW run_fork_fact_revisions AS SELECT * FROM %s UNION ALL SELECT * FROM %s`, qualified, qualified))
				_, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, f.eventID)))
				if err == nil || !strings.Contains(err.Error(), "duplicate latest") {
					t.Fatalf("duplicate latest ledger error=%v", err)
				}
			})
		})
	})
}

func TestRunForkExactFactsUntouchedCorruptionBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		for _, corrupt := range []string{"current", "ledger"} {
			t.Run(corrupt, func(t *testing.T) {
				f := newExactFactFixture(t, s)
				seedExactEvent(t, s, f)
				ctx := testAuthorActivityContext()
				if _, err := s.selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID}); err != nil {
					t.Fatalf("clean fork control: %v", err)
				}
				unrelated := uuid.NewString()
				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					if corrupt == "current" {
						mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE events SET payload_bytes=$1 WHERE event_id=$2`, []byte(`{"unrevisioned":true}`), f.eventID)
					} else {
						mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='events' AND fact_key=$3`, `{"contradictory":true}`, f.runID, f.eventID)
					}
					seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, unrelated, f.at, s.postgres)
					result, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEvents, unrelated)))
					if err != nil || result[f.runID] != (runforkrevision.Result{Revision: 2, Changed: true}) {
						t.Fatalf("unrelated exact write result=%#v err=%v", result, err)
					}
					if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.runID); err == nil {
						t.Fatal("full validator accepted untouched corruption")
					}
				})
				facts := loadRunForkRevisionMatrixFacts(t, ctx, s.db, f.runID, 2)
				if len(facts) != 1 || facts[0].Key != unrelated || !facts[0].Present {
					t.Fatalf("unrelated write rewrote/tombstoned untouched fact: %#v", facts)
				}
				if _, err := s.selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID}); err == nil || !strings.Contains(err.Error(), "unrevisioned") {
					t.Fatalf("corrupt historical fork refusal=%v", err)
				}
			})
		}
	})
}
