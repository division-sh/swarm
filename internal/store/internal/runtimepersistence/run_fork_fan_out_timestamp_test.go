package runtimepersistence

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

// Current-row readback is deliberately separate from historical ledger JSON.
// The real first materialization writes the child; only its repeat evidence is
// then made hostile. Every comparison baseline is taken after that injection.
func TestRunForkFanOutTimestampCurrentReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			store := owner.(runForkSelectedLifecycleStore)
			at := time.Date(2026, 9, 8, 10, 11, 12, 123456000, time.UTC)
			source := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 2, at)
			captureFanOutBarrierForkRevision(t, ctx, db, source.runID, postgres)
			outcome := eventtest.ExistingRunRootIngress(uuid.NewString(), "timestamp.fanout_finished", "timestamp-proof", "", []byte(`{}`), 0, source.runID, events.EventEnvelope{}, at.Add(time.Second))
			if err := commitSemanticPipelineProcessedEventFixture(ctx, owner, outcome); err != nil {
				t.Fatal(err)
			}
			// One committed ordinal and one remaining item form a legal open
			// intent, allowing a present lease to carry its required claim tuple.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at) VALUES ($1,$2,$3,'fan_out',$4,0,'committed',$5,$6)`, source.runID, source.deliveryID, source.flowPath, source.semanticPath, outcome.ID(), at.Add(time.Second))
			mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE fan_out_intents SET cursor=1,status='open',updated_at=$1 WHERE run_id=$2`, at.Add(time.Second), source.runID)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			captureFanOutBarrierForkRevision(t, ctx, db, source.runID, postgres)
			point := historicalLineageCheckpoint(t, db, source.runID, postgres, at.Add(2*time.Second))
			request := runfork.RunForkMaterializeRequest{SourceRunID: source.runID, At: point}
			child, err := store.MaterializeRunFork(ctx, request)
			if err != nil || child.MaterializedFanOutCount != 1 || child.ForkRunID == source.runID {
				t.Fatalf("real first materialization: %#v err=%v", child, err)
			}
			for _, column := range []string{"created_at", "lease_expires_at", "last_served_at"} {
				t.Run(column, func(t *testing.T) {
					table := "fan_out_intents"
					if column == "created_at" {
						table = "fan_out_outcomes"
					}
					query := fmt.Sprintf("UPDATE %s SET %s=$1 WHERE run_id=$2", table, column)
					if column == "lease_expires_at" {
						query = `UPDATE fan_out_intents SET lease_expires_at=$1,claim_owner=CASE WHEN $1 IS NULL THEN NULL ELSE 'timestamp-proof' END,claim_generation=CASE WHEN $1 IS NULL THEN 0 ELSE 1 END WHERE run_id=$2`
						if postgres {
							query = `UPDATE fan_out_intents SET lease_expires_at=$1::timestamptz,claim_owner=CASE WHEN $1::timestamptz IS NULL THEN NULL ELSE 'timestamp-proof' END,claim_generation=CASE WHEN $1::timestamptz IS NULL THEN 0 ELSE 1 END WHERE run_id=$2`
						}
					}
					var original any
					if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE run_id=$1", column, table), child.ForkRunID).Scan(&original); err != nil {
						t.Fatal(err)
					}
					for _, cell := range []struct {
						name  string
						value any
					}{
						{"valid_native", at},
						{"valid_offset_text", at.In(time.FixedZone("fixture", 5*3600+30*60)).Format(time.RFC3339Nano)},
						{"zero_native", time.Time{}},
						{"zero_text", time.Time{}.Format(time.RFC3339Nano)},
						{"empty", ""},
						{"malformed", "not-a-timestamp"},
						{"null", nil},
					} {
						t.Run(cell.name, func(t *testing.T) {
							beforeInjection := historicalContextDatabaseRows(t, db, postgres)
							result, injectionErr := db.ExecContext(ctx, query, cell.value, child.ForkRunID)
							wantSchemaRefusal := (column == "created_at" && cell.name == "null") || (postgres && (cell.name == "empty" || cell.name == "malformed"))
							if wantSchemaRefusal {
								requireFanOutTimestampSchemaRefusal(t, injectionErr, postgres, cell.name == "null")
								historicalContextRequireUnchanged(t, beforeInjection, historicalContextDatabaseRows(t, db, postgres))
								t.Logf("schema refused %s.%s=%s before reader execution: %v; entire database unchanged", table, column, cell.name, injectionErr)
								return
							}
							if injectionErr != nil {
								t.Fatalf("timestamp fixture injection: %v", injectionErr)
							}
							if count, err := result.RowsAffected(); err != nil || count != 1 {
								t.Fatalf("timestamp injection affected %d rows, err=%v", count, err)
							}
							t.Cleanup(func() {
								if _, err := db.ExecContext(ctx, query, original, child.ForkRunID); err != nil {
									t.Errorf("restore deliberate timestamp injection: %v", err)
								}
							})
							var raw any
							if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE run_id=$1", column, table), child.ForkRunID).Scan(&raw); err != nil {
								t.Fatal(err)
							}
							if (raw == nil) != (cell.value == nil) {
								t.Fatalf("SQL presence changed before reader: input=%#v stored=%#v", cell.value, raw)
							}
							if column == "lease_expires_at" && raw != nil {
								var claimOwner, status string
								var generation int
								if err := db.QueryRow(`SELECT claim_owner,claim_generation,status FROM fan_out_intents WHERE run_id=$1`, child.ForkRunID).Scan(&claimOwner, &generation, &status); err != nil || claimOwner != "timestamp-proof" || generation != 1 || status != "open" {
									t.Fatalf("present lease lacks legal open-claim fixture: owner=%q generation=%d status=%q err=%v", claimOwner, generation, status, err)
								}
							}
							beforeRetry := historicalContextDatabaseRows(t, db, postgres)
							repeated, retryErr := store.MaterializeRunFork(ctx, request)
							historicalContextRequireUnchanged(t, beforeRetry, historicalContextDatabaseRows(t, db, postgres))
							wantSuccess := (column == "created_at" && (cell.name == "valid_native" || cell.name == "valid_offset_text")) || (column != "created_at" && cell.name == "null")
							if wantSuccess {
								if retryErr != nil || repeated.ForkRunID != child.ForkRunID || repeated.MaterializedFanOutCount != 1 || repeated.SourceRunID != source.runID {
									t.Fatalf("lawful %s=%s repeat changed/refused exact source-child identity: %#v err=%v", column, cell.name, repeated, retryErr)
								}
							} else if retryErr == nil || !reflect.DeepEqual(repeated, runfork.RunForkMaterialization{}) {
								t.Fatalf("hostile %s=%s accepted as absent/valid repeat: %#v err=%v", column, cell.name, repeated, retryErr)
							}
							t.Logf("current-row %s=%s driver=%T SQL_NULL=%v success=%v error=%v; full source/child database unchanged", column, cell.name, raw, raw == nil, wantSuccess, retryErr)
						})
					}
				})
			}
		})
	}
}

func requireFanOutTimestampSchemaRefusal(t *testing.T, err error, postgres, requiredNull bool) {
	t.Helper()
	if err == nil {
		t.Fatal("invalid timestamp fixture unexpectedly passed the declared schema boundary")
	}
	if postgres {
		var state interface{ SQLState() string }
		if !errors.As(err, &state) {
			t.Fatalf("PostgreSQL refusal lacks SQLSTATE: %v", err)
		}
		want := "22007"
		if requiredNull {
			want = "23502"
		}
		if state.SQLState() != want {
			t.Fatalf("PostgreSQL timestamp refusal SQLSTATE=%s, want %s: %v", state.SQLState(), want, err)
		}
		return
	}
	var code interface{ Code() int }
	if !requiredNull || !errors.As(err, &code) || code.Code() != 1299 {
		t.Fatalf("SQLite required-null refusal must be SQLITE_CONSTRAINT_NOTNULL (1299): %v", err)
	}
}
