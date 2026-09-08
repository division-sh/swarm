package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// This is a reduced reader fixture, not a production-writer proof. It retains
// current timestamp SQL types but omits NOT NULL/claim-tuple CHECK constraints
// so requiredness and forbidden presence are isolated at the actual reader.
// The full-schema materialization matrix separately proves legal claim tuples.
func TestFanOutTimestampReaderPresenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutTimestampReaderDatabase(t, backend)
			at := time.Date(2026, 9, 8, 10, 11, 12, 123456000, time.UTC)
			childID, triggerID, eventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			ref := runtimecontracts.FanOutPlanRef{
				BundleHash: "timestamp-reader-bundle", SemanticDigest: "timestamp-reader-digest",
				ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: `nodes["worker"].handlers["items.ready"].fan_out`},
			}
			source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: eventID, Field: "items"}
			intent := fanoutobligation.Intent{
				Request: fanoutobligation.IntentRequest{
					Key:     fanoutobligation.IntentKey{RunID: uuid.NewString(), TriggeringDeliveryID: triggerID, ElementRef: ref.ElementRef},
					PlanRef: ref, Source: source, Cardinality: 2,
				},
				Source: source, Cursor: 1, Status: fanoutobligation.StatusOpen,
			}
			outcome := fanoutobligation.Outcome{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(), InheritedDisposition: "no_route"}
			plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent, Outcomes: []fanoutobligation.Outcome{outcome}}}}
			refs := map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef{ref.ElementRef: ref}
			capsule, err := json.Marshal(intent.Request.Capsule)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO fan_out_intents (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,claim_generation) VALUES ($1,$2,'.','fan_out',$3,$4,$5,'event_payload_field',$6,'items',2,1,'open',4,$7,0)`, childID, triggerID, ref.ElementRef.SemanticPath, ref.BundleHash, ref.SemanticDigest, eventID, string(capsule)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,source_event_id,inherited_disposition,created_at) VALUES ($1,$2,'.','fan_out',$3,0,'committed',$4,'no_route',$5)`, childID, triggerID, ref.ElementRef.SemanticPath, outcome.SourceEventID, at); err != nil {
				t.Fatal(err)
			}
			for _, column := range []string{"created_at", "lease_expires_at", "last_served_at"} {
				t.Run(column, func(t *testing.T) {
					table := "fan_out_intents"
					var restore any
					if column == "created_at" {
						table, restore = "fan_out_outcomes", at
					}
					query := fmt.Sprintf("UPDATE %s SET %s=$1 WHERE run_id=$2", table, column)
					for _, cell := range []struct {
						name string
						raw  any
					}{
						{"valid_native", at}, {"valid_text", at.Format(time.RFC3339Nano)},
						{"zero_native", time.Time{}}, {"zero_text", time.Time{}.Format(time.RFC3339Nano)},
						{"empty", ""}, {"malformed", "not-a-timestamp"}, {"null", nil},
					} {
						t.Run(cell.name, func(t *testing.T) {
							beforeInjection := fanOutTimestampReaderRows(t, db)
							_, err := db.Exec(query, cell.raw, childID)
							if backend == "postgres" && (cell.name == "empty" || cell.name == "malformed") {
								var state interface{ SQLState() string }
								if !errors.As(err, &state) || state.SQLState() != "22007" {
									t.Fatalf("invalid TIMESTAMPTZ must fail PostgreSQL type admission: %v", err)
								}
								if after := fanOutTimestampReaderRows(t, db); !reflect.DeepEqual(beforeInjection, after) {
									t.Fatalf("SQL refusal mutated reduced fixture: before=%v after=%v", beforeInjection, after)
								}
								t.Logf("PostgreSQL type rejected input before reduced reader: %v", err)
								return
							}
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() {
								if _, err := db.Exec(query, restore, childID); err != nil {
									t.Error(err)
								}
							})
							before := fanOutTimestampReaderRows(t, db)
							tx, err := db.BeginTx(context.Background(), nil)
							if err != nil {
								t.Fatal(err)
							}
							readErr := requireExactMaterializedRunForkFanOut(context.Background(), tx, backend == "postgres", childID, plan, refs)
							if err := tx.Rollback(); err != nil {
								t.Fatal(err)
							}
							if after := fanOutTimestampReaderRows(t, db); !reflect.DeepEqual(before, after) {
								t.Fatalf("reader changed rows: before=%v after=%v", before, after)
							}
							wantOK := (column == "created_at" && (cell.name == "valid_native" || cell.name == "valid_text")) || (column != "created_at" && cell.raw == nil)
							if (readErr == nil) != wantOK {
								t.Fatalf("isolated %s=%s read error=%v, want success=%v; all other repeat evidence matches", column, cell.name, readErr, wantOK)
							}
							t.Logf("reduced reader %s=%s success=%v error=%v; all rows unchanged", column, cell.name, wantOK, readErr)
						})
					}
				})
			}
		})
	}
}

func fanOutTimestampReaderDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	timestamp := "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		timestamp = "TIMESTAMPTZ"
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "fan-out.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	for _, query := range []string{
		fmt.Sprintf(`CREATE TABLE fan_out_intents (run_id TEXT,triggering_delivery_id TEXT,flow_path TEXT,declaration_family TEXT,semantic_path TEXT,bundle_hash TEXT,semantic_digest TEXT,source_kind TEXT,source_event_id TEXT,source_run_id TEXT,source_entity_id TEXT,source_field TEXT,source_mutation_id TEXT,source_resource_flow_path TEXT,source_resource_event_name TEXT,source_resource_version_id TEXT,cardinality INTEGER,cursor INTEGER,status TEXT,next_chunk_size INTEGER,capsule TEXT,claim_owner TEXT,claim_generation BIGINT,lease_expires_at %s,last_served_at %s,blocked_reason TEXT)`, timestamp, timestamp),
		fmt.Sprintf(`CREATE TABLE fan_out_outcomes (run_id TEXT,triggering_delivery_id TEXT,flow_path TEXT,declaration_family TEXT,semantic_path TEXT,ordinal INTEGER,outcome_kind TEXT,event_id TEXT,source_event_id TEXT,inherited_disposition TEXT,failure TEXT,created_at %s)`, timestamp),
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func fanOutTimestampReaderRows(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, table := range []string{"fan_out_intents", "fan_out_outcomes"} {
		rows, err := db.Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var records [][]any
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			records = append(records, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		raw, err := json.Marshal(records)
		if err != nil {
			t.Fatal(err)
		}
		out[table] = string(raw)
	}
	return out
}
