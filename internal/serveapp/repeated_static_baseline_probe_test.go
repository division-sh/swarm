package serveapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestBaselineServedRepeatedStaticRunProbe(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyRepeatedStaticBaselineProbe(t))
			var firstRun string
			var firstProducer string
			var firstSnapshot map[string][][]string
			for attempt := 1; attempt <= 2; attempt++ {
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "parent.seeded", "bundle_hash": rt.BundleHash,
					"payload":         map[string]any{"work_id": fmt.Sprintf("work-%d", attempt)},
					"idempotency_key": fmt.Sprintf("baseline-static-%d", attempt),
				})
				if seed.RunID == firstRun {
					t.Fatal("second public root did not create an independent run")
				}
				count := 0
				for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance='producer' AND current_state='active'`, seed.RunID).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count == 1 {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				if count != 1 {
					t.Fatalf("independent run %d failed static materialization before any transition history read\n%s", attempt, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				requireServedRunStatusWithDebug(t, rt.Endpoint, rt.DB, rt.Backend, seed.RunID, "completed")
				for _, check := range []struct {
					query string
					want  int
				}{
					{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, 3},
					{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, 3},
					{`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.outcome='success'`, 3},
					{`SELECT COUNT(*) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1`, 0},
					{`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND instance_path='producer' AND mode='static' AND status='active'`, 1},
					{`SELECT COUNT(*) FROM runs WHERE run_id=$1 AND status='completed'`, 1},
				} {
					if err := rt.DB.QueryRow(check.query, seed.RunID).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != check.want {
						t.Fatalf("run %d: %s = %d, want %d\n%s", attempt, check.query, count, check.want, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
					}
				}
				var producer, rawFields string
				if err := rt.DB.QueryRow(`SELECT entity_id,CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='producer'`, seed.RunID).Scan(&producer, &rawFields); err != nil {
					t.Fatal(err)
				}
				var fields map[string]any
				if err := json.Unmarshal([]byte(rawFields), &fields); err != nil {
					t.Fatal(err)
				}
				if fields["work_id"] != fmt.Sprintf("work-%d", attempt) {
					t.Fatalf("run %d borrowed another run's state: %s", attempt, rawFields)
				}
				if attempt == 2 && producer != firstProducer {
					t.Fatal("proof changed the producer entity identity between runs")
				}
				requireReceiverPublicReadback(t, rt, seed.RunID)
				if attempt == 1 {
					firstProducer = producer
					firstRun = seed.RunID
					firstSnapshot = repeatedStaticRunSnapshot(t, rt.DB, firstRun)
				} else {
					if got := repeatedStaticRunSnapshot(t, rt.DB, firstRun); !reflect.DeepEqual(got, firstSnapshot) {
						t.Fatalf("run 2 mutated run 1: before=%#v after=%#v", firstSnapshot, got)
					}
					requireReceiverPublicReadback(t, rt, firstRun)
				}
			}
		})
	}
}

// Read every persisted field, not just counts: another run must not rebind or
// rewrite a predecessor's lifecycle, state, events or settlement evidence.
func repeatedStaticRunSnapshot(t *testing.T, db *sql.DB, runID string) map[string][][]string {
	t.Helper()
	result := map[string][][]string{}
	for name, query := range map[string]string{
		"flow_instances": `SELECT * FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`,
		"entity_state":   `SELECT * FROM entity_state WHERE run_id=$1 ORDER BY entity_id`,
		"events":         `SELECT * FROM events WHERE run_id=$1 ORDER BY event_id`,
		"deliveries":     `SELECT * FROM event_deliveries WHERE run_id=$1 ORDER BY delivery_id`,
		"receipts":       `SELECT r.* FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 ORDER BY r.event_id,r.subscriber_type,r.subscriber_id`,
		"outcomes":       `SELECT o.* FROM event_delivery_outcomes o JOIN event_deliveries d ON d.delivery_id=o.delivery_id WHERE d.run_id=$1 ORDER BY o.delivery_id,o.claim_version`,
	} {
		rows, err := db.Query(query, runID)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			record := make([]string, len(columns))
			for i, value := range values {
				if bytes, ok := value.([]byte); ok {
					record[i] = string(bytes)
				} else {
					record[i] = fmt.Sprint(value)
				}
			}
			result[name] = append(result[name], record)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}
