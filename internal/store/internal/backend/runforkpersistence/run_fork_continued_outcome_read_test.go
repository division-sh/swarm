package runforkpersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRunForkContinuedOutcomeReadExactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			if _, err := db.Exec(`CREATE TABLE fan_out_outcomes (
				run_id TEXT,triggering_delivery_id TEXT,flow_path TEXT,declaration_family TEXT,semantic_path TEXT,
				deployment_feed_id TEXT,ordinal INTEGER,outcome_kind TEXT,event_id TEXT,source_event_id TEXT,
				inherited_disposition TEXT,failure TEXT,created_at TEXT)`); err != nil {
				t.Fatal(err)
			}
			for _, deployment := range []bool{false, true} {
				for _, fault := range []string{"exact", "absent", "foreign_run", "different_ordinal", "duplicate", "missing_time", "invalid_time"} {
					name := "handler/" + fault
					if deployment {
						name = "deployment/" + fault
					}
					t.Run(name, func(t *testing.T) {
						ctx := context.Background()
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						key := continuedBindingComponentKey(deployment)
						at := time.Unix(200, 123).UTC()
						childEvent, sourceEvent := uuid.NewString(), uuid.NewString()
						var stamp any = at.Format(time.RFC3339Nano)
						run, ordinal := key.RunID, 0
						switch fault {
						case "foreign_run":
							run = uuid.NewString()
						case "different_ordinal":
							ordinal = 1
						case "missing_time":
							stamp = nil
						case "invalid_time":
							stamp = "not-a-time"
						}
						insert := `INSERT INTO fan_out_outcomes VALUES ($1,$2,$3,$4,$5,$6,$7,'continued',$8,$9,'dead_lettered',NULL,$10)`
						args := []any{run, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath,
							key.DeploymentFeedID, ordinal, childEvent, sourceEvent, stamp}
						count := 0
						if fault != "absent" {
							count = 1
						}
						if fault == "duplicate" {
							count = 2
						}
						for i := 0; i < count; i++ {
							if _, err := tx.ExecContext(ctx, insert, args...); err != nil {
								t.Fatal(err)
							}
						}
						row, found, err := loadRunForkContinuedOutcomeTx(ctx, tx, key, 0)
						bad := fault == "duplicate" || fault == "missing_time" || fault == "invalid_time"
						if (err != nil) != bad || (err == nil && found != (fault == "exact")) {
							t.Fatalf("exact outcome read found=%t err=%v fault=%s", found, err, fault)
						}
						if fault == "exact" {
							if row.kind != "continued" || row.eventID.String != childEvent || row.sourceEventID.String != sourceEvent ||
								!row.disposition.Valid || row.disposition.String != "dead_lettered" || row.failurePresent || !row.createdAt.Equal(at) {
								t.Fatalf("outcome lost immutable binding: %+v", row)
							}
							repeated, found, err := loadRunForkContinuedOutcomeTx(ctx, tx, key, 0)
							if err != nil || !found || !reflect.DeepEqual(row, repeated) {
								t.Fatalf("readback changed retained outcome: %+v err=%v", repeated, err)
							}
						}
						var remaining int
						if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes`).Scan(&remaining); err != nil || remaining != count {
							t.Fatalf("readback mutated outcome rows: got=%d want=%d err=%v", remaining, count, err)
						}
					})
				}
			}
		})
	}
}
