package pipelinepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// Native observer controls; served workload and historical-cut proofs remain
// separate. Each hostile change is rolled back before full pair conservation.
func TestQ10H2HubWitnessUsesCanonicalHeaderBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(q10Dialect(postgres), func(t *testing.T) {
			db := q10FreshDatabase(t, postgres)
			ctx := context.Background()
			run, entity := uuid.NewString(), uuid.NewString()
			if postgres {
				materializePostgresStatementRuns(t, db, false, run)
			} else {
				materializeSQLiteStatementRuns(t, db, false, run)
			}
			at := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
			record := pipeline.WorkflowEngineStateRecord{
				Identity: flowidentity.RunScopedFlowInstance{RunID: run, Route: flowidentity.StoredRoute("hub", "h01", "hub/h01")},
				EntityID: entity, EntityType: "hub", WorkflowName: "hub", Mode: "template", InstanceKey: "h01", Status: "active", StageDefined: true,
				CurrentState: "active", Fields: []byte(`{"hub_id":"h01","count":0,"c1":0,"c2":0}`),
				Gates: []byte(`{}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`),
				Config:    []byte(`{"instance_id":"h01","storage_ref":"hub/h01","flow_path":"hub/h01"}`),
				CreatedAt: at, UpdatedAt: at, EnteredStageAt: at,
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := commitWorkflowHeaderAndFields(ctx, tx, postgres, record, workflowEngineStateDecision{createState: true}); err != nil {
				t.Fatal(err)
			}
			root := record
			root.Identity.Route = flowidentity.StoredRoute(".", run, run)
			root.EntityID, root.EntityType, root.WorkflowName = run, "", "."
			root.Mode, root.Fields = "static", nil
			root.Config, err = json.Marshal(map[string]string{"instance_id": run, "storage_ref": run, "flow_path": run})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := commitWorkflowHeaderAndFields(ctx, tx, postgres, root, workflowEngineStateDecision{createState: true}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			before, err := loadWorkflowTargetPersistence(ctx, db, record.Identity, identity.NormalizeEntityID(entity), !postgres)
			if err != nil {
				t.Fatal(err)
			}
			var rootData int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, run, run).Scan(&rootData); err != nil || rootData != 0 {
				t.Fatalf("fieldless root acquired data membership: count=%d err=%v", rootData, err)
			}
			for _, test := range []struct{ name, mutation, want string }{
				{"complete", "", ""},
				{"missing_data", `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, "H2 fielded header/data cardinality disagreement: headers=1 data=0"},
				{"missing_header", `DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, "H2 hub " + entity + " lacks named lifecycle evidence"},
				{"foreign_path", `UPDATE entity_state SET flow_instance='hub/foreign' WHERE run_id=$1 AND entity_id=$2`, "workflow target lifecycle companion is incomplete or disagrees with exact receiver route"},
				{"foreign_type", `UPDATE entity_state SET entity_type='foreign' WHERE run_id=$1 AND entity_id=$2`, "workflow target field row disagrees with constructed header contract"},
				{"no_stage", `UPDATE flow_instances SET stage_defined=FALSE,current_state='pending' WHERE run_id=$1 AND entity_id=$2`, "H2 hub " + entity + " lacks named lifecycle evidence"},
				{"invalid_fields", `UPDATE entity_state SET fields='null' WHERE run_id=$1 AND entity_id=$2`, "workflow target fields require a persisted JSON object"},
			} {
				t.Run(test.name, func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if test.mutation != "" {
						result, err := tx.ExecContext(ctx, test.mutation, run, entity)
						if err != nil {
							t.Fatal(err)
						}
						if affected, err := result.RowsAffected(); err != nil || affected != 1 {
							t.Fatalf("hostile observation changed %d rows, want one: %v", affected, err)
						}
					}
					got, err := observeH2HubsForTest(ctx, tx, run)
					if test.want != "" {
						if err == nil || err.Error() != test.want || got != nil {
							t.Fatalf("wrong H2 refusal: evidence=%+v err=%v want=%q", got, err, test.want)
						}
					} else {
						if err != nil || len(got) != 1 || got[0].Entity != entity || got[0].Instance != "hub/h01" || got[0].Stage != "active" || got[0].Revision != 1 {
							t.Fatalf("wrong constructed H2 witness: %+v err=%v", got, err)
						}
						var wantFields, gotFields map[string]any
						if err := json.Unmarshal(record.Fields, &wantFields); err != nil {
							t.Fatal(err)
						}
						if err := json.Unmarshal(got[0].Fields, &gotFields); err != nil || !reflect.DeepEqual(wantFields, gotFields) {
							t.Fatalf("H2 field/config evidence changed: %+v err=%v", got, err)
						}
						var wantConfig, gotConfig map[string]any
						if err := json.Unmarshal(record.Config, &wantConfig); err != nil {
							t.Fatal(err)
						}
						if err := json.Unmarshal(got[0].Config, &gotConfig); err != nil || !reflect.DeepEqual(wantConfig, gotConfig) {
							t.Fatalf("H2 full configuration changed: %+v err=%v", got, err)
						}
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					after, err := loadWorkflowTargetPersistence(ctx, db, record.Identity, identity.NormalizeEntityID(entity), !postgres)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("observer control changed the complete header/data pair: before=%+v after=%+v err=%v", before, after, err)
					}
				})
			}
		})
	}
}
