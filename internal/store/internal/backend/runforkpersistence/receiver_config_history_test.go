package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestReceiverConfigHistoricalCaptureAndReadinessBothStores(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Consumer: canonicalrouting.TemplateInstanceAgentConsumer}))
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, opened, cleanup := testutil.StartPostgres(t)
				db = opened
				t.Cleanup(cleanup)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "receiver-history.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			jsonType := "TEXT"
			if backend == "postgres" {
				jsonType = "JSONB"
			}
			for _, ddl := range []string{
				`CREATE TEMP TABLE runs (run_id TEXT PRIMARY KEY)`,
				`CREATE TEMP TABLE run_fork_revision_heads (run_id TEXT PRIMARY KEY, last_revision BIGINT NOT NULL DEFAULT 0, updated_at TEXT)`,
				`CREATE TEMP TABLE run_fork_revisions (run_id TEXT, revision BIGINT, recorded_at TEXT)`,
				fmt.Sprintf(`CREATE TEMP TABLE run_fork_fact_revisions (run_id TEXT, revision BIGINT, family TEXT, fact_key TEXT, fact %s, present BOOLEAN)`, jsonType),
				`CREATE TEMP TABLE entity_state (run_id TEXT, entity_id TEXT, flow_instance TEXT, entity_type TEXT, slug TEXT, name TEXT, created_at TEXT)`,
				fmt.Sprintf(`CREATE TEMP TABLE flow_instances (run_id TEXT, instance_path TEXT, config %s)`, jsonType),
			} {
				if _, err := tx.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			plan, planning, _, modes, _ := workflowOwnershipProjection(t, source, "consumer")
			runID, entityID := plan.SourceRunID, plan.Entities[0].EntityID
			const path = "consumer/item"
			const config = `{"instance_id":"item","storage_ref":"consumer/item","flow_path":"consumer/item","workflow_version":"v1","config":{"vertical_id":"original-business-key","nested":[7,7.0,null],"status":false,"flow_path":["business","path"]}}`
			if _, err := tx.Exec(`INSERT INTO runs VALUES ($1)`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO entity_state VALUES ($1,$2,$3,'deployment','','','2026-09-21T00:00:00Z')`, runID, entityID, path); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO flow_instances VALUES ($1,$2,$3)`, runID, path, config); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO flow_instances VALUES ($1,$2,$3)`, "44444444-4444-4444-8444-444444444444", path, `{"config":{"vertical_id":"foreign-run"}}`); err != nil {
				t.Fatal(err)
			}
			var persisted string
			if err := tx.QueryRow(`SELECT config FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, path).Scan(&persisted); err != nil {
				t.Fatal(err)
			}
			effects := runforkrevision.NewEffects()
			if err := effects.AddFact(runID, runforkrevision.FamilyEntityMetadata, entityID); err != nil {
				t.Fatal(err)
			}
			finalize := runforkrevision.FinalizeSQLite
			if backend == "postgres" {
				finalize = runforkrevision.FinalizePostgres
			}
			result, err := finalize(context.Background(), tx, effects)
			if err != nil || !result[runID].Changed {
				t.Fatalf("capture existing entity/config rows: %#v %v", result, err)
			}
			var fact string
			if err := tx.QueryRow(`SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision=$3`, runID, entityID, result[runID].Revision).Scan(&fact); err != nil {
				t.Fatal(err)
			}
			t.Logf("source flow_instances.config=%s", persisted)
			t.Logf("ledger run=%s family=entity_metadata key=%s revision=%d fact=%s", runID, entityID, result[runID].Revision, fact)
			var record map[string]json.RawMessage
			if err := json.Unmarshal([]byte(fact), &record); err != nil {
				t.Fatal(err)
			}
			if _, exists := record["flow_config"]; !exists {
				t.Fatal("recorded entity metadata omitted existing flow_instances.config before snapshot or sealed-plan projection")
			}
			// A later current row cannot replace the selected historical config.
			if _, err := tx.Exec(`UPDATE flow_instances SET config=$1 WHERE run_id=$2`, strings.Replace(config, "[7,7.0,null]", "[7,7,null]", 1), runID); err != nil {
				t.Fatal(err)
			}
			numericChange, err := finalize(context.Background(), tx, effects)
			if err != nil || !numericChange[runID].Changed || numericChange[runID].Revision != 2 {
				t.Fatalf("numeric-kind change was not captured: %#v %v", numericChange, err)
			}
			later := `{"instance_id":"item","storage_ref":"consumer/item","flow_path":"consumer/item","config":{"vertical_id":"later-business-key"}}`
			if _, err := tx.Exec(`UPDATE flow_instances SET config=$1 WHERE run_id=$2`, later, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := finalize(context.Background(), tx, effects); err != nil {
				t.Fatal(err)
			}
			// The snapshot reader also requires an event at the selected revision.
			const eventID = "33333333-3333-4333-8333-333333333333"
			if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,1,'events',$2,$3,TRUE)`, runID, eventID, `{"event_id":"`+eventID+`","event_name":"proof","payload_base64":"e30="}`); err != nil {
				t.Fatal(err)
			}
			snapshot, err := loadRunForkRevisionSnapshot(context.Background(), tx, runID, 1)
			if err != nil {
				t.Fatal(err)
			}
			entities, admission, err := attachRunForkMaterializedEntitySnapshotMetadata(snapshot, plan.Entities)
			if err != nil || len(admission.Blockers) != 0 {
				t.Fatalf("attach recorded config: %#v %v", admission, err)
			}
			plan.Entities = entities
			plan.ForkPoint.Revision = snapshot.Revision
			projection, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{ExecutionPosture: executionposture.Live})
			if err != nil {
				t.Fatal(err)
			}
			want := `{"flow_path":["business","path"],"nested":[7,7.0,null],"status":false,"vertical_id":"original-business-key"}`
			for _, business := range []map[string]any{projection.States[0].Config, projection.Flows[0].Config} {
				got, err := canonicaljson.MarshalPreservingNumberKinds(business)
				if err != nil || string(got) != want {
					t.Fatalf("fixed-revision config = %s, want %s: %v", got, want, err)
				}
			}
			if string(plan.Entities[0].MaterializationMetadata.FlowConfig) != string(snapshot.EntityMetadata[0].FlowConfig) {
				t.Fatal("snapshot config changed during readiness projection")
			}
		})
	}
}

func TestSelectedForkStaticReceiverConfigPreservesBusinessControlCollisions(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyForkReceiverNestedOwnership(t, []canonicalrouting.ForkReceiver{{Path: "left", Policy: canonicalrouting.ForkReceiverOptionalExisting}}))
	for _, flow := range []string{".", "branch", "branch/left"} {
		t.Run(flow, func(t *testing.T) {
			plan, planning, _, modes, forkID := workflowOwnershipProjection(t, source, flow)
			business := map[string]any{"status": false, "flow_path": []any{"business", "path"}, "nested": []any{int64(7), float64(7)}}
			payload, err := pipeline.WorkflowInstanceConfigPayloadForRoute(flowidentity.RouteForInstancePath(plan.Entities[0].MaterializationMetadata.FlowInstance), source.WorkflowVersion(), business)
			if err != nil {
				t.Fatal(err)
			}
			plan.Entities[0].MaterializationMetadata.FlowConfig, err = canonicaljson.MarshalPreservingNumberKinds(payload)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{ExecutionPosture: executionposture.Live})
			if err != nil {
				t.Fatal(err)
			}
			state := projection.States[0]
			route, err := selectedContractProjectedWorkflowStateRoute(forkID, state)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := selectedContractWorkflowStateConfig(selectedContractWorkflowState{Route: route.InstancePath, WorkflowVersion: state.WorkflowVersion, Mode: state.Mode, Config: state.Config})
			if err != nil {
				t.Fatal(err)
			}
			readback, err := pipeline.WorkflowInstanceBusinessConfigForRoute(route, encoded)
			if err != nil {
				t.Fatal(err)
			}
			got, err := canonicaljson.MarshalPreservingNumberKinds(readback)
			want, _ := canonicaljson.MarshalPreservingNumberKinds(business)
			if err != nil || string(got) != string(want) {
				t.Fatalf("static fork receiver config = %s, want %s: %v", got, want, err)
			}
		})
	}
}
