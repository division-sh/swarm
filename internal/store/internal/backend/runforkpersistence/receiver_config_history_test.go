package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRuntimeHeaderHistoricalCaptureAndReadinessBothStores(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyReceiverConfigHistory(t))
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
				`CREATE TEMP TABLE runs (run_id TEXT PRIMARY KEY, origin_kind TEXT, trigger_event_id TEXT, trigger_event_type TEXT, origin_service_id TEXT, origin_generation BIGINT, forked_from_run_id TEXT, forked_from_point_kind TEXT, forked_from_revision BIGINT, forked_from_event_id TEXT)`,
				`CREATE TEMP TABLE run_fork_revision_heads (run_id TEXT PRIMARY KEY, last_revision BIGINT NOT NULL DEFAULT 0, updated_at TEXT)`,
				`CREATE TEMP TABLE run_fork_revisions (run_id TEXT, revision BIGINT, recorded_at TEXT)`,
				fmt.Sprintf(`CREATE TEMP TABLE run_fork_fact_revisions (run_id TEXT, revision BIGINT, family TEXT, fact_key TEXT, fact %s, present BOOLEAN)`, jsonType),
				`CREATE TEMP TABLE entity_state (run_id TEXT, entity_id TEXT, flow_instance TEXT, entity_type TEXT, slug TEXT, name TEXT, created_at TEXT)`,
				fmt.Sprintf(`CREATE TEMP TABLE entity_mutations (run_id TEXT, mutation_id TEXT, entity_id TEXT, domain TEXT, path TEXT, new_value %s, caused_by_event TEXT, created_at TEXT)`, jsonType),
				fmt.Sprintf(`CREATE TEMP TABLE flow_instances (run_id TEXT, instance_path TEXT, entity_id TEXT, entity_type TEXT, slug TEXT, name TEXT, config %s, stage_defined BOOLEAN, flow_template TEXT, mode TEXT, parent_instance TEXT, instance_key TEXT, status TEXT, current_state TEXT, entered_state_at TEXT, created_at TEXT, updated_at TEXT, terminated_at TEXT)`, jsonType),
				fmt.Sprintf(`CREATE TEMP TABLE workflow_instance_initial_materializations (run_id TEXT, instance_path TEXT, entity_id TEXT, projection_version INTEGER NOT NULL, projection %s)`, jsonType),
			} {
				if _, err := tx.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			plan, planning, _, modes, _ := workflowOwnershipProjection(t, source, "consumer")
			runID, entityID := plan.SourceRunID, plan.Entities[0].EntityID
			const path = "consumer/item"
			parent := workflowOwnershipKeylessInstance(t, source, runID, ".")
			instance, err := flowidentity.KeyedChild(source, parent, "consumer", "item")
			if err != nil {
				t.Fatal(err)
			}
			instance.EntityID = entityID
			payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(instance, "v1")
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := canonicaljson.MarshalPreservingNumberKinds(payload)
			if err != nil {
				t.Fatal(err)
			}
			config := string(encoded)
			if _, err := tx.Exec(`INSERT INTO runs (run_id,origin_kind) VALUES ($1,'deployment')`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO entity_state VALUES ($1,$2,$3,'deployment','','','2026-09-21T00:00:00Z')`, runID, entityID, path); err != nil {
				t.Fatal(err)
			}
			insertHeader := `INSERT INTO flow_instances (run_id,instance_path,entity_id,entity_type,slug,name,config,stage_defined,flow_template,mode,parent_instance,instance_key,status,current_state,entered_state_at,created_at,updated_at)
				VALUES ($1,$2,$3,'deployment','','',$4,TRUE,'consumer','template',$6,$7,'active',$5,'2026-09-21T00:00:00Z','2026-09-21T00:00:00Z','2026-09-21T00:00:00Z')`
			metadata := plan.Entities[0].MaterializationMetadata
			bundle, found := semanticview.Bundle(source)
			if !found || bundle.SourceArtifact == nil {
				t.Fatal("historical capture fixture requires its exact admitted artifact")
			}
			at := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
			construction := selectedWorkflowConstructionRecordFixture(t, runID, bundle.SourceArtifact.BundleHash(), instance, pipeline.WorkflowInstance{
				WorkflowVersion: "v1", Mode: metadata.Mode, Status: "active", EntityType: "deployment", InstanceKey: metadata.InstanceKey,
				CurrentState: plan.Entities[0].CurrentState, Fields: plan.Entities[0].Fields, StageDefined: true, EnteredStageAt: at, CreatedAt: at,
			}, executionmode.Live)
			metadata.InitialMaterialization = construction.InitialMaterialization
			if _, err := tx.Exec(insertHeader, runID, path, entityID, config, plan.Entities[0].CurrentState, instance.ParentRoute.FlowInstance, metadata.InstanceKey); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO workflow_instance_initial_materializations (run_id,instance_path,entity_id,projection_version,projection) VALUES ($1,$2,$3,$4,$5)`, runID, path, entityID, pipeline.FlowConstructionReceiptVersion, string(metadata.InitialMaterialization)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(insertHeader, "44444444-4444-4444-8444-444444444444", path, entityID, strings.Replace(config, "v1", "foreign-run", 1), plan.Entities[0].CurrentState, "44444444-4444-4444-8444-444444444444", metadata.InstanceKey); err != nil {
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
			const mutationID = "55555555-5555-4555-8555-555555555555"
			initialState, err := json.Marshal(plan.Entities[0].CurrentState)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO entity_mutations (run_id,mutation_id,entity_id,domain,path,new_value,created_at) VALUES ($1,$2,$3,'lifecycle_state','',$4,'2026-09-21T00:00:00Z')`, runID, mutationID, entityID, string(initialState)); err != nil {
				t.Fatal(err)
			}
			if err := effects.AddFact(runID, runforkrevision.FamilyEntityMutations, mutationID); err != nil {
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
			// A later header cannot replace the selected historical version.
			if _, err := tx.Exec(`UPDATE flow_instances SET config=$1 WHERE run_id=$2`, strings.Replace(config, "v1", "v2", 1), runID); err != nil {
				t.Fatal(err)
			}
			numericChange, err := finalize(context.Background(), tx, effects)
			if err != nil || !numericChange[runID].Changed || numericChange[runID].Revision != 2 {
				t.Fatalf("version change was not captured: %#v %v", numericChange, err)
			}
			later := strings.Replace(config, "v1", "v3", 1)
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
			if projection.States[0].WorkflowVersion != source.WorkflowVersion() {
				t.Fatalf("selected readiness lost the selected source: %+v", projection.States[0])
			}
			var historicalHeader map[string]any
			if err := canonicaljson.DecodeInto(plan.Entities[0].MaterializationMetadata.FlowConfig, &historicalHeader); err != nil {
				t.Fatal(err)
			}
			if historicalHeader["workflow_version"] != "v1" {
				t.Fatalf("selected historical version replaced by current header: %+v", historicalHeader)
			}
			if string(plan.Entities[0].MaterializationMetadata.FlowConfig) != string(snapshot.EntityMetadata[0].FlowConfig) {
				t.Fatal("snapshot config changed during readiness projection")
			}
		})
	}
}

func TestSelectedForkHeaderDoesNotDuplicateBusinessControlNames(t *testing.T) {
	for _, flow := range []string{".", "branch", "branch/left"} {
		t.Run(flow, func(t *testing.T) {
			dir := canonicalrouting.CopyForkReceiverNestedOwnership(t, []canonicalrouting.ForkReceiver{
				{Path: "left", Policy: canonicalrouting.ForkReceiverOptionalExisting},
				{Path: "right", Policy: canonicalrouting.ForkReceiverOptionalExisting},
			})
			source := workflowOwnershipSource(t, dir)
			plan, planning, _, modes, forkID := workflowOwnershipProjection(t, source, flow)
			business := map[string]any{"status": false, "flow_path": []any{"business", "path"}, "nested": []any{int64(7), float64(7)}}
			plan.Entities[0].Fields = business
			want, _ := canonicaljson.MarshalPreservingNumberKinds(business)
			instance := workflowOwnershipKeylessInstance(t, source, plan.SourceRunID, flow)
			payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(instance, source.WorkflowVersion())
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
			owner, err := ProjectRunForkEntityOwnership(plan.SourceRunID, forkID, state.EntityID, plan.Entities[0].MaterializationMetadata.FlowInstance)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := selectedContractWorkflowStateConfig(selectedContractWorkflowState{
				SourceRunID: plan.SourceRunID, RunID: forkID, EntityID: owner.Fork.EntityID, WorkflowName: state.FlowID,
				Route: route.InstancePath, WorkflowVersion: state.WorkflowVersion, Mode: state.Mode, History: plan.Entities[0],
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pipeline.DecodeWorkflowInstanceRecordedHeader(route, encoded); err != nil {
				t.Fatal(err)
			}
			var header map[string]any
			if err := canonicaljson.DecodeInto(encoded, &header); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"config", "nested"} {
				if _, exists := header[key]; exists {
					t.Fatalf("business copy in header: %s", encoded)
				}
			}
			got, err := canonicaljson.MarshalPreservingNumberKinds(plan.Entities[0].Fields)
			if err != nil || string(got) != string(want) {
				t.Fatalf("header projection rewrote entity state = %s, want %s: %v", got, want, err)
			}
		})
	}
}
