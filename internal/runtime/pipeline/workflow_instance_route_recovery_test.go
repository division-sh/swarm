package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStoreLoadRouteRecoveryProjection(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*sql.DB, *workflowInstanceStore)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*sql.DB, *workflowInstanceStore) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				return db, newSQLiteWorkflowInstanceStoreForTest(t, db)
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*sql.DB, *workflowInstanceStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return db, newPostgresWorkflowInstanceStoreForTest(db)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, store := tc.setup(t)
			runID := uuid.NewString()
			ensurePipelineTestRun(t, store, runID)
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
			scopeKey := "route-recovery-" + uuid.NewString()
			instanceID := "inst-1"
			instancePath := scopeKey + "/" + instanceID
			entityID := uuid.NewString()
			parentEntityID := uuid.NewString()
			route := runtimeflowidentity.StoredRoute(scopeKey, instanceID, instancePath)
			flowIdentity := testRunScopedWorkflowRoute(ctx, route)
			config := map[string]any{
				"workflow_version":     "1.0.0",
				"instance_id":          instanceID,
				"storage_ref":          instancePath,
				"flow_path":            instancePath,
				"instance_kind":        "materialized",
				"parent_flow_id":       "parent",
				"parent_flow_instance": "parent/root",
				"parent_entity_id":     parentEntityID,
				"vertical_id":          "vertical-1",
			}
			configRaw, err := json.Marshal(config)
			if err != nil {
				t.Fatalf("marshal config: %v", err)
			}

			insert := "INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"
			if tc.name == "sqlite" {
				// SQLite uses the portable statement above.
			} else {
				insert = "INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, NOW())"
			}
			if _, err := db.ExecContext(ctx, insert, runID, instancePath, "review", "template", string(configRaw), "active"); err != nil {
				t.Fatalf("seed active flow instance: %v", err)
			}
			entityInsert := "INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES (?, ?, ?, 'test_entity', 'active')"
			if tc.name == "postgres" {
				entityInsert = "INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state) VALUES ($1::uuid, $2::uuid, $3, 'test_entity', 'active')"
			}
			if _, err := db.ExecContext(ctx, entityInsert, runID, entityID, instancePath); err != nil {
				t.Fatalf("seed active flow entity: %v", err)
			}
			projection, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity)
			if err != nil {
				t.Fatalf("LoadRouteRecoveryProjection: %v", err)
			}
			if got := projection.Identity.Route(); got != route {
				t.Fatalf("projection route = %#v, want %#v", got, route)
			}
			if got := projection.Identity.EntityID; got != entityID {
				t.Fatalf("projection entity_id = %q, want persisted entity %q", got, entityID)
			}
			if got := projection.Identity.ParentRoute; got.FlowID != "parent" || got.FlowInstance != "parent/root" || got.EntityID != parentEntityID {
				t.Fatalf("projection parent route = %#v, want complete persisted parent", got)
			}
			if got := strings.TrimSpace(asString(projection.Config["vertical_id"])); got != "vertical-1" {
				t.Fatalf("projection config vertical_id = %q, want vertical-1", got)
			}
			projection.Config["vertical_id"] = "mutated"
			again, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity)
			if err != nil {
				t.Fatalf("reload route recovery projection: %v", err)
			}
			if got := strings.TrimSpace(asString(again.Config["vertical_id"])); got != "vertical-1" {
				t.Fatalf("persisted config aliased returned projection: got %q", got)
			}

			historicalRunID := uuid.NewString()
			ensurePipelineTestRun(t, store, historicalRunID)
			if _, err := db.ExecContext(ctx, insert, historicalRunID, instancePath, "review", "template", string(configRaw), "active"); err != nil {
				t.Fatalf("seed same path in another run: %v", err)
			}
			retiredEntityID := uuid.NewString()
			if _, err := db.ExecContext(ctx, entityInsert, historicalRunID, retiredEntityID, instancePath); err != nil {
				t.Fatalf("seed same owner in another run: %v", err)
			}
			projection, err = store.LoadRouteRecoveryProjection(ctx, flowIdentity)
			if err != nil || projection.Identity.EntityID != entityID {
				t.Fatalf("same path in another run escaped exact recovery: projection=%#v err=%v", projection, err)
			}
			historicalIdentity := testRunScopedWorkflowInstanceForRun(historicalRunID, instancePath)
			historicalProjection, err := store.LoadRouteRecoveryProjection(ctx, historicalIdentity)
			if err != nil || historicalProjection.Identity.EntityID != retiredEntityID {
				t.Fatalf("historical exact recovery = %#v err=%v, want owner %s", historicalProjection, err, retiredEntityID)
			}
			transitionWorkflowActivationRunForTest(t, store, ctx, historicalRunID, runtimerunlifecycle.StateCancelled)
			statusQuery := "SELECT status FROM runs WHERE run_id = ?"
			if tc.name == "postgres" {
				statusQuery = "SELECT status FROM runs WHERE run_id = $1::uuid"
			}
			var historicalStatus string
			if err := db.QueryRowContext(ctx, statusQuery, historicalRunID).Scan(&historicalStatus); err != nil {
				t.Fatalf("load retired historical run status: %v", err)
			}
			if historicalStatus != string(runtimerunlifecycle.StateCancelled) {
				t.Fatalf("historical run status = %q, want cancelled", historicalStatus)
			}
			projection, err = store.LoadRouteRecoveryProjection(ctx, flowIdentity)
			if err != nil {
				t.Fatalf("recover current owner with retired predecessor: %v", err)
			}
			if projection.Identity.EntityID != entityID {
				t.Fatalf("recovered entity_id = %q, want current owner %q instead of retired owner %q", projection.Identity.EntityID, entityID, retiredEntityID)
			}

			t.Run("route identity mismatch", func(t *testing.T) {
				mismatched := runtimeflowidentity.StoredRoute(scopeKey, "wrong-instance", instancePath)
				_, err := store.LoadRouteRecoveryProjection(ctx, testRunScopedWorkflowRoute(ctx, mismatched))
				if err == nil || !strings.Contains(err.Error(), "disagrees with requested route") {
					t.Fatalf("mismatched route error = %v, want exact-route mismatch", err)
				}
			})

			t.Run("missing or ambiguous entity owner", func(t *testing.T) {
				deleteEntity := "DELETE FROM entity_state WHERE run_id = ? AND flow_instance = ?"
				if tc.name == "postgres" {
					deleteEntity = "DELETE FROM entity_state WHERE run_id = $1::uuid AND flow_instance = $2"
				}
				if _, err := db.ExecContext(ctx, deleteEntity, runID, instancePath); err != nil {
					t.Fatal(err)
				}
				if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "exactly one current persisted entity owner") {
					t.Fatalf("missing owner error = %v", err)
				}
				if _, err := db.ExecContext(ctx, entityInsert, runID, entityID, instancePath); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, entityInsert, runID, uuid.NewString(), instancePath); err != nil {
					t.Fatal(err)
				}
				if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "exactly one current persisted entity owner") {
					t.Fatalf("ambiguous owner error = %v", err)
				}
				if _, err := db.ExecContext(ctx, deleteEntity, runID, instancePath); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, entityInsert, runID, entityID, instancePath); err != nil {
					t.Fatal(err)
				}
			})

			badConfig := `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"` + instancePath + `","flow_path":7}`
			updateConfig := "UPDATE flow_instances SET config = "
			if tc.name == "sqlite" {
				updateConfig += "? WHERE run_id = ? AND instance_path = ?"
				_, err = db.ExecContext(ctx, updateConfig, badConfig, runID, instancePath)
			} else {
				updateConfig += "$1::jsonb WHERE run_id = $2::uuid AND instance_path = $3"
				_, err = db.ExecContext(ctx, updateConfig, badConfig, runID, instancePath)
			}
			if err != nil {
				t.Fatalf("write malformed config: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "flow_path must be a string") {
				t.Fatalf("malformed config error = %v, want flow_path teaching failure", err)
			}

			statusUpdate := "UPDATE flow_instances SET status = "
			if tc.name == "sqlite" {
				statusUpdate += "? WHERE run_id = ? AND instance_path = ?"
				_, err = db.ExecContext(ctx, statusUpdate, "terminated", runID, instancePath)
			} else {
				statusUpdate += "$1 WHERE run_id = $2::uuid AND instance_path = $3"
				_, err = db.ExecContext(ctx, statusUpdate, "terminated", runID, instancePath)
			}
			if err != nil {
				t.Fatalf("mark flow instance inactive: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("inactive row error = %v, want active-row failure", err)
			}

			deleteQuery := "DELETE FROM flow_instances WHERE run_id = ? AND instance_path = ?"
			if tc.name == "postgres" {
				deleteQuery = "DELETE FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2"
			}
			if _, err := db.ExecContext(ctx, deleteQuery, runID, instancePath); err != nil {
				t.Fatalf("delete flow instance: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("missing row error = %v, want active-row failure", err)
			}
		})
	}
}

func TestWorkflowInstanceStoreLoadRouteRecoveryProjectionRejectsTerminatedTimestamp(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newTestSQLiteWorkflowInstanceStore(db)
	runID := uuid.NewString()
	ensurePipelineTestRun(t, store, runID)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	route := runtimeflowidentity.StoredRoute("review", "inst-1", "review/inst-1")
	flowIdentity := testRunScopedWorkflowRoute(ctx, route)
	config := `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"review/inst-1","flow_path":"review/inst-1"}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, terminated_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, runID, route.InstancePath, "review", "template", config, "active", time.Now().UTC()); err != nil {
		t.Fatalf("seed terminated flow instance: %v", err)
	}
	if _, err := store.LoadRouteRecoveryProjection(ctx, flowIdentity); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
		t.Fatalf("terminated row error = %v, want active-row failure", err)
	}
}
