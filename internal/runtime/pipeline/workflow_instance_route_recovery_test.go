package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestWorkflowInstanceStoreLoadRouteRecoveryProjection(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*sql.DB, *WorkflowInstanceStore)
		bind  string
		json  string
		now   string
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*sql.DB, *WorkflowInstanceStore) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				return db, NewSQLiteWorkflowInstanceStore(db)
			},
			bind: "?",
			json: "?",
			now:  "CURRENT_TIMESTAMP",
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*sql.DB, *WorkflowInstanceStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return db, NewWorkflowInstanceStore(db)
			},
			bind: "$1",
			json: "$4::jsonb",
			now:  "NOW()",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, store := tc.setup(t)
			scopeKey := "route-recovery-" + uuid.NewString()
			instanceID := "inst-1"
			instancePath := scopeKey + "/" + instanceID
			parentEntityID := uuid.NewString()
			route := runtimeflowidentity.StoredRoute(scopeKey, instanceID, instancePath)
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

			insert := "INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES (" + tc.bind + ", "
			if tc.name == "sqlite" {
				insert += "?, ?, " + tc.json + ", ?, " + tc.now + ")"
			} else {
				insert += "$2, $3, " + tc.json + ", $5, " + tc.now + ")"
			}
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), insert, instancePath, "review", "template", string(configRaw), "active"); err != nil {
				t.Fatalf("seed active flow instance: %v", err)
			}

			projection, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route)
			if err != nil {
				t.Fatalf("LoadRouteRecoveryProjection without run context: %v", err)
			}
			if got := projection.Identity.Route(); got != route {
				t.Fatalf("projection route = %#v, want %#v", got, route)
			}
			if got := projection.Identity.EntityID; got != runtimeflowidentity.EntityID(instancePath) {
				t.Fatalf("projection entity_id = %q, want deterministic id for %q", got, instancePath)
			}
			if got := projection.Identity.ParentRoute; got.FlowID != "parent" || got.FlowInstance != "parent/root" || got.EntityID != parentEntityID {
				t.Fatalf("projection parent route = %#v, want complete persisted parent", got)
			}
			if got := strings.TrimSpace(asString(projection.Config["vertical_id"])); got != "vertical-1" {
				t.Fatalf("projection config vertical_id = %q, want vertical-1", got)
			}
			projection.Config["vertical_id"] = "mutated"
			again, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route)
			if err != nil {
				t.Fatalf("reload route recovery projection: %v", err)
			}
			if got := strings.TrimSpace(asString(again.Config["vertical_id"])); got != "vertical-1" {
				t.Fatalf("persisted config aliased returned projection: got %q", got)
			}

			t.Run("route identity mismatch", func(t *testing.T) {
				mismatched := runtimeflowidentity.StoredRoute("wrong-scope", instanceID, instancePath)
				_, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), mismatched)
				if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
					t.Fatalf("mismatched route error = %v, want identity mismatch", err)
				}
			})

			badConfig := `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"` + instancePath + `","flow_path":7}`
			updateConfig := "UPDATE flow_instances SET config = "
			if tc.name == "sqlite" {
				updateConfig += "? WHERE instance_id = ?"
				_, err = db.ExecContext(testAuthorActivityContext(context.Background()), updateConfig, badConfig, instancePath)
			} else {
				updateConfig += "$1::jsonb WHERE instance_id = $2"
				_, err = db.ExecContext(testAuthorActivityContext(context.Background()), updateConfig, badConfig, instancePath)
			}
			if err != nil {
				t.Fatalf("write malformed config: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route); err == nil || !strings.Contains(err.Error(), "flow_path must be a string") {
				t.Fatalf("malformed config error = %v, want flow_path teaching failure", err)
			}

			statusUpdate := "UPDATE flow_instances SET status = "
			if tc.name == "sqlite" {
				statusUpdate += "? WHERE instance_id = ?"
				_, err = db.ExecContext(testAuthorActivityContext(context.Background()), statusUpdate, "terminated", instancePath)
			} else {
				statusUpdate += "$1 WHERE instance_id = $2"
				_, err = db.ExecContext(testAuthorActivityContext(context.Background()), statusUpdate, "terminated", instancePath)
			}
			if err != nil {
				t.Fatalf("mark flow instance inactive: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("inactive row error = %v, want active-row failure", err)
			}

			deleteQuery := "DELETE FROM flow_instances WHERE instance_id = " + tc.bind
			if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), deleteQuery, instancePath); err != nil {
				t.Fatalf("delete flow instance: %v", err)
			}
			if _, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("missing row error = %v, want active-row failure", err)
			}
		})
	}
}

func TestWorkflowInstanceStoreLoadRouteRecoveryProjectionRejectsTerminatedTimestamp(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	route := runtimeflowidentity.StoredRoute("review", "inst-1", "review/inst-1")
	config := `{"workflow_version":"1.0.0","instance_id":"inst-1","storage_ref":"review/inst-1","flow_path":"review/inst-1"}`
	if _, err := db.ExecContext(testAuthorActivityContext(context.Background()), `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, route.InstancePath, "review", "template", config, "active", time.Now().UTC()); err != nil {
		t.Fatalf("seed terminated flow instance: %v", err)
	}
	if _, err := store.LoadRouteRecoveryProjection(testAuthorActivityContext(context.Background()), route); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
		t.Fatalf("terminated row error = %v, want active-row failure", err)
	}
}
