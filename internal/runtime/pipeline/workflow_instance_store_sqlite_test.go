package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

func TestSQLiteWorkflowInstanceStore_PreservesCreateEntityInitialValueMutationRows(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = withWorkflowCreateEntityInitialValues(ctx, map[string]any{
		"region": "west",
		"tier":   float64(1),
	})
	storageRef := "root/acme"
	entityID := FlowInstanceEntityID(storageRef)

	if err := store.Create(ctx, WorkflowInstance{
		InstanceID:      "acme",
		StorageRef:      storageRef,
		WorkflowName:    "root",
		WorkflowVersion: "v1",
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Metadata: map[string]any{
			"flow_path": storageRef,
			"region":    "west",
			"tier":      float64(2),
		},
	}); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	assertSQLiteMutationCount(t, db, entityID, "region", "entity_initial_value", "create_entity", "null", `"west"`, 1)
	assertSQLiteMutationCount(t, db, entityID, "region", "workflow_instance_store", "create", "", "", 0)
	assertSQLiteMutationCount(t, db, entityID, "tier", "entity_initial_value", "create_entity", "null", "1", 1)
	assertSQLiteMutationCount(t, db, entityID, "tier", "workflow_instance_store", "create", "1", "2", 1)
}

func newSQLiteWorkflowInstanceStoreTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE runs (
			run_id TEXT PRIMARY KEY,
			status TEXT,
			started_at TIMESTAMP
		)`,
		`CREATE TABLE flow_instances (
			instance_id TEXT PRIMARY KEY,
			flow_template TEXT,
			mode TEXT,
			config TEXT,
			status TEXT,
			terminated_at TIMESTAMP,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE entity_state (
			run_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			entity_type TEXT,
			slug TEXT,
			name TEXT,
			current_state TEXT,
			gates TEXT,
			fields TEXT,
			accumulator TEXT,
			revision INTEGER,
			entered_state_at TIMESTAMP,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			PRIMARY KEY (run_id, entity_id)
		)`,
		`CREATE TABLE timers (
			timer_id TEXT PRIMARY KEY,
			run_id TEXT,
			timer_name TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			fire_event TEXT,
			fire_payload TEXT,
			fire_at TIMESTAMP,
			recurring BOOLEAN,
			owner_node TEXT,
			owner_agent TEXT,
			task_type TEXT,
			status TEXT,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE entity_mutations (
			mutation_id TEXT PRIMARY KEY,
			run_id TEXT,
			entity_id TEXT,
			field TEXT,
			old_value TEXT,
			new_value TEXT,
			caused_by_event TEXT,
			writer_type TEXT,
			writer_id TEXT,
			handler_step TEXT,
			created_at TIMESTAMP
		)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create sqlite test schema: %v", err)
		}
	}
	return db
}

func assertSQLiteMutationCount(t *testing.T, db *sql.DB, entityID, field, writerID, handlerStep, oldValue, newValue string, want int) {
	t.Helper()
	query := `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE entity_id = ?
		  AND field = ?
		  AND writer_id = ?
		  AND handler_step = ?
	`
	args := []any{entityID, field, writerID, handlerStep}
	if oldValue != "" {
		query += ` AND old_value = ?`
		args = append(args, oldValue)
	}
	if newValue != "" {
		query += ` AND new_value = ?`
		args = append(args, newValue)
	}
	var got int
	if err := db.QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatalf("count sqlite mutation rows: %v", err)
	}
	if got != want {
		t.Fatalf("mutation count for field=%s writer=%s step=%s old=%s new=%s = %d, want %d", field, writerID, handlerStep, oldValue, newValue, got, want)
	}
}
