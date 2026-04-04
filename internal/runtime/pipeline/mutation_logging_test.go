package pipeline

import (
	"context"
	"testing"

	"github.com/google/uuid"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/testutil"
)

func TestUpdateEntityState_LogsMutationRowForStateTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)

	entityID := uuid.NewString()
	pc := &PipelineCoordinator{
		workflowStore: NewWorkflowInstanceStore(db),
		module: &previewWorkflowModule{
			bundle: &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					Name:    "mutation-flow",
					Version: "1.0.0",
				},
			},
		},
	}
	if err := pc.workflowStore.Upsert(context.Background(), WorkflowInstance{
		InstanceID:      entityID,
		StorageRef:      entityID,
		WorkflowName:    "mutation-flow",
		WorkflowVersion: "1.0.0",
		CurrentState:    "queued",
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	if err := pc.updateEntityState(context.Background(), entityID, "done", "flow.transitioned"); err != nil {
		t.Fatalf("updateEntityState: %v", err)
	}

	var (
		field      string
		oldValue   string
		newValue   string
		writerType string
		step       string
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT
			COALESCE(field, ''),
			COALESCE(old_value::text, ''),
			COALESCE(new_value::text, ''),
			COALESCE(writer_type, ''),
			COALESCE(handler_step, '')
		FROM entity_mutations
		WHERE entity_id = $1::uuid AND field = 'current_state'
		ORDER BY created_at DESC
		LIMIT 1
	`, entityID).Scan(&field, &oldValue, &newValue, &writerType, &step); err != nil {
		t.Fatalf("load entity mutation: %v", err)
	}
	if field != "current_state" {
		t.Fatalf("mutation field = %q, want current_state", field)
	}
	if oldValue != `"queued"` {
		t.Fatalf("mutation old_value = %s, want \"queued\"", oldValue)
	}
	if newValue != `"done"` {
		t.Fatalf("mutation new_value = %s, want \"done\"", newValue)
	}
	if writerType == "" {
		t.Fatal("mutation writer_type is empty")
	}
	if step == "" {
		t.Fatal("mutation handler_step is empty")
	}
}
