package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const workflowInitialMaterializationProjectionVersion = 2

func validateWorkflowInitialEntry(source semanticview.Source, instance WorkflowInstance, initialStage string) error {
	graph, found := semanticview.WorkflowStageTopology(source, instance.WorkflowName)
	if !found || graph.FlowID != instance.WorkflowName {
		return fmt.Errorf("workflow initial entry requires the exact compiled flow %q", instance.WorkflowName)
	}
	want := graph.InitialStage
	// Stateless instances retain the existing storage posture, not a graph edge.
	if want == "" && len(graph.Stages) == 0 {
		want = "pending"
	}
	if want == "" || initialStage != want || instance.CurrentState != want {
		return fmt.Errorf("workflow initial entry must match canonical initial stage %q and prepared state %q, got %q", want, instance.CurrentState, initialStage)
	}
	return nil
}

// workflowInitialMaterializationProjection is immutable creation identity.
// Mutable workflow progress is deliberately absent from replay comparison.
type workflowInitialMaterializationProjection struct {
	Version         int                                 `json:"version"`
	RunID           string                              `json:"run_id"`
	EntityID        string                              `json:"entity_id"`
	FlowInstance    string                              `json:"flow_instance"`
	WorkflowName    string                              `json:"workflow_name"`
	WorkflowVersion string                              `json:"workflow_version"`
	InitialState    string                              `json:"initial_state"`
	OccurredAt      time.Time                           `json:"occurred_at"`
	Persisted       workflowInstancePersistedProjection `json:"persisted"`
}

func newWorkflowInitialMaterializationProjection(
	ctx context.Context,
	identity runtimeflowidentity.Persisted,
	instance WorkflowInstance,
	occurredAt time.Time,
) (workflowInitialMaterializationProjection, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	persisted, err := workflowInstancePersistedProjectionFromInstance(instance, identity.StorageRef)
	if err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	projection := workflowInitialMaterializationProjection{
		Version:         workflowInitialMaterializationProjectionVersion,
		RunID:           strings.TrimSpace(runID),
		EntityID:        strings.TrimSpace(identity.RowID()),
		FlowInstance:    strings.Trim(strings.TrimSpace(identity.StorageRef), "/"),
		WorkflowName:    strings.TrimSpace(instance.WorkflowName),
		WorkflowVersion: strings.TrimSpace(instance.WorkflowVersion),
		InitialState:    strings.TrimSpace(instance.CurrentState),
		OccurredAt:      canonicalWorkflowInstancePersistedTime(occurredAt),
		Persisted:       persisted,
	}
	if projection.RunID == "" || projection.EntityID == "" || projection.FlowInstance == "" ||
		projection.WorkflowName == "" || projection.WorkflowVersion == "" ||
		projection.InitialState == "" || projection.OccurredAt.IsZero() {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("workflow initial materialization projection requires exact identity, workflow, state, and occurrence")
	}
	return projection, nil
}
