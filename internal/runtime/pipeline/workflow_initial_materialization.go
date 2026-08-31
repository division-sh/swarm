package pipeline

import (
	"fmt"
	"strings"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

const workflowInitialMaterializationProjectionVersion = 2

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
	owner runtimeflowidentity.RunScopedFlowInstance,
	identity runtimeflowidentity.Persisted,
	instance WorkflowInstance,
	occurredAt time.Time,
) (workflowInitialMaterializationProjection, error) {
	owner = owner.Normalize()
	if err := owner.Validate(); err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	if owner.Route != identity.Instance.Route() {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("workflow initial materialization owner disagrees with persisted route")
	}
	persisted, err := workflowInstancePersistedProjectionFromInstance(instance, identity.StorageRef)
	if err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	projection := workflowInitialMaterializationProjection{
		Version:         workflowInitialMaterializationProjectionVersion,
		RunID:           owner.RunID,
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
