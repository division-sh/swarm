package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const workflowInitialMaterializationProjectionVersion = 2

func validateWorkflowInitialEntry(source semanticview.Source, instance WorkflowInstance, initialStage string) error {
	graph, found := semanticview.WorkflowStageTopology(source, instance.WorkflowName)
	if !found || graph.FlowID != instance.WorkflowName {
		return fmt.Errorf("workflow initial entry requires the exact compiled flow %q", instance.WorkflowName)
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		return fmt.Errorf("workflow initial entry: %w", err)
	}
	want := initial.ID()
	if want == "" || initialStage != want || instance.CurrentState != want {
		return fmt.Errorf("workflow initial entry must match canonical initial stage %q and prepared state %q, got %q", want, instance.CurrentState, initialStage)
	}
	return nil
}

// ValidateFlowConstructionPublication consumes the immutable constructor
// receipt. Neither handler settlement nor current attachment progress is proof
// that this publication constructed its exact receiver.
func ValidateFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID, eventID string) error {
	receipt, err := decodeFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return err
	}
	return matchFlowConstructionPublication(receipt.CreatingInput, eventID)
}

type FlowConstructionPublicationEvidence struct {
	CreatingInput FlowConstructionInput
	Fields        map[string]any
}

// ProjectFlowConstructionPublication discovers the immutable creating input
// independently of the incoming delivery or its projected target kind.
func ProjectFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (FlowConstructionPublicationEvidence, error) {
	receipt, err := decodeFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return FlowConstructionPublicationEvidence{}, err
	}
	evidence := FlowConstructionPublicationEvidence{CreatingInput: receipt.CreatingInput}
	if receipt.Persisted.Fields == nil {
		return evidence, nil
	}
	fields, err := canonicaljson.CloneRuntimeValue(receipt.Persisted.Fields)
	if err != nil {
		return FlowConstructionPublicationEvidence{}, err
	}
	evidence.Fields = fields.(map[string]any)
	return evidence, nil
}

// FlowConstructionPublicationFields projects immutable initial state through
// the same receipt admission used by execution, never from current state.
func FlowConstructionPublicationFields(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID, eventID string) (map[string]any, error) {
	evidence, err := ProjectFlowConstructionPublication(raw, owner, entityID)
	if err != nil {
		return nil, err
	}
	if err := matchFlowConstructionPublication(evidence.CreatingInput, eventID); err != nil {
		return nil, err
	}
	return evidence.Fields, nil
}

func matchFlowConstructionPublication(input FlowConstructionInput, eventID string) error {
	if eventID == "" || input.EventID != eventID {
		return fmt.Errorf("flow construction receipt contradicts exact receiver or creating publication")
	}
	return nil
}

func decodeFlowConstructionPublication(raw []byte, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (workflowInitialMaterializationProjection, error) {
	if err := owner.Validate(); err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt: %w", err)
	}
	// Preserve explicit no-publication construction without accepting an absent
	// or null provenance member as an equally valid receipt.
	var wire struct {
		workflowInitialMaterializationProjection
		CreatingInput *FlowConstructionInput `json:"creating_input"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&wire); err != nil {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt requires one object")
	}
	if wire.CreatingInput == nil {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt requires creating_input")
	}
	receipt := wire.workflowInitialMaterializationProjection
	receipt.CreatingInput = *wire.CreatingInput
	if err := receipt.CreatingInput.Validate(); err != nil {
		return workflowInitialMaterializationProjection{}, err
	}
	if receipt.Version != workflowInitialMaterializationProjectionVersion ||
		receipt.RunID != owner.RunID || receipt.FlowInstance != owner.Route.InstancePath || receipt.EntityID != entityID ||
		receipt.WorkflowName != owner.Route.ScopeKey || receipt.WorkflowVersion == "" || receipt.OccurredAt.IsZero() ||
		receipt.Persisted.Control.StorageRef != owner.Route.InstancePath || receipt.Persisted.Control.EntityID != entityID || receipt.Readiness == nil {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt contradicts exact receiver or creating publication")
	}
	readinessOwner, err := receipt.Readiness.FlowIdentity()
	if err != nil || readinessOwner != owner || receipt.Readiness.Identity.EntityID != entityID {
		return workflowInitialMaterializationProjection{}, fmt.Errorf("flow construction receipt readiness identity contradicts receiver")
	}
	return receipt, nil
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
	Readiness       *DynamicFlowRuntimeReadinessPlan    `json:"readiness,omitempty"`
	CreatingInput   FlowConstructionInput               `json:"creating_input"`
}
