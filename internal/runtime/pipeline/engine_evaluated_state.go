package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type evaluatedWorkflowState struct {
	address  runtimeengine.StateAddress
	instance WorkflowInstance
	config   json.RawMessage
}

func (e evaluatedWorkflowState) EvaluationRevision() int64 { return e.instance.Revision }

func workflowEngineEvaluationSnapshot(source semanticview.Source, flowID string, address runtimeengine.StateAddress, instance WorkflowInstance, config json.RawMessage) (runtimeengine.StateSnapshot, bool, error) {
	snapshot, found, err := workflowInstanceEngineStateSnapshot(source, flowID, address.EntityID, instance)
	if err != nil || !found {
		return snapshot, found, err
	}
	snapshot.Persisted = evaluatedWorkflowState{address: address, instance: cloneWorkflowInstanceForEngineMutation(instance), config: append(json.RawMessage(nil), config...)}
	return snapshot, true, nil
}

func evaluatedWorkflowInstance(source semanticview.Source, address runtimeengine.StateAddress, snapshot runtimeengine.StateSnapshot) (evaluatedWorkflowState, error) {
	evaluated, ok := snapshot.Persisted.(evaluatedWorkflowState)
	if !ok || evaluated.address != address || snapshot.Revision <= 0 || snapshot.Revision != evaluated.instance.Revision {
		return evaluatedWorkflowState{}, fmt.Errorf("workflow mutation requires its exact evaluated persistence snapshot")
	}
	entityID := identity.NormalizeEntityID(address.EntityID.String())
	if _, err := requireWorkflowInstanceIdentity(address.FlowInstance.Route, entityID, evaluated.instance); err != nil {
		return evaluatedWorkflowState{}, err
	}
	expected, _, err := workflowInstanceEngineStateSnapshot(source, address.FlowID.String(), entityID, evaluated.instance)
	if err != nil {
		return evaluatedWorkflowState{}, err
	}
	expected.Persisted = snapshot.Persisted
	if !reflect.DeepEqual(expected, snapshot) {
		return evaluatedWorkflowState{}, fmt.Errorf("workflow mutation evaluation snapshot contradicts its persisted projection")
	}
	return evaluated, nil
}
