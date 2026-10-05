package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// Stop at the next independent owner: these tests exercise the recovery
// header comparison, not readiness admission or agent execution.
type receiverConfigRecoveryReader struct {
	pipeline.WorkflowPersistenceOwner
	instance       pipeline.WorkflowInstance
	readinessCalls int
	nextOwnerError error
}

func (r *receiverConfigRecoveryReader) LoadWorkflowInstance(context.Context, flowidentity.RunScopedFlowInstance) (pipeline.WorkflowInstance, bool, error) {
	return r.instance, true, nil
}

func (r *receiverConfigRecoveryReader) LoadDynamicFlowRuntimeReadiness(context.Context, string, flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	r.readinessCalls++
	return pipeline.DynamicFlowRuntimeReadiness{}, false, r.nextOwnerError
}

func TestRecoveredSelectedHeaderDoesNotReinitializeBusinessState(t *testing.T) {
	for _, tc := range []struct {
		name             string
		sealed, recorded any
		wantMatch        bool
	}{
		{"unchanged_double", json.Number("7.0"), float64(7), true},
		{"unchanged_integer", json.Number("7"), int64(7), true},
		{"double_became_integer", float64(7), int64(7), false},
		{"integer_became_double", int64(7), float64(7), false},
		{"nested_double", map[string]any{"items": []any{json.Number("7.0"), false, nil}}, map[string]any{"items": []any{float64(7), false, nil}}, true},
		{"nested_kind_change", map[string]any{"items": []any{float64(7)}}, map[string]any{"items": []any{int64(7)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := flowidentity.RouteForInstancePath("worker-flow/one")
			state := runfork.RunForkSelectedContractWorkflowState{
				AddressKind: runfork.RunForkSelectedContractWorkflowStateExact,
				Route:       route, EntityID: "entity-one", EntityType: "worker", FlowID: "worker-flow",
				WorkflowVersion: "version-one", Mode: "template",
			}
			reader := &receiverConfigRecoveryReader{instance: pipeline.WorkflowInstance{
				EntityID: state.EntityID, EntityType: state.EntityType, WorkflowName: state.FlowID,
				WorkflowVersion: state.WorkflowVersion, Mode: state.Mode, StorageRef: route.InstancePath,
				InstanceID: route.InstanceID, Fields: map[string]any{"value": tc.recorded},
			}, nextOwnerError: errors.New("readiness owner reached")}
			_, err := bindRecoveredSelectedContractAgentRuntime(context.Background(), pipeline.NewWorkflowPersistence(reader),
				selectedContractAgentTestRunID, LoadedSelectedContractSource{}, []runfork.RunForkSelectedContractWorkflowState{state}, selectedContractAgentRuntimePlan{})
			if !errors.Is(err, reader.nextOwnerError) || reader.readinessCalls != 1 {
				t.Fatalf("valid header rejected before readiness: calls=%d err=%v", reader.readinessCalls, err)
			}
			if !reflect.DeepEqual(reader.instance.Fields["value"], tc.recorded) {
				t.Fatal("recovery rewrote business state")
			}
		})
	}
}
