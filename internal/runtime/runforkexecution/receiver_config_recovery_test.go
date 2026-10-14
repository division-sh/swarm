package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

// Recovery consumes admitted snapshot evidence rather than two raw readers.
// Real construction, materialization and settlement are covered by the
// both-store constructed recovery proof.
type receiverConfigRecoveryReader struct {
	pipeline.WorkflowPersistenceOwner
	instance       pipeline.WorkflowInstance
	readinessCalls int
	nextOwnerError error
	observation    pipeline.FlowInstanceObservation
	lookupCalls    int
}

func (r *receiverConfigRecoveryReader) LookupFlowInstance(_ context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	r.lookupCalls++
	if r.nextOwnerError != nil {
		return pipeline.FlowInstanceObservation{}, false, r.nextOwnerError
	}
	if err := r.observation.ValidateSelection(request); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	return r.observation, true, nil
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
			loaded, state, reader, runtime := recoveredSelectedHeaderFixture(t, tc.recorded)
			_, err := bindRecoveredSelectedContractAgentRuntime(context.Background(), pipeline.NewWorkflowPersistence(reader),
				selectedContractAgentTestRunID, loaded, []runfork.RunForkSelectedContractWorkflowState{state}, runtime)
			if err != nil || reader.lookupCalls != 1 || reader.readinessCalls != 0 {
				t.Fatalf("valid captured header rejected or raw readiness consulted: lookup=%d readiness=%d err=%v", reader.lookupCalls, reader.readinessCalls, err)
			}
			if !reflect.DeepEqual(reader.instance.Fields["value"], tc.recorded) {
				t.Fatal("recovery rewrote business state")
			}
		})
	}
}

func recoveredSelectedHeaderFixture(t *testing.T, value any) (LoadedSelectedContractSource, runfork.RunForkSelectedContractWorkflowState, *receiverConfigRecoveryReader, selectedContractAgentRuntimePlan) {
	t.Helper()
	loaded := selectedContractReadinessFixture(t, 0)
	runID, sourceRunID := selectedContractAgentTestRunID, uuid.NewString()
	parent := flowidentity.Stored(loaded.Source, ".", runID, runID, runID, "")
	identity, err := flowidentity.KeyedChild(loaded.Source, parent, "worker-flow", "one")
	if err != nil {
		t.Fatal(err)
	}
	request, err := pipeline.NewExactFlowInstanceLookup(loaded.Source, loaded.SourceArtifactFact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	instance := pipeline.WorkflowInstance{
		EntityID: identity.EntityID, EntityType: "worker", WorkflowName: identity.TemplateID,
		WorkflowVersion: "captured-version", Mode: "template", StorageRef: identity.InstancePath,
		InstanceID: identity.InstanceID, InstanceKey: "one", ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: runID,
		CurrentState: "idle", Status: "active", Revision: 1, CreatedAt: at, UpdatedAt: at, Fields: map[string]any{"value": value},
	}
	origin, err := runlifecycle.ForkMaterializationRunOrigin(sourceRunID, runlifecycle.ForkOriginPointDeploymentRevision, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	readiness := pipeline.DynamicFlowRuntimeReadiness{
		Plan:         pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: loaded.SourceArtifactFact.BundleHash(), WorkflowVersion: loaded.Source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		InstancePath: identity.InstancePath, OwningRunSource: loaded.SourceArtifactFact, RunStatus: "running", InstanceStatus: "active",
	}
	observation, err := pipeline.AdmitHistoricalFlowInstanceObservation(request, instance,
		runlifecycle.Snapshot{RunID: runID, Origin: origin, State: runlifecycle.StateRunning, BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: at}, 3,
		pipeline.HistoricalFlowInstanceConstruction{Identity: identity, InstanceKey: instance.InstanceKey, SourceRevision: 2, WorkflowVersion: instance.WorkflowVersion,
			SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRunID, Route: identity.Route()}}, &readiness)
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := agenttopology.NewSelectedDeclarationPlan(loaded.SourceArtifactFact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	state := runfork.RunForkSelectedContractWorkflowState{AddressKind: runfork.RunForkSelectedContractWorkflowStateExact,
		Route: identity.Route(), EntityID: identity.EntityID, EntityType: instance.EntityType, FlowID: identity.TemplateID,
		WorkflowVersion: loaded.Source.WorkflowVersion(), Mode: instance.Mode, ExecutionMode: executionmode.Live}
	return loaded, state, &receiverConfigRecoveryReader{instance: instance, observation: observation}, selectedContractAgentRuntimePlan{Declarations: declarations}
}

func TestRecoveredSelectedHeaderRefusesCrossedIndexAndSelectedReadiness(t *testing.T) {
	for _, mismatch := range []string{"entity", "type", "mode", "selected_version", "execution_mode", "index_refusal"} {
		t.Run(mismatch, func(t *testing.T) {
			loaded, state, reader, runtime := recoveredSelectedHeaderFixture(t, int64(7))
			before, err := reader.observation.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			switch mismatch {
			case "entity":
				state.EntityID = uuid.NewString()
			case "type":
				state.EntityType = "foreign"
			case "mode":
				state.Mode = "static"
			case "selected_version":
				state.WorkflowVersion = reader.instance.WorkflowVersion
			case "execution_mode":
				state.ExecutionMode = executionmode.Mock
			case "index_refusal":
				reader.nextOwnerError = errors.New("exact fixed cut refused")
			}
			_, err = bindRecoveredSelectedContractAgentRuntime(context.Background(), pipeline.NewWorkflowPersistence(reader),
				selectedContractAgentTestRunID, loaded, []runfork.RunForkSelectedContractWorkflowState{state}, runtime)
			if err == nil || reader.lookupCalls != 1 || reader.readinessCalls != 0 {
				t.Fatalf("crossed recovery was accepted or bypassed index: err=%v lookups=%d rawReadiness=%d", err, reader.lookupCalls, reader.readinessCalls)
			}
			if reader.nextOwnerError != nil && !errors.Is(err, reader.nextOwnerError) || mismatch == "selected_version" && !strings.Contains(err.Error(), "regenerated fork state") {
				t.Fatalf("recovery lost its exact refusal owner: %v", err)
			}
			after, readErr := reader.observation.WorkflowInstance()
			if readErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refusal reinitialized business state: %v", readErr)
			}
		})
	}
}
