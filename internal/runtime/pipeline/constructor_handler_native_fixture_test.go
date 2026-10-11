package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func constructNativeQueryEntitiesGuardInstanceForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, entityID, requestID string) WorkflowInstance {
	t.Helper()
	storageRef := "validation/" + entityID
	// Construct both collection scopes through the same original selected owner.
	instance := materializedWorkflowInstanceForSource(t, pc.SemanticSource(), ctx, WorkflowInstance{
		InstanceID:      runtimeflowidentity.LogicalInstanceID(storageRef),
		StorageRef:      storageRef,
		EntityID:        FlowInstanceEntityID(storageRef),
		WorkflowName:    "validation",
		WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
		Mode:            "template",
		StageDefined:    true,
		CurrentState:    "queued",
		Fields:          map[string]any{"validation_id": entityID, "request_id": requestID},
		EntityType:      "validation_request",
	})
	instance.InitialFieldValues = cloneStringAnyMap(instance.Fields)
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatalf("seed query_entities guard instance %s: %v", entityID, err)
	}
	return instance
}

func nativeEmitPersistenceBundleForTest(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: validation\nstages:\n  researching: {}\n  mvp_speccing: {}\n",
		"entities.yaml": "test_entity:\n  business_brief: {type: BusinessBrief}\n",
		"types.yaml":    "types:\n  BusinessBrief:\n    summary: text\n",
		"events.yaml":   "research.completed:\n  business_brief: BusinessBrief?\nspec.requested:\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [research.completed]\n  event_handlers:\n    research.completed:\n      advances_to: mvp_speccing\n",
	})
}

func assertNativeCreatedChildFlowIdentityCoherentForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, flowID, entityID string, emitted events.Event, instance WorkflowInstance) {
	t.Helper()
	instanceID := strings.TrimSpace(instance.InstanceID)
	if instanceID == "" {
		t.Fatalf("created %s entity %s missing typed instance_id", flowID, entityID)
	}
	flowPath := flowID
	if got := strings.TrimSpace(instance.StorageRef); got != flowPath {
		t.Fatalf("created %s entity storage_ref = %q, want %q", flowID, got, flowPath)
	}
	if got := entityID; got != FlowInstanceEntityID(flowPath) {
		t.Fatalf("created %s entity id = %q, want %q for flow path %q", flowID, got, FlowInstanceEntityID(flowPath), flowPath)
	}
	if got := emitted.FlowInstance(); got != flowID {
		t.Fatalf("created %s emitted flow_instance = %q, want static scope %q", flowID, got, flowID)
	}
	wantSource := (events.RouteIdentity{FlowID: flowID, FlowInstance: flowID, EntityID: entityID}).Normalized()
	if got := emitted.SourceRoute(); got != wantSource {
		t.Fatalf("created %s emitted source route = %#v, want static scope %#v", flowID, got, wantSource)
	}
	rowOwner, err := fixture.EntityStateOwner(ctx, entityID)
	if err != nil {
		t.Fatalf("query created %s entity_state owner: %v", flowID, err)
	}
	if got := strings.TrimSpace(rowOwner); got != flowPath {
		t.Fatalf("created %s entity_state.flow_instance = %q, want %q", flowID, got, flowPath)
	}
}
