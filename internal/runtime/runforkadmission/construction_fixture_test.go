package runforkadmission

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Component admission fixtures declare their fixed header explicitly. Pending
// delivery paths are not construction evidence and cannot create this record.
func withConstructedHeader(t testing.TB, plan runfork.RunForkPlan, source semanticview.Source, flowID, instanceID string) runfork.RunForkPlan {
	t.Helper()
	root := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), plan.SourceRunID, plan.SourceRunID, flowidentity.EntityID(plan.SourceRunID), "")
	instance, err := flowidentity.KeyedChild(source, root, flowID, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.ValidateConstruction(source, plan.SourceRunID); err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(map[string]any{
		"instance_id": instance.InstanceID, "storage_ref": instance.InstancePath, "flow_path": instance.InstancePath,
		"parent_flow_id": instance.ParentRoute.FlowID, "parent_flow_instance": instance.ParentRoute.FlowInstance,
		"parent_entity_id": instance.ParentEntityID,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := entityruntime.ResolveForFlow(source, flowID)
	metadata := &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
		Mode: "template", FlowTemplate: flowID, FlowInstance: instance.InstancePath, EntityType: contract.EntityType, FlowConfig: config,
	}
	entered := plan.ForkPoint.Timestamp
	plan.Entities = append(append([]runfork.RunForkEntityState(nil), plan.Entities...), runfork.RunForkEntityState{
		EntityID: instance.EntityID, CurrentState: "active", EnteredStateAt: &entered, MaterializationMetadata: metadata,
	})
	return plan
}
