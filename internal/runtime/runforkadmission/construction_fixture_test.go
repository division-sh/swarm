package runforkadmission

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
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
	graph, found := semanticview.WorkflowStageTopology(source, flowID)
	if !found {
		t.Fatal("construction fixture requires its initial lifecycle declaration")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("construction fixture requires its immutable source bundle")
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	entered := plan.ForkPoint.Timestamp
	construction := pipeline.FlowInstanceActivationPlan{
		Identity: instance, OccurredAt: entered,
		Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: instance, RunID: plan.SourceRunID, BundleHash: hash,
			WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
		},
		CreatingInput: pipeline.FlowConstructionInput{},
		Instance: pipeline.WorkflowInstance{
			InstanceID: instance.InstanceID, StorageRef: instance.InstancePath, EntityID: instance.EntityID,
			EntityType: contract.EntityType, InstanceKind: "template", Mode: "template", Status: "active",
			ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance,
			ParentEntityID: instance.ParentEntityID, WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(),
			CurrentState: initial.ID(), StageDefined: graph.StageCount() != 0, EnteredStageAt: entered, CreatedAt: entered,
		},
	}
	record, err := construction.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	metadata := &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
		Mode: "template", FlowTemplate: flowID, FlowInstance: instance.InstancePath, EntityType: contract.EntityType, FlowConfig: config,
		InitialMaterialization: record.InitialMaterialization,
	}
	plan.Entities = append(append([]runfork.RunForkEntityState(nil), plan.Entities...), runfork.RunForkEntityState{
		EntityID: instance.EntityID, CurrentState: "active", EnteredStateAt: &entered, MaterializationMetadata: metadata,
	})
	return plan
}
