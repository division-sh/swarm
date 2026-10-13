package runforkreadiness

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Initialization is produced independently of the fixture's current-at-cut state.
func selectedConstructionReceipt(t testing.TB, source semanticview.Source, runID string, instance flowidentity.Instance, initialFields map[string]any, occurredAt time.Time) json.RawMessage {
	t.Helper()
	if err := instance.ValidateConstruction(source, runID); err != nil {
		t.Fatal(err)
	}
	graph, found := semanticview.WorkflowStageTopology(source, instance.TemplateID)
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
	schema, found := source.FlowSchemaByID(instance.TemplateID)
	if !found {
		t.Fatal("construction fixture requires its exact flow schema")
	}
	contract, _ := entityruntime.ResolveForFlow(source, instance.TemplateID)
	construction := pipeline.FlowInstanceActivationPlan{
		Identity: instance, OccurredAt: occurredAt,
		Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: instance, RunID: runID, BundleHash: hash,
			WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
		},
		CreatingInput: pipeline.FlowConstructionInput{},
		Instance: pipeline.WorkflowInstance{
			InstanceID: instance.InstanceID, StorageRef: instance.InstancePath, EntityID: instance.EntityID,
			EntityType: contract.EntityType, InstanceKind: schema.EffectiveMode(), Mode: schema.EffectiveMode(), Status: "active",
			ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance,
			ParentEntityID: instance.ParentEntityID, WorkflowName: instance.TemplateID, WorkflowVersion: source.WorkflowVersion(),
			CurrentState: initial.ID(), StageDefined: graph.StageCount() != 0, Fields: initialFields,
			EnteredStageAt: occurredAt, CreatedAt: occurredAt,
		},
	}
	record, err := construction.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	return record.InitialMaterialization
}
