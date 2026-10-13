package runforkpersistence

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestReceiverConstructionHistoryExactCoordinates(t *testing.T) {
	sourceRun, forkRun := uuid.NewString(), uuid.NewString()
	entityID := flowidentity.EntityID("child")
	parent := flowidentity.ParentRoute{FlowID: ".", FlowInstance: sourceRun, EntityID: sourceRun}
	identity := flowidentity.Instance{TemplateID: "child", ScopeKey: "child", InstanceID: "child", InstancePath: "child", EntityID: entityID, HasStoredPath: true, ParentRoute: parent, ParentEntityID: sourceRun}
	payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(identity, "v1")
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	recordedHeader, err := pipeline.DecodeWorkflowInstanceRecordedHeader(identity.Route(), config)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1700000000, 0).UTC()
	initial := selectedWorkflowConstructionRecordFixture(t, sourceRun, "bundle-v2:sha256:"+strings.Repeat("a", 64), identity,
		pipeline.WorkflowInstance{WorkflowVersion: "v1", Mode: "static", Status: "active", CurrentState: "pending", EnteredStageAt: at, CreatedAt: at}, executionmode.Live)
	state := runfork.RunForkEntityState{EntityID: entityID, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
		FlowTemplate: "child", FlowInstance: "child", FlowConfig: config, Mode: "static",
		InitialMaterialization: initial.InitialMaterialization,
	}}
	exact := pipeline.WorkflowInstance{InstanceID: "child", StorageRef: "child", EntityID: entityID, WorkflowName: "child", WorkflowVersion: recordedHeader.WorkflowVersion(), Mode: "static", ParentFlowID: ".", ParentFlowInstance: forkRun, ParentEntityID: forkRun}
	construction := historicalFlowInstanceConstructionCut{
		plan:  runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Revision: 1}, Entities: []runfork.RunForkEntityState{state}},
		route: identity.Route(),
	}
	for _, cell := range []struct {
		name   string
		mutate func(*pipeline.WorkflowInstance)
	}{
		{"exact", func(*pipeline.WorkflowInstance) {}},
		{"key", func(h *pipeline.WorkflowInstance) { h.InstanceID = "foreign" }},
		{"declaration", func(h *pipeline.WorkflowInstance) { h.WorkflowName = "foreign" }},
		{"entity", func(h *pipeline.WorkflowInstance) { h.EntityID = uuid.NewString() }},
		{"parent_entity", func(h *pipeline.WorkflowInstance) { h.ParentEntityID = sourceRun }},
		{"parent_path", func(h *pipeline.WorkflowInstance) { h.ParentFlowInstance = sourceRun }},
		{"parent_declaration", func(h *pipeline.WorkflowInstance) { h.ParentFlowID = "foreign" }},
		{"mode", func(h *pipeline.WorkflowInstance) { h.Mode = "template" }},
		{"entity_type", func(h *pipeline.WorkflowInstance) { h.EntityType = "foreign" }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			header := exact
			cell.mutate(&header)
			_, found, err := construction.project(runlifecycle.Snapshot{RunID: forkRun}, header, identity)
			if cell.name == "exact" {
				if err != nil || !found {
					t.Fatalf("exact captured receiver refused: found=%t err=%v", found, err)
				}
			} else if err == nil || found {
				t.Fatalf("hostile captured receiver admitted: found=%t err=%v", found, err)
			}
		})
	}
	cut := &runForkRevisionSnapshot{RunID: sourceRun, EntityMetadata: []runForkRevisionEntityMetadata{{EntityID: entityID, FlowInstance: "child", ConstructionKind: "constructed"}}}
	if _, found, err := findProjectedFlowInstanceMetadata(cut, forkRun, "later"); err != nil || found {
		t.Fatalf("later native receiver adopted absent history: %+v %v", found, err)
	}
	cut.EntityMetadata = append(cut.EntityMetadata, cut.EntityMetadata[0])
	if _, found, err := findProjectedFlowInstanceMetadata(cut, forkRun, exact.StorageRef); err == nil || found {
		t.Fatalf("contradictory captured receivers admitted: %+v %v", found, err)
	}
}
