package runforkpersistence

import (
	"encoding/json"
	"errors"
	"reflect"
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
	exact := pipeline.WorkflowInstance{InstanceID: "child", StorageRef: "child", EntityID: entityID, WorkflowName: "child", WorkflowVersion: recordedHeader.WorkflowVersion(), Mode: "static", ParentFlowID: ".", ParentFlowInstance: forkRun, ParentEntityID: forkRun, CreatedAt: at}
	childParent := flowidentity.ParentRoute{FlowID: ".", FlowInstance: forkRun, EntityID: forkRun}
	childConfig, err := recordedHeader.Project(identity.Route(), childParent, "v1")
	if err != nil {
		t.Fatal(err)
	}
	childHeader, err := pipeline.DecodeWorkflowInstanceRecordedHeader(identity.Route(), childConfig)
	if err != nil {
		t.Fatal(err)
	}
	construction := historicalFlowInstanceConstructionCut{
		plan:  runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Revision: 1}, Entities: []runfork.RunForkEntityState{state}},
		route: identity.Route(),
		birth: runForkReceiverBirth{owner: flowidentity.RunScopedFlowInstance{RunID: forkRun, Route: identity.Route()}, header: childHeader, createdAt: at},
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
		{"version", func(h *pipeline.WorkflowInstance) { h.WorkflowVersion = "foreign" }},
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
	t.Run("selected_version_bound_to_child_birth", func(t *testing.T) {
		selectedConfig, err := recordedHeader.Project(identity.Route(), childParent, "selected-v2")
		if err != nil {
			t.Fatal(err)
		}
		selectedHeader, err := pipeline.DecodeWorkflowInstanceRecordedHeader(identity.Route(), selectedConfig)
		if err != nil {
			t.Fatal(err)
		}
		selected := construction
		selected.birth.header = selectedHeader
		header := exact
		header.WorkflowVersion = "selected-v2"
		before, err := json.Marshal(selected.plan)
		if err != nil {
			t.Fatal(err)
		}
		observed, found, err := selected.project(runlifecycle.Snapshot{RunID: forkRun}, header, identity)
		if err != nil || !found || observed.WorkflowVersion != "selected-v2" || observed.SourceOwner.RunID != sourceRun || observed.SourceRevision != 1 {
			t.Fatalf("selected version lost its immutable birth/source cut: observation=%+v found=%t err=%v", observed, found, err)
		}
		for _, fault := range []string{"version", "birth_missing", "birth_run", "birth_route", "birth_parent", "birth_time", "birth_version_missing"} {
			t.Run(fault, func(t *testing.T) {
				badCut, badHeader := selected, header
				switch fault {
				case "version":
					badHeader.WorkflowVersion = "foreign"
				case "birth_missing":
					badCut.birth = runForkReceiverBirth{}
				case "birth_run":
					badCut.birth.owner.RunID = sourceRun
				case "birth_route":
					badCut.birth.owner.Route = flowidentity.StoredRoute("other", "other", "other")
				case "birth_parent":
					foreignConfig, err := recordedHeader.Project(identity.Route(), parent, "selected-v2")
					if err != nil {
						t.Fatal(err)
					}
					badCut.birth.header, err = pipeline.DecodeWorkflowInstanceRecordedHeader(identity.Route(), foreignConfig)
					if err != nil {
						t.Fatal(err)
					}
				case "birth_time":
					badCut.birth.createdAt = at.Add(time.Second)
				case "birth_version_missing":
					badCut.birth.header = pipeline.WorkflowInstanceRecordedHeader{}
				}
				_, found, err := badCut.project(runlifecycle.Snapshot{RunID: forkRun}, badHeader, identity)
				var corruption *pipeline.FlowInstanceConstructionCorruption
				if found || !errors.As(err, &corruption) || corruption.RunID != forkRun || corruption.InstancePath != header.StorageRef {
					t.Fatalf("crossed birth/version was not exact corruption: found=%t err=%v", found, err)
				}
			})
		}
		after, err := json.Marshal(selected.plan)
		if err != nil || !reflect.DeepEqual(before, after) || recordedHeader.WorkflowVersion() != "v1" {
			t.Fatal("selected version observation changed immutable source construction")
		}
	})
	cut := &runForkRevisionSnapshot{RunID: sourceRun, EntityMetadata: []runForkRevisionEntityMetadata{{EntityID: entityID, FlowInstance: "child", ConstructionKind: "constructed"}}}
	if _, found, err := findProjectedFlowInstanceMetadata(cut, forkRun, "later"); err != nil || found {
		t.Fatalf("later native receiver adopted absent history: %+v %v", found, err)
	}
	cut.EntityMetadata = append(cut.EntityMetadata, cut.EntityMetadata[0])
	if _, found, err := findProjectedFlowInstanceMetadata(cut, forkRun, exact.StorageRef); err == nil || found {
		t.Fatalf("contradictory captured receivers admitted: %+v %v", found, err)
	}
}
