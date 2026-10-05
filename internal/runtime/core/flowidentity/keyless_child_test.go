package flowidentity

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestKeylessChildOfRunRootKeepsAuthoredCoordinate(t *testing.T) {
	root := contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "root"}}
	root.Children = []contracts.FlowContractView{{Path: "child", Paths: contracts.FlowContractPaths{FlowPath: "child"}, Schema: contracts.FlowSchemaDocument{Name: "child"}, Parent: &root}}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{RootSchema: &root.Schema, FlowSchemas: map[string]contracts.FlowSchemaDocument{"child": root.Children[0].Schema}, FlowTree: contracts.FlowTree{
		Root: &root, ByID: map[string]*contracts.FlowContractView{".": &root, "child": &root.Children[0]},
		ByPath: map[string]*contracts.FlowContractView{".": &root, "child": &root.Children[0]},
	}})
	for _, runID := range []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"} {
		parent := Stored(source, ".", runID, runID, "", "")
		child, err := KeylessChild(source, parent, "child")
		if err != nil {
			t.Fatal(err)
		}
		if child.InstancePath != "child" || child.ScopeKey != "child" || child.InstanceID != "child" || child.ParentRoute.FlowInstance != runID || child.ParentEntityID != parent.EntityID {
			t.Fatalf("root execution identity became a child discriminator: %+v", child)
		}
		if err := child.ValidateConstruction(source, runID); err != nil {
			t.Fatalf("exact run parent refused: %v", err)
		}
		if err := child.ValidateConstruction(source, "33333333-3333-4333-8333-333333333333"); err == nil {
			t.Fatal("same authored child path admitted another run's parent")
		}
	}
}

func TestStandingConstructionUsesSelectedGenerationRun(t *testing.T) {
	root := contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "root"}}
	root.Children = []contracts.FlowContractView{{Path: "child", Paths: contracts.FlowContractPaths{FlowPath: "child"}, Schema: contracts.FlowSchemaDocument{Name: "child"}, Parent: &root}}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{RootSchema: &root.Schema, FlowSchemas: map[string]contracts.FlowSchemaDocument{"child": root.Children[0].Schema}, FlowTree: contracts.FlowTree{
		Root: &root, ByID: map[string]*contracts.FlowContractView{".": &root, "child": &root.Children[0]},
		ByPath: map[string]*contracts.FlowContractView{".": &root, "child": &root.Children[0]},
	}})
	serviceID := StandingServiceID(".")
	for _, generation := range []int64{1, 2, 7} {
		runID := StandingGenerationRunID(serviceID, generation)
		parent, err := StandingForGeneration(source, ".", runID)
		if err != nil {
			t.Fatal(err)
		}
		if parent.InstancePath != runID || parent.InstanceID != runID || parent.EntityID != runID || parent.ScopeKey != "." {
			t.Fatalf("generation %d construction adopted service or stale run coordinates: %+v", generation, parent)
		}
		child, err := KeylessChild(source, parent, "child")
		if err != nil || child.InstancePath != "child" || child.ParentRoute.FlowInstance != runID || child.ParentEntityID != parent.EntityID {
			t.Fatalf("generation %d child=%+v err=%v", generation, child, err)
		}
	}
	for _, invalid := range []string{"", ".", serviceID + "/stale"} {
		if _, err := StandingForGeneration(source, ".", invalid); err == nil {
			t.Fatalf("non-run coordinate %q admitted standing construction", invalid)
		}
	}
	if _, err := StandingForGeneration(nil, ".", StandingGenerationRunID(serviceID, 1)); err == nil {
		t.Fatal("missing selected source admitted standing construction")
	}
}

func TestRootConstructionIdentityAlwaysUsesExactRun(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		name := "keyless"
		if keyed {
			name = "keyed"
		}
		t.Run(name, func(t *testing.T) {
			root := contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "root"}}
			if keyed {
				var err error
				root.Schema.Instance, err = contracts.ParseTemplateInstanceField("request_id")
				if err != nil {
					t.Fatal(err)
				}
			}
			source := semanticview.Wrap(&contracts.WorkflowContractBundle{RootSchema: &root.Schema, FlowTree: contracts.FlowTree{
				Root: &root, ByID: map[string]*contracts.FlowContractView{".": &root}, ByPath: map[string]*contracts.FlowContractView{".": &root},
			}})
			const runID = "11111111-1111-4111-8111-111111111111"
			const foreign = "22222222-2222-4222-8222-222222222222"
			rootIdentity := Stored(source, ".", runID, runID, runID, "")
			if err := rootIdentity.ValidateConstruction(source, runID); err != nil {
				t.Fatalf("exact %s root refused: %v", name, err)
			}
			for _, invalid := range []Instance{
				Stored(source, ".", foreign, foreign, foreign, ""),
				Stored(source, ".", runID, runID, foreign, ""),
				Stored(source, ".", "./"+runID, runID, runID, ""),
				Stored(source, ".", runID, runID, runID, foreign),
			} {
				if err := invalid.ValidateConstruction(source, runID); err == nil {
					t.Errorf("invalid %s root accepted: %+v", name, invalid)
				}
			}
		})
	}
}
