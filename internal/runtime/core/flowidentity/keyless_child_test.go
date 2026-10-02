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
	}
}
