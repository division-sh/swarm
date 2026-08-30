package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestExecutableNodeSemanticScopeUsesExactFilesystemFlow(t *testing.T) {
	node := SystemNodeContract{EventHandlers: map[string]SystemNodeEventHandler{"task.start": {Emit: EmitSpec{Event: "task.done"}}}}
	root := FlowContractView{
		Paths:  FlowContractPaths{FlowPath: "."},
		Path:   ".",
		Policy: PolicyDocument{Values: map[string]PolicyValue{"root_only": {Value: "root"}}},
		Children: []FlowContractView{{
			Paths:  FlowContractPaths{FlowPath: "orders", NodesFile: "orders/nodes.yaml"},
			Path:   "orders",
			Nodes:  map[string]SystemNodeContract{"shared": node},
			Events: map[string]EventCatalogEntry{"task.start": {Note: "input"}, "task.done": {Note: "output"}},
			Policy: PolicyDocument{Values: map[string]PolicyValue{"flow_only": {Value: "flow"}}},
		}},
	}
	bundle := &WorkflowContractBundle{
		FlowTree: FlowTree{
			Root:   &root,
			ByPath: map[string]*FlowContractView{".": &root, "orders": &root.Children[0]},
			ByID:   map[string]*FlowContractView{".": &root, "orders": &root.Children[0]},
		},
	}
	ref := identitytest.ExecutableNode(t, "orders", "shared")

	scope, err := bundle.ExecutableNodeSemanticScope(ref)
	if err != nil {
		t.Fatalf("ExecutableNodeSemanticScope: %v", err)
	}
	if scope.Declaration.Source.FlowPath != "orders" || scope.Declaration.Source.Family != "nodes" || scope.Declaration.Source.File != "orders/nodes.yaml" {
		t.Fatalf("declaration source = %#v", scope.Declaration.Source)
	}
	owner, ok := scope.OwningFlow()
	if !ok || owner.Paths.FlowPath != "orders" {
		t.Fatalf("owning flow = %#v, ok=%v", owner, ok)
	}
	if got := bundle.ResolveExecutableNodeEventReference(ref, "task.done"); got != "orders/task.done" {
		t.Fatalf("event reference = %q, want orders/task.done", got)
	}
	if entry, key, ok := bundle.ResolveExecutableNodeEventCatalogEntry(ref, "task.done"); !ok || key != "orders/task.done" || entry.Note != "output" {
		t.Fatalf("flow catalog = key:%q entry:%#v ok:%v", key, entry, ok)
	}
}

func TestExecutableNodeSemanticScopeSeparatesSiblingLocalNames(t *testing.T) {
	root := FlowContractView{
		Paths: FlowContractPaths{FlowPath: "."},
		Path:  ".",
		Children: []FlowContractView{
			{Paths: FlowContractPaths{FlowPath: "a", NodesFile: "a/nodes.yaml"}, Path: "a", Nodes: map[string]SystemNodeContract{"shared": {}}},
			{Paths: FlowContractPaths{FlowPath: "b", NodesFile: "b/nodes.yaml"}, Path: "b", Nodes: map[string]SystemNodeContract{"shared": {}}},
		},
	}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{
		Root: &root,
		ByID: map[string]*FlowContractView{".": &root, "a": &root.Children[0], "b": &root.Children[1]},
	}}
	for _, flowPath := range []string{"a", "b"} {
		ref := identitytest.ExecutableNode(t, flowPath, "shared")
		scope, err := bundle.ExecutableNodeSemanticScope(ref)
		if err != nil {
			t.Fatalf("ExecutableNodeSemanticScope(%s): %v", flowPath, err)
		}
		if scope.Declaration.Source.FlowPath != flowPath {
			t.Fatalf("scope source = %#v, want flow %q", scope.Declaration.Source, flowPath)
		}
	}
}

func TestExecutableNodeSemanticScopeRejectsMissingOwningFlow(t *testing.T) {
	source := ContractItemSource{FlowPath: "orders", Family: "nodes", File: "orders/nodes.yaml"}
	key := contractScopeKey(source, "shared")
	bundle := &WorkflowContractBundle{
		scopedNodes:       map[string]SystemNodeContract{key: {}},
		scopedNodeSources: map[string]ContractItemSource{key: source},
	}
	ref := identitytest.ExecutableNode(t, "orders", "shared")
	if _, err := bundle.ExecutableNodeSemanticScope(ref); err == nil || !strings.Contains(err.Error(), "missing flow") {
		t.Fatalf("semantic scope error = %v, want missing flow", err)
	}
}
