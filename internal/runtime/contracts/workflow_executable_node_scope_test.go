package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestExecutableNodeSemanticScopeSeparatesDeclarationPackageFromOwningFlow(t *testing.T) {
	node := SystemNodeContract{ID: "shared", EventHandlers: map[string]SystemNodeEventHandler{"task.start": {Emit: EmitSpec{Event: "task.done"}}}}
	child := FlowContractView{
		Paths:  FlowContractPaths{PackageKey: "packages/a", NodesFile: "packages/a/nodes.yaml"},
		Nodes:  map[string]SystemNodeContract{"shared": node},
		Events: map[string]EventCatalogEntry{"package.local": {Note: "project catalog"}},
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"project_only":       {Value: "project"},
			"project_precedence": {Value: "project"},
			"precedence":         {Value: "project"},
		}},
	}
	flow := FlowContractView{
		Path: "orders",
		Paths: FlowContractPaths{
			ID: "orders", PackageKey: "flows/orders", NodesFile: "flows/orders/nodes.yaml",
		},
		Events: map[string]EventCatalogEntry{
			"task.start": {Note: "flow input"},
			"task.done":  {Note: "flow output"},
		},
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"flow_only":  {Value: "flow"},
			"precedence": {Value: "flow"},
		}},
		Children: []FlowContractView{child},
	}
	root := FlowContractView{
		Paths:    FlowContractPaths{PackageKey: "."},
		Policy:   PolicyDocument{Values: map[string]PolicyValue{"project_precedence": {Value: "root-tree"}}},
		Children: []FlowContractView{flow},
	}
	bundle := &WorkflowContractBundle{
		Policy:   PolicyDocument{Values: map[string]PolicyValue{"root_only": {Value: "root"}}},
		FlowTree: FlowTree{Root: &root, ByID: map[string]*FlowContractView{"orders": &root.Children[0]}},
		projectContracts: map[string]ProjectContractView{
			"packages/a": {
				Paths:  ProjectPackagePaths{Key: "packages/a"},
				Nodes:  map[string]SystemNodeContract{"shared": node},
				Events: child.Events,
				Policy: child.Policy,
			},
		},
	}
	ref := identitytest.ExecutableNode(t, "packages/a", "orders", "shared")

	scope, err := bundle.ExecutableNodeSemanticScope(ref)
	if err != nil {
		t.Fatalf("ExecutableNodeSemanticScope: %v", err)
	}
	if scope.Declaration.Source.Layer != "project" || scope.Declaration.Source.PackageKey != "packages/a" || scope.Declaration.Source.FlowID != "orders" {
		t.Fatalf("declaration source = %#v", scope.Declaration.Source)
	}
	project, ok := scope.PackageView()
	if !ok || project.Paths.Key != "packages/a" {
		t.Fatalf("declaration package = %#v, ok=%v", project, ok)
	}
	owner, ok := scope.OwningFlow()
	if !ok || owner.Paths.ID != "orders" || owner.Paths.PackageKey != "flows/orders" {
		t.Fatalf("owning flow = %#v, ok=%v", owner, ok)
	}
	if got := bundle.ResolveExecutableNodeEventReference(ref, "task.done"); got != "orders/task.done" {
		t.Fatalf("event reference = %q, want orders/task.done", got)
	}
	if got := bundle.ResolveExecutableNodeEventPattern(ref, "task.*"); got != "orders/task.*" {
		t.Fatalf("event pattern = %q, want orders/task.*", got)
	}
	if entry, key, ok := bundle.ResolveExecutableNodeEventCatalogEntry(ref, "task.done"); !ok || key != "orders/task.done" || entry.Note != "flow output" {
		t.Fatalf("flow catalog = key:%q entry:%#v ok:%v", key, entry, ok)
	}
	if entry, key, ok := bundle.ResolveExecutableNodeEventCatalogEntry(ref, "package.local"); !ok || key != "orders/package.local" || entry.Note != "project catalog" {
		t.Fatalf("project catalog = key:%q entry:%#v ok:%v", key, entry, ok)
	}
	policy := bundle.ResolvedPolicyForExecutableNode(ref)
	for key, want := range map[string]any{"root_only": "root", "project_only": "project", "project_precedence": "project", "flow_only": "flow", "precedence": "flow"} {
		if got := policy.Values[key].Value; got != want {
			t.Fatalf("policy %s = %#v, want %#v", key, got, want)
		}
	}
}

func TestExecutableNodeEventDescendantsPreserveDeclarationPackageBoundaryInEitherOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "a_then_b"
		if reverse {
			name = "b_then_a"
		}
		t.Run(name, func(t *testing.T) {
			node := SystemNodeContract{ID: "consumer"}
			packageA := FlowContractView{
				Paths: FlowContractPaths{PackageKey: "packages/a", NodesFile: "packages/a/nodes.yaml"},
				Nodes: map[string]SystemNodeContract{"consumer": node},
				Children: []FlowContractView{
					{
						Path:   "orders/a-local",
						Paths:  FlowContractPaths{ID: "a-local", PackageKey: "packages/a"},
						Events: map[string]EventCatalogEntry{"local.done": {}},
					},
					{
						Path:   "orders/a-child",
						Paths:  FlowContractPaths{ID: "a-child", PackageKey: "packages/a/child"},
						Events: map[string]EventCatalogEntry{"a.done": {}},
					},
				},
			}
			packageB := FlowContractView{
				Paths: FlowContractPaths{PackageKey: "packages/b"},
				Children: []FlowContractView{{
					Path:   "orders/b-child",
					Paths:  FlowContractPaths{ID: "b-child", PackageKey: "packages/b"},
					Events: map[string]EventCatalogEntry{"b.done": {}},
				}},
			}
			packages := []FlowContractView{packageA, packageB}
			if reverse {
				packages[0], packages[1] = packages[1], packages[0]
			}
			flow := FlowContractView{
				Path:     "orders",
				Paths:    FlowContractPaths{ID: "orders", PackageKey: "flows/orders"},
				Children: packages,
			}
			root := FlowContractView{Paths: FlowContractPaths{PackageKey: "."}, Children: []FlowContractView{flow}}
			bundle := &WorkflowContractBundle{
				FlowTree: FlowTree{Root: &root, ByID: map[string]*FlowContractView{"orders": &root.Children[0]}},
				projectContracts: map[string]ProjectContractView{
					"packages/a": {
						Paths: ProjectPackagePaths{Key: "packages/a"},
						Nodes: map[string]SystemNodeContract{"consumer": node},
					},
				},
			}
			ref := identitytest.ExecutableNode(t, "packages/a", "orders", "consumer")

			for _, resolve := range []struct {
				name string
				fn   func(string) string
			}{
				{name: "reference", fn: func(value string) string { return bundle.ResolveExecutableNodeEventReference(ref, value) }},
				{name: "pattern", fn: func(value string) string { return bundle.ResolveExecutableNodeEventPattern(ref, value) }},
			} {
				t.Run(resolve.name, func(t *testing.T) {
					if got := resolve.fn("a-local/local.done"); got != "orders/a-local/local.done" {
						t.Fatalf("exact-package descendant = %q, want orders/a-local/local.done", got)
					}
					if got := resolve.fn("a-child/a.done"); got != "orders/a-child/a.done" {
						t.Fatalf("child-package descendant = %q, want orders/a-child/a.done", got)
					}
					if got := resolve.fn("b-child/b.done"); got != "b-child/b.done" {
						t.Fatalf("sibling-package descendant = %q, want unchanged b-child/b.done", got)
					}
				})
			}
		})
	}
}

func TestExecutableNodeSemanticScopeRejectsMissingOwningFlowBeforeLoadPublication(t *testing.T) {
	source := ContractItemSource{PackageKey: "packages/a", FlowID: "orders", Layer: "project", File: "packages/a/nodes.yaml"}
	key := contractScopeKey(source, "shared")
	bundle := &WorkflowContractBundle{
		scopedNodes:       map[string]SystemNodeContract{key: {ID: "shared"}},
		scopedNodeSources: map[string]ContractItemSource{key: source},
	}
	ref := identitytest.ExecutableNode(t, "packages/a", "orders", "shared")

	if _, err := bundle.ExecutableNodeSemanticScope(ref); err == nil || !strings.Contains(err.Error(), "declaration context") {
		t.Fatalf("semantic scope error = %v, want missing declaration context", err)
	}
	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "semantic scope") {
		t.Fatalf("load validation error = %v, want semantic scope rejection", err)
	}
}

func TestExecutableNodeSemanticScopeRejectsDuplicateExactDeclarationContextsInEitherOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "first_then_second"
		if reverse {
			name = "second_then_first"
		}
		t.Run(name, func(t *testing.T) {
			children := []FlowContractView{
				{Paths: FlowContractPaths{PackageKey: "packages/a", NodesFile: "packages/a/first.yaml"}, Nodes: map[string]SystemNodeContract{"shared": {ID: "shared"}}},
				{Paths: FlowContractPaths{PackageKey: "packages/a", NodesFile: "packages/a/second.yaml"}, Nodes: map[string]SystemNodeContract{"shared": {ID: "shared"}}},
			}
			if reverse {
				children[0], children[1] = children[1], children[0]
			}
			flow := FlowContractView{Path: "orders", Paths: FlowContractPaths{ID: "orders", PackageKey: "flows/orders"}, Children: children}
			root := FlowContractView{Paths: FlowContractPaths{PackageKey: "."}, Children: []FlowContractView{flow}}
			bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: &root, ByID: map[string]*FlowContractView{"orders": &root.Children[0]}}}
			ref := identitytest.ExecutableNode(t, "packages/a", "orders", "shared")

			if _, err := bundle.ExecutableNodeSemanticScope(ref); err == nil || !strings.Contains(err.Error(), "exactly one declaration record, found 2") {
				t.Fatalf("semantic scope error = %v, want duplicate exact-owner rejection", err)
			}
			if err := validateWorkflowContractBundleLoadConstraints(bundle); err == nil || !contractErrorContains(err, "exactly one declaration record, found 2") {
				t.Fatalf("load validation error = %v, want duplicate exact-owner rejection", err)
			}
		})
	}
}

func TestExecutableNodeSemanticScopeRejectsCrossLayerExactOwnerContradiction(t *testing.T) {
	flow := FlowContractView{
		Path: "orders",
		Paths: FlowContractPaths{
			ID: "orders", PackageKey: "flows/orders", NodesFile: "flows/orders/nodes.yaml",
		},
		Nodes: map[string]SystemNodeContract{"shared": {ID: "shared"}},
		Children: []FlowContractView{{
			Paths: FlowContractPaths{PackageKey: "flows/orders", NodesFile: "flows/orders/addon/nodes.yaml"},
			Nodes: map[string]SystemNodeContract{"shared": {ID: "shared"}},
		}},
	}
	root := FlowContractView{Paths: FlowContractPaths{PackageKey: "."}, Children: []FlowContractView{flow}}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: &root, ByID: map[string]*FlowContractView{"orders": &root.Children[0]}}}
	ref := identitytest.ExecutableNode(t, "flows/orders", "orders", "shared")

	if _, err := bundle.ExecutableNodeSemanticScope(ref); err == nil || !strings.Contains(err.Error(), "exactly one declaration record, found 2") {
		t.Fatalf("semantic scope error = %v, want cross-layer exact-owner rejection", err)
	}
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err == nil || !contractErrorContains(err, "exactly one declaration record, found 2") {
		t.Fatalf("load validation error = %v, want cross-layer exact-owner rejection", err)
	}
}
