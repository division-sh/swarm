package flowidentity

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func nestedKeyedIdentitySource(t *testing.T) semanticview.Source {
	t.Helper()
	key, err := contracts.ParseTemplateInstanceField("id")
	if err != nil {
		t.Fatal(err)
	}
	root := contracts.FlowContractView{Path: ".", Paths: contracts.FlowContractPaths{FlowPath: "."}, Schema: contracts.FlowSchemaDocument{Name: "root"}}
	root.Children = []contracts.FlowContractView{{Path: "parent", Paths: contracts.FlowContractPaths{FlowPath: "parent"},
		Schema: contracts.FlowSchemaDocument{Name: "parent", Instance: key}, Children: []contracts.FlowContractView{{
			Path: "parent/middle", Paths: contracts.FlowContractPaths{FlowPath: "parent/middle"}, Schema: contracts.FlowSchemaDocument{Name: "middle"},
			Children: []contracts.FlowContractView{{Path: "parent/middle/leaf", Paths: contracts.FlowContractPaths{FlowPath: "parent/middle/leaf"}, Schema: contracts.FlowSchemaDocument{Name: "leaf", Instance: key}}},
		}}}}
	bundle := &contracts.WorkflowContractBundle{RootSchema: &root.Schema, FlowSchemas: map[string]contracts.FlowSchemaDocument{},
		FlowTree: contracts.FlowTree{Root: &root, ByID: map[string]*contracts.FlowContractView{}, ByPath: map[string]*contracts.FlowContractView{}},
	}
	var index func(*contracts.FlowContractView, *contracts.FlowContractView)
	index = func(view, parent *contracts.FlowContractView) {
		view.Parent = parent
		bundle.FlowTree.ByID[view.Paths.FlowPath] = view
		bundle.FlowTree.ByPath[view.Paths.FlowPath] = view
		bundle.FlowSchemas[view.Paths.FlowPath] = view.Schema
		for i := range view.Children {
			index(&view.Children[i], view)
		}
	}
	index(&root, nil)
	return semanticview.Wrap(bundle)
}

func TestA9NestedKeyedIdentityPreservesParentAndSeparateKeySlots(t *testing.T) {
	source := nestedKeyedIdentitySource(t)
	const runID = "11111111-1111-4111-8111-111111111111"
	root := Stored(source, ".", runID, runID, runID, "")
	var leaves []Instance
	for _, parentKey := range []string{"left", "right"} {
		parent, err := KeyedChild(source, root, "parent", parentKey)
		if err != nil {
			t.Fatal(err)
		}
		middle, err := KeylessChild(source, parent, "parent/middle")
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := KeyedChild(source, middle, "parent/middle/leaf", "same")
		if err != nil {
			t.Fatal(err)
		}
		if leaf.ScopeKey != "parent/middle/leaf" || leaf.InstancePath != "parent/"+parentKey+"/middle/leaf/same" ||
			leaf.ParentEntityID != middle.EntityID || leaf.ParentRoute.FlowInstance != middle.InstancePath {
			t.Fatalf("leaf discarded structural parent: %+v", leaf)
		}
		for _, instance := range []Instance{parent, middle, leaf} {
			if err := instance.ValidateConstruction(source, runID); err != nil {
				t.Fatalf("canonical construction %+v rejected: %v", instance, err)
			}
		}
		leaves = append(leaves, leaf)
	}
	if leaves[0].EntityID == leaves[1].EntityID || leaves[0].InstancePath == leaves[1].InstancePath {
		t.Fatal("same leaf key under two parents collapsed")
	}
	for _, mutate := range []func(*Instance){
		func(i *Instance) { i.ParentRoute = ParentRoute{}; i.ParentEntityID = "" },
		func(i *Instance) { i.ParentRoute = leaves[1].ParentRoute; i.ParentEntityID = leaves[1].ParentEntityID },
		func(i *Instance) { i.ParentRoute.FlowID = "parent" },
		func(i *Instance) { i.InstancePath = "parent/middle/leaf/same" },
	} {
		bad := leaves[0]
		mutate(&bad)
		if err := bad.ValidateConstruction(source, runID); err == nil {
			t.Fatalf("invalid nested identity admitted: %+v", bad)
		}
	}
}

func TestA9KeyedChildRefusesAbsentOrNonStructuralParent(t *testing.T) {
	source := nestedKeyedIdentitySource(t)
	const runID = "11111111-1111-4111-8111-111111111111"
	root := Stored(source, ".", runID, runID, runID, "")
	for _, parent := range []Instance{{}, root, Derive(source, "parent", "left")} {
		if _, err := KeyedChild(source, parent, "parent/middle/leaf", "same"); err == nil {
			t.Fatalf("non-parent admitted as nested parent: %+v", parent)
		}
	}
	for _, key := range []string{"", " same", "same/foreign", "same "} {
		if _, err := KeyedChild(source, root, "parent", key); err == nil {
			t.Fatalf("noncanonical discriminator %q admitted", key)
		}
	}
}
