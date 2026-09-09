package contracts

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestExecutableNodeSelectiveProjectionMatchesFullCensus(t *testing.T) {
	root := &FlowContractView{Paths: FlowContractPaths{FlowPath: "."}, Nodes: map[string]SystemNodeContract{
		"worker": {Description: "root"}, " worker ": {Description: "first sorted normalized key"},
	}}
	root.Children = []FlowContractView{
		{Paths: FlowContractPaths{FlowPath: "right"}, Nodes: map[string]SystemNodeContract{"worker": {Description: "right"}}},
		{Paths: FlowContractPaths{FlowPath: "left"}, Nodes: map[string]SystemNodeContract{"worker": {Description: "left"}}},
	}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: root}}
	check := func() {
		t.Helper()
		for _, path := range []string{".", "left", "right", "missing"} {
			for _, nodeID := range []string{"worker", "missing"} {
				ref, err := identity.AdmitExecutableNodeDeclaration(path, nodeID)
				if err != nil {
					t.Fatal(err)
				}
				var want ScopedNodeRecord
				found := false
				for _, record := range bundle.ScopedNodeRecords() {
					candidate, err := record.Identity()
					if err == nil && candidate.Equal(ref) {
						want, found = record, true
						break
					}
				}
				got, ok := bundle.ExecutableNode(ref)
				if ok != found || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s/%s selective lookup = %#v/%v, full census = %#v/%v", path, nodeID, got, ok, want, found)
				}
			}
		}
	}
	check()
	delete(root.Nodes, " worker ")
	delete(root.Children[0].Nodes, "worker")
	root.Children[1].Nodes["worker"] = SystemNodeContract{Description: "changed"}
	check()
}

func BenchmarkExecutableNodeLookup(b *testing.B) {
	root := &FlowContractView{Paths: FlowContractPaths{FlowPath: "."}, Nodes: map[string]SystemNodeContract{}}
	for i := range 128 {
		root.Nodes[fmt.Sprintf("node-%03d", i)] = SystemNodeContract{Description: "node"}
	}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: root}}
	ref, err := identity.AdmitExecutableNodeDeclaration(".", "node-127")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := bundle.ExecutableNode(ref); !ok {
			b.Fatal("node missing")
		}
	}
}
