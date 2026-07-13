package contracts

import (
	"testing"

	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestSystemNodeContractTracksProducesDeclarationPresence(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/contracts/workflow_contract_node_assertion_test.go:TestSystemNodeContractTracksProducesDeclarationPresence"))
	for _, tc := range []struct {
		name     string
		yaml     string
		declared bool
		count    int
	}{
		{name: "omitted", yaml: "id: worker\nevent_handlers: {}\n", declared: false},
		{name: "explicit empty", yaml: "id: worker\nproduces: []\nevent_handlers: {}\n", declared: true},
		{name: "explicit value", yaml: "id: worker\nproduces: [work.completed]\nevent_handlers: {}\n", declared: true, count: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var node SystemNodeContract
			if err := yaml.Unmarshal([]byte(tc.yaml), &node); err != nil {
				t.Fatalf("decode node: %v", err)
			}
			if node.ProducesDeclared != tc.declared || len(node.Produces) != tc.count {
				t.Fatalf("node = %#v, want declared=%v count=%d", node, tc.declared, tc.count)
			}
		})
	}
}
