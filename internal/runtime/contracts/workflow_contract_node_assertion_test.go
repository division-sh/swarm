package contracts

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSystemNodeContractTracksProducesDeclarationPresence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		yaml     string
		declared bool
		count    int
	}{
		{name: "omitted", yaml: "event_handlers: {}\n", declared: false},
		{name: "explicit empty", yaml: "produces: []\nevent_handlers: {}\n", declared: true},
		{name: "explicit value", yaml: "produces: [work.completed]\nevent_handlers: {}\n", declared: true, count: 1},
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
