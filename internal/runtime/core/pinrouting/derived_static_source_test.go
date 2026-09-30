package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDerivedStaticSourceValidatesBeforeNormalization(t *testing.T) {
	for _, scopePath := range []string{"worker", "parent/worker"} {
		for _, tc := range []struct {
			name, instance, flow string
			reject               bool
		}{
			{name: "omitted"},
			{name: "exact", instance: scopePath},
			{name: "owned slash descendant", instance: scopePath + "/item"},
			{name: "prefix lookalike", instance: scopePath + "ish/item", reject: true},
			{name: "foreign instance", instance: "foreign/item", reject: true},
			{name: "foreign flow", instance: scopePath, flow: "foreign", reject: true},
		} {
			t.Run(scopePath+"/"+tc.name, func(t *testing.T) {
				source, err := admitFlowExecutionRoutingSource(nil, "node", "worker", contracts.ContractItemSource{FlowPath: scopePath}, events.RouteIdentity{FlowID: tc.flow, FlowInstance: tc.instance, EntityID: "entity"}, semanticview.FlowScope{ID: scopePath, Path: scopePath, Mode: contracts.FlowModeStatic})
				if (err != nil) != tc.reject {
					t.Fatalf("source admission = %v; reject=%t", err, tc.reject)
				}
				if err == nil && (source.Route().FlowID != scopePath || source.Route().FlowInstance != scopePath) {
					t.Fatalf("source did not retain canonical declaration: %#v", source.Route())
				}
			})
		}
	}
}
