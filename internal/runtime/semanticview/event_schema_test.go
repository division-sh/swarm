package semanticview

import (
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
)

func TestResolveEventSchema_ReportsUnresolvedTypesAfterBundleResolution(t *testing.T) {
	root := &runtimecontracts.FlowContractView{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"handoff.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"evidence": {Type: "NotDeclared"},
					},
					Required: []string{"evidence"},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
		},
	}

	resolution := ResolveEventSchema(Wrap(bundle), "", "handoff.completed")
	if !resolution.HasSchema {
		t.Fatal("expected event schema resolution")
	}
	if len(resolution.UnresolvedTypes) != 1 || resolution.UnresolvedTypes[0] != "NotDeclared" {
		t.Fatalf("UnresolvedTypes = %#v, want [NotDeclared]", resolution.UnresolvedTypes)
	}
	if err := resolution.UnresolvedTypeError(); err == nil || !strings.Contains(err.Error(), "NotDeclared") {
		t.Fatalf("UnresolvedTypeError = %v, want NotDeclared", err)
	}
}
