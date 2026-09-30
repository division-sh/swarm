package accumulator

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestA2AccumulatorKeyRequiresCatalogTextField(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Name: "accumulator-test"},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"BusinessRecord": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}, "number": {Type: "integer"}}},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Type: "object", Properties: map[string]runtimecontracts.EventFieldSpec{
				"id": {Type: "text"}, "number": {Type: "integer"}, "record": {Type: "BusinessRecord"}, "event_id": {Type: "text"},
			}}},
		},
	})
	bundle, _ := semanticview.Bundle(source)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "collector")
	for _, key := range []string{"payload.id", "payload.record.id", "payload.event_id", ""} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Key: key}); err != nil {
			t.Fatalf("key %q: %v (schema: %#v)", key, err, semanticview.ResolveEventSchema(source, node.FlowPath(), "item.received"))
		}
	}
	for _, key := range []string{"payload.number", "payload.record.number", "payload.record.absent", "payload.absent", "event.id", "id", "payload", "payload..id", "payload.id.", " payload.id", "payload. id", "${payload.id}"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Key: key}); err == nil {
			t.Fatalf("key %q admitted without an exact catalog text field", key)
		}
	}
	if err := ValidateSpecForHandler(source, node, "unknown", &runtimecontracts.AccumulateSpec{Key: "payload.id"}); err == nil {
		t.Fatal("unknown event catalog admitted a business key")
	}
}
