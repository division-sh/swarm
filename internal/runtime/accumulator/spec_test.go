package accumulator

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestA2AccumulatorKeyRequiresCatalogScalarField(t *testing.T) {
	fields := map[string]runtimecontracts.TypeFieldSpec{
		"id": {Type: "text"}, "number": {Type: "integer"}, "flag": {Type: "boolean"},
		"optional": {Type: "text", IsOptional: true}, "record": {Type: "Record"},
	}
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Name: "accumulator-test"},
		RootEntities: runtimecontracts.EntityContractsDocument{"state": {Fields: map[string]runtimecontracts.EntityFieldDecl{
			"items": {Type: "[Item]"}, "scalar": {Type: "text"},
		}}},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"Record": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}}},
			"Item":   {Fields: fields},
		}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"id": {Type: "text"}, "number": {Type: "integer"}, "flag": {Type: "boolean"},
				"optional": {Type: "text"}, "record": {Type: "Record"},
			}, Required: []string{"id", "number", "flag", "record"}}},
		},
	})
	bundle, _ := semanticview.Bundle(source)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "collector")
	for _, key := range []string{"id", "number", "flag"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Target: "items", Key: key}); err != nil {
			t.Fatalf("key %q: %v", key, err)
		}
	}
	for _, key := range []string{"optional", "record", "record.id", "absent", "payload.id", "state.id", "", " id", "${payload.id}"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Target: "items", Key: key}); err == nil {
			t.Fatalf("key %q admitted without an exact required scalar field", key)
		}
	}
	for _, target := range []string{"missing", "scalar", "state.items"} {
		if err := ValidateSpecForHandler(source, node, "item.received", &runtimecontracts.AccumulateSpec{Target: target, Key: "id"}); err == nil {
			t.Fatalf("target %q admitted without a declared state list", target)
		}
	}
	if err := ValidateSpecForHandler(source, node, "unknown", &runtimecontracts.AccumulateSpec{Target: "items", Key: "id"}); err == nil {
		t.Fatal("unknown event catalog admitted an accumulator")
	}
}
