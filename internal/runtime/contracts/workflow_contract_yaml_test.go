package contracts

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestHandlerRuleEntryDecode_PreservesLegacyComputeShorthand(t *testing.T) {
	var rule HandlerRuleEntry
	if err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  output_field: composite
  expression: "weighted_average(accumulated.scores, accumulated.weights)"
`), &rule); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if rule.Compute == nil {
		t.Fatal("expected rule compute to be preserved")
	}
	if got := rule.Compute.Operation.String(); got != "weighted_average" {
		t.Fatalf("Compute.Operation = %q", got)
	}
	if got := rule.Compute.StoreAs; got != "entity.composite" {
		t.Fatalf("Compute.StoreAs = %q", got)
	}
	if got := rule.Compute.ValueField; got != "score" {
		t.Fatalf("Compute.ValueField = %q", got)
	}
	if got := rule.Compute.WeightField; got != "weight" {
		t.Fatalf("Compute.WeightField = %q", got)
	}
}

func TestFanOutSpecDecode_PreservesStructuredEmitMapping(t *testing.T) {
	var spec FanOutSpec
	if err := yaml.Unmarshal([]byte(`
items_from: payload.items
emit_mapping:
  key_field: item.kind
  mapping:
    a: routed.a
    b: routed.b
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := spec.EmitMappingKey; got != "item.kind" {
		t.Fatalf("EmitMappingKey = %q", got)
	}
	if got := spec.EmitMapping["a"]; got != "routed.a" {
		t.Fatalf("EmitMapping[a] = %q", got)
	}
	if got := spec.EmitMapping["b"]; got != "routed.b" {
		t.Fatalf("EmitMapping[b] = %q", got)
	}
}

func TestGroupBySpecDecode_HydratesPaths(t *testing.T) {
	var spec GroupBySpec
	if err := yaml.Unmarshal([]byte(`
items_from: payload.items
key: category
store_as: entity.grouped
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := spec.ItemsFrom; got != "payload.items" {
		t.Fatalf("ItemsFrom = %q", got)
	}
	if got := spec.Key; got != "category" {
		t.Fatalf("Key = %q", got)
	}
	if got := spec.StoreAs; got != "entity.grouped" {
		t.Fatalf("StoreAs = %q", got)
	}
}

func TestWorkflowDataWriteDecode_TreatsScalarValueAsLiteral(t *testing.T) {
	var write WorkflowDataWrite
	if err := yaml.Unmarshal([]byte(`
source_field: category
value: premium
`), &write); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := write.SourceField; got != "category" {
		t.Fatalf("SourceField = %q", got)
	}
	if got := write.Target(); got != "category" {
		t.Fatalf("Target() = %q", got)
	}
	if got := write.Value.Literal; got != "premium" {
		t.Fatalf("Value.Literal = %#v", got)
	}
	if got := write.Value.CEL; got != "" {
		t.Fatalf("Value.CEL = %q", got)
	}
}
