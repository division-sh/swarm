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

func TestFlowPinsDecode_AcceptsStructuredEventEntries(t *testing.T) {
	var schema FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
states:
  - pending
initial_state: pending
terminal_states: []
pins:
  inputs:
    events:
      - name: check.requested
        required: false
    reads:
      - field: entity.score
        type: number
  outputs:
    events:
      - name: check.passed
        required: false
    writes:
      - field: entity.status
        type: string
`), &schema); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(schema.Pins.Inputs.Events); got != 1 {
		t.Fatalf("len(Inputs.Events) = %d", got)
	}
	if got := schema.Pins.Inputs.Events[0]; got != "check.requested" {
		t.Fatalf("Inputs.Events[0] = %q", got)
	}
	if got := schema.Pins.Outputs.Events[0]; got != "check.passed" {
		t.Fatalf("Outputs.Events[0] = %q", got)
	}
	if got := schema.Pins.Inputs.Reads[0]; got != "entity.score" {
		t.Fatalf("Inputs.Reads[0] = %q", got)
	}
	if got := schema.Pins.Outputs.Writes[0]; got != "entity.status" {
		t.Fatalf("Outputs.Writes[0] = %q", got)
	}
}

func TestFlowPinsDecode_PreservesLegacyStringEventEntries(t *testing.T) {
	var schema FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
states:
  - pending
initial_state: pending
terminal_states: []
pins:
  inputs:
    events: [check.requested]
    reads: []
  outputs:
    events: [check.passed]
    writes: []
`), &schema); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := schema.Pins.Inputs.Events[0]; got != "check.requested" {
		t.Fatalf("Inputs.Events[0] = %q", got)
	}
	if got := schema.Pins.Outputs.Events[0]; got != "check.passed" {
		t.Fatalf("Outputs.Events[0] = %q", got)
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

func TestWorkflowDataAccumulationDecode_PreservesShorthandWrites(t *testing.T) {
	var spec WorkflowDataAccumulation
	if err := yaml.Unmarshal([]byte(`
stage_one_result: payload.result
resolution_method: "first"
dispatch_count:
  source: fan_out.count
result:
  status: processed
score_expr: "entity.score + 1"
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(spec.Writes); got != 5 {
		t.Fatalf("len(Writes) = %d", got)
	}
	if got := spec.Writes[0].Target(); got != "stage_one_result" {
		t.Fatalf("Writes[0].Target() = %q", got)
	}
	if got := spec.Writes[0].Source(); got != "payload.result" {
		t.Fatalf("Writes[0].Source() = %q", got)
	}
	if got := spec.Writes[1].Value.Literal; got != "first" {
		t.Fatalf("Writes[1].Value.Literal = %#v", got)
	}
	if got := spec.Writes[2].Source(); got != "fan_out.count" {
		t.Fatalf("Writes[2].Source() = %q", got)
	}
	if status, ok := spec.Writes[3].Value.Literal.(map[string]any)["status"]; !ok || status != "processed" {
		t.Fatalf("Writes[3].Value.Literal = %#v", spec.Writes[3].Value.Literal)
	}
	if got := spec.Writes[4].Value.CEL; got != "entity.score + 1" {
		t.Fatalf("Writes[4].Value.CEL = %q", got)
	}
}

func TestWorkflowDataAccumulationDecode_AppliesLegacySourceAlias(t *testing.T) {
	var spec WorkflowDataAccumulation
	if err := yaml.Unmarshal([]byte(`
writes: [value]
source: payload.value
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(spec.Writes); got != 1 {
		t.Fatalf("len(Writes) = %d", got)
	}
	if got := spec.Writes[0].Source(); got != "payload.value" {
		t.Fatalf("Writes[0].Source() = %q", got)
	}
	if got := spec.Writes[0].Target(); got != "value" {
		t.Fatalf("Writes[0].Target() = %q", got)
	}
}

func TestHandlerRuleEntryDecode_PreservesRuleLevelFanOut(t *testing.T) {
	var rule HandlerRuleEntry
	if err := yaml.Unmarshal([]byte(`
condition: "payload.mode == 'parallel'"
fan_out:
  items_from: payload.items
data_accumulation:
  dispatch_count:
    source: fan_out.count
`), &rule); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if rule.FanOut == nil {
		t.Fatal("expected rule fan_out to be preserved")
	}
	if got := rule.FanOut.ItemsFrom; got != "payload.items" {
		t.Fatalf("FanOut.ItemsFrom = %q", got)
	}
	if got := rule.DataAccumulation.Writes[0].Source(); got != "fan_out.count" {
		t.Fatalf("DataAccumulation source = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_MergesScalarActionWithCreateFlowFields(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
action: create_flow_instance
template: worker
instance_id_from: payload.worker_id
config_from:
  name: payload.name
  priority: payload.priority
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := handler.Action.ID; got != "create_flow_instance" {
		t.Fatalf("Action.ID = %q", got)
	}
	if got := handler.Action.Template; got != "worker" {
		t.Fatalf("Action.Template = %q", got)
	}
	if got := handler.Action.InstanceIDFrom; got != "payload.worker_id" {
		t.Fatalf("Action.InstanceIDFrom = %q", got)
	}
	if handler.Action.ConfigFrom == nil {
		t.Fatal("expected Action.ConfigFrom")
	}
	if got := handler.Action.ConfigFrom.Bindings["name"]; got != "payload.name" {
		t.Fatalf("ConfigFrom.Bindings[name] = %q", got)
	}
	if got := handler.Action.ConfigFrom.Bindings["priority"]; got != "payload.priority" {
		t.Fatalf("ConfigFrom.Bindings[priority] = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_PreservesEvidenceTarget(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
action: record_evidence
evidence_target: validation.results
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := handler.Action.ID; got != "record_evidence" {
		t.Fatalf("Action.ID = %q", got)
	}
	if got := handler.EvidenceTarget; got != "validation.results" {
		t.Fatalf("EvidenceTarget = %q", got)
	}
}
