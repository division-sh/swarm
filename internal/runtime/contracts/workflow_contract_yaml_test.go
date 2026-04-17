package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestHandlerRuleEntryDecode_RejectsLegacyComputeExpressionShorthand(t *testing.T) {
	var rule HandlerRuleEntry
	err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  store_as: entity.composite
  expression: "weighted_average(accumulated.scores, accumulated.weights)"
`), &rule)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsLegacyCreateFlowInstanceActionShape(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action:
  type: create_flow_instance
  flow_template: worker-flow
  instance_id: "{{payload.instance_id}}"
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "DEPRECATED: legacy action field") {
		t.Fatalf("yaml.Unmarshal error = %v, want legacy action field rejection", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsActionMappingMissingID(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action:
  template: worker
  instance_id_from: payload.instance_id
  config_from:
    owner: payload.owner
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "action mapping missing id") {
		t.Fatalf("yaml.Unmarshal error = %v, want action mapping missing id", err)
	}
}

func TestHandlerRuleEntryDecode_AcceptsSpecComputeMetadataFields(t *testing.T) {
	var rule HandlerRuleEntry
	if err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  operation: pick_or_average
  description: choose the strongest score
  params:
    strategy: strict
  store_as: entity.composite
  keys:
    numeric_keys: [score]
`), &rule); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if rule.Compute == nil {
		t.Fatal("expected rule compute to be preserved")
	}
	if got := rule.Compute.Description; got != "choose the strongest score" {
		t.Fatalf("Compute.Description = %q", got)
	}
	if got := rule.Compute.Params["strategy"]; got != "strict" {
		t.Fatalf("Compute.Params[strategy] = %#v", got)
	}
}

func TestHandlerRuleEntryDecode_AcceptsPickOrAverageOperation(t *testing.T) {
	var rule HandlerRuleEntry
	if err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  operation: pick_or_average
  store_as: entity.composite
  keys:
    numeric_keys: [score]
`), &rule); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if rule.Compute == nil {
		t.Fatal("expected rule compute to be preserved")
	}
	if got := rule.Compute.Operation.String(); got != "pick_or_average" {
		t.Fatalf("Compute.Operation = %q", got)
	}
}

func TestHandlerRuleEntryDecode_RejectsWeightedSumOperation(t *testing.T) {
	var rule HandlerRuleEntry
	err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  operation: weighted_sum
  store_as: entity.composite
  keys:
    numeric_keys: [score]
`), &rule)
	if err == nil || !strings.Contains(err.Error(), "unsupported compute operation") {
		t.Fatalf("yaml.Unmarshal error = %v, want unsupported compute operation", err)
	}
}

func TestHandlerRuleEntryDecode_RejectsLegacyOutputFieldAlias(t *testing.T) {
	var rule HandlerRuleEntry
	err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  operation: pick_or_average
  output_field: composite
  keys:
    numeric_keys: [score]
`), &rule)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
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

func TestEntitySchemaDecode_AcceptsMappingInitialValue(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: test
version: 1.0.0
entity_schema:
  scoring_phase:
    revision_count:
      type: integer
      initial: 0
    is_duplicate:
      type: boolean
      initial: false
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(doc.EntitySchema.Groups); got != 1 {
		t.Fatalf("len(EntitySchema.Groups) = %d", got)
	}
	fields := doc.EntitySchema.Groups[0].Fields
	if got := fields[0].Name; got != "revision_count" {
		t.Fatalf("Fields[0].Name = %q", got)
	}
	if got := fields[0].Initial; got != 0 {
		t.Fatalf("Fields[0].Initial = %#v", got)
	}
	if got := fields[1].Initial; got != false {
		t.Fatalf("Fields[1].Initial = %#v", got)
	}
}

func TestEntitySchemaDecode_RejectsScalarInitialSuffix(t *testing.T) {
	var doc ProjectPackageDocument
	err := yaml.Unmarshal([]byte(`
name: test
version: 1.0.0
entity_schema:
  scoring_phase:
    revision_count: integer initial 0
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "scalar form cannot declare initial values") {
		t.Fatalf("yaml.Unmarshal error = %v, want scalar initial rejection", err)
	}
}

func TestEntitySchemaDecode_RejectsMappingWithoutType(t *testing.T) {
	var doc ProjectPackageDocument
	err := yaml.Unmarshal([]byte(`
name: test
version: 1.0.0
entity_schema:
  scoring_phase:
    revision_count:
      initial: 0
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "type is required") {
		t.Fatalf("yaml.Unmarshal error = %v, want missing-type rejection", err)
	}
}

func TestFanOutSpecDecode_RejectsLegacyStructuredEmitMapping(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
emit_mapping:
  key_field: item.kind
  mapping:
    a: routed.a
    b: routed.b
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED legacy fan_out emit mapping rejection", err)
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
target_field: category
value: premium
`), &write); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := write.SourceField; got != "" {
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

func TestWorkflowDataAccumulationDecode_PreservesCanonicalWriteForms(t *testing.T) {
	var spec WorkflowDataAccumulation
	if err := yaml.Unmarshal([]byte(`
writes:
  - stage_one_result
  - source_field: result
    target_field: stage_one_result_copy
  - target_field: resolution_method
    value: first
  - target_field: dispatch_count
    expression: fan_out.count
  - target_field: score_expr
    expression: entity.score + 1
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(spec.Writes); got != 5 {
		t.Fatalf("len(Writes) = %d", got)
	}
	if got := spec.Writes[0].Target(); got != "stage_one_result" {
		t.Fatalf("Writes[0].Target() = %q", got)
	}
	if got := spec.Writes[0].Source(); got != "stage_one_result" {
		t.Fatalf("Writes[0].Source() = %q", got)
	}
	if got := spec.Writes[1].Source(); got != "result" {
		t.Fatalf("Writes[1].Source() = %q", got)
	}
	if got := spec.Writes[2].Value.Literal; got != "first" {
		t.Fatalf("Writes[2].Value.Literal = %#v", got)
	}
	if got := spec.Writes[3].Value.CEL; got != "fan_out.count" {
		t.Fatalf("Writes[3].Value.CEL = %q", got)
	}
	if got := spec.Writes[4].Value.CEL; got != "entity.score + 1" {
		t.Fatalf("Writes[4].Value.CEL = %q", got)
	}
}

func TestWorkflowDataAccumulationDecode_RejectsLegacySourceAlias(t *testing.T) {
	var spec WorkflowDataAccumulation
	err := yaml.Unmarshal([]byte(`
writes: [value]
source: payload.value
`), &spec)
	if err != nil {
		if strings.Contains(err.Error(), "unsupported workflow data accumulation field") {
			return
		}
		t.Fatalf("yaml.Unmarshal error = %v", err)
	}
	t.Fatal("expected legacy source alias to be rejected")
}

func TestSystemNodeEventHandlerDecode_PreservesCreateEntity(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
create_entity: true
emit: scoring.requested
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !handler.CreateEntity {
		t.Fatal("expected create_entity to decode as true")
	}
	if got := handler.Emit.EventType(); got != "scoring.requested" {
		t.Fatalf("Emit.EventType() = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsTieredWeightedAverageWithoutDimensionKey(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
compute:
  operation: weighted_average
  keys:
    score_keys: [score]
  tiers:
    - dimensions: [build_complexity]
      weight: 1
  store_as: entity.composite_score
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "keys.dimension_key") {
		t.Fatalf("yaml.Unmarshal error = %v, want keys.dimension_key error", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsTieredWeightedAverageWithoutScoreKeys(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
compute:
  operation: weighted_average
  keys:
    dimension_key: dimension
  tiers:
    - dimensions: [build_complexity]
      weight: 1
  store_as: entity.composite_score
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "keys.score_keys") {
		t.Fatalf("yaml.Unmarshal error = %v, want keys.score_keys error", err)
	}
}

func TestWorkflowDataWriteDecode_PreservesExpressionAliasInListForm(t *testing.T) {
	var write WorkflowDataWrite
	if err := yaml.Unmarshal([]byte(`
target_field: dimensions_requested
expression: policy.scoring_dimensions
`), &write); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := write.Target(); got != "dimensions_requested" {
		t.Fatalf("Target() = %q", got)
	}
	if got := write.Value.CEL; got != "policy.scoring_dimensions" {
		t.Fatalf("Value.CEL = %q", got)
	}
}

func TestWorkflowDataWriteDecode_PreservesLiteralValueAndExpressionForms(t *testing.T) {
	var write WorkflowDataWrite
	if err := yaml.Unmarshal([]byte(`
target_field: scoring_rubric
expression: '"corpus_rubric"'
`), &write); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := write.Target(); got != "scoring_rubric" {
		t.Fatalf("Target() = %q", got)
	}
	if got := write.Value.CEL; got != `"corpus_rubric"` {
		t.Fatalf("Value.CEL = %q", got)
	}
}

func TestWorkflowDataAccumulationDecode_RejectsShorthandMapping(t *testing.T) {
	var spec WorkflowDataAccumulation
	err := yaml.Unmarshal([]byte(`
dimensions_requested:
  expression: policy.scoring_dimensions
`), &spec)
	if err == nil {
		t.Fatal("expected shorthand mapping to be rejected")
	}
}

func TestExpressionValueDecode_PreservesExpressionAliasInMappingForm(t *testing.T) {
	var expr ExpressionValue
	if err := yaml.Unmarshal([]byte(`
expression: entity.score + 1
`), &expr); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := expr.CEL; got != "entity.score + 1" {
		t.Fatalf("CEL = %q", got)
	}
}

func TestHandlerRuleEntryDecode_PreservesRuleLevelFanOut(t *testing.T) {
	var rule HandlerRuleEntry
	if err := yaml.Unmarshal([]byte(`
condition: "payload.mode == 'parallel'"
fan_out:
  items_from: payload.items
data_accumulation:
  writes:
    - target_field: dispatch_count
      expression: fan_out.count
`), &rule); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if rule.FanOut == nil {
		t.Fatal("expected rule fan_out to be preserved")
	}
	if got := rule.FanOut.ItemsFrom; got != "payload.items" {
		t.Fatalf("FanOut.ItemsFrom = %q", got)
	}
	if got := rule.DataAccumulation.Writes[0].Value.CEL; got != "fan_out.count" {
		t.Fatalf("DataAccumulation expression = %q", got)
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

func TestSystemNodeEventHandlerDecode_RejectsConfigFromPolicyKeys(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action: create_flow_instance
template: worker
instance_id_from: payload.worker_id
config_from:
  policy_keys: [priority_profile]
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
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

func TestSystemNodeEventHandlerDecode_RejectsUnsupportedActionID(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action: increment_revision_count
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "unsupported handler action") {
		t.Fatalf("yaml.Unmarshal error = %v, want unsupported handler action", err)
	}
}
