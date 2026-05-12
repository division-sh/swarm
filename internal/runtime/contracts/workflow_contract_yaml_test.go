package contracts

import (
	"fmt"
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

func TestSystemNodeEventHandlerDecode_RejectsTopLevelEmitWhenRulesExistWithoutRuleEmit(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
emit: root.done
rules:
  pass:
    condition: "payload.ok"
    advances_to: done
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-EMIT") {
		t.Fatalf("yaml.Unmarshal error = %v, want AMBIGUOUS-EMIT", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsRetiredPayloadTransform(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
payload_transform:
  fields:
    score: payload.score
emit: score.ready
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED payload_transform rejection", err)
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
	var schema EntitySchema
	if err := yaml.Unmarshal([]byte(`
scoring_phase:
  revision_count:
    type: integer
    initial: 0
  is_duplicate:
    type: boolean
    initial: false
`), &schema); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(schema.Groups); got != 1 {
		t.Fatalf("len(Groups) = %d", got)
	}
	fields := schema.Groups[0].Fields
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
	var schema EntitySchema
	err := yaml.Unmarshal([]byte(`
scoring_phase:
  revision_count: integer initial 0
`), &schema)
	if err == nil || !strings.Contains(err.Error(), "scalar form cannot declare initial values") {
		t.Fatalf("yaml.Unmarshal error = %v, want scalar initial rejection", err)
	}
}

func TestEntitySchemaDecode_RejectsMappingWithoutType(t *testing.T) {
	var schema EntitySchema
	err := yaml.Unmarshal([]byte(`
scoring_phase:
  revision_count:
    initial: 0
`), &schema)
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

func TestFanOutSpecDecode_RejectsLegacyEmitPerItem(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
emit_per_item: routed.item
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED legacy fan_out emit_per_item rejection", err)
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

func TestWorkflowDataWriteDecode_PreservesTargetPathAuthoring(t *testing.T) {
	var write WorkflowDataWrite
	if err := yaml.Unmarshal([]byte(`
source_field: summary
target_path: entity.analysis.summary
`), &write); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := write.Source(); got != "summary" {
		t.Fatalf("Source() = %q", got)
	}
	if got := write.Target(); got != "entity.analysis.summary" {
		t.Fatalf("Target() = %q", got)
	}
	if got := write.TargetPath.String(); got != "entity.analysis.summary" {
		t.Fatalf("TargetPath = %q", got)
	}
}

func TestWorkflowDataWriteDecode_RejectsConflictingTargetFieldAndTargetPath(t *testing.T) {
	var write WorkflowDataWrite
	err := yaml.Unmarshal([]byte(`
source_field: summary
target_field: analysis
target_path: entity.analysis.summary
`), &write)
	if err == nil {
		t.Fatal("expected conflicting target_field/target_path error")
	}
	if !strings.Contains(err.Error(), "target_field and target_path") {
		t.Fatalf("error = %v", err)
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

func TestSystemNodeEventHandlerDecode_PreservesSelectEntity(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
select_entity:
  by:
    vertical_id: payload.vertical_id
emit: treasury.spend_approved
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if handler.SelectEntity == nil {
		t.Fatal("expected select_entity to decode")
	}
	if got := len(handler.SelectEntity.Bindings); got != 1 {
		t.Fatalf("len(select_entity bindings) = %d, want 1", got)
	}
	binding := handler.SelectEntity.Bindings[0]
	if binding.Field != "vertical_id" || binding.Ref != "payload.vertical_id" {
		t.Fatalf("binding = %+v, want vertical_id -> payload.vertical_id", binding)
	}
	if binding.RefPath.Root.String() != "payload" {
		t.Fatalf("binding root = %q, want payload", binding.RefPath.Root.String())
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnknownSelectEntityField(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
select_entity:
  where:
    vertical_id: payload.vertical_id
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestSystemNodeEventHandlerDecode_PreservesSelectOrCreateEntity(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
select_or_create_entity:
  by:
    repo_id: payload.repo_id
emit: spec_repo.ready
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if handler.SelectOrCreateEntity == nil {
		t.Fatal("expected select_or_create_entity to decode")
	}
	if got := len(handler.SelectOrCreateEntity.Bindings); got != 1 {
		t.Fatalf("len(select_or_create_entity bindings) = %d, want 1", got)
	}
	binding := handler.SelectOrCreateEntity.Bindings[0]
	if binding.Field != "repo_id" || binding.Ref != "payload.repo_id" {
		t.Fatalf("binding = %+v, want repo_id -> payload.repo_id", binding)
	}
	if binding.RefPath.Root.String() != "payload" {
		t.Fatalf("binding root = %q, want payload", binding.RefPath.Root.String())
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnknownSelectOrCreateEntityField(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
select_or_create_entity:
  where:
    repo_id: payload.repo_id
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestHandlerRuleEntryDecode_RejectsEmitFieldsWithoutEvent(t *testing.T) {
	var rule HandlerRuleEntry
	err := yaml.Unmarshal([]byte(`
condition: "else"
emit:
  fields:
    scan_id: payload.scan_id
`), &rule)
	if err == nil || !strings.Contains(err.Error(), "INVALID-EMIT") {
		t.Fatalf("yaml.Unmarshal error = %v, want INVALID-EMIT", err)
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

func TestExpressionValueDecode_PreservesScalarAsLiteralOutsideEmitFields(t *testing.T) {
	var expr ExpressionValue
	if err := yaml.Unmarshal([]byte(`target_state`), &expr); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if expr.Kind != ExpressionKindLiteral {
		t.Fatalf("Kind = %q, want %q", expr.Kind, ExpressionKindLiteral)
	}
	if got := expr.Literal; got != "target_state" {
		t.Fatalf("Literal = %#v, want target_state", got)
	}
}

func TestEmitSpecDecode_ScalarFieldsHydrateAsCELOnlyOnEmitFields(t *testing.T) {
	var spec EmitSpec
	if err := yaml.Unmarshal([]byte(`
event: signals.category_ready
fields:
  mode: payload.mode
  batch: "{'scan_id': payload.scan_id, 'geography': payload.geography}"
  count: 0
  quoted_literal: "'ready'"
  explicit_literal:
    literal: ready
  explicit_ref:
    ref: payload.mode
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	cases := map[string]string{
		"mode":           "payload.mode",
		"batch":          "{'scan_id': payload.scan_id, 'geography': payload.geography}",
		"count":          "0",
		"quoted_literal": "'ready'",
	}
	for field, want := range cases {
		expr := spec.Fields[field]
		if expr.Kind != ExpressionKindCEL || expr.CEL != want {
			t.Fatalf("Fields[%s] = %#v, want CEL %q", field, expr, want)
		}
	}
	if expr := spec.Fields["explicit_literal"]; expr.Kind != ExpressionKindLiteral || expr.Literal != "ready" {
		t.Fatalf("explicit_literal = %#v, want literal ready", expr)
	}
	if expr := spec.Fields["explicit_ref"]; expr.Kind != ExpressionKindRef || expr.Ref != "payload.mode" {
		t.Fatalf("explicit_ref = %#v, want ref payload.mode", expr)
	}
}

func TestEmitSpecDecode_RejectsUnstructuredObjectFieldMappings(t *testing.T) {
	var spec EmitSpec
	err := yaml.Unmarshal([]byte(`
event: signals.category_ready
fields:
  batch:
    scan_id: payload.scan_id
`), &spec)
	if err == nil {
		t.Fatal("expected unstructured emit.fields object mapping to be rejected")
	}
	if !strings.Contains(err.Error(), "explicit expression keys") {
		t.Fatalf("unexpected error: %v", err)
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

func TestSystemNodeEventHandlerDecode_PreservesMailboxWriteAction(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
action:
  id: mailbox_write
  mailbox:
    item_type:
      literal: review_request
    severity:
      literal: urgent
    summary:
      literal: Review validation package
    entity_id:
      ref: event.entity_id
    flow_instance:
      ref: event.flow_instance
    payload:
      review_kind:
        ref: payload.review_kind
      operator_hint:
        literal: inspect_package
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := handler.Action.ID; got != "mailbox_write" {
		t.Fatalf("Action.ID = %q", got)
	}
	if handler.Action.Mailbox == nil {
		t.Fatal("expected Action.Mailbox")
	}
	if got := handler.Action.Mailbox.ItemType.Literal; got != "review_request" {
		t.Fatalf("Mailbox.ItemType.Literal = %#v", got)
	}
	if got := handler.Action.Mailbox.EntityID.Ref; got != "event.entity_id" {
		t.Fatalf("Mailbox.EntityID.Ref = %q", got)
	}
	if got := handler.Action.Mailbox.Payload["review_kind"].Ref; got != "payload.review_kind" {
		t.Fatalf("Mailbox.Payload[review_kind].Ref = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnknownMailboxWriteField(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action:
  id: mailbox_write
  mailbox:
    item_type:
      literal: review_request
    summary:
      literal: Review validation package
    implicit_payload: true
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnsupportedMailboxExpressionShape(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action:
  id: mailbox_write
  mailbox:
    item_type:
      literal: review_request
    summary:
      from_payload: summary
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "explicit expression keys") {
		t.Fatalf("yaml.Unmarshal error = %v, want explicit expression keys", err)
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

func TestSystemNodeEventHandlerDecode_PreservesArtifactRepoCommitAction(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
action:
  id: artifact_repo_commit
  artifact_repo:
    provider: local_git
    repo_id:
      ref: entity.repo_id
    namespace:
      ref: event.run_id
    partition_key:
      ref: entity.project_id
    display_slug:
      ref: entity.display_slug
    request_id:
      ref: payload.request_id
    author:
      literal: artifact-writer
    provenance:
      scope:
        literal: fixture
      source_record_id:
        ref: entity.source_record_id
    allowed_paths:
      - specs/mvp.yaml
    files:
      - path:
          literal: specs/mvp.yaml
        content:
          ref: payload.mvp_yaml
        content_type: yaml
        schema:
          type: object
          required_fields:
            - name
        max_bytes: 4096
    output:
      repo_url: repo_url
      current_ref: current_ref
      file_manifest: file_manifest
      status: status
      failure_reason: failure_reason
      last_request_id: last_request_id
      last_source_event_id: last_source_event_id
    limits:
      max_yaml_bytes: 4096
      max_repo_bytes: 1048576
    failure_event: artifact_repo.commit_failed
    failure_payload:
      request_id:
        ref: payload.request_id
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := handler.Action.ID; got != "artifact_repo_commit" {
		t.Fatalf("Action.ID = %q", got)
	}
	if handler.Action.ArtifactRepo == nil {
		t.Fatal("expected ArtifactRepo")
	}
	if got := handler.Action.ArtifactRepo.Provider; got != "local_git" {
		t.Fatalf("ArtifactRepo.Provider = %q", got)
	}
	if got := handler.Action.ArtifactRepo.Namespace.Ref; got != "event.run_id" {
		t.Fatalf("ArtifactRepo.Namespace = %#v", handler.Action.ArtifactRepo.Namespace)
	}
	if got := handler.Action.ArtifactRepo.PartitionKey.Ref; got != "entity.project_id" {
		t.Fatalf("ArtifactRepo.PartitionKey = %#v", handler.Action.ArtifactRepo.PartitionKey)
	}
	if got := handler.Action.ArtifactRepo.Provenance["scope"].Literal; got != "fixture" {
		t.Fatalf("ArtifactRepo.Provenance[scope] = %#v", handler.Action.ArtifactRepo.Provenance["scope"])
	}
	if got := handler.Action.ArtifactRepo.Files[0].Path.Literal; got != "specs/mvp.yaml" {
		t.Fatalf("ArtifactRepo.Files[0].Path = %#v", got)
	}
	if got := handler.Action.ArtifactRepo.Files[0].Schema.Type; got != "object" {
		t.Fatalf("ArtifactRepo.Files[0].Schema.Type = %q", got)
	}
	if got := handler.Action.ArtifactRepo.Output.CurrentRef; got != "current_ref" {
		t.Fatalf("ArtifactRepo.Output.CurrentRef = %q", got)
	}
	if got := handler.Action.ArtifactRepo.FailureEvent; got != "artifact_repo.commit_failed" {
		t.Fatalf("ArtifactRepo.FailureEvent = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnknownArtifactRepoField(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action:
  id: artifact_repo_commit
  artifact_repo:
    provider: local_git
    shell: git commit
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsLegacyArtifactRepoProductFields(t *testing.T) {
	for _, field := range []string{"vertical_id", "source_validation_case_id", "business_slug", "spec_repo", "spec-repos"} {
		t.Run(field, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(fmt.Sprintf(`
action:
  id: artifact_repo_commit
  artifact_repo:
    provider: local_git
    %s:
      literal: old
`, field)), &handler)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf(`artifact_repo field "%s"`, field)) {
				t.Fatalf("yaml.Unmarshal error = %v, want legacy product field rejection", err)
			}
		})
	}
}

func TestEntityFieldDeclDecode_PreservesMaterializeFromProjection(t *testing.T) {
	var field EntityFieldDecl
	if err := yaml.Unmarshal([]byte(`
type: list<DimensionVerdict>
materialize_from: scoring-node.dimensions_received
project:
  dimension: source.dimension
  score: source.score
`), &field); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := field.MaterializeFrom; got != "scoring-node.dimensions_received" {
		t.Fatalf("MaterializeFrom = %q", got)
	}
	if got := field.Project["dimension"]; got != "source.dimension" {
		t.Fatalf("Project[dimension] = %#v", got)
	}
}

func TestAccumulateSpecDecode_PreservesDescriptionAndRejectsUnknownField(t *testing.T) {
	var spec AccumulateSpec
	if err := yaml.Unmarshal([]byte(`
into: dimensions_received
description: all dimension receipts have arrived
dedup_by: payload.dimension
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := spec.Into; got != "dimensions_received" {
		t.Fatalf("Into = %q", got)
	}
	if got := spec.Description; got != "all dimension receipts have arrived" {
		t.Fatalf("Description = %q", got)
	}

	err := yaml.Unmarshal([]byte(`
legacy_buffer: dimensions_received
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}
