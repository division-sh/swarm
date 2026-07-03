package contracts

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProjectPackageDocumentDecode_PreservesRequiresAndImportBinds(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: package-boundary
version: "1.0.0"
platform_version: ">=1.0.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
  policy: [provider.threshold]
  credentials: [provider_token]
  platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    bind:
      inputs:
        work.requested: parent.work_requested
      outputs:
        work.completed: parent.work_completed
      policy:
        provider.threshold: parent.policy.threshold
      credentials:
        provider_token: parent_provider_token
packages:
  - path: packages/child
    bind:
      inputs:
        child.requested: parent.child_requested
      outputs:
        child.completed: parent.child_completed
      policy:
        child.policy: parent.policy.child
      credentials:
        child_token: parent_child_token
connect:
  - from: worker.work.completed
    to: worker.work.requested
    delivery: one
    map:
      work_id:
        source: payload.work_id
        target: entity.work_id
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := strings.Join(doc.Requires.Inputs, ","); got != "work.requested" {
		t.Fatalf("Requires.Inputs = %q", got)
	}
	if got := strings.Join(doc.Requires.Outputs, ","); got != "work.completed" {
		t.Fatalf("Requires.Outputs = %q", got)
	}
	if got := strings.Join(doc.Requires.Policy, ","); got != "provider.threshold" {
		t.Fatalf("Requires.Policy = %q", got)
	}
	if got := strings.Join(doc.Requires.Credentials, ","); got != "provider_token" {
		t.Fatalf("Requires.Credentials = %q", got)
	}
	if got := doc.Requires.PlatformVersion; got != ">=1.6.0" {
		t.Fatalf("Requires.PlatformVersion = %q", got)
	}
	if got := doc.Flows[0].Bind.Inputs["work.requested"]; got != "parent.work_requested" {
		t.Fatalf("flow bind input = %q", got)
	}
	if got := doc.Flows[0].Bind.Outputs["work.completed"]; got != "parent.work_completed" {
		t.Fatalf("flow bind output = %q", got)
	}
	if got := doc.Flows[0].Bind.Policy["provider.threshold"]; got != "parent.policy.threshold" {
		t.Fatalf("flow bind policy = %q", got)
	}
	if got := doc.Flows[0].Bind.Credentials["provider_token"]; got != "parent_provider_token" {
		t.Fatalf("flow bind credential = %q", got)
	}
	if got := doc.Packages[0].Bind.Inputs["child.requested"]; got != "parent.child_requested" {
		t.Fatalf("package bind input = %q", got)
	}
	if len(doc.Connect) != 1 {
		t.Fatalf("Connect len = %d, want 1", len(doc.Connect))
	}
	if got, want := doc.Connect[0].From, "worker.work.completed"; got != want {
		t.Fatalf("Connect[0].From = %q, want %q", got, want)
	}
	if got, want := doc.Connect[0].Map["work_id"].Target, "entity.work_id"; got != want {
		t.Fatalf("Connect[0].Map[work_id].Target = %q, want %q", got, want)
	}
}

func TestProjectPackageDocumentDecode_PreservesPolicyRequiresDefaults(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: package-boundary
requires:
  policy:
    provider.threshold:
      type: number
      description: Non-secret provider threshold.
      default: 0.8
    provider.mode: {}
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := strings.Join(doc.Requires.Policy, ","); got != "provider.threshold,provider.mode" {
		t.Fatalf("Requires.Policy = %q", got)
	}
	threshold, ok := doc.Requires.PolicyDefaults["provider.threshold"]
	if !ok {
		t.Fatalf("provider.threshold default missing: %#v", doc.Requires.PolicyDefaults)
	}
	if got, ok := threshold.Value.(float64); !ok || got != 0.8 {
		t.Fatalf("provider.threshold default = %#v, want 0.8", threshold.Value)
	}
	if _, ok := doc.Requires.PolicyDefaults["provider.mode"]; ok {
		t.Fatalf("provider.mode unexpectedly has a default: %#v", doc.Requires.PolicyDefaults["provider.mode"])
	}
}

func TestProjectPackageDocumentDecode_ListPolicyRequiresAreRequiredNoDefault(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: package-boundary
requires:
  policy: [provider.threshold]
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := strings.Join(doc.Requires.Policy, ","); got != "provider.threshold" {
		t.Fatalf("Requires.Policy = %q", got)
	}
	if len(doc.Requires.PolicyDefaults) != 0 {
		t.Fatalf("PolicyDefaults = %#v, want none", doc.Requires.PolicyDefaults)
	}
}

func TestProjectPackageDocumentDecode_RejectsMalformedRequiresAndBindShape(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "unknown policy requirement option",
			body: `
name: invalid
requires:
  policy:
    provider.threshold:
      fallback: 0.8
`,
			wantErr: "UNDEFINED-FIELD",
		},
		{
			name: "policy requirement must be mapping",
			body: `
name: invalid
requires:
  policy:
    provider.threshold: 0.8
`,
			wantErr: "policy requirement must be a mapping",
		},
		{
			name: "unknown requires field",
			body: `
name: invalid
requires:
  inputz: [work.requested]
`,
			wantErr: "UNDEFINED-FIELD",
		},
		{
			name: "bind inputs must be mapping",
			body: `
name: invalid
flows:
  - id: worker
    flow: worker
    bind:
      inputs: [work.requested]
`,
			wantErr: "bind.inputs",
		},
		{
			name: "unknown bind field",
			body: `
name: invalid
packages:
  - path: packages/child
    bind:
      credential: {}
`,
			wantErr: "UNDEFINED-FIELD",
		},
		{
			name: "unknown connect field",
			body: `
name: invalid
connect:
  - from: producer.ready
    to: consumer.ready
    topic: unsupported
`,
			wantErr: "UNDEFINED-FIELD",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc ProjectPackageDocument
			err := yaml.Unmarshal([]byte(tc.body), &doc)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestFlowSchemaDocumentDecode_PreservesAddressedInputPins(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: addressed-pins
pins:
  inputs:
    events:
      - work.started
      - name: deploy_completed
        event: deploy.completed
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
          mode: select_existing
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id, component_id]
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := strings.Join(doc.Pins.Inputs.Events, ","), "work.started,deploy.completed"; got != want {
		t.Fatalf("input Events = %q, want %q", got, want)
	}
	if len(doc.Pins.Inputs.EventPins) != 2 {
		t.Fatalf("input EventPins len = %d, want 2", len(doc.Pins.Inputs.EventPins))
	}
	addressed := doc.Pins.Inputs.EventPins[1]
	if got, want := addressed.PinName(), "deploy_completed"; got != want {
		t.Fatalf("addressed PinName = %q, want %q", got, want)
	}
	if got, want := addressed.EventType(), "deploy.completed"; got != want {
		t.Fatalf("addressed EventType = %q, want %q", got, want)
	}
	if addressed.Address == nil {
		t.Fatal("expected addressed input pin address")
	}
	if got, want := addressed.Address.Target, "entity.vertical_id"; got != want {
		t.Fatalf("Address.Target = %q, want %q", got, want)
	}
	if got, want := strings.Join(doc.Pins.Outputs.Events, ","), "deploy.done"; got != want {
		t.Fatalf("output Events = %q, want %q", got, want)
	}
	if got, want := doc.Pins.Outputs.EventPins[0].PinName(), "deploy_done"; got != want {
		t.Fatalf("output PinName = %q, want %q", got, want)
	}
	if got, want := doc.Pins.Outputs.EventPins[0].Key, "vertical_id"; got != want {
		t.Fatalf("output Key = %q, want %q", got, want)
	}
	if got, want := strings.Join(doc.Pins.Outputs.EventPins[0].Carries, ","), "vertical_id,component_id"; got != want {
		t.Fatalf("output Carries = %q, want %q", got, want)
	}
}

func TestFlowSchemaDocumentDecode_RejectsUnsupportedAddressedPinFields(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: invalid-pins
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.completed
        address:
          by: vertical_id
          unsupported: nope
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestFlowSchemaDocumentDecode_RejectsUnsupportedOutputPinFields(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: invalid-output-pins
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        unknown: nope
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

func TestFlowSchemaDocumentDecode_RejectsRetiredAndUnsupportedTopLevelFields(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		wantErr string
	}{
		{name: "namespace_prefix", field: "namespace_prefix: worker", wantErr: "RETIRED"},
		{name: "namespace_rule", field: "namespace_rule: path", wantErr: "RETIRED"},
		{name: "namespace", field: "namespace: worker", wantErr: "UNDEFINED-FIELD"},
		{name: "unknown", field: "legacy_owner: worker", wantErr: "UNDEFINED-FIELD"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc FlowSchemaDocument
			err := yaml.Unmarshal([]byte("name: invalid-schema\n"+tc.field+"\n"), &doc)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestFlowSchemaDocumentDecode_PreservesTemplateInstanceDeclaration(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: template
instance:
  by: [scope, scope_id, artifact_type]
  on_missing: create
  on_conflict: reject
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := doc.Mode, "template"; got != want {
		t.Fatalf("Mode = %q, want %q", got, want)
	}
	if got, want := strings.Join(doc.Instance.By, ","), "scope,scope_id,artifact_type"; got != want {
		t.Fatalf("Instance.By = %q, want %q", got, want)
	}
	if got, want := doc.Instance.OnMissing, "create"; got != want {
		t.Fatalf("Instance.OnMissing = %q, want %q", got, want)
	}
	if !doc.Instance.OnMissingDeclared {
		t.Fatal("Instance.OnMissingDeclared = false, want true")
	}
	if got, want := doc.Instance.OnConflict, "reject"; got != want {
		t.Fatalf("Instance.OnConflict = %q, want %q", got, want)
	}
	if !doc.Instance.OnConflictDeclared {
		t.Fatal("Instance.OnConflictDeclared = false, want true")
	}
}

func TestFlowSchemaDocumentDecode_PreservesOmittedTemplateInstancePolicyPresence(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: template
instance:
  by: scope_id
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !doc.Instance.Declared {
		t.Fatal("Instance.Declared = false, want true")
	}
	if got, want := strings.Join(doc.Instance.By, ","), "scope_id"; got != want {
		t.Fatalf("Instance.By = %q, want %q", got, want)
	}
	if doc.Instance.OnMissingDeclared || doc.Instance.OnConflictDeclared {
		t.Fatalf("policy presence = %v/%v, want both omitted", doc.Instance.OnMissingDeclared, doc.Instance.OnConflictDeclared)
	}
	if doc.Instance.OnMissing != "" || doc.Instance.OnConflict != "" {
		t.Fatalf("raw policies = %q/%q, want empty before resolver defaults", doc.Instance.OnMissing, doc.Instance.OnConflict)
	}
}

func TestFlowSchemaDocumentDecode_PreservesExplicitEmptyTemplateInstancePolicies(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: template
instance:
  by: scope_id
  on_missing: ""
  on_conflict:
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !doc.Instance.OnMissingDeclared || !doc.Instance.OnConflictDeclared {
		t.Fatalf("policy presence = %v/%v, want both explicit", doc.Instance.OnMissingDeclared, doc.Instance.OnConflictDeclared)
	}
	if doc.Instance.OnMissing != "" || doc.Instance.OnConflict != "" {
		t.Fatalf("raw policies = %q/%q, want empty explicit values", doc.Instance.OnMissing, doc.Instance.OnConflict)
	}
}

func TestFlowSchemaDocumentDecode_PreservesSingletonMode(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: coordinator-flow
mode: singleton
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := doc.Mode, "singleton"; got != want {
		t.Fatalf("Mode = %q, want %q", got, want)
	}
}

func TestFlowSchemaDocumentDecode_PreservesTemplateInstanceDuplicateKeysForResolver(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: template
instance:
  by: [tenant_id, tenant_id]
  on_missing: reject
  on_conflict: reuse
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := strings.Join(doc.Instance.By, ","), "tenant_id,tenant_id"; got != want {
		t.Fatalf("Instance.By = %q, want duplicate-preserving %q", got, want)
	}
}

func TestFlowSchemaDocumentDecode_PreservesEmptyTemplateInstancePresence(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: static
instance: {}
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !doc.Instance.Declared {
		t.Fatal("Instance.Declared = false, want explicit empty declaration preserved")
	}
	if doc.Instance.Empty() {
		t.Fatal("Instance.Empty() = true for explicit empty declaration, want false")
	}
}

func TestFlowSchemaDocumentDecode_RejectsUnsupportedTemplateInstanceFields(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: invalid-template
mode: template
instance:
  by: account_id
  on_missing: create
  on_conflict: reject
  fallback: legacy
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "UNDEFINED-FIELD") {
		t.Fatalf("yaml.Unmarshal error = %v, want UNDEFINED-FIELD", err)
	}
}

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

func TestSystemNodeEventHandlerDecode_RejectsRuleLevelSetsGate(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
rules:
  gated:
    condition: "else"
    sets_gate: approved
`), &handler)
	if err == nil || !strings.Contains(err.Error(), `UNDEFINED-FIELD: rule field "sets_gate"`) {
		t.Fatalf("yaml.Unmarshal error = %v, want rule-level sets_gate rejection", err)
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

func TestSystemNodeEventHandlerDecode_RejectsRetiredClearTarget(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
clear:
  target: entity.summary
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "targets") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED clear.target rejection", err)
	}
}

func TestSystemNodeEventHandlerDecode_PreservesCanonicalClearTargets(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
clear:
  targets:
    - entity.summary
    - pending_dedup
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if handler.Clear == nil || len(handler.Clear.Targets) != 2 {
		t.Fatalf("Clear = %#v, want two canonical targets", handler.Clear)
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
      - name: check_requested
        event: check.requested
    reads:
      - field: entity.score
        type: number
  outputs:
    events:
      - name: check_passed
        event: check.passed
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
	if got := schema.Pins.Inputs.EventPins[0].PinName(); got != "check_requested" {
		t.Fatalf("Inputs.EventPins[0].PinName() = %q", got)
	}
	if got := schema.Pins.Outputs.Events[0]; got != "check.passed" {
		t.Fatalf("Outputs.Events[0] = %q", got)
	}
	if got := schema.Pins.Outputs.EventPins[0].PinName(); got != "check_passed" {
		t.Fatalf("Outputs.EventPins[0].PinName() = %q", got)
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

func TestSystemNodeContractDecode_PreservesSupportedTopLevelFields(t *testing.T) {
	var node SystemNodeContract
	if err := yaml.Unmarshal([]byte(`
id: worker
description: Worker node
execution_type: system_node
subscribes_to: [task.requested]
produces: [task.completed]
state_table: worker_state
state_schema:
  fields:
    count:
      type: integer
timers:
  - id: task_timeout
    event: timer.task.timeout
    delay: 1m
gate_state:
  ready: Worker is ready
event_handlers:
  task.requested:
    advances_to: done
`), &node); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := strings.TrimSpace(node.StateTable), "worker_state"; got != want {
		t.Fatalf("StateTable = %q, want %q", got, want)
	}
	if _, ok := node.EventHandlers["task.requested"]; !ok {
		t.Fatalf("task.requested handler missing: %#v", node.EventHandlers)
	}
	if len(node.StateSchema.Fields) != 1 {
		t.Fatalf("StateSchema fields = %#v, want one field", node.StateSchema.Fields)
	}
}

func TestWorkflowTimerContractDecode_RejectsRetiredDurationAliases(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "seconds", field: "delay_seconds: 30"},
		{name: "minutes", field: "delay_minutes: 5"},
		{name: "hours", field: "delay_hours: 2"},
		{name: "days", field: "delay_days: 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var timer WorkflowTimerContract
			err := yaml.Unmarshal([]byte(`
id: reminder
event: timer.reminder
`+tc.field+`
`), &timer)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), strings.Split(tc.field, ":")[0]) || !strings.Contains(err.Error(), "delay") {
				t.Fatalf("yaml.Unmarshal error = %v, want retired alias rejection for %s", err, tc.field)
			}
		})
	}
}

func TestWorkflowTimerContractDecode_RejectsMixedCanonicalAndRetiredDurationAlias(t *testing.T) {
	var timer WorkflowTimerContract
	err := yaml.Unmarshal([]byte(`
id: reminder
event: timer.reminder
delay: 30m
delay_minutes: 30
`), &timer)
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with canonical delay") {
		t.Fatalf("yaml.Unmarshal error = %v, want mixed canonical+retired alias rejection", err)
	}
}

func TestWorkflowTimerContractDecode_RejectsMergedRetiredDurationAliases(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "seconds", field: "delay_seconds: 30"},
		{name: "minutes", field: "delay_minutes: 5"},
		{name: "hours", field: "delay_hours: 2"},
		{name: "days", field: "delay_days: 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc struct {
				Timer WorkflowTimerContract `yaml:"timer"`
			}
			err := yaml.Unmarshal([]byte(`
timer_defaults: &timer_defaults
  `+tc.field+`
timer:
  <<: *timer_defaults
  id: reminder
  event: timer.reminder
`), &doc)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), strings.Split(tc.field, ":")[0]) {
				t.Fatalf("yaml.Unmarshal error = %v, want merged retired alias rejection for %s", err, tc.field)
			}
		})
	}
}

func TestWorkflowTimerContractDecode_PreservesCanonicalDelay(t *testing.T) {
	var timer WorkflowTimerContract
	if err := yaml.Unmarshal([]byte(`
id: reminder
event: timer.reminder
delay: 7d
`), &timer); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := strings.TrimSpace(timer.Delay); got != "7d" {
		t.Fatalf("Delay = %q, want 7d", got)
	}
}

func TestFlowSchemaDocumentDecode_PreservesRequiredAgentSubscribesTo(t *testing.T) {
	var schema FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: worker
required_agents:
  - role: analyst
    subscribes_to: [task.requested]
    emits: [task.completed]
`), &schema); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if len(schema.RequiredAgents) != 1 || len(schema.RequiredAgents[0].SubscribesTo) != 1 || schema.RequiredAgents[0].SubscribesTo[0] != "task.requested" {
		t.Fatalf("RequiredAgents = %#v, want canonical required-agent subscribes_to", schema.RequiredAgents)
	}
}

func TestSystemNodeContractDecode_RejectsRetiredAndUnsupportedTopLevelFields(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		wantErr string
	}{
		{name: "permissions", field: "permissions: [create_flow_instance]", wantErr: "RETIRED"},
		{name: "implementation", field: "implementation: builtin", wantErr: "RETIRED"},
		{name: "owned_transitions", field: "owned_transitions: [ticket-open]", wantErr: "RETIRED"},
		{name: "idempotency_table", field: "idempotency_table: worker_idempotency", wantErr: "RETIRED"},
		{name: "unknown", field: "legacy_owner: worker", wantErr: "UNDEFINED-FIELD"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var node SystemNodeContract
			err := yaml.Unmarshal([]byte("id: worker\n"+tc.field+"\n"), &node)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
		})
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

func TestFanOutSpecDecode_RejectsRetiredTarget(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
target: worker-a
emit:
  event: routed.item
`), &spec)
	if err == nil || !strings.Contains(err.Error(), `fan_out field "target" is retired`) {
		t.Fatalf("yaml.Unmarshal error = %v, want retired fan_out target rejection", err)
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

func TestWorkflowDataWriteDecode_PreservesContainedOperationForms(t *testing.T) {
	var spec WorkflowDataAccumulation
	if err := yaml.Unmarshal([]byte(`
writes:
  - op: append
    target: entity.verticals.active_jobs
    key:
      ref: payload.vertical_id
    value:
      ref: payload.job
  - op: update
    target: entity.queue
    index: 0
    value: reviewed
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(spec.Writes); got != 2 {
		t.Fatalf("len(Writes) = %d, want 2", got)
	}
	appendWrite := spec.Writes[0]
	if !appendWrite.IsContainedOperation() {
		t.Fatal("append write did not decode as contained operation")
	}
	if got := appendWrite.Operation; got != WorkflowDataOperationAppend {
		t.Fatalf("Operation = %q, want append", got)
	}
	if got := appendWrite.Target(); got != "entity.verticals.active_jobs" {
		t.Fatalf("Target() = %q", got)
	}
	if got := appendWrite.Key.Ref; got != "payload.vertical_id" {
		t.Fatalf("Key.Ref = %q", got)
	}
	if got := appendWrite.Value.Ref; got != "payload.job" {
		t.Fatalf("Value.Ref = %q", got)
	}
	updateWrite := spec.Writes[1]
	if got := updateWrite.Index.Literal; got != 0 {
		t.Fatalf("Index.Literal = %#v, want 0", got)
	}
	if got := updateWrite.Value.Literal; got != "reviewed" {
		t.Fatalf("Value.Literal = %#v, want reviewed", got)
	}
}

func TestWorkflowDataWriteDecode_RejectsAmbiguousContainedOperationShape(t *testing.T) {
	var spec WorkflowDataAccumulation
	err := yaml.Unmarshal([]byte(`
writes:
  - op: append
    target_path: entity.verticals.active_jobs
    target: entity.verticals.active_jobs
    key: north
    value: job-1
`), &spec)
	if err == nil {
		t.Fatal("expected contained operation target_path ambiguity error")
	}
	if !strings.Contains(err.Error(), "must use target") {
		t.Fatalf("error = %v, want target-only rejection", err)
	}
}

func TestWorkflowDataWriteDecode_RejectsContainedSetOrMergeIndex(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{name: "set", op: "set"},
		{name: "merge", op: "merge"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var spec WorkflowDataAccumulation
			err := yaml.Unmarshal([]byte(fmt.Sprintf(`
writes:
  - op: %s
    target: entity.verticals
    key: north
    index: 0
    value:
      status: active
`, tc.op)), &spec)
			if err == nil {
				t.Fatalf("expected op %s index rejection", tc.op)
			}
			if !strings.Contains(err.Error(), "must not declare index") {
				t.Fatalf("error = %v, want index rejection", err)
			}
		})
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

func TestSystemNodeEventHandlerDecode_RejectsEventlessRuleEmitWithoutTemplate(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
rules:
  done:
    condition: "else"
    emit:
      fields:
        scan_id: payload.scan_id
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "rules[0].emit.event is required") {
		t.Fatalf("yaml.Unmarshal error = %v, want eventless rule emit rejection", err)
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

func TestGuardSpecDecode_OnFailEscalateObjectFields(t *testing.T) {
	var spec GuardSpec
	if err := yaml.Unmarshal([]byte(`
id: score_check
check: payload.score >= policy.threshold
on_fail:
  escalate:
    event: check.escalated
    fields:
      score: payload.score
      threshold: policy.threshold
      reason:
        literal: score_below_threshold
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := strings.TrimSpace(spec.OnFail); got != "escalate:check.escalated" {
		t.Fatalf("OnFail = %q, want scalar shorthand mirror", got)
	}
	failure, err := spec.FailureSpec()
	if err != nil {
		t.Fatalf("FailureSpec error: %v", err)
	}
	if failure.Action != GuardFailureActionEscalate {
		t.Fatalf("failure action = %q, want %q", failure.Action, GuardFailureActionEscalate)
	}
	emit := failure.EscalationEmitSpec()
	if got := emit.Event; got != "check.escalated" {
		t.Fatalf("escalation event = %q, want check.escalated", got)
	}
	if expr := emit.Fields["score"]; expr.Kind != ExpressionKindCEL || expr.CEL != "payload.score" {
		t.Fatalf("score field = %#v, want CEL payload.score", expr)
	}
	if expr := emit.Fields["threshold"]; expr.Kind != ExpressionKindCEL || expr.CEL != "policy.threshold" {
		t.Fatalf("threshold field = %#v, want CEL policy.threshold", expr)
	}
	if expr := emit.Fields["reason"]; expr.Kind != ExpressionKindLiteral || expr.Literal != "score_below_threshold" {
		t.Fatalf("reason field = %#v, want literal score_below_threshold", expr)
	}
}

func TestGuardSpecDecode_RejectsNestedScalarEscalateShortcut(t *testing.T) {
	var spec GuardSpec
	err := yaml.Unmarshal([]byte(`
id: score_check
check: payload.score >= policy.threshold
on_fail:
  escalate: check.escalated
`), &spec)
	if err == nil {
		t.Fatal("expected nested scalar guard escalation shortcut to be rejected")
	}
	if !strings.Contains(err.Error(), "guard.on_fail.escalate must be a mapping") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGuardSpecDecode_RejectsMalformedOnFailObjectForms(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "empty object",
			body: `
id: score_check
check: payload.score >= policy.threshold
on_fail: {}
`,
			wantErr: "guard.on_fail object form requires escalate",
		},
		{
			name: "missing escalate key",
			body: `
id: score_check
check: payload.score >= policy.threshold
on_fail:
  reject: true
`,
			wantErr: "UNDEFINED-FIELD",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var spec GuardSpec
			err := yaml.Unmarshal([]byte(tc.body), &spec)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
		})
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

func TestSystemNodeEventHandlerDecode_PreservesRuleLevelAction(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  needs_human:
    condition: "payload.amount >= 100"
    advances_to: awaiting_human
    action:
      id: mailbox_write
      mailbox:
        item_type:
          literal: approval
        summary:
          literal: Review refund
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := len(handler.Rules); got != 1 {
		t.Fatalf("Rules len = %d, want 1", got)
	}
	rule := handler.Rules[0]
	if got := rule.ID; got != "needs_human" {
		t.Fatalf("rule ID = %q, want needs_human", got)
	}
	if got := rule.Action.ID; got != "mailbox_write" {
		t.Fatalf("rule Action.ID = %q, want mailbox_write", got)
	}
	if rule.Action.Mailbox == nil {
		t.Fatal("expected rule Action.Mailbox")
	}
	if got := rule.Action.Mailbox.ItemType.Literal; got != "approval" {
		t.Fatalf("rule Action.Mailbox.ItemType = %#v, want approval", got)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsHandlerActionWithRules(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
action: mailbox_write
rules:
  needs_human:
    condition: "else"
    advances_to: awaiting_human
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-ACTION") {
		t.Fatalf("yaml.Unmarshal error = %v, want AMBIGUOUS-ACTION", err)
	}
}

func TestSystemNodeEventHandlerDecode_AllowsOnSuccessEmitWithRules(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
on_success:
  emit:
    event: handler.succeeded
    fields:
      audit:
        literal: ok
rules:
  needs_human:
    condition: "payload.amount >= 100"
    emit:
      event: rule.needs_human
      fields:
        amount: payload.amount
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := handler.OnSuccess.Emit.EventType(); got != "handler.succeeded" {
		t.Fatalf("OnSuccess.Emit.EventType = %q, want handler.succeeded", got)
	}
	if got := len(handler.Rules); got != 1 {
		t.Fatalf("Rules len = %d, want 1", got)
	}
	if got := HandlerEmitEvents(handler); !reflect.DeepEqual(got, []string{"rule.needs_human", "handler.succeeded"}) {
		t.Fatalf("HandlerEmitEvents = %#v", got)
	}
}

func TestSystemNodeEventHandlerDecode_AllowsRulesEmitTemplateSpecialization(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
emit:
  event: account.bucketed
  fields:
    account_id: entity.id
    score: payload.score
rules:
  high:
    condition: payload.score >= 80
    emit:
      fields:
        bucket: '"high"'
  medium:
    condition: payload.score >= 40
    emit:
      fields:
        bucket: '"medium"'
  low:
    condition: else
    emit:
      fields:
        bucket: '"low"'
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := HandlerEmitEvents(handler); !reflect.DeepEqual(got, []string{"account.bucketed"}) {
		t.Fatalf("HandlerEmitEvents = %#v, want account.bucketed once", got)
	}
	sites := HandlerRuleEmitTemplateSites(handler)
	if got := len(sites); got != 3 {
		t.Fatalf("template sites len = %d, want 3", got)
	}
	if got := sites[0].Source; got != "handler.rules.emit_template" {
		t.Fatalf("site source = %q, want handler.rules.emit_template", got)
	}
	if got := sites[0].Spec.EventType(); got != "account.bucketed" {
		t.Fatalf("merged event = %q, want account.bucketed", got)
	}
	for _, field := range []string{"account_id", "score", "bucket"} {
		if _, ok := sites[0].Spec.Fields[field]; !ok {
			t.Fatalf("merged fields missing %s: %#v", field, sites[0].Spec.Fields)
		}
	}
	if expr := sites[0].Spec.Fields["bucket"]; expr.Kind != ExpressionKindCEL || expr.CEL != `"high"` {
		t.Fatalf("bucket expression = %#v, want CEL \"high\"", expr)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsInvalidRulesEmitTemplateSpecialization(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		contains string
	}{
		{
			name: "missing_else",
			raw: `
emit:
  event: account.bucketed
rules:
  high:
    condition: payload.score >= 80
    emit:
      fields:
        bucket: '"high"'
`,
			contains: "requires an else rule",
		},
		{
			name: "field_conflict",
			raw: `
emit:
  event: account.bucketed
  fields:
    bucket: '"base"'
rules:
  low:
    condition: else
    emit:
      fields:
        bucket: '"low"'
`,
			contains: "conflicts with handler emit template field",
		},
		{
			name: "rule_own_event",
			raw: `
emit:
  event: account.bucketed
rules:
  high:
    condition: payload.score >= 80
    emit:
      fields:
        bucket: '"high"'
  low:
    condition: else
    emit:
      event: account.dropped
      fields:
        bucket: '"low"'
`,
			contains: "rules[1].emit.event cannot be combined",
		},
		{
			name: "rule_target_override",
			raw: `
emit:
  event: account.bucketed
rules:
  low:
    condition: else
    emit:
      target: sender
      fields:
        bucket: '"low"'
`,
			contains: "may only contribute fields",
		},
		{
			name: "on_success_split",
			raw: `
emit:
  event: account.bucketed
on_success:
  emit: account.audit
rules:
  low:
    condition: else
    emit:
      fields:
        bucket: '"low"'
`,
			contains: "cannot be combined with on_success.emit",
		},
		{
			name: "rule_literal_field_value",
			raw: `
emit:
  event: account.bucketed
  fields:
    account_id: entity.id
rules:
  low:
    condition: else
    emit:
      fields:
        bucket:
          literal: low
`,
			contains: "rules[0].emit.fields.bucket to be a CEL expression string",
		},
		{
			name: "handler_template_literal_field_value",
			raw: `
emit:
  event: account.bucketed
  fields:
    account_id:
      literal: acct-1
rules:
  low:
    condition: else
    emit:
      fields:
        bucket: '"low"'
`,
			contains: "handler.emit.fields.account_id to be a CEL expression string",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tc.raw), &handler)
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want containing %q", err, tc.contains)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_RejectsUnsupportedOnSuccessEmitShapes(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		contains string
	}{
		{
			name: "without_rules",
			raw: `
on_success:
  emit: handler.succeeded
`,
			contains: "only supported on handlers with rules",
		},
		{
			name: "with_bare_emit",
			raw: `
emit: handler.default
on_success:
  emit: handler.succeeded
rules:
  done:
    condition: "else"
    emit: rule.done
`,
			contains: "handler-top-level emit is only allowed on single-emit handlers",
		},
		{
			name: "with_on_complete",
			raw: `
on_success:
  emit: handler.succeeded
rules:
  done:
    condition: "else"
    emit: rule.done
on_complete:
  - id: complete
    emit: flow.complete
`,
			contains: "not supported with on_complete",
		},
		{
			name: "with_fan_out",
			raw: `
on_success:
  emit: handler.succeeded
rules:
  done:
    condition: "else"
    emit: rule.done
fan_out:
  items_from: payload.items
  emit: item.done
`,
			contains: "not supported with fan_out",
		},
		{
			name: "with_rule_fan_out",
			raw: `
on_success:
  emit: handler.succeeded
rules:
  done:
    condition: "else"
    fan_out:
      items_from: payload.items
      emit: item.done
`,
			contains: "not supported with rules[0].fan_out",
		},
		{
			name: "unknown_on_success_field",
			raw: `
on_success:
  action: notify
rules:
  done:
    condition: "else"
    emit: rule.done
`,
			contains: "on_success field",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tc.raw), &handler)
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want containing %q", err, tc.contains)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_RejectsActionOutsideRulesContext(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "on_complete",
			raw: `
on_complete:
  - id: done
    condition: "else"
    action:
      id: mailbox_write
`,
		},
		{
			name: "accumulate_on_complete",
			raw: `
accumulate:
  into: approvals
  expected_from: entity.expected_approvals
  on_complete:
    - id: done
      condition: "else"
      action:
        id: mailbox_write
`,
		},
		{
			name: "accumulate_on_timeout",
			raw: `
accumulate:
  into: approvals
  expected_from: entity.expected_approvals
  on_timeout:
    advances_to: timed_out
    action:
      id: mailbox_write
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tc.raw), &handler)
			if err == nil || !strings.Contains(err.Error(), "UNSUPPORTED-ACTION") {
				t.Fatalf("yaml.Unmarshal error = %v, want UNSUPPORTED-ACTION", err)
			}
		})
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
      ref: _entity.id
    flow_instance:
      ref: _entity.flow_instance
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
	if got := handler.Action.Mailbox.EntityID.Ref; got != "_entity.id" {
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
    success_event: artifact_repo.commit_completed
    success_payload:
      producer:
        literal: artifact-writer
    failure_event: artifact_repo.commit_failed
    failure_payload:
      producer:
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
	if got := handler.Action.ArtifactRepo.SuccessEvent; got != "artifact_repo.commit_completed" {
		t.Fatalf("ArtifactRepo.SuccessEvent = %q", got)
	}
	if got := handler.Action.ArtifactRepo.SuccessPayload["producer"].Literal; got != "artifact-writer" {
		t.Fatalf("ArtifactRepo.SuccessPayload[producer] = %#v", handler.Action.ArtifactRepo.SuccessPayload["producer"])
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

func TestEntityFieldDeclDecode_PreservesIndexed(t *testing.T) {
	var field EntityFieldDecl
	if err := yaml.Unmarshal([]byte(`
type: text
indexed: true
`), &field); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !field.Indexed {
		t.Fatal("Indexed = false, want true")
	}
}

func TestEntityFieldDeclDecode_PreservesUnusedReaderReason(t *testing.T) {
	var field EntityFieldDecl
	if err := yaml.Unmarshal([]byte(`
type: text
_unused_reader_reason: External operator readout
`), &field); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := field.UnusedReaderReason; got != "External operator readout" {
		t.Fatalf("UnusedReaderReason = %q", got)
	}
}

func TestEntityFieldDeclDecode_RejectsShortUnusedReaderReason(t *testing.T) {
	var field EntityFieldDecl
	err := yaml.Unmarshal([]byte(`
type: text
_unused_reader_reason: short
`), &field)
	if err == nil || !strings.Contains(err.Error(), "_unused_reader_reason must be at least 10 characters") {
		t.Fatalf("yaml.Unmarshal error = %v, want _unused_reader_reason length error", err)
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
