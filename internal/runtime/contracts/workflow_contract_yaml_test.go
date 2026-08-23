package contracts

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestProjectPackageDocumentDecode_PreservesRequiresAndImportBinds(t *testing.T) {

	var doc ProjectPackageDocument
	snippet := canonicalrouting.PackageRequiresBindConnectSnippet(t)
	if err := snippet.Decode(&doc); err != nil {
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
	if got := doc.Requires.PlatformVersion; got != ">=0.7.0 <0.8.0" {
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
	if got, want := doc.Connect[0].Event, "parent.work_completed"; got != want {
		t.Fatalf("Connect[0].Event = %q, want %q", got, want)
	}
	if got, want := doc.Connect[0].From, "worker"; got != want {
		t.Fatalf("Connect[0].From = %q, want %q", got, want)
	}
	if got, want := doc.Connect[0].To, "worker"; got != want {
		t.Fatalf("Connect[0].To = %q, want %q", got, want)
	}
	if got, want := doc.Connect[0].Rename, "parent.work_requested"; got != want {
		t.Fatalf("Connect[0].Rename = %q, want %q", got, want)
	}
}

func TestProjectPackageDocumentDecodeRejectsRetiredChildPackageSpellings(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "children only", yaml: "children: []\n", want: "children is no longer supported"},
		{name: "subpackages only", yaml: "subpackages: []\n", want: "subpackages is no longer supported"},
		{name: "equal dual collection", yaml: "packages: [{id: child, path: child}]\nchildren: [{id: child, path: child}]\n", want: "children is no longer supported"},
		{name: "conflicting dual collection", yaml: "packages: [{id: child, path: child}]\nsubpackages: [{id: other, path: other}]\n", want: "subpackages is no longer supported"},
		{name: "package location", yaml: "packages: [{id: child, package: child}]\n", want: "child reference package is no longer supported"},
		{name: "dir location", yaml: "packages: [{id: child, dir: child}]\n", want: "child reference dir is no longer supported"},
		{name: "equal dual location", yaml: "packages: [{id: child, path: child, package: child}]\n", want: "child reference package is no longer supported"},
		{name: "conflicting dual location", yaml: "packages: [{id: child, path: child, dir: other}]\n", want: "child reference dir is no longer supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc ProjectPackageDocument
			err := yaml.Unmarshal([]byte("name: test\n"+tc.yaml), &doc)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestFlowPackageConnectDecodePinsCanonicalEventCentricShape(t *testing.T) {
	var connect FlowPackageConnect
	if err := yaml.Unmarshal([]byte("event: work.ready\nfrom: producer\nto: consumer\nrename: work.accepted\nadapter: ready_to_accepted\n"), &connect); err != nil {
		t.Fatalf("decode canonical connect row: %v", err)
	}
	if connect.Event != "work.ready" || connect.From != "producer" || connect.To != "consumer" || connect.Rename != "work.accepted" || connect.Adapter != "ready_to_accepted" {
		t.Fatalf("canonical connect = %#v", connect)
	}
	if err := yaml.Unmarshal([]byte("event: producer/work.ready\nfrom: producer\nto: consumer\nrename: consumer/work.accepted\n"), &connect); err != nil {
		t.Fatalf("decode canonical slash-qualified connect row: %v", err)
	}
	if connect.Event != "producer/work.ready" || connect.Rename != "consumer/work.accepted" {
		t.Fatalf("slash-qualified connect = %#v", connect)
	}

	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "old row without event", yaml: "from: producer.work_ready\nto: consumer.work_ready\n", want: "endpoint-centric connect rows"},
		{name: "missing source", yaml: "event: work.ready\nto: consumer\n", want: "requires non-empty event, from, and to"},
		{name: "missing receiver", yaml: "event: work.ready\nfrom: producer\n", want: "requires non-empty event, from, and to"},
		{name: "redundant rename", yaml: "event: work.ready\nfrom: producer\nto: consumer\nrename: work.ready\n", want: "redundant with event"},
		{name: "leading slash event", yaml: "event: /work.ready\nfrom: producer\nto: consumer\n", want: "exact canonical event identity"},
		{name: "trailing slash event", yaml: "event: work.ready/\nfrom: producer\nto: consumer\n", want: "exact canonical event identity"},
		{name: "normalized equal rename", yaml: "event: work.ready\nfrom: producer\nto: consumer\nrename: /work.ready/\n", want: "redundant with event"},
		{name: "non-canonical rename", yaml: "event: work.ready\nfrom: producer\nto: consumer\nrename: work.accepted/\n", want: "exact canonical event identity"},
		{name: "duplicate event equal", yaml: "event: work.ready\nevent: work.ready\nfrom: producer\nto: consumer\n", want: "repeats key"},
		{name: "duplicate endpoint conflicting", yaml: "event: work.ready\nfrom: producer\nfrom: other\nto: consumer\n", want: "repeats key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var invalid FlowPackageConnect
			err := yaml.Unmarshal([]byte(tc.yaml), &invalid)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestToolSchemaEntryDecodeRejectsDuplicateManagedCredentialTokenHeadersBeforeCanonicalizing(t *testing.T) {
	var tool ToolSchemaEntry
	err := yaml.Unmarshal([]byte(`
category: provider_connector
handler_type: http
managed_credential:
  key: notion_oauth
  token_request:
    static_headers:
      X-Provider-Version: "2026-03-11"
      x-provider-version: "2026-04-01"
http:
  method: GET
  url: https://example.invalid
`), &tool)
	if err == nil {
		t.Fatal("yaml.Unmarshal error = nil, want duplicate token header rejection")
	}
	if !strings.Contains(err.Error(), "duplicate header") {
		t.Fatalf("yaml.Unmarshal error = %v, want duplicate header rejection", err)
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

func TestFlowSchemaDocumentDecodeStagesKeyedMap(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: validation
stages:
  queued:
    initial: true
    description: Waiting for work
  approved:
    terminal: true
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !doc.UsesAuthoredStages() {
		t.Fatalf("UsesAuthoredStages = false, want true")
	}
	if got, want := doc.LoweredInitialState(), "queued"; got != want {
		t.Fatalf("LoweredInitialState = %q, want %q", got, want)
	}
	if got, want := doc.LoweredStates(), []string{"queued", "approved"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LoweredStates = %#v, want %#v", got, want)
	}
	if got, want := doc.LoweredTerminalStates(), []string{"approved"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LoweredTerminalStates = %#v, want %#v", got, want)
	}
	stages := doc.LoweredWorkflowStages("validation")
	if len(stages) != 2 || stages[0].ID != "queued" || stages[0].Phase != "validation" || stages[0].Description != "Waiting for work" {
		t.Fatalf("LoweredWorkflowStages = %#v", stages)
	}
}

func TestFlowSchemaDocumentDecodeStageTimersUseCanonicalSyntax(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: validation
stages:
  awaiting_review:
    timers:
      - after: 48h
        emit: review.sla_escalated
      - after: "{{marginal_park_days}}d"
        advances_to: expired
  expired:
    terminal: true
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if len(doc.StageDeclarations.Entries) != 2 {
		t.Fatalf("stages = %#v, want two stages", doc.StageDeclarations.Entries)
	}
	timers := doc.StageDeclarations.Entries[0].Timers
	if len(timers) != 2 {
		t.Fatalf("timers = %#v, want two timer rows", timers)
	}
	if got, want := timers[0].ID, "awaiting_review.review.sla_escalated"; got != want {
		t.Fatalf("emit timer default ID = %q, want %q", got, want)
	}
	if got, want := timers[1].ID, "awaiting_review.expired"; got != want {
		t.Fatalf("advance timer default ID = %q, want %q", got, want)
	}
	if got, want := timers[1].After, "{{marginal_park_days}}d"; got != want {
		t.Fatalf("advance timer after = %q, want %q", got, want)
	}
}

func TestFlowSchemaDocumentDecodeTypedStageGate(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: launch
stages:
  awaiting_launch_approval:
    initial: true
    gate:
      decision: launch_review
      context:
        staging: entity.staging_url
        qa_summary: entity.qa_summary
      outcomes:
        approve:
          advances_to: operating
          emit: opco.launched
        reject:
          input:
            feedback: {type: text, required: true}
          advances_to: building
          emit:
            event: launch.rejected
            fields:
              feedback: decision.feedback
  building: {}
  operating: {terminal: true}
`), &doc)
	if err != nil {
		t.Fatalf("decode gate: %v", err)
	}
	plans := doc.StageDeclarations.GatePlans("launch")
	if len(plans) != 1 {
		t.Fatalf("gate plans = %#v", plans)
	}
	plan := plans[0]
	if plan.FlowID != "launch" || plan.Stage != "awaiting_launch_approval" || plan.Decision != "launch_review" {
		t.Fatalf("gate identity = %#v", plan)
	}
	if got := plan.Context["staging"]; got.Kind != ExpressionKindCEL || got.CEL != "entity.staging_url" {
		t.Fatalf("context staging = %#v", got)
	}
	if got := plan.Outcomes["reject"].Input["feedback"]; got.Type != "text" || !got.Required {
		t.Fatalf("feedback schema = %#v", got)
	}
	if got := plan.Outcomes["reject"].Emit.Fields["feedback"]; got.CEL != "decision.feedback" {
		t.Fatalf("reject emit feedback = %#v", got)
	}
}

func TestFlowSchemaDocumentRejectsNormalizedGateKeyCollisions(t *testing.T) {
	tests := []struct {
		name string
		gate string
	}{
		{name: "gate field", gate: `decision: launch_review
" decision ": other
outcomes: {approve: {advances_to: operating}}`},
		{name: "verdict", gate: `decision: launch_review
outcomes:
  approve: {advances_to: operating}
  " approve ": {advances_to: operating}`},
		{name: "input", gate: `decision: launch_review
outcomes:
  reject:
    advances_to: operating
    input:
      feedback: {type: text}
      " feedback ": {type: text}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gate FlowStageGateDeclaration
			err := yaml.Unmarshal([]byte(tc.gate), &gate)
			if err == nil || !strings.Contains(err.Error(), "duplicate normalized key") {
				t.Fatalf("decode error = %v, want normalized collision", err)
			}
		})
	}
}

func TestFlowSchemaDocumentRejectsGateOutcomeWithoutAdvance(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: launch
stages:
  awaiting:
    initial: true
    gate:
      decision: launch_review
      outcomes:
        approve: {emit: opco.launched}
  operating: {terminal: true}
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "requires advances_to") {
		t.Fatalf("decode error = %v, want direct outcome closure", err)
	}
}

func TestFlowSchemaDocumentRejectsUnknownGateField(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: launch
stages:
  awaiting:
    gate:
      decision: launch_review
      authority: operator
      outcomes:
        approve: {advances_to: operating}
  operating: {terminal: true}
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "authority") {
		t.Fatalf("decode error = %v, want unknown-field rejection", err)
	}
}

func TestFlowSchemaDocumentDecodeBoundedLoopCanonicalSyntax(t *testing.T) {
	var schema FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
stages:
  drafting: {initial: true}
  review: {}
  exhausted: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: "{{policy.inner_revision_max}}"
    escape:
      advances_to: exhausted
`), &schema); err != nil {
		t.Fatalf("decode loop schema: %v", err)
	}
	if len(schema.LoopDeclarations.Entries) != 1 {
		t.Fatalf("loops = %#v", schema.LoopDeclarations.Entries)
	}
	loop := schema.LoopDeclarations.Entries[0]
	if loop.ID != "revision" || loop.RevisionField != "revision_id" || loop.MaxAttempts.PolicyRef != "inner_revision_max" || loop.Escape.AdvancesTo != "exhausted" {
		t.Fatalf("loop = %#v", loop)
	}
}

func TestToolSchemaEntryDecodeRejectsRetiredHandlerTypesAtLexicalAdmission(t *testing.T) {
	for _, handler := range []string{"workflow_registered", "api_call"} {
		t.Run(handler, func(t *testing.T) {
			var tool ToolSchemaEntry
			err := yaml.Unmarshal([]byte(`
handler_type: `+handler+`
input_schema: {type: object}
output_schema: {type: object}
`), &tool)
			if err == nil || !strings.Contains(err.Error(), "unsupported handler_type") {
				t.Fatalf("decode retired handler_type %q error = %v", handler, err)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecodeLoopOperationRequiresExactOperationAndFrom(t *testing.T) {
	for _, raw := range []string{
		"loop: {repeat: revision}",
		"loop: {repeat: revision, close: revision, from: review}",
	} {
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(raw), &handler); err == nil {
			t.Fatalf("decode %q succeeded, want closed loop operation error", raw)
		}
	}
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("loop: {repeat: revision, from: review}\nadvances_to: drafting\n"), &handler); err != nil {
		t.Fatalf("decode canonical operation: %v", err)
	}
	if handler.Loop == nil || handler.Loop.Repeat != "revision" || handler.Loop.From != "review" {
		t.Fatalf("handler loop = %#v", handler.Loop)
	}
}

func TestSystemNodeEventHandlerRejectsRetiredTopLevelLoopShadowFields(t *testing.T) {
	for _, field := range []string{"completion_rule", "policy_ref"} {
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(field+": legacy\n"), &handler); err == nil {
			t.Fatalf("decode retired handler field %s succeeded", field)
		}
	}
}

func TestFlowSchemaDocumentDecodeStageTimersRejectSupersededFields(t *testing.T) {
	for _, field := range []string{"delay", "interrupting", "repeat"} {
		t.Run(field, func(t *testing.T) {
			var doc FlowSchemaDocument
			err := yaml.Unmarshal([]byte(fmt.Sprintf(`
name: validation
stages:
  awaiting_review:
    timers:
      - after: 48h
        emit: review.sla_escalated
        %s: true
`, field)), &doc)
			if err == nil || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("yaml.Unmarshal error = %v, want unsupported field rejection", err)
			}
		})
	}
}

func TestFlowSchemaDocumentDecodeStageTimersRequireExplicitIDOnDerivedCollision(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: validation
stages:
  awaiting_review:
    timers:
      - after: 48h
        emit: review.sla_escalated
      - after: 72h
        emit: review.sla_escalated
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Fatalf("yaml.Unmarshal error = %v, want duplicate derived id rejection", err)
	}
}

func TestFlowSchemaDocumentDecodeStageTimersRejectExplicitIDCollisionAcrossStages(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: validation
stages:
  awaiting_review:
    timers:
      - id: sla
        after: 48h
        emit: review.sla_escalated
  parked:
    timers:
      - id: sla
        after: 72h
        advances_to: expired
  expired:
    terminal: true
`), &doc)
	if err == nil || !strings.Contains(err.Error(), `stage timer id "sla" is declared in both stage "awaiting_review" and stage "parked"`) {
		t.Fatalf("yaml.Unmarshal error = %v, want cross-stage timer id rejection", err)
	}
}

func TestFlowSchemaDocumentDecodeStagesExplicitEmpty(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: discovery
stages: []
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if !doc.UsesAuthoredStages() {
		t.Fatalf("UsesAuthoredStages = false, want true")
	}
	if len(doc.LoweredStates()) != 0 || doc.LoweredInitialState() != "" || len(doc.LoweredTerminalStates()) != 0 {
		t.Fatalf("lowered explicit stateless lifecycle = initial %q states %#v terminals %#v, want empty", doc.LoweredInitialState(), doc.LoweredStates(), doc.LoweredTerminalStates())
	}
}

func TestSystemNodeHandlerDecodeJoinCanonicalShape(t *testing.T) {
	var nodes map[string]SystemNodeContract
	err := yaml.Unmarshal([]byte(`
coordinator:
  id: coordinator
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting_items
        members:
          from: entity.expected_item_ids
          by: payload.item_id
        window:
          from: entity.dispatch_id
          by: payload.dispatch_id
        output: payload.result
        complete_when: join.completed >= 2
        remaining: ignore
        on_complete:
          emit:
            event: items.completed
            fields:
              results: join.results
          advances_to: ready
        timeout:
          after: 24h
          emit:
            event: items.timed_out
            fields:
              missing: join.missing
          advances_to: attention
`), &nodes)
	if err != nil {
		t.Fatalf("yaml.Unmarshal join: %v", err)
	}
	join := nodes["coordinator"].EventHandlers["item.completed"].Join
	if join == nil {
		t.Fatal("join = nil")
	}
	if got, want := join.EffectiveID(), "awaiting_items"; got != want {
		t.Fatalf("join id = %q, want %q", got, want)
	}
	if join.Members.FromPath.Root != paths.RootEntity || join.Members.ByPath.Root != paths.RootPayload {
		t.Fatalf("join member paths = %#v/%#v", join.Members.FromPath, join.Members.ByPath)
	}
	if join.Window == nil || join.Window.FromPath.Root != paths.RootEntity || join.Window.ByPath.Root != paths.RootPayload {
		t.Fatalf("join window = %#v", join.Window)
	}
	if !join.OnCompleteFound || join.OnComplete.Emit.EventType() != "items.completed" || join.OnComplete.AdvancesTo != "ready" {
		t.Fatalf("join on_complete = %#v", join.OnComplete)
	}
	if !join.TimeoutFound || join.Timeout.After != "24h" || join.Timeout.Outcome.Emit.EventType() != "items.timed_out" {
		t.Fatalf("join timeout = %#v", join.Timeout)
	}
}

func TestSystemNodeHandlerDecodeJoinRejectsUnsupportedFields(t *testing.T) {
	for _, input := range []string{
		`join: {stage: waiting, members: {from: entity.ids, by: payload.id}, output: payload.result, interrupting: true}`,
		`join: {stage: waiting, members: {from: entity.ids, by: payload.id, dedup_by: payload.id}, output: payload.result}`,
		`join: {stage: waiting, members: {from: entity.ids, by: payload.id}, output: payload.result, on_complete: {action: {id: noop}}}`,
		`join: {stage: waiting, members: {from: entity.ids, by: payload.id}, output: payload.result, timeout: {after: 1h, repeat: 2}}`,
	} {
		var handler SystemNodeEventHandler
		if err := yaml.Unmarshal([]byte(input), &handler); err == nil {
			t.Fatalf("yaml.Unmarshal(%q) error = nil", input)
		}
	}
}

func TestFlowSchemaDocumentDecodeRejectsNonEmptyStageSequence(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: validation
stages:
  - id: queued
    initial: true
`), &doc)
	if err == nil || !strings.Contains(err.Error(), "stages must be a keyed mapping") {
		t.Fatalf("yaml.Unmarshal error = %v, want keyed mapping rejection", err)
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

func TestProjectPackageDocumentDecode_PreservesStrictSelfFacts(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: package-self-facts
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
author: platform-team
description: Strict manifest metadata fixture.
keywords:
  - dedup-index
  - catalog
license: Apache-2.0
repository: https://github.com/division-sh/swarm
extra:
  colony.division.sh/display_name: Dedup Index
  colony.division.sh/owner_team: Runtime
flows: []
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := strings.Join(doc.Keywords, ","), "dedup-index,catalog"; got != want {
		t.Fatalf("Keywords = %q, want %q", got, want)
	}
	if got, want := doc.License, "Apache-2.0"; got != want {
		t.Fatalf("License = %q, want %q", got, want)
	}
	if got, want := doc.Repository, "https://github.com/division-sh/swarm"; got != want {
		t.Fatalf("Repository = %q, want %q", got, want)
	}
	wantExtra := map[string]string{
		"colony.division.sh/display_name": "Dedup Index",
		"colony.division.sh/owner_team":   "Runtime",
	}
	if !reflect.DeepEqual(doc.Extra, wantExtra) {
		t.Fatalf("Extra = %#v, want %#v", doc.Extra, wantExtra)
	}
}

func TestProjectPackageDocumentDecode_RejectsStrictSelfFactDrift(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "unknown category",
			body: `
name: invalid
category: examples
`,
			wantErr: "not supported",
		},
		{
			name: "unknown homepage",
			body: `
name: invalid
homepage: https://division.sh
`,
			wantErr: "not supported",
		},
		{
			name: "unknown capabilities",
			body: `
name: invalid
capabilities: []
`,
			wantErr: "not supported",
		},
		{
			name: "loose license alias",
			body: `
name: invalid
license: Apache
`,
			wantErr: "SPDX",
		},
		{
			name: "license expression",
			body: `
name: invalid
license: MIT OR Apache-2.0
`,
			wantErr: "SPDX",
		},
		{
			name: "ssh repository",
			body: `
name: invalid
repository: git@github.com:division-sh/swarm.git
`,
			wantErr: "GitHub HTTPS",
		},
		{
			name: "repository branch URL",
			body: `
name: invalid
repository: https://github.com/division-sh/swarm/tree/master
`,
			wantErr: "https://github.com/{owner}/{repo}",
		},
		{
			name: "repository git suffix",
			body: `
name: invalid
repository: https://github.com/division-sh/swarm.git
`,
			wantErr: "https://github.com/{owner}/{repo}",
		},
		{
			name: "uppercase keyword",
			body: `
name: invalid
keywords: [Runtime]
`,
			wantErr: "lowercase slug",
		},
		{
			name: "duplicate keyword",
			body: `
name: invalid
keywords: [runtime, runtime]
`,
			wantErr: "duplicates",
		},
		{
			name: "extra missing namespace",
			body: `
name: invalid
extra:
  display_name: Runtime
`,
			wantErr: "namespaced",
		},
		{
			name: "extra non-string value",
			body: `
name: invalid
extra:
  colony.division.sh/enabled: true
`,
			wantErr: "must be a string",
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

func TestProjectPackageDocumentDecode_RejectsMalformedRequiresAndBindShape(t *testing.T) {

	tests := []struct {
		name    string
		body    canonicalrouting.ParserSnippet
		wantErr string
	}{
		{
			name: "unknown policy requirement option",
			body: canonicalrouting.NewParserSnippet(t, `
name: invalid
requires:
  policy:
    provider.threshold:
      fallback: 0.8
`),
			wantErr: `requires.policy field "fallback" is not supported.`,
		},
		{
			name: "policy requirement must be mapping",
			body: canonicalrouting.NewParserSnippet(t, `
name: invalid
requires:
  policy:
    provider.threshold: 0.8
`),
			wantErr: "policy requirement must be a mapping",
		},
		{
			name: "unknown requires field",
			body: canonicalrouting.NewParserSnippet(t, `
name: invalid
requires:
  inputz: [work.requested]
`),
			wantErr: `requires field "inputz" is not supported.`,
		},
		{
			name: "bind inputs must be mapping",
			body: canonicalrouting.NewParserSnippet(t, `
name: invalid
flows:
  - id: worker
    flow: worker
    bind:
      inputs: [work.requested]
`),
			wantErr: "bind.inputs",
		},
		{
			name: "unknown bind field",
			body: canonicalrouting.NewParserSnippet(t, `
name: invalid
packages:
  - path: packages/child
    bind:
      credential: {}
`),
			wantErr: `bind field "credential" is not supported.`,
		},
		{
			name:    "unknown connect field",
			body:    canonicalrouting.InvalidPackageConnectFieldSnippet(t),
			wantErr: `connect field "topic" is not supported.`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc ProjectPackageDocument
			err := tc.body.Decode(&doc)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestFlowSchemaDocumentDecodeRejectsRetiredInputAddressOnPresence(t *testing.T) {
	for _, specimen := range []canonicalrouting.RetiredReceiverRoutingSnippet{
		canonicalrouting.RetiredInputAddressEmpty,
		canonicalrouting.RetiredInputAddressMalformed,
		canonicalrouting.RetiredInputAddressPopulated,
		canonicalrouting.RetiredInputAddressMixed,
	} {
		t.Run(string(specimen), func(t *testing.T) {
			var doc FlowSchemaDocument
			err := canonicalrouting.RetiredReceiverRoutingParserSnippet(t, specimen).Decode(&doc)
			if err == nil || !strings.Contains(err.Error(), "retired input pin address") {
				t.Fatalf("yaml.Unmarshal error = %v, want retired input address rejection", err)
			}
		})
	}
}

func TestFlowPackageConnectDecodeRejectsRetiredMapOnPresence(t *testing.T) {
	for _, specimen := range []canonicalrouting.RetiredReceiverRoutingSnippet{
		canonicalrouting.RetiredConnectMapEmpty,
		canonicalrouting.RetiredConnectMapMalformed,
		canonicalrouting.RetiredConnectMapPopulated,
		canonicalrouting.RetiredConnectMapMixed,
	} {
		t.Run(string(specimen), func(t *testing.T) {
			var doc ProjectPackageDocument
			err := canonicalrouting.RetiredReceiverRoutingParserSnippet(t, specimen).Decode(&doc)
			if err == nil || !strings.Contains(err.Error(), "retired connect.map") {
				t.Fatalf("yaml.Unmarshal error = %v, want retired connect.map rejection", err)
			}
		})
	}
}

func TestFlowPackageConnectDecodeRejectsRetiredUsingInstanceOnPresence(t *testing.T) {
	for _, specimen := range []canonicalrouting.RetiredReceiverRoutingSnippet{
		canonicalrouting.RetiredConnectUsingEmpty,
		canonicalrouting.RetiredConnectUsingMalformed,
		canonicalrouting.RetiredConnectUsingPopulated,
		canonicalrouting.RetiredConnectUsingComposite,
		canonicalrouting.RetiredConnectUsingMixed,
	} {
		t.Run(string(specimen), func(t *testing.T) {
			var doc ProjectPackageDocument
			err := canonicalrouting.RetiredReceiverRoutingParserSnippet(t, specimen).Decode(&doc)
			if err == nil || !strings.Contains(err.Error(), "retired connect.using.instance") {
				t.Fatalf("yaml.Unmarshal error = %v, want retired connect.using.instance rejection", err)
			}
		})
	}
}

func TestFlowSchemaDocumentDecode_NormalizesClosedInputPinSourceEnum(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source string
		want   string
	}{
		{name: "empty", source: "", want: ""},
		{name: "external", source: "  EXTERNAL  ", want: "external"},
		{name: "harness", source: "  HARNESS  ", want: "harness"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var doc FlowSchemaDocument
			raw := "name: source-enum\npins:\n  inputs:\n    events:\n      - name: work_requested\n        event: work.requested\n"
			if tc.source != "" {
				raw += "        source: '" + tc.source + "'\n"
			}
			if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if got := doc.Pins.Inputs.EventPins[0].Source; got != tc.want {
				t.Fatalf("Source = %q, want %q", got, tc.want)
			}
		})
	}

	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte("name: source-enum\npins:\n  inputs:\n    events:\n      - name: work_requested\n        source: fallback\n"), &doc)
	if err == nil || !strings.Contains(err.Error(), "input event pin source must be external or harness") {
		t.Fatalf("yaml.Unmarshal error = %v, want closed source-enum rejection", err)
	}
}

func TestFlowSchemaDocumentDecodeRejectsRetiredAddressBeforeNestedFields(t *testing.T) {
	var doc FlowSchemaDocument
	err := canonicalrouting.RetiredReceiverRoutingParserSnippet(t, canonicalrouting.RetiredInputAddressUnsupportedNested).Decode(&doc)
	if err == nil || !strings.Contains(err.Error(), "retired input pin address") {
		t.Fatalf("yaml.Unmarshal error = %v, want retired address rejection", err)
	}
}

func TestFlowSchemaDocumentDecode_PreservesInputPinResolutionModes(t *testing.T) {

	var doc FlowSchemaDocument
	snippet := canonicalrouting.InputPinResolutionModesSnippet(t)
	if err := snippet.Decode(&doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	pins := doc.Pins.Inputs.EventPins
	if len(pins) != 6 {
		t.Fatalf("input EventPins len = %d, want 6", len(pins))
	}
	create := pins[0]
	if got, want := create.Resolution.Mode, FlowInputResolutionModeCreate; got != want {
		t.Fatalf("create Resolution.Mode = %q, want %q", got, want)
	}
	carry := create.Carries["validation_case_id"]
	if got, want := carry.From, FlowInputCarrySourceGeneratedUUID; got != want {
		t.Fatalf("create carry From = %q, want %q", got, want)
	}
	if got, want := carry.Type, "uuid"; got != want {
		t.Fatalf("create carry Type = %q, want %q", got, want)
	}
	if got, want := pins[1].Carries["account_id"].From, "payload.account_id"; got != want {
		t.Fatalf("select identity carry source = %q, want %q", got, want)
	}
	if got, want := pins[2].Carries["account_id"].From, "payload.account_id"; got != want {
		t.Fatalf("select-or-create identity carry source = %q, want %q", got, want)
	}
	if got, want := pins[3].Resolution.Aggregation, "stream"; got != want {
		t.Fatalf("fan-in aggregation = %q, want %q", got, want)
	}
	if got, want := strings.Join(pins[3].Resolution.DedupBy, ","), "event.id,payload.operating_id"; got != want {
		t.Fatalf("fan-in dedup_by = %q, want %q", got, want)
	}
	if got, want := pins[4].Resolution.Mode, FlowInputResolutionModeFanOut; got != want {
		t.Fatalf("fan-out mode = %q, want %q", got, want)
	}
	if got, want := pins[5].Resolution.RepliesTo, "provider_requested"; got != want {
		t.Fatalf("reply replies_to = %q, want %q", got, want)
	}
}

func TestFlowSchemaDocumentDecode_RejectsUnsupportedInputPinResolutionFields(t *testing.T) {

	tests := []struct {
		name string
		body canonicalrouting.ParserSnippet
		want string
	}{
		{
			name: "resolution",
			body: canonicalrouting.UnsupportedInputPinResolutionSnippet(t, canonicalrouting.UnsupportedResolutionField),
			want: "is not supported",
		},
		{
			name: "instance_key",
			body: canonicalrouting.UnsupportedInputPinResolutionSnippet(t, canonicalrouting.UnsupportedInstanceKeyField),
			want: "resolution.instance_key is retired",
		},
		{
			name: "carries",
			body: canonicalrouting.UnsupportedInputPinResolutionSnippet(t, canonicalrouting.UnsupportedResolutionCarry),
			want: "is not supported",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc FlowSchemaDocument
			err := tc.body.Decode(&doc)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want typed field rejection", err)
			}
		})
	}
}

func TestFlowSchemaDocumentDecodeRejectsRetiredInstanceKeyCarrySource(t *testing.T) {
	var doc FlowSchemaDocument
	err := canonicalrouting.UnsupportedInputPinResolutionSnippet(t, canonicalrouting.RetiredInstanceKeyCarry).Decode(&doc)
	if err == nil {
		t.Fatal("yaml.Unmarshal succeeded, want strict retired carry-source rejection")
	}
	var diagnostic *LoaderDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if !strings.Contains(diagnostic.Problem, "instance.key.* carry sources are retired") {
		t.Fatalf("diagnostic problem = %q, want retired source teaching error", diagnostic.Problem)
	}
	for _, want := range []string{
		"generated.uuid",
		"event.id",
		"payload.<field>",
	} {
		if !strings.Contains(diagnostic.Remediation, want) {
			t.Fatalf("diagnostic remediation = %q, want teaching detail %q", diagnostic.Remediation, want)
		}
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
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("yaml.Unmarshal error = %v, want unsupported field", err)
	}
}

func TestFlowSchemaDocumentDecode_NormalizesClosedOutputPinSinkEnum(t *testing.T) {
	var doc FlowSchemaDocument
	err := yaml.Unmarshal([]byte(`
name: harness-output
pins:
  outputs:
    events:
      - name: work_completed
        event: work.completed
        sink: "  HARNESS  "
`), &doc)
	if err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got := doc.Pins.Outputs.EventPins[0].Sink; got != FlowOutputSinkHarness {
		t.Fatalf("Sink = %q, want %q", got, FlowOutputSinkHarness)
	}

	for _, tc := range []struct {
		name string
		sink string
	}{
		{name: "unknown", sink: "external"},
		{name: "empty", sink: `""`},
		{name: "null", sink: "null"},
		{name: "mapping", sink: "{kind: harness}"},
		{name: "sequence", sink: "[harness]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var invalid FlowSchemaDocument
			raw := "name: harness-output\npins:\n  outputs:\n    events:\n      - name: work_completed\n        event: work.completed\n        sink: " + tc.sink + "\n"
			err := yaml.Unmarshal([]byte(raw), &invalid)
			if err == nil || !strings.Contains(err.Error(), `output event pin sink must be "harness"`) {
				t.Fatalf("yaml.Unmarshal error = %v, want closed sink-enum rejection", err)
			}
		})
	}
}

func TestFlowPackageConnectDecodeRejectsRetiredDeliveryAndReplyOnPresence(t *testing.T) {
	if _, ok := flowPackageConnectFieldOptions["delivery"]; ok {
		t.Fatal("connect valid options retain retired delivery field")
	}
	if _, ok := flowPackageConnectFieldOptions["reply"]; ok {
		t.Fatal("connect valid options retain retired reply field")
	}
	tests := []struct {
		name        string
		field       string
		code        string
		problem     string
		remediation string
	}{
		{name: "delivery one", field: "delivery: one", code: "contract_loader.retired_connect_delivery", problem: "connect.delivery is retired.", remediation: "receiver input resolution"},
		{name: "delivery many", field: "delivery: many", code: "contract_loader.retired_connect_delivery", problem: "connect.delivery is retired.", remediation: "multiple rows"},
		{name: "delivery broadcast", field: "delivery: broadcast", code: "contract_loader.retired_connect_delivery", problem: "connect.delivery is retired.", remediation: "multiple rows"},
		{name: "delivery reply", field: "delivery: reply", code: "contract_loader.retired_connect_delivery", problem: "connect.delivery is retired.", remediation: "receiver input resolution"},
		{name: "delivery malformed", field: "delivery: [one]", code: "contract_loader.retired_connect_delivery", problem: "connect.delivery is retired.", remediation: "receiver input resolution"},
		{name: "reply mapping", field: "reply: {source_event_id: event.source_event_id}", code: "contract_loader.retired_connect_reply", problem: "connect.reply is retired.", remediation: "resolution mode reply"},
		{name: "reply empty", field: "reply: {}", code: "contract_loader.retired_connect_reply", problem: "connect.reply is retired.", remediation: "separate connect edges"},
		{name: "reply malformed", field: "reply: [legacy]", code: "contract_loader.retired_connect_reply", problem: "connect.reply is retired.", remediation: "resolution mode reply"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var connect FlowPackageConnect
			err := yaml.Unmarshal([]byte("event: work.ready\nfrom: producer\nto: consumer\n"+tc.field+"\n"), &connect)
			if err == nil {
				t.Fatal("yaml.Unmarshal succeeded, want retired connect field rejection")
			}
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok {
				t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
			}
			if diagnostic.Code != tc.code || diagnostic.Problem != tc.problem || !strings.Contains(diagnostic.Remediation, tc.remediation) {
				t.Fatalf("diagnostic = %#v, want code %q problem %q remediation containing %q", diagnostic, tc.code, tc.problem, tc.remediation)
			}
			if diagnostic.Location.YAMLPath == "" {
				t.Fatalf("diagnostic location = %#v, want retired field path", diagnostic.Location)
			}
		})
	}
}

func TestFlowSchemaDocumentDecode_RejectsRetiredAndUnsupportedTopLevelFields(t *testing.T) {
	tests := []struct {
		name         string
		field        string
		wantErr      string
		wantDiagCode string
	}{
		{name: "namespace_prefix", field: "namespace_prefix: worker", wantErr: "RETIRED"},
		{name: "namespace_rule", field: "namespace_rule: path", wantErr: "RETIRED"},
		{name: "namespace", field: "namespace: worker", wantErr: "schema field \"namespace\" is not supported.", wantDiagCode: "contract_loader.undefined_field"},
		{name: "unknown", field: "legacy_owner: worker", wantErr: "schema field \"legacy_owner\" is not supported.", wantDiagCode: "contract_loader.undefined_field"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc FlowSchemaDocument
			err := yaml.Unmarshal([]byte("name: invalid-schema\n"+tc.field+"\n"), &doc)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
			if tc.wantDiagCode == "" {
				return
			}
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok {
				t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
			}
			if diagnostic.Code != tc.wantDiagCode {
				t.Fatalf("diagnostic code = %q, want %q", diagnostic.Code, tc.wantDiagCode)
			}
			if len(diagnostic.ValidOptions) == 0 {
				t.Fatalf("diagnostic valid options empty: %#v", diagnostic)
			}
			foundTerminalStates := false
			for _, option := range diagnostic.ValidOptions {
				if option == "terminal_states" {
					foundTerminalStates = true
					break
				}
			}
			if !foundTerminalStates {
				t.Fatalf("diagnostic valid options = %#v, want terminal_states", diagnostic.ValidOptions)
			}
		})
	}
}

func TestFlowTemplateInstanceDecodeAcceptsScalarIdentity(t *testing.T) {
	var doc FlowSchemaDocument
	if err := yaml.Unmarshal([]byte(`
name: template-flow
mode: template
instance: scope_id
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := doc.Mode, "template"; got != want {
		t.Fatalf("Mode = %q, want %q", got, want)
	}
	if got, want := doc.Instance.Path(), "scope_id"; got != want {
		t.Fatalf("Instance = %q, want %q", got, want)
	}
}

func TestFlowTemplateInstanceDecodeRejectsRetiredMappingForms(t *testing.T) {
	tests := []struct {
		name     string
		instance string
	}{
		{name: "by only", instance: "{by: scope_id}"},
		{name: "empty", instance: "{}"},
		{name: "malformed", instance: "{unexpected: scope_id}"},
		{name: "list", instance: "{by: [scope_id]}"},
		{name: "composite", instance: "{by: [scope, scope_id, artifact_type]}"},
		{name: "on_missing only", instance: "{on_missing: create}"},
		{name: "on_conflict only", instance: "{on_conflict: reject}"},
		{name: "both policies", instance: "{on_missing: create, on_conflict: reject}"},
		{name: "mixed old and new", instance: "{field: scope_id, on_missing: create}"},
		{name: "sequence", instance: "[scope_id]"},
		{name: "null", instance: "null"},
		{name: "empty scalar", instance: `""`},
		{name: "nested scalar", instance: "tenant.scope_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var doc FlowSchemaDocument
			err := yaml.Unmarshal([]byte("name: template-flow\nmode: template\ninstance: "+tc.instance+"\n"), &doc)
			if err == nil || !strings.Contains(err.Error(), "instance: <field>") {
				t.Fatalf("yaml.Unmarshal error = %v, want scalar instance teaching error", err)
			}
		})
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

func TestHandlerRuleEntryDecode_RejectsLegacyComputeExpressionShorthand(t *testing.T) {
	var rule HandlerRuleEntry
	err := yaml.Unmarshal([]byte(`
condition: "else"
compute:
  store_as: entity.composite
  expression: "weighted_average(accumulated.scores, accumulated.weights)"
`), &rule)
	if err == nil || !strings.Contains(err.Error(), `compute field "expression" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed compute field rejection", err)
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
	if err == nil || !strings.Contains(err.Error(), `rule field "sets_gate" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want rule-level sets_gate rejection", err)
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if !containsString(diagnostic.ValidOptions, "advances_to") || !containsString(diagnostic.ValidOptions, "emit") {
		t.Fatalf("diagnostic valid options = %#v, want rule options", diagnostic.ValidOptions)
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

func TestSystemNodeEventHandlerDecode_RejectsRetiredBranch(t *testing.T) {
	var handler SystemNodeEventHandler
	err := yaml.Unmarshal([]byte(`
branch:
  - condition: payload.priority == 'urgent'
    then:
      emit:
        event: item.completed
    else:
      emit:
        event: item.rejected
`), &handler)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "branch") || !strings.Contains(err.Error(), "rules") {
		t.Fatalf("yaml.Unmarshal error = %v, want RETIRED branch rejection pointing to rules", err)
	}
}

func TestSystemNodeEventHandlerDecode_LowersPolicySheetSelectionRows(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  - id: cto_revision_gate
    when: "payload.spec_revision > entity.last_cto_reviewed_revision && entity.revision_count >= policy.inner_revision_max"
    advances_to: cto_review
  - id: deep_scan
    case:
      selector: payload.mode
      equals: deep
    emit: scan.deep_requested
  - id: repository_quick_scan
    case:
      selectors: [payload.mode, payload.target]
      equals: [quick, repository]
    emit: scan.quick_repo_requested
  - id: treasury_warning
    range:
      value: entity.spend_ratio
      gte: policy.warning_pct / 100
      lt: policy.throttle_pct / 100
      monotonicity:
        - policy.warning_pct / 100 <= policy.throttle_pct / 100 <= 1.0
    emit: treasury.warning_recorded
  - id: treasury_throttle
    range:
      value: entity.spend_ratio
      gte: policy.throttle_pct / 100
      lt: 1.0
      monotonicity:
        - policy.warning_pct / 100 <= policy.throttle_pct / 100 <= 1.0
    emit: treasury.throttle_recorded
  - id: default_route
    default: true
    emit: scan.default_requested
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := len(handler.Rules), 6; got != want {
		t.Fatalf("rules len = %d, want %d", got, want)
	}
	wantConditions := []string{
		"payload.spec_revision > entity.last_cto_reviewed_revision && entity.revision_count >= policy.inner_revision_max",
		`payload.mode == "deep"`,
		`payload.mode == "quick" && payload.target == "repository"`,
		`entity.spend_ratio >= policy.warning_pct / 100 && entity.spend_ratio < policy.throttle_pct / 100`,
		`entity.spend_ratio >= policy.throttle_pct / 100 && entity.spend_ratio < 1.0`,
		"else",
	}
	wantKinds := []PolicySheetRowKind{
		PolicySheetRowKindWhen,
		PolicySheetRowKindCase,
		PolicySheetRowKindCase,
		PolicySheetRowKindRange,
		PolicySheetRowKindRange,
		PolicySheetRowKindDefault,
	}
	for idx := range handler.Rules {
		if got := handler.Rules[idx].Condition; got != wantConditions[idx] {
			t.Fatalf("rules[%d].Condition = %q, want %q", idx, got, wantConditions[idx])
		}
		if got := handler.Rules[idx].PolicyRow.Kind; got != wantKinds[idx] {
			t.Fatalf("rules[%d].PolicyRow.Kind = %q, want %q", idx, got, wantKinds[idx])
		}
	}
	if got := handler.Rules[2].PolicyRow.Selectors; !reflect.DeepEqual(got, []string{"payload.mode", "payload.target"}) {
		t.Fatalf("tuple selectors = %#v", got)
	}
	if got := handler.Rules[3].PolicyRow.RangeLower.Value; got != "policy.warning_pct / 100" {
		t.Fatalf("range lower = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_LowersPolicySheetLookupValueRows(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  - id: scaffold_paths
    lookup:
      on: [payload.scaffold_type, payload.language]
      entries:
        - key: [service, go]
          value: templates/service/go
        - key: [library, go]
          value: templates/library/go
      into: computed.template_path
      default: fail
  - id: use_service_template
    when: computed.template_path == "templates/service/go"
    emit: repo.service_template_selected
  - id: fallback
    default: true
    emit: repo.other_template_selected
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := len(handler.Rules), 3; got != want {
		t.Fatalf("rules len = %d, want %d", got, want)
	}
	lookupRule := handler.Rules[0]
	if got := lookupRule.PolicyRow.Kind; got != PolicySheetRowKindLookup {
		t.Fatalf("lookup PolicyRow.Kind = %q, want lookup", got)
	}
	if lookupRule.Compute == nil {
		t.Fatal("lookup row Compute = nil")
	}
	if got := lookupRule.Compute.Operation; got != ComputeOpLookup {
		t.Fatalf("lookup Compute.Operation = %q, want lookup", got)
	}
	if got := lookupRule.Compute.StoreAs; got != "computed.template_path" {
		t.Fatalf("lookup StoreAs = %q, want computed.template_path", got)
	}
	if lookupRule.Compute.Lookup == nil {
		t.Fatal("lookup Compute.Lookup = nil")
	}
	if got := lookupRule.Compute.Lookup.On; !reflect.DeepEqual(got, []string{"payload.scaffold_type", "payload.language"}) {
		t.Fatalf("lookup on = %#v", got)
	}
	if got := len(lookupRule.Compute.Lookup.Entries); got != 2 {
		t.Fatalf("lookup entries len = %d, want 2", got)
	}
	if got := lookupRule.Compute.Lookup.Entries[0].Value; got != "templates/service/go" {
		t.Fatalf("lookup first value = %#v", got)
	}
	if !lookupRule.Compute.Lookup.DefaultDeclared || !lookupRule.Compute.Lookup.DefaultFail {
		t.Fatalf("lookup default flags = declared:%v fail:%v, want declared fail", lookupRule.Compute.Lookup.DefaultDeclared, lookupRule.Compute.Lookup.DefaultFail)
	}
	if got := handler.Rules[1].Condition; got != `computed.template_path == "templates/service/go"` {
		t.Fatalf("consumer condition = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_LowersPolicySheetValidateValueRows(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  - id: validate_manifest
    validate:
      set: deploy_manifest
      input:
        source_ref: payload.source_ref
        manifest_source_ref: payload.file_manifest.source_ref
      into: computed.validation.deploy_manifest
  - id: invalid_manifest
    when: computed.validation.deploy_manifest.valid == false
    emit: deploy.manifest_invalid
  - id: fallback
    default: true
    emit: deploy.manifest_accepted
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := len(handler.Rules), 3; got != want {
		t.Fatalf("rules len = %d, want %d", got, want)
	}
	validateRule := handler.Rules[0]
	if got := validateRule.PolicyRow.Kind; got != PolicySheetRowKindValidate {
		t.Fatalf("validate PolicyRow.Kind = %q, want validate", got)
	}
	if validateRule.Compute == nil {
		t.Fatal("validate row Compute = nil")
	}
	if got := validateRule.Compute.Operation; got != ComputeOpValidate {
		t.Fatalf("validate Compute.Operation = %q, want validate", got)
	}
	if got := validateRule.Compute.StoreAs; got != "computed.validation.deploy_manifest" {
		t.Fatalf("validate StoreAs = %q, want computed.validation.deploy_manifest", got)
	}
	if validateRule.Compute.Validation == nil {
		t.Fatal("validate Compute.Validation = nil")
	}
	if got := validateRule.Compute.Validation.Set; got != "deploy_manifest" {
		t.Fatalf("validate set = %q, want deploy_manifest", got)
	}
	if got := validateRule.Compute.Validation.Input["source_ref"]; got != "payload.source_ref" {
		t.Fatalf("validate input source_ref = %q, want payload.source_ref", got)
	}
	if got := handler.Rules[1].Condition; got != `computed.validation.deploy_manifest.valid == false` {
		t.Fatalf("consumer condition = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_LowersPolicySheetComputeModuleValueRows(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  - id: render_bundle
    compute_module:
      module: structured_renderer
      input:
        component: payload.component
        owner: payload.owner
        language: payload.language
        files: payload.files
      into: computed.rendered_bundle
  - id: emit_rendered_bundle
    when: computed.rendered_bundle.format == "yaml"
    emit: bundle.rendered
  - id: fallback
    default: true
    emit: bundle.render_failed
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := len(handler.Rules), 3; got != want {
		t.Fatalf("rules len = %d, want %d", got, want)
	}
	moduleRule := handler.Rules[0]
	if got := moduleRule.PolicyRow.Kind; got != PolicySheetRowKindModule {
		t.Fatalf("compute_module PolicyRow.Kind = %q, want compute_module", got)
	}
	if moduleRule.Compute == nil {
		t.Fatal("compute_module row Compute = nil")
	}
	if got := moduleRule.Compute.Operation; got != ComputeOpModule {
		t.Fatalf("compute_module Compute.Operation = %q, want compute_module", got)
	}
	if got := moduleRule.Compute.StoreAs; got != "computed.rendered_bundle" {
		t.Fatalf("compute_module StoreAs = %q, want computed.rendered_bundle", got)
	}
	if moduleRule.Compute.Module == nil {
		t.Fatal("compute_module Compute.Module = nil")
	}
	if got := moduleRule.Compute.Module.Module; got != "structured_renderer" {
		t.Fatalf("compute_module module = %q, want structured_renderer", got)
	}
	if got := moduleRule.Compute.Module.Input["component"]; got != "payload.component" {
		t.Fatalf("compute_module input component = %q, want payload.component", got)
	}
	if got := handler.Rules[1].Condition; got != `computed.rendered_bundle.format == "yaml"` {
		t.Fatalf("consumer condition = %q", got)
	}
}

func TestSystemNodeEventHandlerDecode_PreservesPolicyRowWordsAsRuleIDsInKeyedMap(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
rules:
  case:
    condition: payload.mode == "case"
    emit: scan.case_requested
  default:
    condition: else
    emit: scan.default_requested
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got, want := len(handler.Rules), 2; got != want {
		t.Fatalf("rules len = %d, want %d", got, want)
	}
	if got := handler.Rules[0].ID; got != "case" {
		t.Fatalf("rules[0].ID = %q, want case", got)
	}
	if got := handler.Rules[0].PolicyRow.Kind; got != "" {
		t.Fatalf("rules[0].PolicyRow.Kind = %q, want empty", got)
	}
	if got := handler.Rules[1].ID; got != "default" {
		t.Fatalf("rules[1].ID = %q, want default", got)
	}
	if got := handler.Rules[1].Condition; got != "else" {
		t.Fatalf("rules[1].Condition = %q, want else", got)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsInvalidPolicySheetRows(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{
			name: "missing default",
			body: `
rules:
  - id: deep_scan
    case:
      selector: payload.mode
      equals: deep
    emit: scan.deep_requested
`,
			contains: "require an else/default row",
		},
		{
			name: "duplicate case key",
			body: `
rules:
  - id: deep_a
    case:
      selector: payload.mode
      equals: deep
    emit: scan.deep_a
  - id: deep_b
    case:
      selector: payload.mode
      equals: deep
    emit: scan.deep_b
  - id: fallback
    default: true
    emit: scan.default
`,
			contains: "duplicate case key",
		},
		{
			name: "overlapping literal ranges",
			body: `
rules:
  - id: low
    range:
      value: payload.score
      gte: 0
      lt: 10
    advances_to: low
  - id: overlap
    range:
      value: payload.score
      gte: 5
      lt: 15
    advances_to: overlap
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "overlapping literal ranges",
		},
		{
			name: "dynamic bound",
			body: `
rules:
  - id: dynamic_bound
    range:
      value: entity.spend_ratio
      gte: payload.warning_ratio
    advances_to: warning
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "dynamic bound",
		},
		{
			name: "policy bound missing monotonicity",
			body: `
rules:
  - id: warning
    range:
      value: entity.spend_ratio
      gte: policy.warning_ratio
      lt: policy.throttle_ratio
    advances_to: warning
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "requires monotonicity",
		},
		{
			name: "overlapping open ended policy ranges",
			body: `
rules:
  - id: warning
    range:
      value: entity.spend_ratio
      gte: policy.warning_ratio
      monotonicity:
        - policy.warning_ratio <= policy.throttle_ratio <= 1.0
    advances_to: warning
  - id: throttle
    range:
      value: entity.spend_ratio
      gte: policy.throttle_ratio
      monotonicity:
        - policy.warning_ratio <= policy.throttle_ratio <= 1.0
    advances_to: throttle
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "overlapping ranges",
		},
		{
			name: "unsupported selector root",
			body: `
rules:
  - id: fanout_selector
    case:
      selector: fan_out.count
      equals: 3
    advances_to: fanout
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "unsupported root",
		},
		{
			name: "selector operator injection",
			body: `
rules:
  - id: injected
    case:
      selector: 'payload.mode=="admin"||payload.mode'
      equals: deep
    advances_to: injected
  - id: fallback
    default: true
    advances_to: fallback
`,
			contains: "simple dotted path",
		},
		{
			name: "policy field dual owner",
			body: `
rules:
  - id: bad_policy
    policy:
      row: true
    advances_to: bad
`,
			contains: "second policy-sheet authoring owner",
		},
		{
			name: "lookup into entity",
			body: `
rules:
  - id: bad_lookup
    lookup:
      on: payload.kind
      entries:
        - key: service
          value: templates/service/go
      into: entity.template_path
      default: fail
`,
			contains: "computed.*",
		},
		{
			name: "lookup duplicate key",
			body: `
rules:
  - id: duplicate_lookup
    lookup:
      on: [payload.kind, payload.language]
      entries:
        - key: [service, go]
          value: templates/service/go
        - key: [service, go]
          value: templates/service/go-v2
      into: computed.template_path
      default: fail
`,
			contains: "duplicate lookup key",
		},
		{
			name: "lookup entity root unsupported",
			body: `
rules:
  - id: entity_lookup
    lookup:
      on: entity.kind
      entries:
        - key: service
          value: templates/service/go
      into: computed.template_path
      default: fail
`,
			contains: `unsupported root "entity"`,
		},
		{
			name: "lookup policy root unsupported",
			body: `
rules:
  - id: policy_lookup
    lookup:
      on: policy.kind
      entries:
        - key: service
          value: templates/service/go
      into: computed.template_path
      default: fail
`,
			contains: `unsupported root "policy"`,
		},
		{
			name: "lookup event root unsupported",
			body: `
rules:
  - id: event_lookup
    lookup:
      on: event.kind
      entries:
        - key: service
          value: templates/service/go
      into: computed.template_path
      default: fail
`,
			contains: `unsupported root "event"`,
		},
		{
			name: "lookup branch output",
			body: `
rules:
  - id: bad_lookup_branch
    lookup:
      on: payload.kind
      entries:
        - key: service
          value: templates/service/go
      into: computed.template_path
      default: fail
    emit: repo.service_template_selected
`,
			contains: "cannot declare branch outputs",
		},
		{
			name: "compute module into entity",
			body: `
rules:
  - id: bad_module
    compute_module:
      module: structured_renderer
      input:
        component: payload.component
      into: entity.rendered_bundle
`,
			contains: "computed.*",
		},
		{
			name: "compute module branch output",
			body: `
rules:
  - id: bad_module_branch
    compute_module:
      module: structured_renderer
      input:
        component: payload.component
      into: computed.rendered_bundle
    emit: bundle.rendered
`,
			contains: "cannot declare branch outputs",
		},
		{
			name: "public compute lookup",
			body: `
compute:
  operation: lookup
  store_as: computed.template_path
`,
			contains: "internal to policy-sheet value rows",
		},
		{
			name: "public compute validate",
			body: `
compute:
  operation: validate
  store_as: computed.validation.deploy_manifest
`,
			contains: "internal to policy-sheet value rows",
		},
		{
			name: "public compute module",
			body: `
compute:
  operation: compute_module
  store_as: computed.rendered_bundle
`,
			contains: "internal to policy-sheet value rows",
		},
		{
			name: "typed row outside rules",
			body: `
on_complete:
  - id: done
    when: "payload.ok"
    advances_to: done
`,
			contains: "only supported under handler.rules",
		},
		{
			name: "standalone handler switch",
			body: `
switch:
  selector: payload.mode
`,
			contains: `handler field "switch"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tt.body), &handler)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_AllowsAbsentAndDuplicatePolicyDisplayLabels(t *testing.T) {
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  - element_id: 00000000-0000-4000-8000-000000000431
    when: payload.ok
    advances_to: ok
  - element_id: 00000000-0000-4000-8000-000000000432
    id: repeated-label
    when: payload.retry
    advances_to: retry
  - element_id: 00000000-0000-4000-8000-000000000433
    id: repeated-label
    default: true
    advances_to: fallback
`), &handler); err != nil {
		t.Fatal(err)
	}
	if len(handler.Rules) != 3 || handler.Rules[0].ID != "" || handler.Rules[1].ID != handler.Rules[2].ID {
		t.Fatalf("decoded labels = %#v", handler.Rules)
	}
	for index, rule := range handler.Rules {
		if !rule.ElementID.Valid() {
			t.Fatalf("rule %d lost authored element identity", index)
		}
	}
}

func TestSystemNodeEventHandlerDecode_RecognizesEverySingletonRuleFieldFamily(t *testing.T) {
	for _, tc := range []struct {
		name   string
		raw    string
		assert func(testing.TB, HandlerRuleEntry)
	}{
		{
			name: "activity only",
			raw:  "rules: {activity: {tool: notify}}\n",
			assert: func(t testing.TB, rule HandlerRuleEntry) {
				if rule.Activity.Tool != "notify" {
					t.Fatalf("activity = %#v", rule.Activity)
				}
			},
		},
		{
			name: "identity plus activity",
			raw:  "rules: {element_id: 00000000-0000-4000-8000-000000000434, activity: {tool: notify}}\n",
			assert: func(t testing.TB, rule HandlerRuleEntry) {
				if !rule.ElementID.Valid() || rule.Activity.Tool != "notify" {
					t.Fatalf("identity/activity = %#v", rule)
				}
			},
		},
		{
			name: "presentation only",
			raw:  "rules: {id: display-only, description: presentation}\n",
			assert: func(t testing.TB, rule HandlerRuleEntry) {
				if rule.ID != "display-only" || rule.Description != "presentation" {
					t.Fatalf("presentation = %#v", rule)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			if err := yaml.Unmarshal([]byte(tc.raw), &handler); err != nil {
				t.Fatal(err)
			}
			if len(handler.Rules) != 1 {
				t.Fatalf("rules = %#v, want one singleton", handler.Rules)
			}
			tc.assert(t, handler.Rules[0])
		})
	}
}

func TestSystemNodeEventHandlerDecode_KeyedLabelsNeverBecomeSingletonGrammar(t *testing.T) {
	labels := []string{
		"element_id", "id", "description", "condition", "when", "case", "range", "lookup", "validate", "compute_module",
		"else", "default", "advances_to", "emit", "emits", "action", "activity", "data_accumulation", "compute", "fan_out",
	}
	for _, context := range []string{"rules"} {
		for _, label := range labels {
			t.Run(context+"/"+label, func(t *testing.T) {
				raw := fmt.Sprintf(`%s:
  %s:
    element_id: 00000000-0000-4000-8000-000000000436
    condition: else
    advances_to: done
`, context, label)
				var handler SystemNodeEventHandler
				if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
					t.Fatal(err)
				}
				rows := handler.Rules
				if context == "on_complete" {
					rows = handler.OnComplete
				}
				if len(rows) != 1 || rows[0].ID != label || rows[0].Condition != "else" || rows[0].AdvancesTo != "done" || !rows[0].ElementID.Valid() {
					t.Fatalf("decoded keyed row = %#v", rows)
				}
			})
		}
	}
}

func TestSystemNodeEventHandlerDecode_RejectsAmbiguousRuleMapping(t *testing.T) {
	for _, context := range []string{"rules"} {
		t.Run(context, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(context+`: {activity: {id: ambiguous}}
`), &handler)
			if err == nil || !strings.Contains(err.Error(), "AMBIGUOUS-RULE-GRAMMAR") {
				t.Fatalf("yaml.Unmarshal error = %v, want ambiguous grammar rejection", err)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_RejectsMappingOnComplete(t *testing.T) {
	for _, raw := range []string{
		"on_complete: {selected: {condition: else}}\n",
		"on_complete: {activity: {id: ambiguous}}\n",
	} {
		var handler SystemNodeEventHandler
		err := yaml.Unmarshal([]byte(raw), &handler)
		if err == nil || !strings.Contains(err.Error(), "DIALECT-OC-ORDER") {
			t.Fatalf("yaml.Unmarshal error = %v, want ordered-list rejection for %s", err, raw)
		}
	}
}

func TestSystemNodeEventHandlerDecode_RejectsEmptyAuthoredRows(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "rules sequence", raw: "rules:\n  - {}\n"},
		{name: "rules keyed", raw: "rules:\n  selected: {}\n"},
		{name: "on complete sequence", raw: "on_complete:\n  - {}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tc.raw), &handler)
			if err == nil || !strings.Contains(err.Error(), "EMPTY-AUTHORED-RULE") {
				t.Fatalf("yaml.Unmarshal error = %v, want empty authored row rejection", err)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_RejectsInvalidKeyedRuleShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty label", raw: "rules:\n  \"\": {condition: else}\n", want: "label must not be empty"},
		{name: "whitespace label", raw: "rules:\n  \"   \": {condition: else}\n", want: "label must not be empty"},
		{name: "scalar child", raw: "rules:\n  selected: else\n", want: "must be a mapping"},
		{name: "empty collection", raw: "rules: {}\n", want: "must contain at least one row"},
		{name: "null rules", raw: "rules: null\n", want: "rules handler rule collection must not be null"},
		{name: "null on complete", raw: "on_complete: null\n", want: "on_complete handler rule collection must not be null"},
		{name: "duplicate keyed label", raw: "rules:\n  selected: {condition: else}\n  selected: {condition: else}\n", want: "duplicate normalized key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(tc.raw), &handler)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSystemNodeEventHandlerDecode_ResolvesAliasedRowsIndependentOfKeyLabel(t *testing.T) {
	labels := []string{
		"selected", "element_id", "id", "description", "condition", "when", "case", "range", "lookup", "validate",
		"compute_module", "else", "default", "advances_to", "emit", "emits", "action", "activity", "data_accumulation", "compute", "fan_out",
	}
	for _, label := range labels {
		t.Run(label, func(t *testing.T) {
			raw := fmt.Sprintf(`template: &rule
  element_id: 00000000-0000-4000-8000-000000000437
  condition: else
  advances_to: done
handler:
  rules:
    %q: *rule
`, label)
			var document struct {
				Handler SystemNodeEventHandler `yaml:"handler"`
			}
			if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			rules := document.Handler.Rules
			if len(rules) != 1 || rules[0].ID != label || !rules[0].ElementID.Valid() || rules[0].AdvancesTo != "done" {
				t.Fatalf("aliased keyed row = %#v", rules)
			}
		})
	}
}

func TestHandlerRuleMappingClassifierRejectsAliasCycles(t *testing.T) {
	mapping := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	alias := &yaml.Node{Kind: yaml.AliasNode, Alias: mapping}
	mapping.Content = []*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "selected"},
		alias,
	}
	if _, err := classifyHandlerRuleMapping(mapping); err == nil || !strings.Contains(err.Error(), "YAML-ALIAS-CYCLE") {
		t.Fatalf("classifyHandlerRuleMapping error = %v, want alias-cycle rejection", err)
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
	if err == nil || !strings.Contains(err.Error(), `compute field "output_field" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed compute field rejection", err)
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

func TestFlowSchemaDocumentDecode_TracksRequiredAgentsPresence(t *testing.T) {
	tests := []struct {
		name     string
		yamlText string
		declared bool
	}{
		{name: "omitted", yamlText: "name: worker\n", declared: false},
		{name: "explicit empty", yamlText: "name: worker\nrequired_agents: []\n", declared: true},
		{name: "explicit entries", yamlText: "name: worker\nrequired_agents:\n  - role: analyst\n", declared: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var schema FlowSchemaDocument
			if err := yaml.Unmarshal([]byte(tc.yamlText), &schema); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if schema.RequiredAgentsDeclared != tc.declared {
				t.Fatalf("RequiredAgentsDeclared = %v, want %v", schema.RequiredAgentsDeclared, tc.declared)
			}
		})
	}
}

func TestSystemNodeContractDecode_RejectsRetiredAndUnsupportedTopLevelFields(t *testing.T) {
	tests := []struct {
		name         string
		field        string
		wantErr      string
		wantDiagCode string
	}{
		{name: "permissions", field: "permissions: [create_flow_instance]", wantErr: "RETIRED"},
		{name: "implementation", field: "implementation: builtin", wantErr: "RETIRED"},
		{name: "owned_transitions", field: "owned_transitions: [ticket-open]", wantErr: "RETIRED"},
		{name: "idempotency_table", field: "idempotency_table: worker_idempotency", wantErr: "RETIRED"},
		{name: "unknown", field: "legacy_owner: worker", wantErr: "node field \"legacy_owner\" is not supported.", wantDiagCode: "contract_loader.undefined_field"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var node SystemNodeContract
			err := yaml.Unmarshal([]byte("id: worker\n"+tc.field+"\n"), &node)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.wantErr)
			}
			if tc.wantDiagCode == "" {
				return
			}
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok {
				t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
			}
			if diagnostic.Code != tc.wantDiagCode {
				t.Fatalf("diagnostic code = %q, want %q", diagnostic.Code, tc.wantDiagCode)
			}
			if len(diagnostic.ValidOptions) == 0 {
				t.Fatalf("diagnostic valid options empty: %#v", diagnostic)
			}
			foundEventHandlers := false
			for _, option := range diagnostic.ValidOptions {
				if option == "event_handlers" {
					foundEventHandlers = true
					break
				}
			}
			if !foundEventHandlers {
				t.Fatalf("diagnostic valid options = %#v, want event_handlers", diagnostic.ValidOptions)
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

func TestFanOutSpecDecode_RejectsUnknownFieldWithCanonicalOptions(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
foreach: payload.items
emit:
  event: routed.item
`), &spec)
	if err == nil {
		t.Fatal("expected unknown fan_out field rejection")
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if got := diagnostic.Problem; got != `fan_out field "foreach" is not supported.` {
		t.Fatalf("diagnostic problem = %q, want unknown fan_out field problem", got)
	}
	if !reflect.DeepEqual(diagnostic.ValidOptions, []string{"as", "emit", "identity", "items_from", "max_items"}) {
		t.Fatalf("diagnostic valid options = %#v, want canonical fan_out options", diagnostic.ValidOptions)
	}
}

func TestFanOutSpecDecode_RejectsExplicitZeroMaxItems(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
as: line_item
identity: line_item.id
max_items: 0
emit:
  event: routed.item
  fields:
    item_id: line_item.id
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "fan_out.max_items must be a positive integer when set") {
		t.Fatalf("yaml.Unmarshal error = %v, want explicit zero max_items rejection", err)
	}
}

func TestFanOutSpecDecode_RejectsExplicitNullMaxItems(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items
as: line_item
identity: line_item.id
max_items: null
emit:
  event: routed.item
  fields:
    item_id: line_item.id
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "fan_out.max_items must be a positive integer when set") {
		t.Fatalf("yaml.Unmarshal error = %v, want explicit null max_items rejection", err)
	}
}

func TestFanOutSpecDecode_RejectsNestedItemsSource(t *testing.T) {
	var spec FanOutSpec
	err := yaml.Unmarshal([]byte(`
items_from: payload.items.missing
as: line_item
identity: line_item.id
emit:
  event: routed.item
  fields:
    item_id: line_item.id
`), &spec)
	if err == nil || !strings.Contains(err.Error(), "must reference exactly one declared top-level collection field") {
		t.Fatalf("yaml.Unmarshal error = %v, want nested items_from rejection", err)
	}
}

func TestFanOutSpecDecode_DistinguishesOmittedMaxItems(t *testing.T) {
	var spec FanOutSpec
	if err := yaml.Unmarshal([]byte(`
items_from: payload.items
as: line_item
identity: line_item.id
emit:
  event: routed.item
  fields:
    item_id: line_item.id
`), &spec); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if spec.MaxItemsSet || spec.MaxItems != 0 {
		t.Fatalf("decoded max_items = %d set=%v, want omitted", spec.MaxItems, spec.MaxItemsSet)
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
	if err == nil || !strings.Contains(err.Error(), `select_entity field "where" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed select_entity field rejection", err)
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
	if err == nil || !strings.Contains(err.Error(), `select_or_create_entity field "where" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed select_or_create_entity field rejection", err)
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
			wantErr: `guard.on_fail field "reject" is not supported.`,
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

func TestGuardSpecDecode_RejectsUnknownOnFailFieldWithCanonicalOptions(t *testing.T) {
	var spec GuardSpec
	err := yaml.Unmarshal([]byte(`
id: score_check
check: payload.score >= policy.threshold
on_fail:
  reject: true
`), &spec)
	if err == nil {
		t.Fatal("expected unknown guard.on_fail field rejection")
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if got := diagnostic.Problem; got != `guard.on_fail field "reject" is not supported.` {
		t.Fatalf("diagnostic problem = %q, want unknown guard.on_fail field problem", got)
	}
	if !reflect.DeepEqual(diagnostic.ValidOptions, []string{"escalate"}) {
		t.Fatalf("diagnostic valid options = %#v, want only escalate", diagnostic.ValidOptions)
	}
}

func TestAccumulateSpecDecode_RejectsUnknownFieldWithCanonicalOptions(t *testing.T) {
	var spec AccumulateSpec
	err := yaml.Unmarshal([]byte(`
into: entity.items
source: payload.items
`), &spec)
	if err == nil {
		t.Fatal("expected unknown accumulate field rejection")
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if got := diagnostic.Problem; got != `accumulate field "source" is not supported.` {
		t.Fatalf("diagnostic problem = %q, want unknown accumulate field problem", got)
	}
	want := []string{"dedup_by", "description", "from", "into", "window"}
	if !reflect.DeepEqual(diagnostic.ValidOptions, want) {
		t.Fatalf("diagnostic valid options = %#v, want %#v", diagnostic.ValidOptions, want)
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
  as: line_item
  identity: line_item.id
  emit:
    event: item.done
    fields:
      item_id: line_item.id
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
			contains: "RETIRED-EMIT-ROUTING: emit.target",
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
  as: line_item
  identity: line_item.id
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
      as: line_item
      identity: line_item.id
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
	if err == nil || !strings.Contains(err.Error(), `config_from field "policy_keys" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed config_from field rejection", err)
	}
}

func TestSystemNodeEventHandlerDecode_RejectsAuthoredSystemPromptConfigFrom(t *testing.T) {
	for _, key := range []string{"system_prompt", "config.system_prompt", "config[nested][system_prompt]"} {
		t.Run(key, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte(`
action: create_flow_instance
template: worker
instance_id_from: payload.worker_id
config_from:
  `+key+`: payload.prompt
`), &handler)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "intent:") {
				t.Fatalf("yaml.Unmarshal error = %v, want authored system_prompt teaching rejection", err)
			}
		})
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
	if err == nil || !strings.Contains(err.Error(), `mailbox field "implicit_payload" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed mailbox field rejection", err)
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
      failure: failure
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
	if err == nil || !strings.Contains(err.Error(), `artifact_repo field "shell" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed artifact_repo field rejection", err)
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
	if err == nil || !strings.Contains(err.Error(), `accumulate field "legacy_buffer" is not supported.`) {
		t.Fatalf("yaml.Unmarshal error = %v, want typed accumulate field rejection", err)
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("yaml.Unmarshal error = %T %v, want LoaderDiagnostic", err, err)
	}
	if !containsString(diagnostic.ValidOptions, "into") || containsString(diagnostic.ValidOptions, "on_complete") {
		t.Fatalf("diagnostic valid options = %#v, want accumulate options", diagnostic.ValidOptions)
	}
}

func TestAccumulateSpecDecodeRejectsRetiredFiniteBarrierFields(t *testing.T) {
	for _, field := range []string{"expected_from", "completion", "threshold", "timeout_ms", "on_complete", "on_timeout"} {
		t.Run(field, func(t *testing.T) {
			var spec AccumulateSpec
			err := yaml.Unmarshal([]byte("into: items\n"+field+": retired\n"), &spec)
			if err == nil || !strings.Contains(err.Error(), `accumulate field "`+field+`" is not supported`) {
				t.Fatalf("yaml.Unmarshal error = %v, want retired field rejection", err)
			}
		})
	}
}

func TestEmitTargetDecode_RejectsEveryRetiredShapeOnPresence(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "true", yaml: "event: account.notify.requested\ntarget:\n  flow: account\n  match:\n    account_id: payload.account_id\n  allow_fanout: true\n"},
		{name: "false", yaml: "event: account.notify.requested\ntarget:\n  flow: account\n  match:\n    account_id: payload.account_id\n  allow_fanout: false\n"},
		{name: "malformed value", yaml: "event: account.notify.requested\ntarget:\n  flow: account\n  match:\n    account_id: payload.account_id\n  allow_fanout: [legacy]\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var emit EmitSpec
			err := canonicalrouting.NewParserSnippet(t, tc.yaml).Decode(&emit)
			if err == nil {
				t.Fatal("yaml.Unmarshal succeeded, want retired allow_fanout rejection")
			}
			if want := "RETIRED-EMIT-ROUTING: emit.target"; !strings.Contains(err.Error(), want) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, want)
			}
		})
	}
}

func TestEmitSpecDecode_RejectsEveryRetiredProducerRoutingFieldOnPresence(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "target scalar", yaml: "event: task.done\ntarget: sender\n", want: "emit.target"},
		{name: "target empty", yaml: "event: task.done\ntarget: {}\n", want: "emit.target"},
		{name: "target null", yaml: "event: task.done\ntarget: null\n", want: "emit.target"},
		{name: "broadcast true", yaml: "event: task.done\nbroadcast: true\n", want: "emit.broadcast"},
		{name: "broadcast false", yaml: "event: task.done\nbroadcast: false\n", want: "emit.broadcast"},
		{name: "broadcast null", yaml: "event: task.done\nbroadcast: null\n", want: "emit.broadcast"},
		{name: "broadcast malformed", yaml: "event: task.done\nbroadcast: [true]\n", want: "emit.broadcast"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var emit EmitSpec
			err := canonicalrouting.NewParserSnippet(t, tc.yaml).Decode(&emit)
			if err == nil || !strings.Contains(err.Error(), "RETIRED-EMIT-ROUTING: "+tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want retired %s rejection", err, tc.want)
			}
		})
	}
}

func TestFanOutDecode_RejectsRetiredAllowFanoutInNestedEmit(t *testing.T) {
	var spec FanOutSpec
	err := canonicalrouting.NewParserSnippet(t, `
items_from: entity.account_ids
as: account_id
emit:
  event: account.notify.requested
  target:
    flow: account
    match:
      account_id: account_id
    allow_fanout: false
`).Decode(&spec)
	if err == nil || !strings.Contains(err.Error(), "RETIRED-EMIT-ROUTING: emit.target") {
		t.Fatalf("yaml.Unmarshal error = %v, want nested retired target diagnostic", err)
	}
}

func TestEnumTypeDeclDecode_RetiresSequenceFormWithTeachingCodemod(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte(`[low, medium, high]`), &decl)
	if err == nil || !strings.Contains(err.Error(), "RETIRED: enum declaration uses the sequence form") || !strings.Contains(err.Error(), "default: low") {
		t.Fatalf("sequence form error = %v, want teaching codemod naming default: low", err)
	}
}

func TestEnumTypeDeclDecode_RetiresScalarShorthandWithTeachingCodemod(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte(`fast`), &decl)
	if err == nil || !strings.Contains(err.Error(), "RETIRED: enum declaration uses the scalar shorthand") || !strings.Contains(err.Error(), "default: fast") {
		t.Fatalf("scalar shorthand error = %v, want teaching codemod naming default: fast", err)
	}
}

func TestEnumTypeDeclDecode_RejectsDuplicateKeys(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte("values: [low, high]\ndefault: low\ndefault: high\n"), &decl)
	if err == nil || !strings.Contains(err.Error(), `repeats key "default"`) {
		t.Fatalf("duplicate key error = %v, want duplicate-key rejection", err)
	}
}

func TestEnumTypeDeclDecode_UnknownFieldListsValidOptions(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte("values: [low, high]\ndefualt: low\n"), &decl)
	if err == nil {
		t.Fatal("unknown enum field accepted")
	}
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok {
		t.Fatalf("unknown field error = %T %v, want LoaderDiagnostic", err, err)
	}
	if diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Problem, "defualt") {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
	options := strings.Join(diagnostic.ValidOptions, ",")
	if !strings.Contains(options, "values") || !strings.Contains(options, "default") {
		t.Fatalf("valid options = %v, want values and default", diagnostic.ValidOptions)
	}
}

func TestEnumTypeDeclDecode_RequiresDefault(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte("values: [low, medium, high]\n"), &decl)
	if err == nil || !strings.Contains(err.Error(), "requires default") || !strings.Contains(err.Error(), "default: low") {
		t.Fatalf("missing default error = %v, want teaching codemod naming default: low", err)
	}
}

func TestEnumTypeDeclDecode_RejectsNonMemberDefaultNamingMembers(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte("values: [low, medium, high]\ndefault: urgent\n"), &decl)
	if err == nil || !strings.Contains(err.Error(), `default "urgent" is not a declared member`) || !strings.Contains(err.Error(), "low, medium, high") {
		t.Fatalf("non-member default error = %v, want error naming the declared members", err)
	}
}

func TestEnumTypeDeclDecode_AcceptsCanonicalMappingForm(t *testing.T) {
	var decl EnumTypeDecl
	if err := yaml.Unmarshal([]byte("values: [low, medium, high]\ndefault: medium\n"), &decl); err != nil {
		t.Fatalf("canonical mapping form rejected: %v", err)
	}
	if !reflect.DeepEqual(decl.Values, []string{"low", "medium", "high"}) || decl.Default != "medium" {
		t.Fatalf("decoded enum = %#v", decl)
	}
}

func TestEnumTypeDeclDecode_EmptyScalarShorthandRequiresMappingForm(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte(`""`), &decl)
	if err == nil || !strings.Contains(err.Error(), "requires the mapping form") {
		t.Fatalf("empty scalar shorthand error = %v, want mapping-form guidance", err)
	}
}

func TestEnumTypeDeclDecode_NonScalarDefaultGetsAuthorMessage(t *testing.T) {
	var decl EnumTypeDecl
	err := yaml.Unmarshal([]byte("values: [low, high]\ndefault: [low]\n"), &decl)
	if err == nil || !strings.Contains(err.Error(), "default must be a scalar member") {
		t.Fatalf("non-scalar default error = %v, want author-facing scalar-member message", err)
	}
	if strings.Contains(err.Error(), "kind") {
		t.Fatalf("non-scalar default leaked an internal yaml kind: %v", err)
	}
}

func TestTypeCatalogDecode_RejectsNullEnumEntry(t *testing.T) {
	var catalog TypeCatalogDocument
	err := yaml.Unmarshal([]byte("enums:\n  Mode:\n"), &catalog)
	if err == nil || !strings.Contains(err.Error(), `enum Mode is declared without values`) {
		t.Fatalf("null enum entry error = %v, want load-time teaching rejection", err)
	}
}

func TestTypeCatalogDecode_RejectsEmptyEnumKey(t *testing.T) {
	var catalog TypeCatalogDocument
	err := yaml.Unmarshal([]byte("enums:\n  \"\":\n    values: [low]\n    default: low\n"), &catalog)
	if err == nil || !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("empty enum key error = %v, want empty-name rejection", err)
	}
}

func TestTypeCatalogDecode_RejectsWhitespacePaddedEnumKey(t *testing.T) {
	var catalog TypeCatalogDocument
	err := yaml.Unmarshal([]byte("enums:\n  \" Mode\":\n    values: [low]\n    default: low\n"), &catalog)
	if err == nil || !strings.Contains(err.Error(), "surrounding whitespace") {
		t.Fatalf("whitespace-padded enum key error = %v, want whitespace rejection", err)
	}
}

func TestEnumTypeDeclValidate_SharedInvariantNondeterministicFree(t *testing.T) {
	var catalog TypeCatalogDocument
	err := yaml.Unmarshal([]byte("enums:\n  zebra:\n  alpha:\n    values: [low]\n    default: low\n"), &catalog)
	if err == nil || !strings.Contains(err.Error(), "enum zebra is declared without values") {
		t.Fatalf("multi-enum validation error = %v, want sorted deterministic first error naming zebra", err)
	}
}
