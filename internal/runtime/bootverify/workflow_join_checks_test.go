package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRun_ValidatesStagedJoinContract(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*runtimecontracts.SystemNodeEventHandler, *runtimecontracts.WorkflowContractBundle)
		wantError string
	}{
		{name: "valid"},
		{name: "bare join", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.TimeoutFound = false
			h.Join.Timeout = runtimecontracts.JoinTimeoutSpec{}
		}, wantError: "bare joins are invalid"},
		{name: "members must be list text", mutate: func(_ *runtimecontracts.SystemNodeEventHandler, b *runtimecontracts.WorkflowContractBundle) {
			entity := b.RootEntities["Order"]
			entity.Fields["expected"] = runtimecontracts.EntityFieldDecl{Type: "text"}
			b.RootEntities["Order"] = entity
		}, wantError: "must be ordered list<text>"},
		{name: "custom completion requires remaining", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join.completed >= 1"
		}, wantError: "requires remaining: ignore"},
		{name: "terminate unsupported", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join.completed >= 1"
			h.Join.Remaining = "terminate"
		}, wantError: "requires remaining: ignore"},
		{name: "unsupported dotted join fact", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join.active == 0"
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "unsupported join.active"},
		{name: "bracket join fact rejected", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = `join["active"] == 0`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "bracket access on join is unsupported"},
		{name: "approved fact bracket spelling rejected", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = `join["completed"] >= 1`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "bracket access on join is unsupported"},
		{name: "bare join root rejected", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join == join"
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "join must be accessed as join.<field>"},
		{name: "custom completion must be boolean", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join.expected"
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "must return bool"},
		{name: "custom completion rejects invalid missing operand", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.CompleteWhen = "join.missing > 1"
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "no matching overload"},
		{name: "custom completion types results from output schema", mutate: func(h *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
			event := bundle.Events["item.completed"]
			result := event.Payload.Properties["result"]
			result.Type = "text"
			event.Payload.Properties["result"] = result
			bundle.Events["item.completed"] = event
			h.Join.CompleteWhen = "join.results[0] > 1"
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "no matching overload"},
		{name: "custom completion preserves named result fields", mutate: func(h *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
			bundle.RootTypes = runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
				"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "text"}}},
			}}
			event := bundle.Events["item.completed"]
			event.Payload.Properties["result"] = runtimecontracts.EventFieldSpec{Type: "JoinResult"}
			bundle.Events["item.completed"] = event
			h.Join.CompleteWhen = `join.results[0].value == "ok"`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}},
		{name: "custom completion rejects named result as scalar", mutate: func(h *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
			bundle.RootTypes = runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
				"JoinResult": {Fields: map[string]runtimecontracts.TypeFieldSpec{"value": {Type: "text"}}},
			}}
			event := bundle.Events["item.completed"]
			event.Payload.Properties["result"] = runtimecontracts.EventFieldSpec{Type: "JoinResult"}
			bundle.Events["item.completed"] = event
			h.Join.CompleteWhen = `join.results[0] > 1`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "no matching overload"},
		{name: "custom completion preserves enum result", mutate: func(h *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
			bundle.RootTypes = runtimecontracts.TypeCatalogDocument{Enums: map[string]runtimecontracts.EnumTypeDecl{"Decision": {Values: []string{"accept", "reject"}, Default: "accept"}}}
			event := bundle.Events["item.completed"]
			event.Payload.Properties["result"] = runtimecontracts.EventFieldSpec{Type: "Decision"}
			bundle.Events["item.completed"] = event
			h.Join.CompleteWhen = `join.results[0] > 1`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "no matching overload"},
		{name: "custom completion preserves scalar alias result", mutate: func(h *runtimecontracts.SystemNodeEventHandler, bundle *runtimecontracts.WorkflowContractBundle) {
			bundle.RootTypes = runtimecontracts.TypeCatalogDocument{Scalars: map[string]runtimecontracts.ScalarTypeDecl{"Score": {Base: "integer"}}}
			event := bundle.Events["item.completed"]
			event.Payload.Properties["result"] = runtimecontracts.EventFieldSpec{Type: "Score"}
			bundle.Events["item.completed"] = event
			h.Join.CompleteWhen = `join.results[0].startsWith("1")`
			h.Join.Remaining = runtimecontracts.JoinRemainingIgnore
		}, wantError: "no matching overload"},
		{name: "outcome payload forbidden", mutate: func(h *runtimecontracts.SystemNodeEventHandler, _ *runtimecontracts.WorkflowContractBundle) {
			h.Join.OnComplete.Emit.Fields["results"] = runtimecontracts.CELExpression("payload.result")
		}, wantError: "may not reference payload.*"},
		{name: "reentry requires window", mutate: func(_ *runtimecontracts.SystemNodeEventHandler, b *runtimecontracts.WorkflowContractBundle) {
			node := b.Nodes["join-node"]
			node.EventHandlers["retry.requested"] = runtimecontracts.SystemNodeEventHandler{AdvancesTo: "awaiting"}
			b.Nodes["join-node"] = node
		}, wantError: "stage is re-entrant"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := joinValidationBundle()
			h := bundle.Nodes["join-node"].EventHandlers["item.completed"]
			if tc.mutate != nil {
				tc.mutate(&h, bundle)
				bundle.Nodes["join-node"].EventHandlers["item.completed"] = h
			}
			rebuildJoinValidationTopology(bundle)
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if tc.wantError == "" {
				if reportContains(report.HardInvalidities(), joinValidationCheckID, "") {
					t.Fatalf("unexpected join invalidity: %#v", report.HardInvalidities())
				}
				if reportContains(report.LintEvidence(), "entity_reader_coverage", "expected") {
					t.Fatalf("join.members.from must count as canonical entity reader coverage: %#v", report.LintEvidence())
				}
				return
			}
			if !reportContains(report.HardInvalidities(), joinValidationCheckID, tc.wantError) {
				t.Fatalf("expected %q, got %#v", tc.wantError, report.HardInvalidities())
			}
		})
	}
}

func TestRun_JoinValidationRequiresExactDeclarationFlow(t *testing.T) {
	t.Run("same-leaf sibling cannot replace root", func(t *testing.T) {
		bundle := joinValidationBundle()
		bundle.Semantics.Joins[0].FlowID = "orders"
		report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
		if !reportContains(report.HardInvalidities(), joinValidationCheckID, "no effective WorkflowJoinPlan") {
			t.Fatalf("hard invalidities = %#v", report.HardInvalidities())
		}
	})

	t.Run("root remains exact beside same-leaf sibling", func(t *testing.T) {
		bundle := joinValidationBundle()
		sibling := bundle.Semantics.Joins[0]
		sibling.FlowID = "orders"
		bundle.Semantics.Joins = append(bundle.Semantics.Joins, sibling)
		report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
		if reportContains(report.HardInvalidities(), joinValidationCheckID, "no effective WorkflowJoinPlan") {
			t.Fatalf("root declaration was shadowed by sibling: %#v", report.HardInvalidities())
		}
	})
}

func TestJoinMembersContributeCanonicalEntityReaderCoverage(t *testing.T) {
	bundle := joinValidationBundle()
	source := semanticview.Wrap(bundle)
	handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
	var memberReference *expressionReference
	for _, ref := range handlerExecutableReaderExpressionsForSource(source, "", "join-node", "item.completed", handler) {
		if ref.Kind == "join.members.from" {
			candidate := ref
			memberReference = &candidate
			break
		}
	}
	if memberReference == nil {
		t.Fatal("join.members.from missing from canonical handler expression inventory")
	}
	if _, ownerFlowID, err := wave1ResolveEntityPathWithOwner(source, "", memberReference.Expression); err != nil || ownerFlowID != "" {
		t.Fatalf("join.members.from direct resolution = owner:%q err:%v", ownerFlowID, err)
	}
	resolved := wave1ResolvedExpressionRefs(source, "", "join-node", "item.completed", *memberReference)
	if len(resolved) != 1 || resolved[0].OwnerFlowID != "" || resolved[0].Field != "expected" {
		t.Fatalf("join.members.from resolved refs = %#v", resolved)
	}
	readers := wave1EntityReaderCoverageByFlow(source)
	if _, ok := readers[""]["expected"]; !ok {
		t.Fatalf("join.members.from reader coverage = %#v", readers)
	}
}

func TestRun_JoinValidationPreservesDuplicateScopedNodeIDs(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.CopyDuplicateScopedSingletonDemand(t)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "a", "schema.yaml"), `
name: a
mode: singleton
initial_state: active
states: [active, done, failed]
pins:
  inputs:
    events:
      - {name: item_received, event: item.received, source: harness}
  outputs:
    events: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "a", "entities.yaml"), `
state:
  expected:
    type: '[text]'
    initial: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "a", "events.yaml"), `
item.received:
  member_id: text
  result: text
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "a", "nodes.yaml"), `
shared-node:
  id: shared-node
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received:
      join:
        stage: missing
        members: {from: entity.expected, by: payload.member_id}
        output: payload.result
        on_complete: {advances_to: done}
        timeout: {after: 1h, advances_to: failed}
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	if _, ok := bundle.Nodes["shared-node"]; ok {
		t.Fatal("duplicate local node ID unexpectedly survived as a flattened global alias")
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !findingContainsAll(report.HardInvalidities(), joinValidationCheckID, "flow a", "shared-node", `unknown stage "missing"`) {
		t.Fatalf("join findings = %#v, want exact invalid scoped join rejection", report.HardInvalidities())
	}
}

func joinValidationBundle() *runtimecontracts.WorkflowContractBundle {
	spec := runtimecontracts.JoinSpec{
		ID: "awaiting", Stage: "awaiting",
		Members: runtimecontracts.JoinMembersSpec{From: "entity.expected", By: "payload.member_id"},
		Output:  "payload.result", OnCompleteFound: true,
		OnComplete:   runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready", Emit: runtimecontracts.EmitSpec{Event: "join.completed", Fields: map[string]runtimecontracts.ExpressionValue{"results": runtimecontracts.CELExpression("join.results")}}},
		TimeoutFound: true,
		Timeout:      runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention", Emit: runtimecontracts.EmitSpec{Event: "join.timed_out", Fields: map[string]runtimecontracts.ExpressionValue{"missing": runtimecontracts.CELExpression("join.missing")}}}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema:   &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "awaiting", Initial: true}, {ID: "ready"}, {ID: "attention", Terminal: true}}}},
		RootEntities: runtimecontracts.EntityContractsDocument{"Order": {Fields: map[string]runtimecontracts.EntityFieldDecl{"expected": {Type: "[text]", Initial: []any{}}}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.completed": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"member_id": {Type: "text"}, "result": {Type: "jsonb"}}}},
			"join.completed": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"results": {Type: "list<jsonb>"}}}},
			"join.timed_out": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"missing": {Type: "list<text>"}}}},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"join-node": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"item.completed": {Join: &spec}}}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "awaiting", Stages: []runtimecontracts.WorkflowStageContract{{ID: "awaiting"}, {ID: "ready"}, {ID: "attention"}}, TerminalStages: []string{"attention"},
			Transitions: []runtimecontracts.WorkflowTransitionContract{{ID: "complete", From: []string{"awaiting"}, To: "ready"}, {ID: "timeout", From: []string{"awaiting"}, To: "attention"}},
		},
	}
	rebuildJoinValidationTopology(bundle)
	return bundle
}

func rebuildJoinValidationTopology(bundle *runtimecontracts.WorkflowContractBundle) {
	transitions := []runtimecontracts.HandlerTransitionSemantic{}
	joins := []runtimecontracts.WorkflowJoinPlan{}
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			transitions = append(transitions, runtimecontracts.HandlerTransitionSemantic{
				NodeID:       nodeID,
				EventType:    eventType,
				CreateEntity: handler.CreateEntity,
				AdvancesTo:   handler.AdvancesTo,
				OnComplete:   handler.OnComplete,
				Rules:        handler.Rules,
				Accumulate:   handler.Accumulate,
				Join:         handler.Join,
				Loop:         handler.Loop,
			})
			if handler.Join != nil {
				resultType, _ := runtimecontracts.ResolveEventFieldType(bundle, "", eventType, "result")
				joins = append(joins, runtimecontracts.WorkflowJoinPlan{NodeID: nodeID, HandlerEvent: eventType, Spec: *handler.Join, ResultType: resultType})
			}
		}
	}
	bundle.Semantics.HandlerTransitions = transitions
	bundle.Semantics.Joins = joins
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{
		"": runtimecontracts.BuildWorkflowStageTopology(
			"",
			"awaiting",
			[]string{"awaiting", "ready", "attention"},
			[]string{"attention"},
			transitions,
			nil,
			nil,
		),
	}
}
