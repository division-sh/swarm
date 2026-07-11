package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestRun_RejectsStagedBarrierAccumulate(t *testing.T) {
	bundle := joinValidationBundle()
	h := bundle.Nodes["join-node"].EventHandlers["item.completed"]
	h.Join = nil
	h.Accumulate = &runtimecontracts.AccumulateSpec{ExpectedFrom: "entity.expected", Completion: runtimecontracts.ParseAccumulateCompletion("all")}
	bundle.Nodes["join-node"].EventHandlers["item.completed"] = h
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.HardInvalidities(), joinValidationCheckID, "must use handler.join") {
		t.Fatalf("expected staged accumulate retirement, got %#v", report.HardInvalidities())
	}
}

func TestJoinMembersContributeCanonicalEntityReaderCoverage(t *testing.T) {
	bundle := joinValidationBundle()
	source := semanticview.Wrap(bundle)
	handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
	var memberReference *expressionReference
	for _, ref := range handlerEntityExpressions(handler) {
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

func joinValidationBundle() *runtimecontracts.WorkflowContractBundle {
	spec := runtimecontracts.JoinSpec{
		ID: "awaiting", Stage: "awaiting",
		Members: runtimecontracts.JoinMembersSpec{From: "entity.expected", By: "payload.member_id"},
		Output:  "payload.result", OnCompleteFound: true,
		OnComplete:   runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready", Emit: runtimecontracts.EmitSpec{Event: "join.completed", Fields: map[string]runtimecontracts.ExpressionValue{"results": runtimecontracts.CELExpression("join.results")}}},
		TimeoutFound: true,
		Timeout:      runtimecontracts.JoinTimeoutSpec{After: "1h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention", Emit: runtimecontracts.EmitSpec{Event: "join.timed_out", Fields: map[string]runtimecontracts.ExpressionValue{"missing": runtimecontracts.CELExpression("join.missing")}}}},
	}
	return &runtimecontracts.WorkflowContractBundle{
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
}
