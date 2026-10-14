package contracts

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestHandlerDeclarativeEmitSitesOwnsGuardEscalation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		guard *GuardSpec
		want  string
	}{
		{name: "absent"},
		{name: "reject", guard: &GuardSpec{Check: "payload.score > 0"}},
		{name: "discard", guard: &GuardSpec{Check: "payload.score > 0", OnFail: "discard"}},
		{name: "kill", guard: &GuardSpec{Check: "payload.score > 0", OnFail: "kill"}},
		{name: "invalid", guard: &GuardSpec{Check: "payload.score > 0", OnFail: "escalate:"}},
		{name: "scalar", guard: &GuardSpec{ID: "score", Check: "payload.score > 0", OnFail: "escalate:work.escalated"}, want: "work.escalated"},
		{name: "object", guard: &GuardSpec{ID: "score", Check: "payload.score > 0", OnFailSpec: GuardFailureSpec{
			Action: GuardFailureActionEscalate,
			Escalation: EmitSpec{Event: "work.escalated", Fields: map[string]ExpressionValue{
				"score": CELExpression("payload.score"),
			}},
		}}, want: "work.escalated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := SystemNodeEventHandler{Guard: tc.guard}
			sites := HandlerDeclarativeEmitSites(handler, nil)
			if tc.want == "" {
				if len(sites) != 0 || len(HandlerEmitEvents(handler, nil)) != 0 {
					t.Fatalf("non-escalating guard manufactured a producer: %#v", sites)
				}
				return
			}
			if len(sites) != 1 || sites[0].Source != "handler.guard.on_fail.escalate" || sites[0].SiteKey != "handler.guard.on_fail.escalate" || sites[0].RuleIndex != -1 || sites[0].RuleID != "score" || sites[0].RuleRef.Valid() || sites[0].EventType() != tc.want {
				t.Fatalf("canonical guard emit site = %#v", sites)
			}
			if got := HandlerEmitEvents(handler, nil); !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("handler producer set = %v, want [%s]", got, tc.want)
			}
			if tc.name == "object" {
				if got := sites[0].Spec.Fields["score"]; !reflect.DeepEqual(got, CELExpression("payload.score")) {
					t.Fatalf("escalation field changed: %#v", got)
				}
				sites[0].Spec.Fields["score"] = LiteralExpression("mutated")
				if got := handler.Guard.OnFailSpec.Escalation.Fields["score"]; !reflect.DeepEqual(got, CELExpression("payload.score")) {
					t.Fatalf("producer projection exposed mutable guard fields: %#v", got)
				}
			}
		})
	}
}

func TestEffectiveSystemNodeProducesOwnsCompleteDerivedSet(t *testing.T) {
	node := SystemNodeContract{EventHandlers: map[string]SystemNodeEventHandler{
		"work.requested": {
			Guard: &GuardSpec{Check: "payload.score > 0", OnFail: "escalate:work.escalated"},
			Emit:  EmitSpec{Event: "work.completed"},
		},
		"audit.requested": {Emit: EmitSpec{Event: "work.completed"}},
		"dispatch.requested": {Activity: ActivitySpec{
			ID: "dispatch", Tool: "proof.write",
			Approval: &ActivityApprovalSpec{Decision: "review"},
		}},
		"dispatch.revision_requested": {},
		"dispatch.rejected":           {},
	}}
	bundle := &WorkflowContractBundle{
		Nodes: map[string]SystemNodeContract{"worker": node},
		Tools: map[string]ToolSchemaEntry{"proof.write": MustToolSchemaEntry(
			WithToolHandler(ToolHandlerHTTP), WithToolEffect(ActivityEffectClassNonIdempotentWrite),
			WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject)),
			WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://example.test"}),
		)},
	}
	compileRootContractTestFixture(bundle)
	owner := identitytest.RootNode(t, "worker")
	want := []string{"dispatch.failed", "dispatch.rejected", "dispatch.revision_requested", "dispatch.succeeded", "work.completed", "work.escalated"}
	if got := EffectiveSystemNodeProduces(bundle, owner, node); !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical node producer set = %v, want %v", got, want)
	}
	if got := bundle.Semantics.EffectiveNodes[owner.Key()].Produces; !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled producer set = %v, want %v", got, want)
	}
}
