package handlerselection

import (
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestHandlerRuleSelectionFactClosedMatrix(t *testing.T) {
	ref, err := runtimeidentity.AdmitDeclarationIdentity("scout", "handler_rule", `nodes["scout"].handlers["task.requested"].rules[0]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, context := range []Context{ContextRules, ContextOnComplete, ContextJoinComplete, ContextJoinTimeout} {
		fact, err := Selected(context, ref, "display only")
		if err != nil || fact.Context() != context || fact.Disposition() != DispositionSelected || !fact.Ref().Equal(ref) {
			t.Fatalf("Selected(%s) = %#v, %v", context, fact, err)
		}
		hydrated, err := Hydrate(string(context), string(DispositionSelected), ref.Flow().String(), ref.Family(), ref.SemanticPath(), "display only")
		if err != nil || !hydrated.Equal(fact) {
			t.Fatalf("Hydrate(%s) = %#v, %v; want %#v", context, hydrated, err, fact)
		}
	}
	for _, context := range []Context{ContextRules, ContextOnComplete} {
		fact, err := NoMatch(context)
		if err != nil || fact.Disposition() != DispositionNoMatch || fact.Ref().Valid() {
			t.Fatalf("NoMatch(%s) = %#v, %v", context, fact, err)
		}
		failed, err := EvaluationFailed(context, ref, "attempted")
		if err != nil || failed.Disposition() != DispositionEvaluationFailed || !failed.Ref().Equal(ref) {
			t.Fatalf("EvaluationFailed(%s) = %#v, %v", context, failed, err)
		}
		hydrated, err := Hydrate(string(context), string(DispositionEvaluationFailed), ref.Flow().String(), ref.Family(), ref.SemanticPath(), "attempted")
		if err != nil || !hydrated.Equal(failed) {
			t.Fatalf("Hydrate evaluation failure (%s) = %#v, %v; want %#v", context, hydrated, err, failed)
		}
	}
	if fact := NotApplicable(); fact.Validate() != nil || fact.Context() != ContextNone || fact.Disposition() != DispositionNotApplicable {
		t.Fatalf("NotApplicable() = %#v, validation=%v", fact, fact.Validate())
	}
}

func TestHandlerRuleSelectionFactRejectsContradictoryWireFacts(t *testing.T) {
	const semanticPath = `nodes["scout"].handlers["task.requested"].rules[0]`
	for name, wire := range map[string][6]string{
		"selected without ref":           {string(ContextRules), string(DispositionSelected), "", "", "", "label"},
		"no match with ref":              {string(ContextRules), string(DispositionNoMatch), ".", "handler_rule", semanticPath, ""},
		"no match join":                  {string(ContextJoinComplete), string(DispositionNoMatch), "", "", "", ""},
		"not applicable with label":      {string(ContextNone), string(DispositionNotApplicable), "", "", "", "label"},
		"selected none":                  {string(ContextNone), string(DispositionSelected), ".", "handler_rule", semanticPath, ""},
		"evaluation failure without ref": {string(ContextRules), string(DispositionEvaluationFailed), "", "", "", "label"},
		"evaluation failure join":        {string(ContextJoinComplete), string(DispositionEvaluationFailed), ".", "handler_rule", semanticPath, "label"},
		"unknown context":                {"handler_branch", string(DispositionSelected), ".", "handler_rule", semanticPath, ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Hydrate(wire[0], wire[1], wire[2], wire[3], wire[4], wire[5]); err == nil {
				t.Fatal("Hydrate succeeded")
			}
		})
	}
}
