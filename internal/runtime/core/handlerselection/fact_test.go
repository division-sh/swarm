package handlerselection

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
)

func TestHandlerRuleSelectionFactClosedMatrix(t *testing.T) {
	ref, err := contractelementidentity.ParseContractElementRef("flows/scout", "d8fe3c3e-55c6-4f27-8eb4-dcd76a07982c")
	if err != nil {
		t.Fatal(err)
	}
	for _, context := range []Context{ContextRules, ContextOnComplete, ContextJoinComplete, ContextJoinTimeout} {
		fact, err := Selected(context, ref, "display only")
		if err != nil || fact.Context() != context || fact.Disposition() != DispositionSelected || !fact.Ref().Equal(ref) {
			t.Fatalf("Selected(%s) = %#v, %v", context, fact, err)
		}
		hydrated, err := Hydrate(string(context), string(DispositionSelected), "flows/scout", ref.ElementID().String(), "display only")
		if err != nil || !hydrated.Equal(fact) {
			t.Fatalf("Hydrate(%s) = %#v, %v; want %#v", context, hydrated, err, fact)
		}
	}
	for _, context := range []Context{ContextRules, ContextOnComplete} {
		fact, err := NoMatch(context)
		if err != nil || fact.Disposition() != DispositionNoMatch || fact.Ref().Valid() {
			t.Fatalf("NoMatch(%s) = %#v, %v", context, fact, err)
		}
	}
	if fact := NotApplicable(); fact.Validate() != nil || fact.Context() != ContextNone || fact.Disposition() != DispositionNotApplicable {
		t.Fatalf("NotApplicable() = %#v, validation=%v", fact, fact.Validate())
	}
}

func TestHandlerRuleSelectionFactRejectsContradictoryWireFacts(t *testing.T) {
	const element = "d8fe3c3e-55c6-4f27-8eb4-dcd76a07982c"
	for name, wire := range map[string][5]string{
		"selected without ref":      {string(ContextRules), string(DispositionSelected), "", "", "label"},
		"no match with ref":         {string(ContextRules), string(DispositionNoMatch), ".", element, ""},
		"no match join":             {string(ContextJoinComplete), string(DispositionNoMatch), "", "", ""},
		"not applicable with label": {string(ContextNone), string(DispositionNotApplicable), "", "", "label"},
		"selected none":             {string(ContextNone), string(DispositionSelected), ".", element, ""},
		"unknown context":           {"handler_branch", string(DispositionSelected), ".", element, ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Hydrate(wire[0], wire[1], wire[2], wire[3], wire[4]); err == nil {
				t.Fatal("Hydrate succeeded")
			}
		})
	}
}
