package main

import (
	"strings"
	"testing"
)

const nativeCompleteDispatchDecisionShape = "func (f completeEventDispatchFixture) insertDecisionObligationFor(t *testing.T, event events.Event) decisioncard.Card {\n\tt.Helper()\n\tentityID := uuid.NewString()\n\tanchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{\n\t\tRoute: runtimeflowidentity.RouteForInstancePath(\"source-flow/one\"), FlowID: \"source-flow\",\n\t\tEntityID: entityID, Stage: \"awaiting_review\", StageActivationID: uuid.NewString(),\n\t\tSource: eventtest.ConcreteTemplateRoutingSource(\"source-flow\", \"source-flow/one\", entityID),\n\t})\n\tif err != nil {\n\t\tt.Fatalf(\"admit decision route anchor: %v\", err)\n\t}\n\tsnapshot, err := decisioncard.FreezeSnapshot(\"dispatch_review\", \"\", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{\n\t\t\"approve\": {Verdict: \"approve\", AdvancesTo: \"approved\"},\n\t})\n\tif err != nil {\n\t\tt.Fatalf(\"freeze decision route contract: %v\", err)\n\t}\n\tcard, err := decisioncard.New(decisioncard.Card{\n\t\tCardID: uuid.NewString(), RunID: event.RunID(), Anchor: anchor, ExecutionMode: executionmode.Mock,\n\t\tSnapshot: snapshot, BundleHash: authorActivityTestBundleHash, CreatedAt: event.CreatedAt(),\n\t})\n\tif err != nil {\n\t\tt.Fatalf(\"admit decision route card: %v\", err)\n\t}\n\tif err := f.store.CreateDecisionCard(f.ctx, card); err != nil {\n\t\tt.Fatalf(\"create decision route card: %v\", err)\n\t}\n\toutcome, err := storetest.DecisionCardDomain(f.store).ApplyDecisionForTest(f.ctx, decisioncard.DecideRequest{\n\t\tCardID: card.CardID, Verdict: \"approve\", PrincipalID: \"test\", ObservedContentHash: card.CardContentHash,\n\t\tDecisionEventID: event.ID(), Now: event.CreatedAt(),\n\t})\n\tif err != nil {\n\t\tt.Fatalf(\"commit decision route obligation: %v\", err)\n\t}\n\treturn outcome.Card\n}"

func completeDispatchDecisionPreserved(source string) bool {
	want, err := canonicalFunction(nativeCompleteDispatchDecisionShape)
	got, actualErr := canonicalFunction(source)
	return err == nil && actualErr == nil && want == got
}

func TestNativeCompleteDispatchDecisionUsesExistingOwnersAndExactFixtureIdentity(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-complete-dispatch-construction")
	if row.Function != "insertDecisionObligationFor" || !completeDispatchDecisionPreserved(row.After) {
		t.Fatal("decision fixture lost its exact existing owner recipe")
	}
	for _, field := range []string{"'decided'", "'approve'", "'test'", "event.RunID()", "event.ID()", "event.CreatedAt()"} {
		if !strings.Contains(row.Before, field) {
			t.Fatalf("old decision setup lost reviewed posture: %s", field)
		}
	}
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	if !completeDispatchDecisionPreserved(actual) {
		t.Fatal("actual shared decision fixture diverged")
	}
	for _, pair := range [][2]string{
		{"storetest.DecisionCardDomain(f.store)", "storetest.DecisionCardDomain(otherStore)"},
		{"f.store.CreateDecisionCard", "otherStore.CreateDecisionCard"},
		{"DecisionEventID: event.ID()", "DecisionEventID: otherEvent.ID()"},
		{"RunID: event.RunID()", "RunID: otherEvent.RunID()"},
		{"Now: event.CreatedAt()", "Now: time.Now()"},
		{"Verdict: \"approve\", PrincipalID", "Verdict: \"reject\", PrincipalID"},
		{"ObservedContentHash: card.CardContentHash", "ObservedContentHash: \"card-hash\""},
		{"if err != nil {", "if false {"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		if mutant == actual || completeDispatchDecisionPreserved(mutant) {
			t.Fatalf("weakened decision fixture admitted: %v", pair)
		}
	}
}
