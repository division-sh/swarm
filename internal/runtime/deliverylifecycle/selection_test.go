package deliverylifecycle

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func selectionFacts(t *testing.T) []handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	ref, err := runtimeidentity.AdmitDeclarationIdentity("child", "handler_rule", `nodes["worker"].handlers["input"].rules[0]`)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := handlerselection.Selected(handlerselection.ContextRules, ref, "choice")
	if err != nil {
		t.Fatal(err)
	}
	failed, err := handlerselection.EvaluationFailed(handlerselection.ContextRules, ref, "choice")
	if err != nil {
		t.Fatal(err)
	}
	unmatched, err := handlerselection.NoMatch(handlerselection.ContextRules)
	if err != nil {
		t.Fatal(err)
	}
	return []handlerselection.HandlerRuleSelectionFact{handlerselection.NotApplicable(), selected, unmatched, failed}
}

func TestFinalSelectionEffectiveOutcomeMatrix(t *testing.T) {
	for _, status := range []Status{StatusPending, StatusInProgress, StatusFailed, StatusDelivered, StatusDeadLetter} {
		observations := []handlerselection.Observation{handlerselection.NotReached(), {}}
		for _, fact := range selectionFacts(t) {
			observations = append(observations, handlerselection.Resolved(fact))
		}
		for _, observation := range observations {
			fact, factErr := observation.ResolvedFact()
			wantErr := observation.Validate() != nil || (status == StatusDelivered && (factErr != nil || fact.Disposition() == handlerselection.DispositionEvaluationFailed))
			got, err := FinalSelection(status, observation)
			if (err != nil) != wantErr {
				t.Fatalf("%s/%#v error=%v wantError=%v", status, observation, err, wantErr)
			}
			if err != nil {
				continue
			}
			if got.Present() != (status == StatusDelivered || status == StatusDeadLetter) {
				t.Fatalf("wrong presence: %s/%#v", status, got)
			}
			if got.Present() {
				actual, err := got.Fact()
				if !observation.Reached() {
					fact = handlerselection.NotApplicable()
				}
				if err != nil || !actual.Equal(fact) {
					t.Fatalf("final selection changed observation: %#v/%v", actual, err)
				}
			}
		}
	}
}

func TestSelectionPresenceStatusAndWireMatrix(t *testing.T) {
	for _, status := range []Status{StatusPending, StatusInProgress, StatusFailed, StatusDelivered, StatusDeadLetter} {
		for _, present := range []bool{false, true} {
			presence := AbsentSelection()
			if present {
				presence = PresentSelection(handlerselection.NotApplicable())
			}
			wantErr := present != (status == StatusDelivered || status == StatusDeadLetter)
			if err := ValidateSelectionPresence(status, presence); (err != nil) != wantErr {
				t.Fatalf("%s/present=%v: %v", status, present, err)
			}
		}
	}
	presences := []SelectionPresence{AbsentSelection()}
	for _, fact := range selectionFacts(t) {
		presences = append(presences, PresentSelection(fact))
	}
	for _, presence := range presences {
		raw, err := json.Marshal(presence)
		if err != nil {
			t.Fatal(err)
		}
		var restored SelectionPresence
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		if restored != presence {
			t.Fatalf("wire changed exact final evidence: %s", raw)
		}
	}
	for _, raw := range []string{
		`null`, `{}`, `{"kind":"unknown"}`, `{"kind":"present"}`,
		`{"kind":"present","fact":null}`, `{"kind":"present","fact":{}}`,
		`{"kind":"absent","fact":null}`, `{"kind":"absent","fact":{}}`,
		`{"kind":"absent","compat":true}`, `{"kind":"absent"} {}`,
		`{"kind":"present","kind":"absent"}`, `{"kind":"absent","\u006bind":"absent"}`,
		`{"kind":"present","fact":{"context":"none","disposition":"selected","disposition":"not_applicable"}}`,
		`{"kind":"present","fact":{"context":"none","disposition":"not_applicable","unexpected":true}}`,
	} {
		var presence SelectionPresence
		if err := presence.UnmarshalJSON([]byte(raw)); err == nil {
			t.Fatalf("accepted malformed presence: %s", raw)
		}
	}
	for _, presence := range []SelectionPresence{{}, PresentSelection(handlerselection.HandlerRuleSelectionFact{}), {kind: selectionAbsent, fact: handlerselection.NotApplicable()}} {
		if _, err := json.Marshal(presence); err == nil {
			t.Fatalf("serialized malformed presence %#v", presence)
		}
		if err := ValidateSelectionPresence(StatusPending, presence); err == nil {
			t.Fatalf("admitted malformed presence %#v", presence)
		}
	}
}
