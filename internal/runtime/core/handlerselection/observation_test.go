package handlerselection

import "testing"

func TestObservationDoesNotDefaultUnreachedToNotApplicable(t *testing.T) {
	unreached := NotReached()
	if err := unreached.Validate(); err != nil || unreached.Reached() {
		t.Fatalf("unreached observation: %#v, %v", unreached, err)
	}
	if _, err := unreached.ResolvedFact(); err == nil {
		t.Fatal("unreached observation invented a selection")
	}
	resolved := Resolved(NotApplicable())
	if err := resolved.Validate(); err != nil || !resolved.Reached() || resolved.Equal(unreached) {
		t.Fatalf("resolved not-applicable conflated with unreached: %v", err)
	}
	fact, err := resolved.ResolvedFact()
	if err != nil || !fact.Equal(NotApplicable()) {
		t.Fatalf("fact = %#v, %v", fact, err)
	}
}

func TestObservationRejectsZeroAndContradictions(t *testing.T) {
	for _, observation := range []Observation{
		{}, Resolved(HandlerRuleSelectionFact{}),
		{kind: observationNotReached, fact: NotApplicable()},
		{kind: observationKind(99), fact: NotApplicable()},
	} {
		if err := observation.Validate(); err == nil {
			t.Fatalf("accepted invalid observation: %#v", observation)
		}
		if _, err := observation.ResolvedFact(); err == nil {
			t.Fatalf("projected invalid observation: %#v", observation)
		}
	}
}
