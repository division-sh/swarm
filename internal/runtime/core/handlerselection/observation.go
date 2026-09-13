package handlerselection

import "fmt"

type observationKind uint8

const (
	observationNotReached observationKind = iota + 1
	observationResolved
)

// Observation is attempt-local evidence, not an immutable delivery fact.
// In particular, a retry may observe a different rule against newer state.
type Observation struct {
	kind observationKind
	fact HandlerRuleSelectionFact
}

func NotReached() Observation { return Observation{kind: observationNotReached} }

func Resolved(fact HandlerRuleSelectionFact) Observation {
	return Observation{kind: observationResolved, fact: fact}
}

func (o Observation) Validate() error {
	switch o.kind {
	case observationNotReached:
		if !o.fact.Empty() {
			return fmt.Errorf("unreached rule observation carries a fact")
		}
		return nil
	case observationResolved:
		return o.fact.Validate()
	default:
		return fmt.Errorf("rule observation requires explicit readiness")
	}
}

func (o Observation) Reached() bool { return o.kind == observationResolved }

func (o Observation) ResolvedFact() (HandlerRuleSelectionFact, error) {
	if err := o.Validate(); err != nil {
		return HandlerRuleSelectionFact{}, err
	}
	if !o.Reached() {
		return HandlerRuleSelectionFact{}, fmt.Errorf("rule observation has not been resolved")
	}
	return o.fact, nil
}

func (o Observation) Equal(other Observation) bool { return o == other }
