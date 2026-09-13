package deliverylifecycle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
)

type selectionWire struct {
	Kind string             `json:"kind"`
	Fact *selectionFactWire `json:"fact,omitempty"`
}

type selectionFactWire struct {
	Context      string `json:"context"`
	Disposition  string `json:"disposition"`
	FlowPath     string `json:"flow_path"`
	Family       string `json:"declaration_family"`
	SemanticPath string `json:"semantic_path"`
	Label        string `json:"display_label"`
}

type selectionPresenceKind uint8

const (
	selectionAbsent selectionPresenceKind = iota + 1
	selectionPresent
)

// SelectionPresence is final delivery-resolution evidence, never an attempt log.
type SelectionPresence struct {
	kind selectionPresenceKind
	fact handlerselection.HandlerRuleSelectionFact
}

func AbsentSelection() SelectionPresence { return SelectionPresence{kind: selectionAbsent} }

func PresentSelection(fact handlerselection.HandlerRuleSelectionFact) SelectionPresence {
	return SelectionPresence{kind: selectionPresent, fact: fact}
}

func (p SelectionPresence) Present() bool { return p.kind == selectionPresent }

func (p SelectionPresence) Fact() (handlerselection.HandlerRuleSelectionFact, error) {
	if !p.Present() {
		return handlerselection.HandlerRuleSelectionFact{}, fmt.Errorf("final delivery selection is absent")
	}
	return p.fact, p.fact.Validate()
}

func (p SelectionPresence) MarshalJSON() ([]byte, error) {
	switch p.kind {
	case selectionAbsent:
		if !p.fact.Empty() {
			return nil, fmt.Errorf("absent selection carries a fact")
		}
		return json.Marshal(selectionWire{Kind: "absent"})
	case selectionPresent:
		if err := p.fact.Validate(); err != nil {
			return nil, err
		}
		w := selectionFactWire{Context: string(p.fact.Context()), Disposition: string(p.fact.Disposition()), Label: p.fact.DisplayLabel()}
		if ref := p.fact.Ref(); ref.Valid() {
			w.FlowPath, w.Family, w.SemanticPath = ref.Flow().String(), ref.Family(), ref.SemanticPath()
		}
		return json.Marshal(selectionWire{Kind: "present", Fact: &w})
	default:
		return nil, fmt.Errorf("delivery selection presence is missing or invalid")
	}
}

func (p *SelectionPresence) UnmarshalJSON(raw []byte) error {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return err
	}
	var w struct {
		Kind string          `json:"kind"`
		Fact json.RawMessage `json:"fact"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&w); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("selection presence contains trailing JSON")
	}
	switch w.Kind {
	case "absent":
		if len(w.Fact) != 0 {
			return fmt.Errorf("absent selection carries a fact")
		}
		*p = AbsentSelection()
	case "present":
		if len(w.Fact) == 0 || bytes.Equal(bytes.TrimSpace(w.Fact), []byte("null")) {
			return fmt.Errorf("present selection lacks its fact")
		}
		var f selectionFactWire
		d := json.NewDecoder(bytes.NewReader(w.Fact))
		d.DisallowUnknownFields()
		if err := d.Decode(&f); err != nil {
			return err
		}
		fact, err := handlerselection.Hydrate(f.Context, f.Disposition, f.FlowPath, f.Family, f.SemanticPath, f.Label)
		if err != nil {
			return err
		}
		*p = PresentSelection(fact)
	default:
		return fmt.Errorf("delivery selection presence is missing or invalid")
	}
	return nil
}

// ValidateSelectionPresence is shared by live, transactional and as-of readers.
func ValidateSelectionPresence(status Status, p SelectionPresence) error {
	switch p.kind {
	case selectionAbsent:
		if !p.fact.Empty() {
			return fmt.Errorf("absent selection carries a fact")
		}
	case selectionPresent:
		if err := p.fact.Validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("delivery selection presence is missing or invalid")
	}
	switch status {
	case StatusPending, StatusInProgress, StatusFailed:
		if p.Present() {
			return fmt.Errorf("nonterminal delivery %s carries final selection", status)
		}
	case StatusDelivered, StatusDeadLetter:
		if !p.Present() {
			return fmt.Errorf("terminal delivery %s lacks final selection", status)
		}
		if status == StatusDelivered && p.fact.Disposition() == handlerselection.DispositionEvaluationFailed {
			return fmt.Errorf("delivered selection cannot be evaluation_failed")
		}
	default:
		return fmt.Errorf("invalid delivery status %q for selection", status)
	}
	return nil
}

// FinalSelection consumes the effective store outcome, including retry exhaustion.
// Unsuccessful attempts' exact observations intentionally have no durable ledger.
func FinalSelection(status Status, observed handlerselection.Observation) (SelectionPresence, error) {
	if err := observed.Validate(); err != nil {
		return SelectionPresence{}, err
	}
	p := AbsentSelection()
	if status == StatusDelivered || status == StatusDeadLetter {
		fact, err := observed.ResolvedFact()
		if !observed.Reached() && status == StatusDeadLetter {
			fact, err = handlerselection.NotApplicable(), nil
		}
		if err != nil {
			return SelectionPresence{}, err
		}
		p = PresentSelection(fact)
	}
	return p, ValidateSelectionPresence(status, p)
}
