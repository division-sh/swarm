// Package handlerselection owns the closed rule-evaluation fact recorded for
// each executable delivery. Delivery, event, route, and subscriber identity
// remain owned by the delivery lifecycle row.
package handlerselection

import (
	"fmt"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

type Context string

const (
	ContextNone         Context = "none"
	ContextRules        Context = "handler_rules"
	ContextOnComplete   Context = "handler_on_complete"
	ContextJoinComplete Context = "join_on_complete"
	ContextJoinTimeout  Context = "join_timeout"
)

func ParseContext(raw string) (Context, error) {
	context := Context(raw)
	switch context {
	case ContextNone, ContextRules, ContextOnComplete, ContextJoinComplete, ContextJoinTimeout:
		return context, nil
	default:
		return "", fmt.Errorf("handler rule selection context %q is invalid", raw)
	}
}

type Disposition string

const (
	DispositionSelected         Disposition = "selected"
	DispositionNoMatch          Disposition = "no_match"
	DispositionEvaluationFailed Disposition = "evaluation_failed"
	DispositionNotApplicable    Disposition = "not_applicable"
)

func ParseDisposition(raw string) (Disposition, error) {
	disposition := Disposition(raw)
	switch disposition {
	case DispositionSelected, DispositionNoMatch, DispositionEvaluationFailed, DispositionNotApplicable:
		return disposition, nil
	default:
		return "", fmt.Errorf("handler rule selection disposition %q is invalid", raw)
	}
}

type HandlerRuleSelectionFact struct {
	context      Context
	disposition  Disposition
	ref          runtimeidentity.DeclarationIdentity
	displayLabel string
}

func Selected(context Context, ref runtimeidentity.DeclarationIdentity, displayLabel string) (HandlerRuleSelectionFact, error) {
	fact := HandlerRuleSelectionFact{context: context, disposition: DispositionSelected, ref: ref, displayLabel: strings.TrimSpace(displayLabel)}
	return fact, fact.Validate()
}

func NoMatch(context Context) (HandlerRuleSelectionFact, error) {
	fact := HandlerRuleSelectionFact{context: context, disposition: DispositionNoMatch}
	return fact, fact.Validate()
}

func EvaluationFailed(context Context, ref runtimeidentity.DeclarationIdentity, displayLabel string) (HandlerRuleSelectionFact, error) {
	fact := HandlerRuleSelectionFact{context: context, disposition: DispositionEvaluationFailed, ref: ref, displayLabel: strings.TrimSpace(displayLabel)}
	return fact, fact.Validate()
}

func NotApplicable() HandlerRuleSelectionFact {
	return HandlerRuleSelectionFact{context: ContextNone, disposition: DispositionNotApplicable}
}

func Hydrate(context, disposition, flowPath, family, semanticPath, displayLabel string) (HandlerRuleSelectionFact, error) {
	parsedContext, err := ParseContext(context)
	if err != nil {
		return HandlerRuleSelectionFact{}, err
	}
	parsedDisposition, err := ParseDisposition(disposition)
	if err != nil {
		return HandlerRuleSelectionFact{}, err
	}
	fact := HandlerRuleSelectionFact{context: parsedContext, disposition: parsedDisposition, displayLabel: strings.TrimSpace(displayLabel)}
	if flowPath != "" || family != "" || semanticPath != "" {
		fact.ref, err = runtimeidentity.AdmitDeclarationIdentity(flowPath, family, semanticPath)
		if err != nil {
			return HandlerRuleSelectionFact{}, err
		}
	}
	return fact, fact.Validate()
}

func (f HandlerRuleSelectionFact) Validate() error {
	if _, err := ParseContext(string(f.context)); err != nil {
		return err
	}
	if _, err := ParseDisposition(string(f.disposition)); err != nil {
		return err
	}
	switch f.disposition {
	case DispositionSelected, DispositionEvaluationFailed:
		if f.context == ContextNone || !f.ref.Valid() {
			return fmt.Errorf("%s handler rule fact requires a concrete context and declaration identity", f.disposition)
		}
		if f.disposition == DispositionEvaluationFailed && f.context != ContextRules && f.context != ContextOnComplete {
			return fmt.Errorf("failed-evaluation handler rule fact requires rules or on-complete context")
		}
	case DispositionNoMatch:
		if f.context != ContextRules && f.context != ContextOnComplete {
			return fmt.Errorf("no-match handler rule fact requires rules or on-complete context")
		}
		if f.ref.Valid() || f.displayLabel != "" {
			return fmt.Errorf("no-match handler rule fact cannot carry selected-rule fields")
		}
	case DispositionNotApplicable:
		if f.context != ContextNone || f.ref.Valid() || f.displayLabel != "" {
			return fmt.Errorf("not-applicable handler rule fact cannot carry selected-rule fields")
		}
	}
	return nil
}

func (f HandlerRuleSelectionFact) Context() Context                          { return f.context }
func (f HandlerRuleSelectionFact) Disposition() Disposition                  { return f.disposition }
func (f HandlerRuleSelectionFact) Ref() runtimeidentity.DeclarationIdentity  { return f.ref }
func (f HandlerRuleSelectionFact) DisplayLabel() string                      { return f.displayLabel }
func (f HandlerRuleSelectionFact) Equal(other HandlerRuleSelectionFact) bool { return f == other }
func (f HandlerRuleSelectionFact) Empty() bool                               { return f == (HandlerRuleSelectionFact{}) }
