package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func selectedHandlerAdvancesTo(handler runtimecontracts.SystemNodeEventHandler, rule *runtimecontracts.HandlerRuleEntry) string {
	if rule != nil && strings.TrimSpace(rule.AdvancesTo) != "" {
		return strings.TrimSpace(rule.AdvancesTo)
	}
	return strings.TrimSpace(handler.AdvancesTo)
}

// ValidateActivityLoopLineage consumes committed execution evidence. It never
// re-evaluates a rule or asks whether a historical generation may execute now.
func ValidateActivityLoopLineage(intent ActivityIntent, parent events.Event, source semanticview.Source, handler runtimecontracts.SystemNodeEventHandler, selection handlerselection.HandlerRuleSelectionFact, activations []loopruntime.Activation) error {
	if handler.Loop == nil {
		if intent.Generation != (attemptgeneration.Generation{}) || intent.LoopStage != "" {
			return fmt.Errorf("non-loop activity cannot carry loop generation or stage")
		}
		return nil
	}
	if err := validateHandlerLoopRuntime(handler); err != nil {
		return err
	}
	kind, loopID, err := handler.Loop.Operation()
	if err != nil || kind != runtimecontracts.LoopOperationAdmit {
		return fmt.Errorf("activity requires an admitted loop operation")
	}
	plan, ok := workflowLoopPlan(source, intent.ExecutionFlowID.String(), loopID)
	g := intent.Generation
	if !ok || !g.Valid() || g.FlowID != plan.FlowID || g.LoopID != plan.ID || g.RevisionField != plan.RevisionField {
		return fmt.Errorf("activity generation differs from its declared loop")
	}
	var payload map[string]any
	err = json.Unmarshal(parent.Payload(), &payload)
	if err != nil || strings.TrimSpace(asString(payload[plan.RevisionField])) != g.RevisionID {
		return fmt.Errorf("activity generation differs from its admitted parent revision")
	}
	owned := false
	for _, activation := range activations {
		owned = owned || activation.OwnsGeneration(g)
	}
	if !owned {
		return fmt.Errorf("activity generation is not owned by its persisted activation")
	}
	rule, err := committedHandlerRule(handler, selection)
	if err != nil {
		return err
	}
	stage := selectedHandlerAdvancesTo(handler, rule)
	if stage == "" {
		stage = strings.TrimSpace(handler.Loop.From)
	}
	if intent.LoopStage != stage {
		return fmt.Errorf("activity stage differs from its committed handler transition")
	}
	return nil
}

func committedHandlerRule(handler runtimecontracts.SystemNodeEventHandler, fact handlerselection.HandlerRuleSelectionFact) (*runtimecontracts.HandlerRuleEntry, error) {
	if err := fact.Validate(); err != nil {
		return nil, err
	}
	if fact.Disposition() == handlerselection.DispositionNotApplicable || fact.Disposition() == handlerselection.DispositionNoMatch {
		return nil, nil
	}
	if fact.Disposition() != handlerselection.DispositionSelected {
		return nil, fmt.Errorf("failed rule evaluation cannot produce an activity")
	}
	var rules []runtimecontracts.HandlerRuleEntry
	switch fact.Context() {
	case handlerselection.ContextRules:
		rules = handler.Rules
	case handlerselection.ContextOnComplete:
		rules = handler.OnComplete
	case handlerselection.ContextJoinComplete:
		if handler.Join != nil {
			rules = []runtimecontracts.HandlerRuleEntry{handler.Join.OnComplete}
		}
	case handlerselection.ContextJoinTimeout:
		if handler.Join != nil {
			rules = []runtimecontracts.HandlerRuleEntry{handler.Join.Timeout.Outcome}
		}
	}
	for _, rule := range rules {
		if ref, ok := rule.DeclarationIdentity(); ok && ref == fact.Ref() {
			return &rule, nil
		}
	}
	return nil, fmt.Errorf("committed rule selection is foreign to the activity handler")
}
