package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func (e *Executor) validateSourceStage(flowID, stage string) error {
	graph, ok := semanticview.WorkflowStageTopology(e.deps.Source, flowID)
	if !ok || graph.FlowID != flowID {
		return fmt.Errorf("%w: selected flow has no exact compiled stage topology", ErrInvalidTransition)
	}
	if stage == "" {
		return nil
	}
	// Stateless persistence uses pending without declaring a lifecycle stage.
	if len(graph.Stages) == 0 && stage == "pending" {
		return nil
	}
	if !slices.Contains(graph.Stages, stage) {
		return fmt.Errorf("%w: source stage %q is not declared in flow %s", ErrInvalidTransition, stage, flowID)
	}
	return nil
}

// ValidateTransitionEvidence binds one selected execution fact to its state and
// lifecycle projections before the selected-store mutation acquires effects.
func (m EngineMutation) ValidateTransitionEvidence() error {
	transition := m.State.Transition
	if transition != nil {
		if err := transition.Validate(); err != nil {
			return err
		}
		if transition.To() != m.State.NextState || !transition.RuleSelection().Equal(m.HandlerRuleSelection) {
			return fmt.Errorf("transition disagrees with state or selected execution rule")
		}
	}
	count := 0
	for _, effect := range m.LifecycleEffects {
		cause, hasCause := effect.Transition()
		if !hasCause {
			continue
		}
		count++
		if transition == nil || cause.ID() != transition.ID() || effect.EventID() != m.State.TriggerEventID || effect.EventType() != m.State.TriggerEventType || !effect.OccurredAt().Equal(m.State.TriggeredAt) {
			return fmt.Errorf("lifecycle cause disagrees with state transition evidence")
		}
	}
	if transition != nil && count != 1 {
		return fmt.Errorf("transition requires exactly one matching lifecycle effect")
	}
	return nil
}

func (e *Executor) admitSelectedTransition(frame *executionFrame, next string) error {
	graph, ok := semanticview.WorkflowStageTopology(e.deps.Source, frame.req.ExecutionFlowID.String())
	if !ok {
		return fmt.Errorf("%w: selected flow has no compiled stage topology", ErrInvalidTransition)
	}
	site := contracts.WorkflowTransitionSite{Node: frame.req.Node, HandlerEvent: frame.req.HandlerEventKey, AdvanceCarrier: contracts.HandlerAdvanceCarrierHandler}
	if frame.req.Handler.Loop != nil {
		kind, id, err := frame.req.Handler.Loop.Operation()
		if err != nil {
			return err
		}
		site.LoopID, site.LoopOperation = id, kind
	}
	if frame.loopEscaped {
		site.LoopEscape, site.AdvanceCarrier = true, ""
	} else if frame.rule != nil && strings.TrimSpace(frame.rule.AdvancesTo) != "" {
		var qualified bool
		site.RuleRef, qualified = frame.rule.DeclarationIdentity()
		if !qualified {
			return fmt.Errorf("selected advancing rule has no compiled declaration identity")
		}
		switch frame.ruleSource {
		case handlerRuleSourceRules:
			site.AdvanceCarrier = contracts.HandlerAdvanceCarrierRules
		case handlerRuleSourceOnComplete:
			site.AdvanceCarrier = contracts.HandlerAdvanceCarrierOnComplete
		case handlerRuleSourceJoinOnComplete:
			site.AdvanceCarrier = contracts.HandlerAdvanceCarrierJoinOnComplete
		case handlerRuleSourceJoinTimeout:
			site.AdvanceCarrier = contracts.HandlerAdvanceCarrierJoinTimeout
			if frame.req.Handler.Join == nil {
				return fmt.Errorf("join timeout transition lacks declaration")
			}
			site.TimerID = frame.req.Handler.Join.EffectiveID()
		default:
			return fmt.Errorf("selected advancing rule has no execution context")
		}
	}
	compiled, err := graph.AdmitTransition(site, frame.result.CurrentState, next)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTransition, err)
	}
	if next == frame.result.CurrentState {
		return nil
	}
	cause, err := workflowlifecycle.NewCompiledTransition(compiled, frame.result.HandlerRuleSelection, frame.result.GuardsEvaluated)
	if err != nil {
		return err
	}
	if err := cause.ValidateHandlerEvidence(frame.req.Handler); err != nil {
		return err
	}
	frame.result.StateMutation.Transition = &cause
	return nil
}
