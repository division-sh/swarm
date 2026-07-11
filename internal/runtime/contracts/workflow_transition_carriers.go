package contracts

import "strings"

type HandlerAdvanceCarrierKind string

const (
	HandlerAdvanceCarrierHandler              HandlerAdvanceCarrierKind = "handler.advances_to"
	HandlerAdvanceCarrierOnComplete           HandlerAdvanceCarrierKind = "handler.on_complete"
	HandlerAdvanceCarrierRules                HandlerAdvanceCarrierKind = "handler.rules"
	HandlerAdvanceCarrierAccumulateOnComplete HandlerAdvanceCarrierKind = "handler.accumulate.on_complete"
	HandlerAdvanceCarrierAccumulateOnTimeout  HandlerAdvanceCarrierKind = "handler.accumulate.on_timeout"
	HandlerAdvanceCarrierJoinOnComplete       HandlerAdvanceCarrierKind = "handler.join.on_complete"
	HandlerAdvanceCarrierJoinTimeout          HandlerAdvanceCarrierKind = "handler.join.timeout"
)

// HandlerAdvanceCarrier describes one authored handler site that can carry an advances_to target.
type HandlerAdvanceCarrier struct {
	Kind       HandlerAdvanceCarrierKind
	AdvancesTo string
	Rule       HandlerRuleEntry
	RuleIndex  int
	RuleID     string
}

// Source returns the stable source label used by verifier and authoring projections.
func (c HandlerAdvanceCarrier) Source() string {
	return string(c.Kind)
}

// HandlerAdvanceCarriers returns the complete authored advances_to carrier set for a handler.
func HandlerAdvanceCarriers(handler SystemNodeEventHandler) []HandlerAdvanceCarrier {
	return handlerAdvanceCarriers(handler.AdvancesTo, handler.OnComplete, handler.Rules, handler.Accumulate, handler.Join)
}

// HandlerTransitionAdvanceCarriers returns the complete authored advances_to carrier set for a semantic transition.
func HandlerTransitionAdvanceCarriers(transition HandlerTransitionSemantic) []HandlerAdvanceCarrier {
	return handlerAdvanceCarriers(transition.AdvancesTo, transition.OnComplete, transition.Rules, transition.Accumulate, transition.Join)
}

// HandlerAdvanceTargets returns the carrier targets without source metadata.
func HandlerAdvanceTargets(handler SystemNodeEventHandler) []string {
	carriers := HandlerAdvanceCarriers(handler)
	out := make([]string, 0, len(carriers))
	for _, carrier := range carriers {
		if target := strings.TrimSpace(carrier.AdvancesTo); target != "" {
			out = append(out, target)
		}
	}
	return out
}

func handlerAdvanceCarriers(
	advancesTo string,
	onComplete []HandlerRuleEntry,
	rules []HandlerRuleEntry,
	accumulate *AccumulateSpec,
	join *JoinSpec,
) []HandlerAdvanceCarrier {
	out := make([]HandlerAdvanceCarrier, 0, 1+len(onComplete)+len(rules))
	if target := strings.TrimSpace(advancesTo); target != "" {
		out = append(out, HandlerAdvanceCarrier{
			Kind:       HandlerAdvanceCarrierHandler,
			AdvancesTo: target,
			RuleIndex:  -1,
		})
	}
	appendRuleCarriers := func(kind HandlerAdvanceCarrierKind, entries []HandlerRuleEntry) {
		for idx, rule := range entries {
			target := strings.TrimSpace(rule.AdvancesTo)
			if target == "" {
				continue
			}
			out = append(out, HandlerAdvanceCarrier{
				Kind:       kind,
				AdvancesTo: target,
				Rule:       rule,
				RuleIndex:  idx,
				RuleID:     strings.TrimSpace(rule.ID),
			})
		}
	}
	appendRuleCarriers(HandlerAdvanceCarrierOnComplete, onComplete)
	appendRuleCarriers(HandlerAdvanceCarrierRules, rules)
	if accumulate != nil {
		appendRuleCarriers(HandlerAdvanceCarrierAccumulateOnComplete, accumulate.OnComplete)
		if accumulate.OnTimeout != nil {
			rule := *accumulate.OnTimeout
			if target := strings.TrimSpace(rule.AdvancesTo); target != "" {
				out = append(out, HandlerAdvanceCarrier{
					Kind:       HandlerAdvanceCarrierAccumulateOnTimeout,
					AdvancesTo: target,
					Rule:       rule,
					RuleIndex:  0,
					RuleID:     strings.TrimSpace(rule.ID),
				})
			}
		}
	}
	if join != nil {
		appendRuleCarriers(HandlerAdvanceCarrierJoinOnComplete, []HandlerRuleEntry{join.OnComplete})
		appendRuleCarriers(HandlerAdvanceCarrierJoinTimeout, []HandlerRuleEntry{join.Timeout.Outcome})
	}
	return out
}
