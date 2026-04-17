package contracts

import "strings"

func HandlerEmitEvents(handler SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	if eventType := handler.Emit.EventType(); eventType != "" {
		out = append(out, eventType)
	}
	for _, rule := range handler.Rules {
		out = append(out, ruleEmitEvents(rule)...)
	}
	for _, rule := range handler.OnComplete {
		out = append(out, ruleEmitEvents(rule)...)
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			out = append(out, ruleEmitEvents(rule)...)
		}
		if handler.Accumulate.OnTimeout != nil {
			out = append(out, ruleEmitEvents(*handler.Accumulate.OnTimeout)...)
		}
	}
	if handler.FanOut != nil {
		if eventType := handler.FanOut.Emit.EventType(); eventType != "" {
			out = append(out, eventType)
		}
	}
	return uniqueOrderedStrings(out)
}

func RuleEmitEvents(rule HandlerRuleEntry) []string {
	return ruleEmitEvents(rule)
}

func ruleEmitEvents(rule HandlerRuleEntry) []string {
	out := make([]string, 0, 2)
	if eventType := rule.Emit.EventType(); eventType != "" {
		out = append(out, eventType)
	}
	if rule.FanOut != nil {
		if eventType := rule.FanOut.Emit.EventType(); eventType != "" {
			out = append(out, eventType)
		}
	}
	return uniqueOrderedStrings(out)
}

func HandlerHasNestedEmitSites(handler SystemNodeEventHandler) bool {
	if handler.FanOut != nil && !handler.FanOut.Emit.Empty() {
		return true
	}
	for _, rule := range handler.Rules {
		if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
			return true
		}
	}
	for _, rule := range handler.OnComplete {
		if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
			return true
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
				return true
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			rule := handler.Accumulate.OnTimeout
			if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
				return true
			}
		}
	}
	return false
}

func HandlerHasAmbiguousTopLevelEmit(handler SystemNodeEventHandler) bool {
	return !handler.Emit.Empty() && HandlerHasNestedEmitSites(handler)
}

func uniqueOrderedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
