package contracts

import (
	"fmt"
	"strings"
)

func HandlerEmitEvents(handler SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	if eventType := handler.Emit.EventType(); eventType != "" {
		out = append(out, eventType)
	}
	out = append(out, actionResultEvents(handler.Action)...)
	for _, rule := range handler.Rules {
		out = append(out, ruleEmitEvents(rule)...)
	}
	if eventType := handler.OnSuccess.Emit.EventType(); eventType != "" {
		out = append(out, eventType)
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
	for _, branch := range handler.Branch {
		out = append(out, branchEmitEvents(branch)...)
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
	out = append(out, actionResultEvents(rule.Action)...)
	if rule.FanOut != nil {
		if eventType := rule.FanOut.Emit.EventType(); eventType != "" {
			out = append(out, eventType)
		}
	}
	return uniqueOrderedStrings(out)
}

func branchEmitEvents(branch BranchSpec) []string {
	out := make([]string, 0, 4)
	if branch.Then != nil {
		out = append(out, ruleEmitEvents(*branch.Then)...)
	}
	if branch.Else != nil {
		out = append(out, ruleEmitEvents(*branch.Else)...)
	}
	return uniqueOrderedStrings(out)
}

func actionResultEvents(action ActionSpec) []string {
	if strings.TrimSpace(action.ID) != "artifact_repo_commit" || action.ArtifactRepo == nil {
		return nil
	}
	return []string{
		action.ArtifactRepo.SuccessEvent,
		action.ArtifactRepo.FailureEvent,
	}
}

func HandlerHasNestedEmitSites(handler SystemNodeEventHandler) bool {
	if handler.FanOut != nil {
		return true
	}
	if len(handler.Rules) > 0 {
		return true
	}
	if len(handler.OnComplete) > 0 {
		return true
	}
	if len(handler.Branch) > 0 {
		return true
	}
	if handler.Accumulate != nil {
		if len(handler.Accumulate.OnComplete) > 0 || handler.Accumulate.OnTimeout != nil {
			return true
		}
	}
	return false
}

func HandlerHasAmbiguousTopLevelEmit(handler SystemNodeEventHandler) bool {
	return !handler.Emit.Empty() && HandlerHasNestedEmitSites(handler)
}

func HandlerEmitSiteOwnershipError(handler SystemNodeEventHandler) error {
	if HandlerHasAmbiguousTopLevelEmit(handler) {
		return fmt.Errorf("AMBIGUOUS-EMIT: handler-top-level emit is only allowed on single-emit handlers; move emit ownership to the active branch, rule, timeout, or fan_out site")
	}
	if handler.OnSuccess.Empty() {
		return nil
	}
	if !handler.Emit.Empty() {
		return fmt.Errorf("AMBIGUOUS-EMIT: handler on_success.emit cannot be combined with handler-level emit")
	}
	if len(handler.Rules) == 0 {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is only supported on handlers with rules")
	}
	if len(handler.OnComplete) > 0 {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is not supported with on_complete")
	}
	if handler.FanOut != nil {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is not supported with fan_out")
	}
	if handler.Accumulate != nil {
		if len(handler.Accumulate.OnComplete) > 0 {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is not supported with accumulate.on_complete")
		}
		if handler.Accumulate.OnTimeout != nil {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is not supported with accumulate.on_timeout")
		}
	}
	return nil
}

func HandlerHasAmbiguousTopLevelAction(handler SystemNodeEventHandler) bool {
	return strings.TrimSpace(handler.Action.ID) != "" && len(handler.Rules) > 0
}

func HandlerRuleActionIDs(handler SystemNodeEventHandler) []string {
	out := make([]string, 0, len(handler.Rules))
	for _, rule := range handler.Rules {
		if id := strings.TrimSpace(rule.Action.ID); id != "" {
			out = append(out, id)
		}
	}
	return uniqueOrderedStrings(out)
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
