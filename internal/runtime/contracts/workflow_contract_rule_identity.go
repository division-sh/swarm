package contracts

import (
	"fmt"
	"strconv"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// QualifySystemNodeHandlerRuleRefsForEvent derives authored child identities
// from the exact declaration position. Display labels never participate in identity.
func QualifySystemNodeHandlerRuleRefsForEvent(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler) (SystemNodeEventHandler, error) {
	if !node.Valid() {
		return SystemNodeEventHandler{}, fmt.Errorf("qualify handler rules: executable node identity is required")
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return SystemNodeEventHandler{}, fmt.Errorf("qualify handler rules: authored handler event is required")
	}
	base := "nodes[" + strconv.Quote(node.NodeID()) + "].handlers[" + strconv.Quote(eventType) + "]"
	qualify := func(context string, index int, rule HandlerRuleEntry) (HandlerRuleEntry, error) {
		if !rule.authored {
			return rule, nil
		}
		identity, err := runtimeidentity.AdmitDeclarationIdentity(node.FlowPath(), "handler_rule", fmt.Sprintf("%s.%s[%d]", base, context, index))
		if err != nil {
			return HandlerRuleEntry{}, fmt.Errorf("%s rule %q identity: %w", context, strings.TrimSpace(rule.ID), err)
		}
		rule.declarationIdentity = identity
		return rule, nil
	}
	qualifyFanOut := func(context string, index int, spec *FanOutSpec) (*FanOutSpec, error) {
		if spec == nil {
			return nil, nil
		}
		out := *spec
		identity, err := runtimeidentity.AdmitDeclarationIdentity(node.FlowPath(), "fan_out", fmt.Sprintf("%s.%s[%d]", base, context, index))
		if err != nil {
			return nil, fmt.Errorf("%s identity: %w", context, err)
		}
		out.declarationIdentity = identity
		return &out, nil
	}
	qualifyMany := func(context string, rules []HandlerRuleEntry) ([]HandlerRuleEntry, error) {
		out := append([]HandlerRuleEntry(nil), rules...)
		for index := range out {
			var err error
			out[index], err = qualify(context, index, out[index])
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	var err error
	handler.Rules, err = qualifyMany("rules", handler.Rules)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	handler.OnComplete, err = qualifyMany("on_complete", handler.OnComplete)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	if handler.Join != nil {
		join := *handler.Join
		if join.OnCompleteFound {
			join.OnComplete, err = qualify("join.on_complete", 0, join.OnComplete)
			if err != nil {
				return SystemNodeEventHandler{}, err
			}
		}
		if join.TimeoutFound {
			join.Timeout.Outcome, err = qualify("join.timeout", 0, join.Timeout.Outcome)
			if err != nil {
				return SystemNodeEventHandler{}, err
			}
		}
		handler.Join = &join
	}
	handler.FanOut, err = qualifyFanOut("fan_out", 0, handler.FanOut)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	for index := range handler.Rules {
		handler.Rules[index].FanOut, err = qualifyFanOut("rules.fan_out", index, handler.Rules[index].FanOut)
		if err != nil {
			return SystemNodeEventHandler{}, err
		}
	}
	for index := range handler.OnComplete {
		handler.OnComplete[index].FanOut, err = qualifyFanOut("on_complete.fan_out", index, handler.OnComplete[index].FanOut)
		if err != nil {
			return SystemNodeEventHandler{}, err
		}
	}
	return handler, nil
}

func HandlerRuleEntries(handler SystemNodeEventHandler) []HandlerRuleEntry {
	out := append([]HandlerRuleEntry(nil), handler.Rules...)
	out = append(out, handler.OnComplete...)
	if handler.Join != nil {
		if handler.Join.OnCompleteFound {
			out = append(out, handler.Join.OnComplete)
		}
		if handler.Join.TimeoutFound {
			out = append(out, handler.Join.Timeout.Outcome)
		}
	}
	return out
}
