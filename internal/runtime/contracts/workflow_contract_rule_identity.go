package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// QualifySystemNodeHandlerRuleRefs binds admitted authored element IDs to the
// package that owns their node declaration. Local labels and indexes remain
// presentation and ordering facts, never identity inputs.
func QualifySystemNodeHandlerRuleRefs(node runtimeidentity.ExecutableNode, handler SystemNodeEventHandler) (SystemNodeEventHandler, error) {
	if !node.Valid() {
		return SystemNodeEventHandler{}, fmt.Errorf("qualify handler rules: executable node identity is required")
	}
	packageKey, err := runtimeidentity.ParsePackageKey(node.PackageKey())
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	qualify := func(context string, rule HandlerRuleEntry) (HandlerRuleEntry, error) {
		if !rule.authored {
			return rule, nil
		}
		if !rule.ElementID.Valid() {
			return HandlerRuleEntry{}, fmt.Errorf("%s rule %q requires canonical element_id; run `swarm mint-element-ids --contracts <path>`", context, strings.TrimSpace(rule.ID))
		}
		ref, err := contractelementidentity.NewContractElementRef(packageKey, rule.ElementID)
		if err != nil {
			return HandlerRuleEntry{}, fmt.Errorf("%s rule %q: %w", context, strings.TrimSpace(rule.ID), err)
		}
		rule.elementRef = ref
		return rule, nil
	}
	qualifyMany := func(context string, rules []HandlerRuleEntry) ([]HandlerRuleEntry, error) {
		out := append([]HandlerRuleEntry(nil), rules...)
		for index := range out {
			out[index], err = qualify(context, out[index])
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	handler.Rules, err = qualifyMany("handler.rules", handler.Rules)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	handler.OnComplete, err = qualifyMany("handler.on_complete", handler.OnComplete)
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	if handler.Join != nil {
		join := *handler.Join
		if join.OnCompleteFound {
			join.OnComplete, err = qualify("handler.join.on_complete", join.OnComplete)
			if err != nil {
				return SystemNodeEventHandler{}, err
			}
		}
		if join.TimeoutFound {
			join.Timeout.Outcome, err = qualify("handler.join.timeout", join.Timeout.Outcome)
			if err != nil {
				return SystemNodeEventHandler{}, err
			}
		}
		handler.Join = &join
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
