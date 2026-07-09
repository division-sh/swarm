package contracts

import (
	"fmt"
	"strconv"
	"strings"
)

type HandlerDeclarativeEmitSite struct {
	Source    string
	SiteKey   string
	RuleID    string
	RuleIndex int
	Spec      EmitSpec
	ItemAlias string
}

func HandlerEmitEvents(handler SystemNodeEventHandler) []string {
	out := make([]string, 0, 8)
	templateSites := HandlerRuleEmitTemplateSites(handler)
	if len(templateSites) == 0 {
		if eventType := handler.Emit.EventType(); eventType != "" {
			out = append(out, eventType)
		}
	} else {
		for _, site := range templateSites {
			if eventType := site.Spec.EventType(); eventType != "" {
				out = append(out, eventType)
			}
		}
	}
	out = append(out, actionResultEvents(handler.Action)...)
	for _, rule := range handler.Rules {
		if len(templateSites) == 0 {
			out = append(out, ruleEmitEvents(rule)...)
			continue
		}
		out = append(out, actionResultEvents(rule.Action)...)
		if rule.FanOut != nil {
			if eventType := rule.FanOut.Emit.EventType(); eventType != "" {
				out = append(out, eventType)
			}
		}
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
	if handler.FanOut != nil {
		if eventType := handler.FanOut.Emit.EventType(); eventType != "" {
			out = append(out, eventType)
		}
	}
	return uniqueOrderedStrings(out)
}

func HandlerDeclarativeEmitSites(handler SystemNodeEventHandler) []HandlerDeclarativeEmitSite {
	out := make([]HandlerDeclarativeEmitSite, 0, 8)
	add := func(source, siteKey, ruleID string, ruleIndex int, spec EmitSpec, itemAlias ...string) {
		if spec.Empty() {
			return
		}
		alias := ""
		if len(itemAlias) > 0 {
			alias = strings.TrimSpace(itemAlias[0])
		}
		out = append(out, HandlerDeclarativeEmitSite{
			Source:    strings.TrimSpace(source),
			SiteKey:   strings.TrimSpace(siteKey),
			RuleID:    strings.TrimSpace(ruleID),
			RuleIndex: ruleIndex,
			Spec:      cloneEmitSpec(spec),
			ItemAlias: alias,
		})
	}
	templateSites := HandlerRuleEmitTemplateSites(handler)
	if len(templateSites) == 0 {
		add("handler.emit", "handler.emit", "", -1, handler.Emit)
		for idx, rule := range handler.Rules {
			add("handler.rules.emit", indexedHandlerEmitSiteKey("handler.rules", idx, "emit"), rule.ID, idx, rule.Emit)
			if rule.FanOut != nil {
				add("handler.rules.fan_out.emit", indexedHandlerEmitSiteKey("handler.rules", idx, "fan_out.emit"), rule.ID, idx, rule.FanOut.Emit, rule.FanOut.As)
			}
		}
	} else {
		out = append(out, templateSites...)
	}
	add("handler.on_success.emit", "handler.on_success.emit", "", -1, handler.OnSuccess.Emit)
	for idx, rule := range handler.OnComplete {
		add("handler.on_complete.emit", indexedHandlerEmitSiteKey("handler.on_complete", idx, "emit"), rule.ID, idx, rule.Emit)
		if rule.FanOut != nil {
			add("handler.on_complete.fan_out.emit", indexedHandlerEmitSiteKey("handler.on_complete", idx, "fan_out.emit"), rule.ID, idx, rule.FanOut.Emit, rule.FanOut.As)
		}
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			add("handler.accumulate.on_complete.emit", indexedHandlerEmitSiteKey("handler.accumulate.on_complete", idx, "emit"), rule.ID, idx, rule.Emit)
			if rule.FanOut != nil {
				add("handler.accumulate.on_complete.fan_out.emit", indexedHandlerEmitSiteKey("handler.accumulate.on_complete", idx, "fan_out.emit"), rule.ID, idx, rule.FanOut.Emit, rule.FanOut.As)
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			add("handler.accumulate.on_timeout.emit", "handler.accumulate.on_timeout.emit", handler.Accumulate.OnTimeout.ID, 0, handler.Accumulate.OnTimeout.Emit)
			if handler.Accumulate.OnTimeout.FanOut != nil {
				add("handler.accumulate.on_timeout.fan_out.emit", "handler.accumulate.on_timeout.fan_out.emit", handler.Accumulate.OnTimeout.ID, 0, handler.Accumulate.OnTimeout.FanOut.Emit, handler.Accumulate.OnTimeout.FanOut.As)
			}
		}
	}
	if handler.FanOut != nil {
		add("handler.fan_out.emit", "handler.fan_out.emit", "", -1, handler.FanOut.Emit, handler.FanOut.As)
	}
	return out
}

func HandlerRuleEmitTemplateSites(handler SystemNodeEventHandler) []HandlerDeclarativeEmitSite {
	if !handlerRulesEmitTemplateSpecializationCandidate(handler) {
		return nil
	}
	out := make([]HandlerDeclarativeEmitSite, 0, len(handler.Rules))
	for idx, rule := range handler.Rules {
		spec, ok := EffectiveRuleEmitTemplateSpec(handler, rule)
		if !ok {
			return nil
		}
		out = append(out, HandlerDeclarativeEmitSite{
			Source:    "handler.rules.emit_template",
			SiteKey:   indexedHandlerEmitSiteKey("handler.rules", idx, "emit_template"),
			RuleID:    strings.TrimSpace(rule.ID),
			RuleIndex: idx,
			Spec:      spec,
		})
	}
	return out
}

func EffectiveRuleEmitTemplateSpec(handler SystemNodeEventHandler, rule HandlerRuleEntry) (EmitSpec, bool) {
	if strings.TrimSpace(handler.Emit.EventType()) == "" {
		return EmitSpec{}, false
	}
	if !emitFieldsAreCELExpressions(handler.Emit.Fields) {
		return EmitSpec{}, false
	}
	if rule.Emit.EventType() != "" || rule.Emit.Empty() || !rule.Emit.HasFields() || !rule.Emit.Target.Empty() || rule.Emit.Broadcast {
		return EmitSpec{}, false
	}
	if !emitFieldsAreCELExpressions(rule.Emit.Fields) {
		return EmitSpec{}, false
	}
	spec := cloneEmitSpec(handler.Emit)
	if strings.TrimSpace(rule.Emit.From) != "" {
		if strings.TrimSpace(spec.From) != "" && strings.TrimSpace(spec.From) != strings.TrimSpace(rule.Emit.From) {
			return EmitSpec{}, false
		}
		spec.From = strings.TrimSpace(rule.Emit.From)
	}
	if spec.Fields == nil {
		spec.Fields = map[string]ExpressionValue{}
	}
	for field, value := range rule.Emit.Fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, exists := spec.Fields[field]; exists {
			return EmitSpec{}, false
		}
		value.hydrate()
		spec.Fields[field] = value
	}
	return spec, true
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
	if handler.Accumulate != nil {
		if len(handler.Accumulate.OnComplete) > 0 || handler.Accumulate.OnTimeout != nil {
			return true
		}
	}
	return false
}

func HandlerHasAmbiguousTopLevelEmit(handler SystemNodeEventHandler) bool {
	if handlerRulesEmitTemplateSpecializationCandidate(handler) {
		return false
	}
	return !handler.Emit.Empty() && HandlerHasNestedEmitSites(handler)
}

func HandlerEmitSiteOwnershipError(handler SystemNodeEventHandler) error {
	if handlerRulesEmitTemplateSpecializationCandidate(handler) {
		if err := validateHandlerRuleEmitTemplateSpecialization(handler); err != nil {
			return err
		}
	} else {
		if err := rejectEventlessEmitSpecs(handler); err != nil {
			return err
		}
		if HandlerHasAmbiguousTopLevelEmit(handler) {
			return fmt.Errorf("AMBIGUOUS-EMIT: handler-top-level emit is only allowed on single-emit handlers; move emit ownership to the active branch, rule, timeout, or fan_out site")
		}
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
	for idx, rule := range handler.Rules {
		if rule.FanOut != nil {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler on_success.emit is not supported with rules[%d].fan_out", idx)
		}
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

func handlerRulesEmitTemplateSpecializationCandidate(handler SystemNodeEventHandler) bool {
	if handler.Emit.EventType() == "" || len(handler.Rules) == 0 {
		return false
	}
	for _, rule := range handler.Rules {
		if rule.Emit.EventType() == "" && !rule.Emit.Empty() {
			return true
		}
	}
	return false
}

func validateHandlerRuleEmitTemplateSpecialization(handler SystemNodeEventHandler) error {
	if handler.Emit.EventType() == "" {
		return fmt.Errorf("INVALID-EMIT: handler emit template specialization requires handler.emit.event")
	}
	if !handler.OnSuccess.Empty() {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization cannot be combined with on_success.emit")
	}
	if len(handler.OnComplete) > 0 {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization is only supported with handler.rules, not on_complete")
	}
	if handler.FanOut != nil {
		return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization is only supported with handler.rules, not fan_out")
	}
	if handler.Accumulate != nil {
		if len(handler.Accumulate.OnComplete) > 0 {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization is not supported with accumulate.on_complete")
		}
		if handler.Accumulate.OnTimeout != nil {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization is not supported with accumulate.on_timeout")
		}
	}
	if err := validateEmitTemplateCELFields("handler.emit.fields", handler.Emit.Fields); err != nil {
		return err
	}
	hasElse := false
	for idx, rule := range handler.Rules {
		if strings.TrimSpace(rule.Condition) == "" || strings.EqualFold(strings.TrimSpace(rule.Condition), "else") {
			hasElse = true
		}
		if rule.FanOut != nil {
			return fmt.Errorf("UNSUPPORTED-EMIT: handler emit template specialization is not supported with rules[%d].fan_out", idx)
		}
		if rule.Emit.Empty() {
			return fmt.Errorf("INVALID-EMIT: handler emit template specialization requires rules[%d].emit.fields", idx)
		}
		if rule.Emit.EventType() != "" {
			return fmt.Errorf("UNSUPPORTED-EMIT: rules[%d].emit.event cannot be combined with handler emit template specialization", idx)
		}
		if !rule.Emit.Target.Empty() || rule.Emit.Broadcast {
			return fmt.Errorf("UNSUPPORTED-EMIT: rules[%d].emit may only contribute fields in handler emit template specialization", idx)
		}
		if strings.TrimSpace(rule.Emit.From) != "" && strings.TrimSpace(handler.Emit.From) != "" && strings.TrimSpace(rule.Emit.From) != strings.TrimSpace(handler.Emit.From) {
			return fmt.Errorf("INVALID-EMIT: rules[%d].emit.from conflicts with handler emit template from", idx)
		}
		if !rule.Emit.HasFields() {
			return fmt.Errorf("INVALID-EMIT: handler emit template specialization requires rules[%d].emit.fields", idx)
		}
		if err := validateEmitTemplateCELFields(fmt.Sprintf("rules[%d].emit.fields", idx), rule.Emit.Fields); err != nil {
			return err
		}
		for field := range rule.Emit.Fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, exists := handler.Emit.Fields[field]; exists {
				return fmt.Errorf("INVALID-EMIT: rules[%d].emit.fields.%s conflicts with handler emit template field", idx, field)
			}
		}
	}
	if !hasElse {
		return fmt.Errorf("INVALID-EMIT: handler emit template specialization requires an else rule")
	}
	return nil
}

func emitFieldsAreCELExpressions(fields map[string]ExpressionValue) bool {
	for _, expr := range fields {
		if expr.Kind != ExpressionKindCEL {
			return false
		}
	}
	return true
}

func validateEmitTemplateCELFields(prefix string, fields map[string]ExpressionValue) error {
	for field, expr := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if expr.Kind != ExpressionKindCEL {
			return fmt.Errorf("INVALID-EMIT: handler emit template specialization requires %s.%s to be a CEL expression string", prefix, field)
		}
	}
	return nil
}

func rejectEventlessEmitSpecs(handler SystemNodeEventHandler) error {
	if err := requireEmitEvent("handler.emit", handler.Emit); err != nil {
		return err
	}
	if err := requireEmitEvent("handler.on_success.emit", handler.OnSuccess.Emit); err != nil {
		return err
	}
	if handler.FanOut != nil {
		if err := requireEmitEvent("handler.fan_out.emit", handler.FanOut.Emit); err != nil {
			return err
		}
	}
	for idx, rule := range handler.Rules {
		if err := requireRuleEmitEvents("rules", idx, rule); err != nil {
			return err
		}
	}
	for idx, rule := range handler.OnComplete {
		if err := requireRuleEmitEvents("on_complete", idx, rule); err != nil {
			return err
		}
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			if err := requireRuleEmitEvents("accumulate.on_complete", idx, rule); err != nil {
				return err
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			if err := requireRuleEmitEvents("accumulate.on_timeout", 0, *handler.Accumulate.OnTimeout); err != nil {
				return err
			}
		}
	}
	return nil
}

func requireRuleEmitEvents(scope string, idx int, rule HandlerRuleEntry) error {
	if err := requireEmitEvent(fmt.Sprintf("%s[%d].emit", scope, idx), rule.Emit); err != nil {
		return err
	}
	if rule.FanOut != nil {
		if err := requireEmitEvent(fmt.Sprintf("%s[%d].fan_out.emit", scope, idx), rule.FanOut.Emit); err != nil {
			return err
		}
	}
	return nil
}

func requireEmitEvent(label string, spec EmitSpec) error {
	if spec.Empty() || spec.EventType() != "" {
		return nil
	}
	return fmt.Errorf("INVALID-EMIT: %s.event is required", label)
}

func indexedHandlerEmitSiteKey(prefix string, index int, suffix string) string {
	return prefix + "[" + strconv.Itoa(index) + "]." + suffix
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
