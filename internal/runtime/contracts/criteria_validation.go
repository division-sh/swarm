package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func validateWorkflowCriteriaContracts(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	errs := []error{}
	errs = append(errs, validateFlowPolicyCriteriaSets(bundle)...)
	errs = append(errs, validateAgentCriteriaReferences(bundle)...)
	errs = append(errs, validateEventCriteriaCitationFields(bundle)...)
	errs = append(errs, validateAgentCriteriaCitationConsumption(bundle)...)
	return errs
}

func validateFlowPolicyCriteriaSets(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, value := range bundle.FlowViews() {
		view := value
		flowID := strings.TrimSpace(view.Paths.FlowPath)
		policy := bundle.ResolvedPolicyForFlow(flowID)
		for name, set := range view.Rules {
			if (set.Criteria == nil) == (set.Validation == nil) {
				errs = append(errs, fmt.Errorf("%w: flow %s rules.%s requires exactly one rule variant", ErrInvalidField, flowID, name))
			}
		}
		setNames := sortedCriteriaSetNames(view.Rules.CriteriaSets())
		for _, setName := range setNames {
			set, _ := view.Rules.Criteria(setName)
			errs = append(errs, validateCriteriaSet("flow "+flowID+" rules."+setName, set, policy)...)
		}
	}
	return errs
}

func validateCriteriaSet(context string, set PolicyCriteriaSet, policy PolicyDocument) []error {
	errs := []error{}
	if len(set.Classes) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s classes must declare at least one class", ErrInvalidField, context))
	}
	classNames := sortedCriteriaClassNames(set.Classes)
	for _, className := range classNames {
		if strings.TrimSpace(set.Classes[className].Disposition) == "" {
			errs = append(errs, fmt.Errorf("%w: %s classes.%s disposition is required", ErrInvalidField, context, className))
		}
	}
	if len(set.Rules) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s rules must declare at least one rule", ErrInvalidField, context))
	}
	seenIDs := map[string]struct{}{}
	for idx, rule := range set.Rules {
		ruleContext := fmt.Sprintf("%s.rules[%d]", context, idx)
		id := strings.TrimSpace(rule.ID)
		if id == "" {
			errs = append(errs, fmt.Errorf("%w: %s id is required", ErrInvalidField, ruleContext))
		} else if _, exists := seenIDs[id]; exists {
			errs = append(errs, fmt.Errorf("%w: %s duplicate stable criteria id %q", ErrInvalidField, context, id))
		} else {
			seenIDs[id] = struct{}{}
		}
		className := strings.TrimSpace(rule.Class)
		if className == "" {
			errs = append(errs, fmt.Errorf("%w: %s class is required", ErrInvalidField, ruleContext))
		} else if _, ok := set.Classes[className]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s class %q is not declared in classes", ErrInvalidField, ruleContext, className))
		}
		if strings.TrimSpace(rule.Text) == "" {
			errs = append(errs, fmt.Errorf("%w: %s text is required", ErrInvalidField, ruleContext))
		}
		paramNames := make([]string, 0, len(rule.Params))
		for name := range rule.Params {
			name = strings.TrimSpace(name)
			if name != "" {
				paramNames = append(paramNames, name)
			}
		}
		sort.Strings(paramNames)
		for _, name := range paramNames {
			if err := validateValidationParam(ruleContext+".params."+name, rule.Params[name].Value, policy); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func validateAgentCriteriaReferences(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	keys := make([]string, 0, len(bundle.scopedAgents))
	for key := range bundle.scopedAgents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, scopedKey := range keys {
		agent := EffectiveAgentRegistryEntry(scopedKey, bundle.scopedAgents[scopedKey])
		if len(agent.Criteria) == 0 {
			continue
		}
		source := bundle.scopedAgentSources[scopedKey]
		flowID := strings.TrimSpace(source.FlowPath)
		if flowID == "" {
			errs = append(errs, fmt.Errorf("%w: agent %s criteria refs require a flow-scoped agent", ErrInvalidField, scopedKey))
			continue
		}
		rules := bundle.ResolvedRulesForFlow(flowID)
		seen := map[string]struct{}{}
		for _, ref := range agent.Criteria {
			ref = strings.TrimSpace(ref)
			if ref == "" {
				errs = append(errs, fmt.Errorf("%w: agent %s criteria contains an empty set reference", ErrInvalidField, scopedKey))
				continue
			}
			if strings.Contains(ref, ".") || strings.Contains(ref, "/") {
				errs = append(errs, fmt.Errorf("%w: agent %s criteria ref %q must be a short set name", ErrInvalidField, scopedKey, ref))
			}
			if _, duplicate := seen[ref]; duplicate {
				errs = append(errs, fmt.Errorf("%w: agent %s criteria ref %q is duplicated", ErrInvalidField, scopedKey, ref))
			}
			seen[ref] = struct{}{}
			if _, ok := rules.Criteria(ref); !ok {
				errs = append(errs, fmt.Errorf("%w: agent %s criteria ref %q does not resolve in flow %s rules.yaml", ErrInvalidField, scopedKey, ref, flowID))
			}
		}
	}
	return errs
}

func validateEventCriteriaCitationFields(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	keys := make([]string, 0, len(bundle.scopedEvents))
	for key := range bundle.scopedEvents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, scopedKey := range keys {
		source := bundle.scopedEventSources[scopedKey]
		entry := bundle.scopedEvents[scopedKey]
		fieldNames := sortedEventFieldNames(entry.Payload.Properties)
		for _, fieldName := range fieldNames {
			field := entry.Payload.Properties[fieldName]
			if field.Citation.Empty() {
				continue
			}
			fieldContext := "event " + scopedKey + " payload." + fieldName + " citation"
			flowID := strings.TrimSpace(source.FlowPath)
			if strings.TrimSpace(field.Citation.Criteria) == "" {
				errs = append(errs, fmt.Errorf("%w: %s criteria is required", ErrInvalidField, fieldContext))
			}
			if len(normalizeStrings(field.Citation.AllowedClasses)) == 0 {
				errs = append(errs, fmt.Errorf("%w: %s allowed_classes is required for PR1 class/disposition compatibility", ErrInvalidField, fieldContext))
			}
			ref := CatalogTypeReference{Type: field.Type, Catalog: bundle.ResolvedTypeCatalogForFlow(flowID)}
			if !criteriaCitationTypeAllowed(ref) {
				errs = append(errs, fmt.Errorf("%w: %s requires text/string or list-of-text field type, got %q", ErrInvalidField, fieldContext, strings.TrimSpace(field.Type)))
			}
			criteriaName := strings.TrimSpace(field.Citation.Criteria)
			if criteriaName == "" {
				continue
			}
			if flowID == "" {
				errs = append(errs, fmt.Errorf("%w: %s requires a flow-scoped event", ErrInvalidField, fieldContext))
				continue
			}
			rules := bundle.ResolvedRulesForFlow(flowID)
			set, ok := rules.Criteria(criteriaName)
			if !ok {
				errs = append(errs, fmt.Errorf("%w: %s criteria set %q does not resolve in flow %s rules.yaml", ErrInvalidField, fieldContext, criteriaName, flowID))
				continue
			}
			errs = append(errs, validateCriteriaCitationAllowedClasses(fieldContext, field.Citation, set)...)
		}
	}
	return errs
}

func validateAgentCriteriaCitationConsumption(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	keys := make([]string, 0, len(bundle.scopedAgents))
	for key := range bundle.scopedAgents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, scopedKey := range keys {
		agent := EffectiveAgentRegistryEntry(scopedKey, bundle.scopedAgents[scopedKey])
		if len(agent.EmitEvents) == 0 {
			continue
		}
		source := bundle.scopedAgentSources[scopedKey]
		flowID := strings.TrimSpace(source.FlowPath)
		if flowID == "" {
			continue
		}
		declared := stringSet(agent.Criteria)
		rules := bundle.ResolvedRulesForFlow(flowID)
		for _, eventType := range agent.EmitEvents {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			schema, key, ok := EventSchemaForFlowEvent(bundle, flowID, eventType)
			if !ok {
				continue
			}
			citationFields := sortedCriteriaCitationFieldNames(schema.CitationFields)
			for _, fieldName := range citationFields {
				citation := schema.CitationFields[fieldName]
				setName := strings.TrimSpace(citation.Criteria)
				context := fmt.Sprintf("agent %s emit %s citation field %s", scopedKey, key, fieldName)
				if _, ok := declared[setName]; !ok {
					errs = append(errs, fmt.Errorf("%w: %s references criteria set %q but the agent does not declare it", ErrInvalidField, context, setName))
					continue
				}
				set, ok := rules.Criteria(setName)
				if !ok {
					errs = append(errs, fmt.Errorf("%w: %s criteria set %q does not resolve in flow %s", ErrInvalidField, context, setName, flowID))
					continue
				}
				errs = append(errs, validateCriteriaCitationAllowedClasses(context, citation, set)...)
			}
		}
	}
	return errs
}

func validateCriteriaCitationAllowedClasses(context string, citation CriteriaCitation, set PolicyCriteriaSet) []error {
	errs := []error{}
	for _, className := range normalizeStrings(citation.AllowedClasses) {
		if _, ok := set.Classes[className]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s allowed class %q is not declared by criteria set %q", ErrInvalidField, context, className, strings.TrimSpace(citation.Criteria)))
		}
	}
	return errs
}

func (c CriteriaCitation) Empty() bool {
	return strings.TrimSpace(c.Criteria) == "" && len(c.AllowedClasses) == 0
}

func criteriaCitationTypeAllowed(ref CatalogTypeReference) bool {
	resolved, err := ref.Resolve()
	if err != nil {
		return false
	}
	if resolved.PlainText() {
		return true
	}
	return resolved.Kind == CatalogTypeList && resolved.Element != nil && resolved.Element.PlainText()
}

func sortedCriteriaSetNames(in map[string]PolicyCriteriaSet) []string {
	names := make([]string, 0, len(in))
	for name := range in {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func sortedCriteriaClassNames(in map[string]PolicyCriteriaClass) []string {
	names := make([]string, 0, len(in))
	for name := range in {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func sortedCriteriaCitationFieldNames(in map[string]CriteriaCitation) []string {
	names := make([]string, 0, len(in))
	for name := range in {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func stringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}
