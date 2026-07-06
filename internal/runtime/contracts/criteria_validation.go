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
	errs = append(errs, validateProjectPolicyCriteriaUnsupported(bundle)...)
	errs = append(errs, validateFlowPolicyCriteriaSets(bundle)...)
	errs = append(errs, validateAgentCriteriaReferences(bundle)...)
	errs = append(errs, validateEventCriteriaCitationFields(bundle)...)
	errs = append(errs, validateAgentCriteriaCitationConsumption(bundle)...)
	return errs
}

func validateProjectPolicyCriteriaUnsupported(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, view := range bundle.ProjectViews() {
		if len(view.Policy.Criteria) == 0 {
			continue
		}
		errs = append(errs, fmt.Errorf("%w: project policy %s declares criteria; criteria must be declared in flow policy.yaml", ErrInvalidField, strings.TrimSpace(view.Paths.Key)))
	}
	return errs
}

func validateFlowPolicyCriteriaSets(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, flowID := range sortedFlowSchemaIDs(bundle.FlowSchemas) {
		view, ok := bundle.FlowViewByID(flowID)
		if !ok || view == nil {
			continue
		}
		policy := bundle.ResolvedPolicyForFlow(flowID)
		setNames := sortedCriteriaSetNames(view.Policy.Criteria)
		for _, setName := range setNames {
			set := view.Policy.Criteria[setName]
			errs = append(errs, validateCriteriaSet("flow "+flowID+" policy.criteria."+setName, set, policy)...)
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
			if err := validateCriteriaParam(ruleContext+".params."+name, rule.Params[name].Value, policy); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func validateCriteriaParam(context string, value any, policy PolicyDocument) error {
	switch typed := value.(type) {
	case nil:
		return fmt.Errorf("%w: %s must be a typed scalar or policy scalar reference", ErrInvalidField, context)
	case string:
		if ref, ok := strings.CutPrefix(strings.TrimSpace(typed), "policy."); ok {
			ref = strings.TrimSpace(ref)
			if ref == "" {
				return fmt.Errorf("%w: %s policy reference is missing a key", ErrInvalidField, context)
			}
			policyValue, exists := policy.Values[ref]
			if !exists {
				return fmt.Errorf("%w: %s references unknown policy scalar %q", ErrInvalidField, context, ref)
			}
			if !criteriaParamScalar(policyValue.Value) {
				return fmt.Errorf("%w: %s references non-scalar policy value %q", ErrInvalidField, context, ref)
			}
		}
		return nil
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return nil
	default:
		return fmt.Errorf("%w: %s must be a typed scalar or policy scalar reference, got %T", ErrInvalidField, context, value)
	}
}

func criteriaParamScalar(value any) bool {
	switch value.(type) {
	case nil:
		return false
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
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
		flowID := strings.TrimSpace(source.FlowID)
		if flowID == "" {
			errs = append(errs, fmt.Errorf("%w: agent %s criteria refs require a flow-scoped agent", ErrInvalidField, scopedKey))
			continue
		}
		policy := bundle.ResolvedPolicyForFlow(flowID)
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
			if _, ok := policy.Criteria[ref]; !ok {
				errs = append(errs, fmt.Errorf("%w: agent %s criteria ref %q does not resolve in flow %s policy.criteria", ErrInvalidField, scopedKey, ref, flowID))
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
			if strings.TrimSpace(field.Citation.Criteria) == "" {
				errs = append(errs, fmt.Errorf("%w: %s criteria is required", ErrInvalidField, fieldContext))
			}
			if len(normalizeStrings(field.Citation.AllowedClasses)) == 0 {
				errs = append(errs, fmt.Errorf("%w: %s allowed_classes is required for PR1 class/disposition compatibility", ErrInvalidField, fieldContext))
			}
			if !criteriaCitationTypeAllowed(field.Type) {
				errs = append(errs, fmt.Errorf("%w: %s requires text/string or list-of-text field type, got %q", ErrInvalidField, fieldContext, strings.TrimSpace(field.Type)))
			}
			if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
				policy := bundle.ResolvedPolicyForFlow(flowID)
				if set, ok := policy.Criteria[strings.TrimSpace(field.Citation.Criteria)]; ok {
					errs = append(errs, validateCriteriaCitationAllowedClasses(fieldContext, field.Citation, set)...)
				}
			}
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
		flowID := strings.TrimSpace(source.FlowID)
		if flowID == "" {
			continue
		}
		declared := stringSet(agent.Criteria)
		policy := bundle.ResolvedPolicyForFlow(flowID)
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
				set, ok := policy.Criteria[setName]
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

func criteriaCitationTypeAllowed(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if criteriaCitationBaseType(raw) == "text" || criteriaCitationBaseType(raw) == "string" {
		return true
	}
	if !criteriaCitationIsList(raw) {
		return false
	}
	item := criteriaCitationListItem(raw)
	return item == "text" || item == "string"
}

func criteriaCitationBaseType(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func criteriaCitationIsList(raw string) bool {
	raw = strings.TrimSpace(raw)
	return strings.HasPrefix(raw, "list<") && strings.HasSuffix(raw, ">") ||
		strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") ||
		strings.HasSuffix(raw, "[]") ||
		strings.HasPrefix(raw, "[]")
}

func criteriaCitationListItem(raw string) string {
	raw = strings.TrimSpace(raw)
	switch {
	case strings.HasPrefix(raw, "list<") && strings.HasSuffix(raw, ">"):
		return strings.ToLower(strings.TrimSpace(raw[len("list<") : len(raw)-1]))
	case strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]"):
		return strings.ToLower(strings.TrimSpace(raw[1 : len(raw)-1]))
	case strings.HasSuffix(raw, "[]"):
		return strings.ToLower(strings.TrimSpace(raw[:len(raw)-2]))
	case strings.HasPrefix(raw, "[]"):
		return strings.ToLower(strings.TrimSpace(raw[2:]))
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
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
