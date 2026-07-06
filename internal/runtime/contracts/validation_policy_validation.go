package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func validateWorkflowPolicyValidationContracts(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	errs := []error{}
	errs = append(errs, validateProjectPolicyValidationUnsupported(bundle)...)
	errs = append(errs, validateFlowPolicyValidationSets(bundle)...)
	errs = append(errs, validatePolicySheetValidationRows(bundle)...)
	return errs
}

func validateProjectPolicyValidationUnsupported(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, view := range bundle.ProjectViews() {
		if len(view.Policy.Validation) == 0 {
			continue
		}
		errs = append(errs, fmt.Errorf("%w: project policy %s declares validation; validation must be declared in flow policy.yaml", ErrInvalidField, strings.TrimSpace(view.Paths.Key)))
	}
	return errs
}

func validateFlowPolicyValidationSets(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, flowID := range sortedFlowSchemaIDs(bundle.FlowSchemas) {
		view, ok := bundle.FlowViewByID(flowID)
		if !ok || view == nil {
			continue
		}
		policy := bundle.ResolvedPolicyForFlow(flowID)
		setNames := sortedValidationSetNames(view.Policy.Validation)
		for _, setName := range setNames {
			set := view.Policy.Validation[setName]
			errs = append(errs, validatePolicyValidationSet("flow "+flowID+" policy.validation."+setName, set, policy)...)
		}
	}
	return errs
}

func validatePolicyValidationSet(context string, set PolicyValidationSet, policy PolicyDocument) []error {
	errs := []error{}
	if len(set.Classes) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s classes must declare at least one class", ErrInvalidField, context))
	}
	classNames := sortedValidationClassNames(set.Classes)
	for _, className := range classNames {
		if strings.TrimSpace(set.Classes[className].Disposition) == "" {
			errs = append(errs, fmt.Errorf("%w: %s classes.%s disposition is required", ErrInvalidField, context, className))
		}
	}
	if len(set.Inputs) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s inputs must declare at least one typed input", ErrInvalidField, context))
	}
	for _, name := range sortedValidationInputNames(set.Inputs) {
		if !isValidationInputTypeSupported(set.Inputs[name]) {
			errs = append(errs, fmt.Errorf("%w: %s inputs.%s type %q is not supported by Slice B validation rows", ErrInvalidField, context, name, strings.TrimSpace(set.Inputs[name])))
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
			errs = append(errs, fmt.Errorf("%w: %s duplicate stable validation id %q", ErrInvalidField, context, id))
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
		if rule.PinCandidate == nil {
			errs = append(errs, fmt.Errorf("%w: %s pin_candidate must be explicitly true or false", ErrInvalidField, ruleContext))
		}
		errs = append(errs, validatePolicyValidationRuleCheck(ruleContext, rule.Check, set.Inputs)...)
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

func validatePolicyValidationRuleCheck(context string, check PolicyValidationCheck, inputs map[string]string) []error {
	errs := []error{}
	if check.Equal == nil {
		return append(errs, fmt.Errorf("%w: %s check.equal is required; Slice B supports only equal checks", ErrInvalidField, context))
	}
	left, leftType, leftErr := validationInputRefType(check.Equal.Left, inputs)
	right, rightType, rightErr := validationInputRefType(check.Equal.Right, inputs)
	if leftErr != nil {
		errs = append(errs, fmt.Errorf("%w: %s check.equal.left %v", ErrInvalidField, context, leftErr))
	}
	if rightErr != nil {
		errs = append(errs, fmt.Errorf("%w: %s check.equal.right %v", ErrInvalidField, context, rightErr))
	}
	if leftErr == nil && rightErr == nil && leftType != rightType {
		errs = append(errs, fmt.Errorf("%w: %s check.equal compares input.%s type %s with input.%s type %s", ErrInvalidField, context, left, leftType, right, rightType))
	}
	return errs
}

func validationInputRefType(ref string, inputs map[string]string) (string, string, error) {
	ref = strings.TrimSpace(ref)
	name, ok := strings.CutPrefix(ref, "input.")
	if !ok || strings.TrimSpace(name) == "" || strings.Contains(strings.TrimSpace(name), ".") {
		return "", "", fmt.Errorf("%q must be an input.* reference", ref)
	}
	name = strings.TrimSpace(name)
	rawType, ok := inputs[name]
	if !ok {
		return name, "", fmt.Errorf("%q references undeclared input %q", ref, name)
	}
	kind := normalizeValidationInputType(rawType)
	if kind == "" {
		return name, "", fmt.Errorf("%q has unsupported declared type %q", ref, strings.TrimSpace(rawType))
	}
	return name, kind, nil
}

func validatePolicySheetValidationRows(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for nodeID, node := range bundle.Nodes {
		source, _ := bundle.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(source.FlowID)
		policy := bundle.ResolvedPolicyForFlow(flowID)
		for eventType, handler := range node.EventHandlers {
			for idx, rule := range handler.Rules {
				if !policySheetRuleIsValidationValueRow(rule) {
					continue
				}
				context := fmt.Sprintf("node %s handler %s rules[%d] validate row %s", strings.TrimSpace(nodeID), strings.TrimSpace(eventType), idx, strings.TrimSpace(rule.ID))
				errs = append(errs, validatePolicySheetValidationRow(context, rule, policy)...)
			}
		}
	}
	return errs
}

func policySheetRuleIsValidationValueRow(rule HandlerRuleEntry) bool {
	if rule.PolicyRow.Kind == PolicySheetRowKindValidate {
		return true
	}
	return rule.Compute != nil && rule.Compute.Operation == ComputeOpValidate
}

func validatePolicySheetValidationRow(context string, rule HandlerRuleEntry, policy PolicyDocument) []error {
	errs := []error{}
	if rule.PolicyRow.Kind != PolicySheetRowKindValidate {
		errs = append(errs, fmt.Errorf("%w: %s validate compute must originate from a policy-sheet validate row", ErrInvalidField, context))
	}
	if rule.Compute == nil || rule.Compute.Operation != ComputeOpValidate || rule.Compute.Validation == nil {
		return append(errs, fmt.Errorf("%w: %s validate row must lower to compute-owned validate operation", ErrInvalidField, context))
	}
	spec := rule.Compute.Validation
	target := strings.TrimSpace(spec.StoreTarget())
	if target == "" {
		errs = append(errs, fmt.Errorf("%w: %s validate.into is required", ErrInvalidField, context))
	} else if err := validatePolicyValidationStoreTarget(target); err != nil {
		errs = append(errs, fmt.Errorf("%w: %s %v", ErrInvalidField, context, err))
	}
	if computeTarget := strings.TrimSpace(rule.Compute.StoreAs); computeTarget != "" && target != "" && computeTarget != target {
		errs = append(errs, fmt.Errorf("%w: %s validate.into %q must match compute.store_as %q", ErrInvalidField, context, target, computeTarget))
	}
	setName := strings.TrimSpace(spec.Set)
	set, ok := policy.Validation[setName]
	if setName == "" {
		errs = append(errs, fmt.Errorf("%w: %s validate.set is required", ErrInvalidField, context))
	} else if !ok {
		errs = append(errs, fmt.Errorf("%w: %s validate.set %q does not resolve in flow policy.validation", ErrInvalidField, context, setName))
	}
	if !ok {
		return errs
	}
	declared := stringSetFromMap(set.Inputs)
	mapped := stringSetFromMap(spec.Input)
	for name := range declared {
		if _, ok := mapped[name]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s validate.input missing required set input %q", ErrInvalidField, context, name))
		}
	}
	for name := range mapped {
		if _, ok := declared[name]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s validate.input maps undeclared set input %q", ErrInvalidField, context, name))
		}
	}
	return errs
}

func validatePolicyValidationStoreTarget(target string) error {
	parsed := paths.Parse(target)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) < 2 || parsed.Segments[0] != "validation" {
		return fmt.Errorf("validate.into %q must target computed.validation.*", strings.TrimSpace(target))
	}
	for _, segment := range parsed.Segments {
		if !isPolicySheetPathSegment(segment) {
			return fmt.Errorf("validate.into %q must be a simple computed.validation.* path", strings.TrimSpace(target))
		}
	}
	return nil
}

func (s ComputeValidationSpec) StoreTarget() string {
	if strings.TrimSpace(s.Into) != "" {
		return strings.TrimSpace(s.Into)
	}
	return ""
}

func normalizeValidationInputType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "string", "text", "uuid", "timestamp", "datetime", "date":
		return "string"
	case "bool", "boolean":
		return "bool"
	case "int", "integer":
		return "int"
	case "number", "numeric", "float", "double":
		return "number"
	default:
		return ""
	}
}

func isValidationInputTypeSupported(raw string) bool {
	return normalizeValidationInputType(raw) != ""
}

func sortedValidationSetNames(in map[string]PolicyValidationSet) []string {
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

func sortedValidationClassNames(in map[string]PolicyValidationClass) []string {
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

func sortedValidationInputNames(in map[string]string) []string {
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

func stringSetFromMap[V any](in map[string]V) map[string]struct{} {
	out := map[string]struct{}{}
	for key := range in {
		key = strings.TrimSpace(key)
		if key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}
