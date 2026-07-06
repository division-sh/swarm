package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const policySheetValidationCheckID = "policy_sheet_validation_value_rows"

func checkPolicySheetValidationValueRows(c *checkerContext) []Finding {
	findings := make([]Finding, 0)
	nodes := c.source.NodeEntries()
	for _, nodeID := range sortedNodeIDs(c.source) {
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		flowID := nodeFlowID(c.source, nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for idx, rule := range handler.Rules {
				if !policySheetRuleIsValidationValueRow(rule) {
					continue
				}
				ref := policySheetValidationRef{
					FlowID:    flowID,
					NodeID:    nodeID,
					EventType: eventType,
					RuleIndex: idx,
					RuleID:    strings.TrimSpace(rule.ID),
				}
				findings = append(findings, validatePolicySheetValidationValueRow(c.source, ref, handler, rule)...)
			}
		}
	}
	return findings
}

type policySheetValidationRef struct {
	FlowID    string
	NodeID    string
	EventType string
	RuleIndex int
	RuleID    string
}

func policySheetValidationFinding(ref policySheetValidationRef, detail string) Finding {
	return Finding{
		CheckID:  policySheetValidationCheckID,
		Severity: SeverityHardInvalidity,
		Message:  fmt.Sprintf("flow %s node %s handler %s validate row %s: %s", defaultFlowLabel(ref.FlowID), ref.NodeID, ref.EventType, ref.RowLabel(), detail),
		Location: ref.NodeID,
	}
}

func (r policySheetValidationRef) RowLabel() string {
	if id := strings.TrimSpace(r.RuleID); id != "" {
		return id
	}
	return fmt.Sprintf("#%d", r.RuleIndex)
}

func policySheetRuleIsValidationValueRow(rule runtimecontracts.HandlerRuleEntry) bool {
	if rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindValidate {
		return true
	}
	return rule.Compute != nil && rule.Compute.Operation == runtimecontracts.ComputeOpValidate
}

func validatePolicySheetValidationValueRow(source semanticview.Source, ref policySheetValidationRef, handler runtimecontracts.SystemNodeEventHandler, rule runtimecontracts.HandlerRuleEntry) []Finding {
	findings := make([]Finding, 0)
	if rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindValidate {
		findings = append(findings, policySheetValidationFinding(ref, "validate compute must originate from a policy-sheet validate row"))
	}
	if rule.Compute == nil || rule.Compute.Operation != runtimecontracts.ComputeOpValidate {
		findings = append(findings, policySheetValidationFinding(ref, "validate row must lower to compute-owned validate operation"))
		return findings
	}
	spec := rule.Compute.Validation
	if spec == nil {
		findings = append(findings, policySheetValidationFinding(ref, "validate row has no canonical validation plan"))
		return findings
	}
	storeAs := strings.TrimSpace(rule.Compute.StoreAs)
	storePath := runtimepaths.Parse(storeAs)
	if storePath.Root != runtimepaths.RootComputed || len(storePath.Segments) < 2 || storePath.Segments[0] != "validation" {
		findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("validate.into must target computed.validation.*, got %q", storeAs)))
	} else if !policySheetLookupPathIsSimple(storePath) {
		findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("validate.into %q must be a simple computed.validation.* path", storeAs)))
	}
	policy := source.ResolvedPolicyForFlow(ref.FlowID)
	set, ok := policy.Validation[strings.TrimSpace(spec.Set)]
	if !ok {
		findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("validate.set %q does not resolve in policy.validation", strings.TrimSpace(spec.Set))))
	} else {
		findings = append(findings, validatePolicySheetValidationDispositionConsumer(ref, handler, rule, storeAs, set)...)
	}
	if !policySheetLookupBindingConsumed(handler, rule, storeAs) {
		findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("validate.into %q is not consumed by a supported downstream condition, emit field, activity input, fan_out, or expression", storeAs)))
	}
	return findings
}

func validatePolicySheetValidationDispositionConsumer(ref policySheetValidationRef, handler runtimecontracts.SystemNodeEventHandler, validateRule runtimecontracts.HandlerRuleEntry, target string, set runtimecontracts.PolicyValidationSet) []Finding {
	dispositions := map[string]struct{}{}
	for _, class := range set.Classes {
		disposition := strings.TrimSpace(class.Disposition)
		if disposition == "" || disposition == "none" {
			continue
		}
		dispositions[disposition] = struct{}{}
	}
	if len(dispositions) == 0 {
		return nil
	}
	matched := false
	findings := make([]Finding, 0)
	for _, rule := range handler.Rules {
		if strings.TrimSpace(rule.ID) == strings.TrimSpace(validateRule.ID) && rule.Compute == validateRule.Compute {
			continue
		}
		if !policySheetValidationConditionConsumesInvalidResult(rule.Condition, target) {
			continue
		}
		eventType := strings.TrimSpace(rule.Emit.EventType())
		if _, ok := dispositions[eventType]; !ok {
			findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("invalid-result consumer row %q emits %q, which is not declared as a policy.validation class disposition", strings.TrimSpace(rule.ID), eventType)))
			continue
		}
		matched = true
	}
	if !matched {
		findings = append(findings, policySheetValidationFinding(ref, fmt.Sprintf("validate result %q has no invalid-result selection row emitting a declared policy.validation disposition", target)))
	}
	return findings
}

func policySheetValidationConditionConsumesInvalidResult(condition, target string) bool {
	condition = strings.Join(strings.Fields(strings.TrimSpace(condition)), " ")
	target = strings.TrimSpace(target)
	if condition == "" || target == "" {
		return false
	}
	validRef := target + ".valid"
	return strings.Contains(condition, validRef+" == false") ||
		strings.Contains(condition, "false == "+validRef) ||
		strings.Contains(condition, "!"+validRef)
}
