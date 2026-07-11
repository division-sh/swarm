package bootverify

import (
	"fmt"
	"regexp"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const joinValidationCheckID = "join_validation"

var joinPolicyDelayPattern = regexp.MustCompile(`^\{\{\s*([a-zA-Z_][a-zA-Z0-9_.-]*)\s*\}\}(ms|s|m|h|d)$`)

func checkJoinValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	findings := make([]Finding, 0)
	seenIDs := map[string]string{}
	for _, nodeID := range sortedNodeIDs(c.source) {
		node := c.source.NodeEntries()[nodeID]
		flowID := nodeFlowID(c.source, nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if flowUsesAuthoredStages(c.source, flowID) && stagedBarrierAccumulator(handler.Accumulate) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, "staged finite barriers must use handler.join; barrier-shaped accumulate fields are retired for stages: bundles"))
			}
			if handler.Join == nil {
				continue
			}
			if err := runtimecontracts.ValidateJoinHandlerIsolation(handler); err != nil {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, err.Error()))
			}
			spec := *handler.Join
			prefix := fmt.Sprintf("join %s", spec.EffectiveID())
			if !flowUsesAuthoredStages(c.source, flowID) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" requires an authored stages: lifecycle"))
			}
			if spec.Stage == "" || !containsString(c.source.FlowStates(flowID), spec.Stage) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("%s references unknown stage %q", prefix, spec.Stage)))
			}
			identityKey := strings.Join([]string{flowID, spec.Stage, spec.EffectiveID()}, "|")
			location := nodeID + ":" + eventType
			if prior, duplicate := seenIDs[identityKey]; duplicate {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("%s has duplicate effective identity; already declared at %s (add explicit unique id values)", prefix, prior)))
			} else {
				seenIDs[identityKey] = location
			}
			findings = append(findings, c.validateJoinPaths(flowID, nodeID, eventType, spec)...)
			if spec.Window == nil && joinStageCanReenter(c.source, flowID, spec.Stage) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" stage is re-entrant; window.from and window.by are required"))
			}
			if spec.HasCustomCompletion() {
				if spec.Remaining != runtimecontracts.JoinRemainingIgnore {
					findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" complete_when requires remaining: ignore"))
				}
				findings = append(findings, validateJoinExpression(flowID, nodeID, eventType, "complete_when", spec.CompleteWhen, true)...)
			} else if spec.Remaining != "" {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" remaining is forbidden when complete_when is omitted"))
			}
			if !spec.OnCompleteFound || joinRuleEmpty(spec.OnComplete) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" requires a non-empty on_complete outcome"))
			}
			if !spec.TimeoutFound || strings.TrimSpace(spec.Timeout.After) == "" || joinRuleEmpty(spec.Timeout.Outcome) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, prefix+" requires timeout.after and a non-empty timeout outcome; bare joins are invalid"))
			} else if !joinDelayValid(c.source, flowID, spec.Timeout.After) {
				findings = append(findings, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("%s timeout.after %q must be a positive duration or resolved policy-scalar duration", prefix, spec.Timeout.After)))
			}
			findings = append(findings, validateJoinOutcome(flowID, nodeID, eventType, "on_complete", spec.OnComplete, c.source.FlowStates(flowID))...)
			findings = append(findings, validateJoinOutcome(flowID, nodeID, eventType, "timeout", spec.Timeout.Outcome, c.source.FlowStates(flowID))...)
		}
	}
	return findings
}

func (c *checkerContext) validateJoinPaths(flowID, nodeID, eventType string, spec runtimecontracts.JoinSpec) []Finding {
	out := make([]Finding, 0, 5)
	view := wave1EntityContractForFlow(c.source, flowID)
	memberField := joinPathField(spec.Members.From, "entity")
	if memberField == "" {
		out = append(out, joinFinding(flowID, nodeID, eventType, "join.members.from must be a top-level entity.<field> path"))
	} else if field, ok := view.Contract.Fields[memberField]; !view.Defined || !ok {
		out = append(out, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.members.from references undeclared entity field %s", memberField)))
	} else if !joinTextListType(field.Type) {
		out = append(out, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.members.from field %s must be ordered list<text>, got %q", memberField, field.Type)))
	}
	proof := semanticview.ResolveFlowEventProof(c.source, flowID, eventType)
	memberBy := joinPathField(spec.Members.By, "payload")
	if memberBy == "" {
		out = append(out, joinFinding(flowID, nodeID, eventType, "join.members.by must be a top-level payload.<field> path"))
	} else if field, ok := proof.Entry.Payload.Properties[memberBy]; !proof.HasSchema || !ok || !joinTextType(field.Type) {
		out = append(out, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.members.by must reference a declared text payload field %s", memberBy)))
	}
	output := joinPathField(spec.Output, "payload")
	if output == "" {
		out = append(out, joinFinding(flowID, nodeID, eventType, "join.output must be a top-level payload.<field> path"))
	} else if _, ok := proof.Entry.Payload.Properties[output]; !proof.HasSchema || !ok {
		out = append(out, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.output references undeclared payload field %s", output)))
	}
	if spec.Window != nil {
		windowFrom := joinPathField(spec.Window.From, "entity")
		if field, ok := view.Contract.Fields[windowFrom]; windowFrom == "" || !view.Defined || !ok || !joinTextType(field.Type) {
			out = append(out, joinFinding(flowID, nodeID, eventType, "join.window.from must reference a declared top-level text entity field"))
		}
		windowBy := joinPathField(spec.Window.By, "payload")
		if field, ok := proof.Entry.Payload.Properties[windowBy]; windowBy == "" || !proof.HasSchema || !ok || !joinTextType(field.Type) {
			out = append(out, joinFinding(flowID, nodeID, eventType, "join.window.by must reference a declared top-level text payload field"))
		}
	}
	return out
}

func validateJoinOutcome(flowID, nodeID, eventType, label string, rule runtimecontracts.HandlerRuleEntry, states []string) []Finding {
	out := make([]Finding, 0)
	if target := strings.TrimSpace(rule.AdvancesTo); target != "" && !containsString(states, target) {
		out = append(out, joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.%s advances_to references unknown stage %s", label, target)))
	}
	for field, expr := range rule.Emit.Fields {
		if text := joinExpressionText(expr); text != "" {
			out = append(out, validateJoinExpression(flowID, nodeID, eventType, label+" emit.fields."+field, text, false)...)
		}
	}
	for idx, write := range rule.DataAccumulation.Writes {
		if text := joinExpressionText(write.Value); text != "" {
			out = append(out, validateJoinExpression(flowID, nodeID, eventType, fmt.Sprintf("%s data_accumulation.writes[%d]", label, idx), text, false)...)
		}
	}
	return out
}

func validateJoinExpression(flowID, nodeID, eventType, label, expression string, joinOnly bool) []Finding {
	if err := workflowexpr.ValidateValueExpressionWithOptions(expression, workflowexpr.ValueExpressionOptions{AllowJoin: true, RequireBool: joinOnly}); err != nil {
		return []Finding{joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.%s expression %q is invalid: %v", label, expression, err))}
	}
	for _, root := range []string{"payload", "event", "policy", "computed", "fan_out", "accumulated", "_entity"} {
		if workflowexpr.ExpressionReferencesRoot(expression, root) {
			return []Finding{joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.%s may not reference %s.*", label, root))}
		}
	}
	if joinOnly && workflowexpr.ExpressionReferencesRoot(expression, "entity") {
		return []Finding{joinFinding(flowID, nodeID, eventType, fmt.Sprintf("join.%s may reference only join.*", label))}
	}
	return nil
}

func joinFinding(flowID, nodeID, eventType, detail string) Finding {
	return NewHardInvalidityFinding(joinValidationCheckID, nodeID,
		fmt.Sprintf("flow %s node %s handler %s: %s", defaultFlowLabel(flowID), nodeID, eventType, detail),
		"Use the canonical staged handler.join contract with typed membership, mandatory timeout, and supported entity/join outcome expressions.")
}

func joinRuleEmpty(rule runtimecontracts.HandlerRuleEntry) bool {
	return strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && !rule.DataAccumulation.HasWrites()
}

func stagedBarrierAccumulator(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil {
		return false
	}
	return strings.TrimSpace(spec.ExpectedFrom) != "" || spec.Threshold > 0 || spec.TimeoutMS > 0 || strings.TrimSpace(string(spec.Completion.Mode)) != "" || len(spec.OnComplete) > 0 || spec.OnTimeout != nil
}

func joinPathField(path, root string) string {
	path = strings.TrimSpace(path)
	prefix := root + "."
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	field := strings.TrimPrefix(path, prefix)
	if field == "" || strings.Contains(field, ".") {
		return ""
	}
	return field
}

func joinTextType(typeRef string) bool {
	switch strings.ToLower(strings.TrimSpace(typeRef)) {
	case "text", "string":
		return true
	default:
		return false
	}
}

func joinTextListType(typeRef string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(typeRef), " ", ""))
	switch normalized {
	case "list<text>", "list<string>", "[text]", "[string]", "[]text", "[]string", "text[]", "string[]":
		return true
	default:
		return false
	}
}

func joinExpressionText(expr runtimecontracts.ExpressionValue) string {
	if expr.Kind == runtimecontracts.ExpressionKindRef {
		return strings.TrimSpace(expr.Ref)
	}
	if expr.Kind == runtimecontracts.ExpressionKindCEL {
		return strings.TrimSpace(expr.CEL)
	}
	return ""
}

func joinDelayValid(source semanticview.Source, flowID, raw string) bool {
	raw = strings.TrimSpace(raw)
	if _, ok := timeridentity.ParseDelayDuration(raw); ok {
		return true
	}
	match := joinPolicyDelayPattern.FindStringSubmatch(raw)
	if len(match) != 3 {
		return false
	}
	value, ok := source.ResolvedPolicyForFlow(flowID).Values[match[1]]
	if !ok {
		return false
	}
	_, ok = timeridentity.ParseDelayDuration(fmt.Sprint(value.Value) + match[2])
	return ok
}

func joinStageCanReenter(source semanticview.Source, flowID, stage string) bool {
	states := declaredStatesForFlow(source, flowID)
	edges := authoredStateGraphEdges(source, flowID, source.FlowInitialStage(flowID), states)
	stage = strings.TrimSpace(stage)
	seen := map[string]struct{}{}
	queue := make([]string, 0, len(edges[stage]))
	for next := range edges[stage] {
		queue = append(queue, next)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == stage {
			return true
		}
		if _, ok := seen[current]; ok {
			continue
		}
		seen[current] = struct{}{}
		for next := range edges[current] {
			queue = append(queue, next)
		}
	}
	return false
}
