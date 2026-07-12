package bootverify

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const stageGateValidationCheckID = "stage_gate_validation"

var stageGateDecisionRefPattern = regexp.MustCompile(`\bdecision\.([A-Za-z_][A-Za-z0-9_]*)\b`)

func checkStageGateValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	findings := make([]Finding, 0)
	seen := map[string]string{}
	for _, plan := range c.source.WorkflowGates() {
		flowID := strings.TrimSpace(plan.FlowID)
		stage := strings.TrimSpace(plan.Stage)
		decision := strings.TrimSpace(plan.Decision)
		location := stageGateLocation(flowID, stage, decision)
		key := flowID + "\x00" + decision
		if previous, ok := seen[key]; ok {
			findings = append(findings, stageGateFinding(location, fmt.Sprintf("decision id %s is also declared at %s; decision ids are stable and unique within a flow", decision, previous)))
		} else {
			seen[key] = location
		}
		states := normalizedGateSet(c.source.FlowStates(flowID))
		terminal := normalizedGateSet(c.source.FlowTerminalStages(flowID))
		if flowID == "" {
			states = normalizedGateSet(workflowStageIDs(c.source.WorkflowStages()))
			terminal = normalizedGateSet(c.source.WorkflowTerminalStages())
		}
		if _, ok := states[stage]; !ok {
			findings = append(findings, stageGateFinding(location, fmt.Sprintf("gate source stage %s is not declared", stage)))
		}
		if _, ok := terminal[stage]; ok {
			findings = append(findings, stageGateFinding(location, fmt.Sprintf("terminal stage %s cannot own an actionable gate", stage)))
		}
		for name, expression := range plan.Context {
			if err := validateStageGateContextExpression(expression); err != nil {
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("context field %s is invalid: %v", strings.TrimSpace(name), err)))
			}
		}
		verdicts := make([]string, 0, len(plan.Outcomes))
		for verdict := range plan.Outcomes {
			verdicts = append(verdicts, verdict)
		}
		sort.Strings(verdicts)
		for _, verdict := range verdicts {
			outcome := plan.Outcomes[verdict]
			target := strings.TrimSpace(outcome.AdvancesTo)
			switch {
			case target == "":
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s requires advances_to; use mailbox.defer to keep waiting", verdict)))
			case target == stage:
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s targets the current stage %s; use a bounded loop row for re-entry", verdict, stage)))
			default:
				if _, ok := states[target]; !ok {
					findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s advances_to undeclared stage %s", verdict, target)))
				}
			}
			findings = append(findings, validateStageGateEmit(c, plan, verdict, outcome, location)...)
		}
	}
	return findings
}

func validateStageGateContextExpression(expression runtimecontracts.ExpressionValue) error {
	text := stageGateExpressionText(expression)
	if text == "" {
		return fmt.Errorf("expression is empty")
	}
	if stageGateDecisionRefPattern.MatchString(text) {
		return fmt.Errorf("decision.* is available only in outcome emit fields")
	}
	return workflowexpr.ValidateValueExpression(text)
}

func validateStageGateEmit(c *checkerContext, plan runtimecontracts.WorkflowGatePlan, verdict string, outcome runtimecontracts.WorkflowGateOutcomePlan, location string) []Finding {
	if outcome.Emit.Empty() {
		return nil
	}
	if strings.TrimSpace(outcome.Emit.From) != "" || outcome.Emit.HasTarget() || outcome.Emit.Broadcast {
		return []Finding{stageGateFinding(location, fmt.Sprintf("outcome %s uses emit.from, emit.target, or emit.broadcast; stage gates support only one frozen event emitted to the current entity", verdict))}
	}
	eventType := strings.TrimSpace(outcome.Emit.EventType())
	entry, _, ok := c.source.ResolveFlowEventCatalogEntry(plan.FlowID, eventType)
	if !ok {
		entry, ok = c.source.EventEntry(eventType)
	}
	if !ok {
		return []Finding{stageGateFinding(location, fmt.Sprintf("outcome %s emits unknown event %s", verdict, eventType))}
	}
	findings := make([]Finding, 0)
	for field, expression := range outcome.Emit.Fields {
		field = strings.TrimSpace(field)
		fieldSpec, declared := entry.Payload.Properties[field]
		if !declared {
			findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s emit field %s is not declared by event %s", verdict, field, eventType)))
			continue
		}
		if expression.HasLiteralValue() {
			continue
		}
		text := stageGateExpressionText(expression)
		matches := stageGateDecisionRefPattern.FindAllStringSubmatch(text, -1)
		if len(matches) > 0 {
			if len(matches) != 1 || strings.TrimSpace(matches[0][0]) != text {
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s emit field %s mixes decision input with another expression; use one declared decision field", verdict, field)))
				continue
			}
			inputName := strings.TrimSpace(matches[0][1])
			input, declared := outcome.Input[inputName]
			if !declared {
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s emit field %s reads undeclared decision.%s", verdict, field, inputName)))
				continue
			}
			if !stageGateTypesCompatible(input.Type, fieldSpec.Type) {
				findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s decision.%s type %s is incompatible with event %s field %s type %s", verdict, inputName, input.Type, eventType, field, fieldSpec.Type)))
			}
			continue
		}
		findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s emit field %s uses expression %q; stage gate emits support only literals or exact decision.<field> references", verdict, field, text)))
	}
	for _, required := range entry.Required {
		required = strings.TrimSpace(required)
		if required == "" {
			continue
		}
		if _, ok := outcome.Emit.Fields[required]; !ok {
			findings = append(findings, stageGateFinding(location, fmt.Sprintf("outcome %s event %s is missing required emit field %s", verdict, eventType, required)))
		}
	}
	return findings
}

func stageGateExpressionText(expression runtimecontracts.ExpressionValue) string {
	switch expression.Kind {
	case runtimecontracts.ExpressionKindCEL:
		return strings.TrimSpace(expression.CEL)
	case runtimecontracts.ExpressionKindRef:
		return strings.TrimSpace(expression.Ref)
	default:
		if text, ok := expression.Literal.(string); ok {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func stageGateTypesCompatible(inputType, eventType string) bool {
	inputType = strings.TrimSpace(strings.Split(inputType, " ")[0])
	eventType = strings.TrimSpace(strings.Split(eventType, " ")[0])
	if inputType == eventType {
		return true
	}
	return inputType == "text" && (eventType == "string" || eventType == "text")
}

func normalizedGateSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func workflowStageIDs(stages []runtimecontracts.WorkflowStageContract) []string {
	out := make([]string, 0, len(stages))
	for _, stage := range stages {
		out = append(out, stage.ID)
	}
	return out
}

func stageGateLocation(flowID, stage, decision string) string {
	if strings.TrimSpace(flowID) == "" {
		flowID = "root"
	}
	return fmt.Sprintf("flow %s stage %s gate %s", flowID, stage, decision)
}

func stageGateFinding(location, message string) Finding {
	return NewHardInvalidityFinding(stageGateValidationCheckID, location, message, "Fix the stage gate so every typed verdict has one valid direct route and one schema-compatible immutable decision contract.")
}
