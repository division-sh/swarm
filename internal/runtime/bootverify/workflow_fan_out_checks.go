package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const fanOutValidationCheckID = "fan_out_validation"

func checkFanOutValidation(c *checkerContext) []Finding { return c.fanOutValidation() }

func (c *checkerContext) fanOutValidation() []Finding {
	if c.fanOutLoaded {
		return c.fanOutFindings
	}
	c.fanOutLoaded = true
	for _, failure := range c.source.FanOutPlanFailures() {
		c.fanOutFindings = append(c.fanOutFindings, Finding{
			CheckID: fanOutValidationCheckID, Severity: SeverityHardInvalidity,
			Message: failure.Error(), Location: failure.Node.Key(),
		})
	}
	for _, plan := range c.source.FanOutPlans() {
		c.fanOutFindings = append(c.fanOutFindings, c.validateFanOutPlan(plan)...)
	}
	return c.fanOutFindings
}

func (c *checkerContext) validateFanOutPlan(plan runtimecontracts.FanOutCompiledPlan) []Finding {
	flowID := plan.Site.Node.FlowPath()
	nodeID := plan.Site.Node.Key()
	eventType := strings.TrimSpace(plan.Site.EventType)
	siteSource := fanOutPlanSiteSource(plan.Site)
	out := make([]Finding, 0, 4)
	add := func(detail string) {
		out = append(out, Finding{
			CheckID:  fanOutValidationCheckID,
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("flow %s node %s handler %s %s: %s", defaultFlowLabel(flowID), nodeID, eventType, siteSource, detail),
			Location: nodeID,
		})
	}
	if err := workflowexpr.ValidateValueExpressionWithOptions(plan.Identity, workflowexpr.ValueExpressionOptions{ItemAlias: plan.ItemAlias}); err != nil {
		add(fmt.Sprintf("fan_out.identity %q is invalid: %v", plan.Identity, err))
	}
	if workflowexpr.ExpressionReferencesFanOutFieldForValidation(plan.Identity, "index") {
		add("fan_out.identity must use the stable item alias, not fan_out.index")
	}
	if !expressionReferencesAlias(plan.Identity, plan.ItemAlias) {
		add(fmt.Sprintf("fan_out.identity %q must reference item alias %q", plan.Identity, plan.ItemAlias))
	}
	if !fanOutEmitCarriesIdentity(plan.Emit, plan.Identity) {
		add(fmt.Sprintf("fan_out.emit.fields must carry identity expression %q", plan.Identity))
	}
	if plan.SourceAfterWrites {
		for _, write := range plan.Writes {
			if fanOutWriteReferencesCount(write) {
				add(fmt.Sprintf("%s mutates fan_out.items_from and a same-handler data write references fan_out.count; remove the cycle or source the count outside this handler", plan.ItemsFrom))
				break
			}
		}
	}
	return out
}

func fanOutPlanSiteSource(site runtimecontracts.FanOutSiteRef) string {
	switch site.Kind {
	case runtimecontracts.FanOutSiteRule:
		return fmt.Sprintf("handler.rules[%d].fan_out", site.Index)
	case runtimecontracts.FanOutSiteOnComplete:
		return fmt.Sprintf("handler.on_complete[%d].fan_out", site.Index)
	default:
		return "handler.fan_out"
	}
}

func fanOutWriteReferencesCount(write runtimecontracts.WorkflowDataWrite) bool {
	for _, expression := range []runtimecontracts.ExpressionValue{write.SourceExpression(), write.Value, write.Key, write.Index} {
		if workflowexpr.ExpressionReferencesFanOutFieldForValidation(fanOutExpressionText(expression), "count") {
			return true
		}
	}
	return false
}

func fanOutEmitCarriesIdentity(spec runtimecontracts.EmitSpec, identity string) bool {
	want := strings.TrimSpace(identity)
	if want == "" {
		return false
	}
	for _, expr := range spec.Fields {
		if strings.TrimSpace(fanOutExpressionText(expr)) == want {
			return true
		}
	}
	return false
}

func fanOutExpressionText(expr runtimecontracts.ExpressionValue) string {
	switch expr.Kind {
	case runtimecontracts.ExpressionKindRef:
		return strings.TrimSpace(expr.Ref)
	case runtimecontracts.ExpressionKindCEL:
		return strings.TrimSpace(expr.CEL)
	default:
		return ""
	}
}

func expressionReferencesAlias(expression, alias string) bool {
	expression = workflowexpr.StripStringLiterals(strings.TrimSpace(expression))
	alias = strings.TrimSpace(alias)
	if expression == "" || alias == "" {
		return false
	}
	for i := 0; i < len(expression); i++ {
		if !strings.HasPrefix(expression[i:], alias) {
			continue
		}
		if i > 0 && fanOutIdentifierPart(expression[i-1]) {
			continue
		}
		end := i + len(alias)
		if end < len(expression) && fanOutIdentifierPart(expression[end]) {
			continue
		}
		return true
	}
	return false
}

func fanOutIdentifierPart(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
