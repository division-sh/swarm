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
	for _, nodeID := range sortedNodeIDs(c.source) {
		node, ok := c.source.NodeEntries()[nodeID]
		if !ok {
			continue
		}
		flowID := nodeFlowID(c.source, nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, site := range runtimecontracts.HandlerFanOutSites(handler) {
				c.fanOutFindings = append(c.fanOutFindings, c.validateFanOutSite(flowID, nodeID, eventType, site)...)
			}
		}
	}
	return c.fanOutFindings
}

func (c *checkerContext) validateFanOutSite(flowID, nodeID, eventType string, site runtimecontracts.WorkflowFanOutSite) []Finding {
	spec := site.Spec
	if spec == nil {
		return nil
	}
	out := make([]Finding, 0, 4)
	add := func(detail string) {
		out = append(out, Finding{
			CheckID:  fanOutValidationCheckID,
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("flow %s node %s handler %s %s: %s", defaultFlowLabel(flowID), nodeID, eventType, site.Source, detail),
			Location: nodeID,
		})
	}
	if err := runtimecontracts.ValidateFanOutAlias(spec.As); err != nil {
		add("fan_out." + err.Error())
	}
	effective, effectiveErr := c.source.ResolveFanOutEffectiveSemantics(flowID, eventType, *spec)
	if effectiveErr != nil {
		add(effectiveErr.Error())
	} else {
		if err := workflowexpr.ValidateValueExpressionWithOptions(effective.Identity, workflowexpr.ValueExpressionOptions{ItemAlias: effective.ItemAlias}); err != nil {
			add(fmt.Sprintf("fan_out.identity %q is invalid: %v", effective.Identity, err))
		}
		if workflowexpr.ExpressionReferencesFanOutFieldForValidation(effective.Identity, "index") {
			add("fan_out.identity must use the stable item alias, not fan_out.index")
		}
		if !expressionReferencesAlias(effective.Identity, effective.ItemAlias) {
			add(fmt.Sprintf("fan_out.identity %q must reference item alias %q", effective.Identity, effective.ItemAlias))
		}
		if !fanOutEmitCarriesIdentity(*spec, effective.Identity) {
			add(fmt.Sprintf("fan_out.emit.fields must carry identity expression %q", effective.Identity))
		}
	}
	return out
}

func fanOutEmitCarriesIdentity(spec runtimecontracts.FanOutSpec, identity string) bool {
	want := strings.TrimSpace(identity)
	if want == "" {
		return false
	}
	for _, expr := range spec.Emit.Fields {
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
