package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			for _, site := range fanOutSites(handler) {
				c.fanOutFindings = append(c.fanOutFindings, c.validateFanOutSite(flowID, nodeID, eventType, site)...)
			}
		}
	}
	return c.fanOutFindings
}

type fanOutValidationSite struct {
	Source string
	Spec   *runtimecontracts.FanOutSpec
}

func fanOutSites(handler runtimecontracts.SystemNodeEventHandler) []fanOutValidationSite {
	out := make([]fanOutValidationSite, 0, 5)
	add := func(source string, spec *runtimecontracts.FanOutSpec) {
		if spec == nil {
			return
		}
		out = append(out, fanOutValidationSite{Source: strings.TrimSpace(source), Spec: spec})
	}
	add("handler.fan_out", handler.FanOut)
	for idx := range handler.Rules {
		add(ruleScope("handler.rules", idx, handler.Rules[idx].ID)+".fan_out", handler.Rules[idx].FanOut)
	}
	for idx := range handler.OnComplete {
		add(ruleScope("handler.on_complete", idx, handler.OnComplete[idx].ID)+".fan_out", handler.OnComplete[idx].FanOut)
	}
	if handler.Accumulate != nil {
		for idx := range handler.Accumulate.OnComplete {
			add(ruleScope("handler.accumulate.on_complete", idx, handler.Accumulate.OnComplete[idx].ID)+".fan_out", handler.Accumulate.OnComplete[idx].FanOut)
		}
		if handler.Accumulate.OnTimeout != nil {
			add(ruleScope("handler.accumulate.on_timeout", 0, handler.Accumulate.OnTimeout.ID)+".fan_out", handler.Accumulate.OnTimeout.FanOut)
		}
	}
	return out
}

func (c *checkerContext) validateFanOutSite(flowID, nodeID, eventType string, site fanOutValidationSite) []Finding {
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
	if strings.TrimSpace(spec.Identity) == "" {
		add("fan_out.identity is required")
	} else {
		if err := workflowexpr.ValidateValueExpressionWithOptions(spec.Identity, workflowexpr.ValueExpressionOptions{ItemAlias: spec.As}); err != nil {
			add(fmt.Sprintf("fan_out.identity %q is invalid: %v", spec.Identity, err))
		}
		if workflowexpr.ExpressionReferencesFanOutFieldForValidation(spec.Identity, "index") {
			add("fan_out.identity must use the stable item alias, not fan_out.index")
		}
		if !expressionReferencesAlias(spec.Identity, spec.As) {
			add(fmt.Sprintf("fan_out.identity %q must reference item alias %q", spec.Identity, strings.TrimSpace(spec.As)))
		}
		if !fanOutEmitCarriesIdentity(*spec) {
			add(fmt.Sprintf("fan_out.emit.fields must carry identity expression %q", strings.TrimSpace(spec.Identity)))
		}
	}
	if err := runtimecontracts.ValidateFanOutMaxItems(*spec); err != nil {
		add(err.Error())
	}
	if err := c.validateFanOutItemsSource(flowID, eventType, *spec); err != nil {
		add(err.Error())
	}
	return out
}

func (c *checkerContext) validateFanOutItemsSource(flowID, eventType string, spec runtimecontracts.FanOutSpec) error {
	path, err := runtimecontracts.ValidateFanOutItemsSource(spec)
	if err != nil {
		return err
	}
	field := strings.TrimSpace(path.Segments[0])
	switch path.Root {
	case runtimepaths.RootPayload:
		proof := semanticview.ResolveFlowEventProof(c.source, flowID, eventType)
		if !proof.HasSchema {
			return fmt.Errorf("fan_out.items_from %q references payload but event %s has no payload schema", strings.TrimSpace(spec.ItemsFrom), eventType)
		}
		fieldSpec, ok := proof.Entry.Payload.Properties[field]
		if !ok {
			return fmt.Errorf("fan_out.items_from %q references undeclared payload field %s", strings.TrimSpace(spec.ItemsFrom), field)
		}
		if !fanOutCollectionTypeRef(fieldSpec.Type) {
			return fmt.Errorf("fan_out.items_from %q must reference a collection payload field; field %s has type %q", strings.TrimSpace(spec.ItemsFrom), field, strings.TrimSpace(fieldSpec.Type))
		}
	case runtimepaths.RootEntity:
		view := wave1EntityContractForFlow(c.source, flowID)
		if !view.Defined {
			return fmt.Errorf("fan_out.items_from %q references entity but flow %s has no primary entity contract", strings.TrimSpace(spec.ItemsFrom), defaultFlowLabel(flowID))
		}
		fieldSpec, ok := view.Contract.Fields[field]
		if !ok {
			return fmt.Errorf("fan_out.items_from %q references undeclared entity field %s", strings.TrimSpace(spec.ItemsFrom), field)
		}
		if !fanOutCollectionTypeRef(fieldSpec.Type) {
			return fmt.Errorf("fan_out.items_from %q must reference a collection entity field; field %s has type %q", strings.TrimSpace(spec.ItemsFrom), field, strings.TrimSpace(fieldSpec.Type))
		}
	}
	return nil
}

func fanOutCollectionTypeRef(typeRef string) bool {
	typeRef = strings.TrimSpace(typeRef)
	lower := strings.ToLower(typeRef)
	if typeRef == "" {
		return false
	}
	switch {
	case lower == "array":
		return true
	case strings.HasPrefix(lower, "array ") || strings.HasPrefix(lower, "array("):
		return true
	case strings.HasPrefix(lower, "array<") && strings.Contains(lower, ">"):
		end := strings.Index(lower, ">")
		return strings.TrimSpace(typeRef[len("array<"):end]) != ""
	case strings.HasPrefix(lower, "list<") && strings.HasSuffix(lower, ">"):
		return strings.TrimSpace(typeRef[len("list<"):len(typeRef)-1]) != ""
	case strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]"):
		return strings.TrimSpace(typeRef[1:len(typeRef)-1]) != ""
	case strings.HasPrefix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[2:]) != ""
	case strings.HasSuffix(typeRef, "[]"):
		return strings.TrimSpace(typeRef[:len(typeRef)-2]) != ""
	default:
		return false
	}
}

func fanOutEmitCarriesIdentity(spec runtimecontracts.FanOutSpec) bool {
	want := strings.TrimSpace(spec.Identity)
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
