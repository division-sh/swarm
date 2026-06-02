package bootverify

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkConditionPolicyAlignment(c *checkerContext) []Finding { return c.conditionPolicyAlignment() }
func checkConditionPayloadAlignment(c *checkerContext) []Finding {
	return c.conditionPayloadAlignment()
}
func checkConfigFromPayloadAlignment(c *checkerContext) []Finding {
	return c.configFromPayloadAlignment()
}

type payloadFieldCoverageSite struct {
	FlowID       string
	NodeID       string
	EventType    string
	Scope        string
	Accumulation runtimecontracts.WorkflowDataAccumulation
}

func (c *checkerContext) conditionPolicyAlignment() []Finding {
	if c.conditionPolicyLoaded {
		return c.conditionPolicyFindings
	}
	c.conditionPolicyLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		resolvedPolicy := policyValueMap(c.source.ResolvedPolicyForNode(nodeID))
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, cond := range handlerConditions(handler) {
				for _, ref := range policyReferences(cond.Expression) {
					if policyFieldExists(resolvedPolicy, ref) {
						continue
					}
					c.conditionPolicyFindings = append(c.conditionPolicyFindings, Finding{
						CheckID:  "condition_policy_alignment",
						Severity: "warning",
						Message:  fmt.Sprintf("node %s handler %s references policy.%s but policy does not define it", strings.TrimSpace(nodeID), eventType, ref),
						Location: strings.TrimSpace(nodeID),
					})
				}
			}
		}
	}
	return c.conditionPolicyFindings
}

func (c *checkerContext) conditionPayloadAlignment() []Finding {
	if c.conditionPayloadLoaded {
		return c.conditionPayloadFindings
	}
	c.conditionPayloadLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			payloadFields, eventExists := eventPayloadFieldsForExistingEvent(c.source, eventType)
			if !eventExists {
				continue
			}
			for _, cond := range handlerConditions(handler) {
				for _, ref := range payloadReferences(cond.Expression) {
					if !payloadFieldExists(payloadFields, ref) {
						c.conditionPayloadFindings = append(c.conditionPayloadFindings, Finding{
							CheckID:  "condition_payload_alignment",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s references payload.%s outside event payload schema", strings.TrimSpace(nodeID), eventType, ref),
							Location: strings.TrimSpace(nodeID),
						})
					}
				}
			}
		}
	}
	return c.conditionPayloadFindings
}

func (c *checkerContext) configFromPayloadAlignment() []Finding {
	if c.configPayloadLoaded {
		return c.configPayloadFindings
	}
	c.configPayloadLoaded = true
	for _, transition := range c.source.DerivedHandlerTransitions() {
		sourceEvent := strings.TrimSpace(transition.DataAccumulation.SourceEvent)
		if sourceEvent == "" {
			continue
		}
		if sourceEvent == strings.TrimSpace(transition.EventType) || derivedAccumulationSource(sourceEvent) {
			continue
		}
		c.configPayloadFindings = append(c.configPayloadFindings, Finding{
			CheckID:  "config_from_payload_alignment",
			Severity: "error",
			Message:  fmt.Sprintf("handler transition %s data_accumulation.source_event %s does not match handler event %s", transition.ID, sourceEvent, transition.EventType),
			Location: strings.TrimSpace(transition.ID),
		})
	}
	return c.configPayloadFindings
}

func (c *checkerContext) payloadFieldCoverage() []Finding {
	if c.payloadCoverageLoaded {
		return c.payloadCoverageFindings
	}
	c.payloadCoverageLoaded = true
	for _, site := range payloadFieldCoverageSites(c.source) {
		sourceEvent := strings.TrimSpace(site.Accumulation.SourceEvent)
		if sourceEvent == "" {
			sourceEvent = strings.TrimSpace(site.EventType)
		}
		if sourceEvent == "" || derivedAccumulationSource(sourceEvent) {
			continue
		}
		sourceFields, sourceEventExists := eventPayloadFieldsForExistingEvent(c.source, sourceEvent)
		if !sourceEventExists {
			continue
		}
		for _, write := range site.Accumulation.Writes {
			for _, ref := range dataAccumulationPayloadSourceRefs(write) {
				if payloadFieldExists(sourceFields, ref.Field) {
					continue
				}
				c.payloadCoverageFindings = append(c.payloadCoverageFindings, Finding{
					CheckID:  "payload_field_coverage",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s node %s handler %s %s %s missing from source event %s payload schema", defaultFlowLabel(site.FlowID), site.NodeID, site.EventType, site.Scope, ref.Description, sourceEvent),
					Location: strings.TrimSpace(site.NodeID),
				})
			}
		}
	}
	return c.payloadCoverageFindings
}

func payloadFieldCoverageSites(source semanticview.Source) []payloadFieldCoverageSite {
	if source == nil {
		return nil
	}
	out := make([]payloadFieldCoverageSite, 0)
	for _, nodeID := range sortedNodeIDs(source) {
		node, ok := source.NodeEntries()[nodeID]
		if !ok {
			continue
		}
		flowID := ""
		if sourceRef, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			add := func(scope string, accumulation runtimecontracts.WorkflowDataAccumulation) {
				if !accumulation.HasWrites() {
					return
				}
				out = append(out, payloadFieldCoverageSite{
					FlowID:       flowID,
					NodeID:       strings.TrimSpace(nodeID),
					EventType:    eventType,
					Scope:        strings.TrimSpace(scope),
					Accumulation: accumulation,
				})
			}
			add("handler.data_accumulation", handler.DataAccumulation)
			for idx, rule := range handler.Rules {
				add(ruleScope("handler.rules", idx, rule.ID)+".data_accumulation", rule.DataAccumulation)
			}
			for idx, rule := range handler.OnComplete {
				add(ruleScope("handler.on_complete", idx, rule.ID)+".data_accumulation", rule.DataAccumulation)
			}
			if handler.Accumulate != nil {
				for idx, rule := range handler.Accumulate.OnComplete {
					add(ruleScope("handler.accumulate.on_complete", idx, rule.ID)+".data_accumulation", rule.DataAccumulation)
				}
				if handler.Accumulate.OnTimeout != nil {
					add(ruleScope("handler.accumulate.on_timeout", 0, handler.Accumulate.OnTimeout.ID)+".data_accumulation", handler.Accumulate.OnTimeout.DataAccumulation)
				}
			}
			for idx, branch := range handler.Branch {
				if branch.Then != nil {
					add(ruleScope("handler.branch.then", idx, branch.Then.ID)+".data_accumulation", branch.Then.DataAccumulation)
				}
				if branch.Else != nil {
					add(ruleScope("handler.branch.else", idx, branch.Else.ID)+".data_accumulation", branch.Else.DataAccumulation)
				}
			}
		}
	}
	return out
}

func ruleScope(prefix string, idx int, id string) string {
	if id = strings.TrimSpace(id); id != "" {
		return prefix + "[" + id + "]"
	}
	return fmt.Sprintf("%s[%d]", prefix, idx)
}

type dataAccumulationPayloadSourceRef struct {
	Field       string
	Description string
}

func dataAccumulationPayloadSourceRefs(write runtimecontracts.WorkflowDataWrite) []dataAccumulationPayloadSourceRef {
	if write.Value.HasLiteralValue() {
		return nil
	}
	if cel := strings.TrimSpace(write.Value.CEL); cel != "" {
		refs := payloadReferences(cel)
		out := make([]dataAccumulationPayloadSourceRef, 0, len(refs))
		for _, ref := range refs {
			out = append(out, dataAccumulationPayloadSourceRef{
				Field:       ref,
				Description: "payload." + ref,
			})
		}
		return out
	}
	if ref := strings.TrimSpace(write.Value.Ref); strings.HasPrefix(ref, "payload.") {
		field := strings.TrimPrefix(ref, "payload.")
		return []dataAccumulationPayloadSourceRef{{
			Field:       field,
			Description: ref,
		}}
	}
	if !write.Value.IsZero() {
		return nil
	}
	if source := strings.TrimSpace(write.Source()); source != "" {
		description := fmt.Sprintf("source field %q", source)
		if strings.TrimSpace(write.Field) != "" && strings.TrimSpace(write.SourceField) == "" {
			description = fmt.Sprintf("writes '%s'", source)
		}
		return []dataAccumulationPayloadSourceRef{{
			Field:       source,
			Description: description,
		}}
	}
	return nil
}

func policyValueMap(policy runtimecontracts.PolicyDocument) map[string]any {
	out := make(map[string]any, len(policy.Values))
	for key, value := range policy.Values {
		out[strings.TrimSpace(key)] = value.Value
	}
	return out
}

var bootverifyPolicyReferencePattern = regexp.MustCompile(`policy\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
var bootverifyPayloadReferencePattern = regexp.MustCompile(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`)

func policyReferences(expression string) []string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	matches := bootverifyPolicyReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		ref := strings.TrimSpace(match[1])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Strings(out)
	return out
}

func payloadReferences(expression string) []string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	matches := bootverifyPayloadReferencePattern.FindAllStringSubmatch(expression, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		ref := strings.TrimSpace(match[1])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Strings(out)
	return out
}

func policyFieldExists(policy map[string]any, ref string) bool {
	if len(policy) == 0 {
		return false
	}
	_, ok := lookupPolicyValue(policy, ref)
	return ok
}

func lookupPolicyValue(policy map[string]any, ref string) (any, bool) {
	current := any(policy)
	for _, part := range strings.Split(strings.TrimSpace(ref), ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := next[part]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func eventPayloadFields(source semanticview.Source, eventType string) map[string]struct{} {
	fields, ok := eventPayloadFieldsForExistingEvent(source, eventType)
	if !ok {
		return nil
	}
	return fields
}

func eventPayloadFieldsForExistingEvent(source semanticview.Source, eventType string) (map[string]struct{}, bool) {
	if source == nil {
		return nil, false
	}
	proof := semanticview.ResolveFlowEventProof(source, "", strings.TrimSpace(eventType))
	if !proof.HasSchema {
		return nil, false
	}
	out := map[string]struct{}{}
	collectPayloadFields("", proof.Entry.Payload.Properties, out)
	return out, true
}

func collectPayloadFields(prefix string, fields map[string]runtimecontracts.EventFieldSpec, out map[string]struct{}) {
	for name := range fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		full := name
		if prefix != "" {
			full = prefix + "." + name
		}
		out[full] = struct{}{}
	}
}

func payloadFieldExists(fields map[string]struct{}, ref string) bool {
	ref = strings.TrimSpace(ref)
	for field := range fields {
		if ref == field || strings.HasPrefix(ref, field+".") || strings.HasPrefix(field, ref+".") {
			return true
		}
	}
	return false
}

func derivedAccumulationSource(sourceEvent string) bool {
	sourceEvent = strings.TrimSpace(sourceEvent)
	switch {
	case sourceEvent == "":
		return false
	case strings.HasPrefix(sourceEvent, "fan_out."):
		return true
	default:
		return false
	}
}
