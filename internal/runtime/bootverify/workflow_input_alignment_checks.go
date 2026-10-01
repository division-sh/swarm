package bootverify

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func checkConditionPolicyAlignment(c *checkerContext) []Finding { return c.conditionPolicyAlignment() }
func checkConditionPayloadAlignment(c *checkerContext) []Finding {
	return c.conditionPayloadAlignment()
}
func checkDataAccumulationSourceAlignment(c *checkerContext) []Finding {
	return c.dataAccumulationSourceAlignment()
}

type payloadFieldCoverageSite struct {
	Node         runtimeidentity.ExecutableNode
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
	check := func(expression, location string, policy runtimecontracts.PolicyDocument, options workflowexpr.ValueExpressionOptions, reader *expressionReference) {
		options.DeclaredPolicy = policyValueMap(policy)
		var err error
		if reader != nil && reader.HasConditionContext {
			err = validateConditionCELLocal(expression, reader.ConditionContext, options)
		} else {
			err = workflowexpr.ValidateValueExpressionWithOptions(expression, options)
		}
		var missing *workflowexpr.PolicyReferenceError
		if errors.As(err, &missing) {
			c.conditionPolicyFindings = append(c.conditionPolicyFindings, Finding{
				CheckID: "condition_policy_alignment", Severity: SeverityHardInvalidity,
				Message: location + ": " + missing.Error(), Location: location,
				Remediation: "Declare the exact policy key in this flow or an ancestor, correct the reference, or explicitly handle absence with has/optional selection.",
			})
		}
		// Expression admission failures belong to the existing typed/context checks.
	}
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		policy := c.source.ResolvedPolicyForExecutableNode(node)
		for event, handler := range c.source.ExecutableNodeEventHandlers(node) {
			if handler.Guard != nil && strings.TrimSpace(handler.Guard.PolicyRef) != "" {
				for _, guard := range handler.Guard.EffectiveChecks() {
					if guard.EffectiveIdentity() == "state_in_phase" {
						if _, ok := semanticview.PolicyValueForFlow(c.source, node.FlowPath(), handler.Guard.PolicyRef); !ok {
							c.conditionPolicyFindings = append(c.conditionPolicyFindings, Finding{
								CheckID: "condition_policy_alignment", Severity: SeverityHardInvalidity,
								Message: "state_in_phase guard references undeclared policy selector " + handler.Guard.PolicyRef, Location: node.Key(),
								Remediation: "Declare the selected policy value in this flow or an ancestor, or correct guard.policy_ref.",
							})
						}
					}
				}
			}
			payload, _ := executablePayloadStructuralType(c.source, node, event)
			entity, _ := semanticview.ResolveEntityStructuralType(c.source, node.FlowPath())
			for _, reader := range c.entityAssignmentReaders(node, event, handler) {
				if len(reader.RequiredEntityPaths) != 0 {
					continue
				}
				options := executableReaderExpressionOptions(reader, payload, entity)
				if from := reader.ConditionCollectionSource; from != "" {
					options.ItemType, _ = executableCollectionItemStructuralType(c.source, node, event, handler, from)
				}
				check(reader.Expression, node.Key()+" handler "+event+" "+reader.Kind, policy, options, &reader)
			}
		}
	}
	for _, timer := range c.source.WorkflowTimers() {
		policy := c.source.ResolvedPolicyForFlow(timer.OwningFlowID())
		for _, key := range pipeline.WorkflowTimerPolicyReferences(timer.Delay) {
			if _, ok := policy.Values[key]; !ok {
				c.conditionPolicyFindings = append(c.conditionPolicyFindings, Finding{
					CheckID: "condition_policy_alignment", Severity: SeverityHardInvalidity,
					Message: "timer " + timer.ID + " references undeclared exact policy key " + fmt.Sprintf("%q", key), Location: timer.ID,
					Remediation: "Declare the exact duration policy key in the timer's flow or an ancestor, or correct the existing timer placeholder.",
				})
			}
		}
	}
	for _, gate := range c.source.WorkflowGates() {
		entity, _ := semanticview.ResolveEntityStructuralType(c.source, gate.FlowID)
		options := workflowexpr.ValueExpressionOptions{EntityType: entity}
		if analysis := c.entityAssignmentAnalysis(gate.FlowID); analysis != nil {
			options.KnownPresence = engine.EntityAssignmentPresencePaths(analysis.StageFacts(gate.Stage))
		}
		for field, value := range gate.Context {
			if value.HasCELValue() {
				check(value.CEL, stageGateLocation(gate.FlowID, gate.Stage, gate.Decision)+" context "+field, c.source.ResolvedPolicyForFlow(gate.FlowID), options, nil)
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
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		nodeID := node.Key()
		for eventType, handler := range c.source.ExecutableNodeEventHandlers(node) {
			eventType = strings.TrimSpace(eventType)
			payloadFields, eventExists := executableNodeEventPayloadFields(c.source, node, eventType)
			if !eventExists {
				continue
			}
			for _, cond := range handlerConditionExpressionsForSource(c.source, node, eventType, handler) {
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

func (c *checkerContext) dataAccumulationSourceAlignment() []Finding {
	if c.dataAccumulationSourceLoaded {
		return c.dataAccumulationSourceFindings
	}
	c.dataAccumulationSourceLoaded = true
	for _, transition := range c.source.DerivedHandlerTransitions() {
		sourceEvent := strings.TrimSpace(transition.DataAccumulation.SourceEvent)
		if sourceEvent == "" {
			continue
		}
		if sourceEvent == strings.TrimSpace(transition.EventType) || derivedAccumulationSource(sourceEvent) {
			continue
		}
		c.dataAccumulationSourceFindings = append(c.dataAccumulationSourceFindings, Finding{
			CheckID:  "data_accumulation_source_alignment",
			Severity: "error",
			Message:  fmt.Sprintf("handler transition %s data_accumulation.source_event %s does not match handler event %s", transition.ID, sourceEvent, transition.EventType),
			Location: strings.TrimSpace(transition.ID),
		})
	}
	return c.dataAccumulationSourceFindings
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
		sourceFields, sourceEventExists := executableNodeEventPayloadFields(c.source, site.Node, sourceEvent)
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
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		nodeID := node.Key()
		flowID := node.FlowPath()
		for eventType, handler := range source.ExecutableNodeEventHandlers(node) {
			eventType = strings.TrimSpace(eventType)
			add := func(scope string, accumulation runtimecontracts.WorkflowDataAccumulation) {
				if !accumulation.HasWrites() {
					return
				}
				out = append(out, payloadFieldCoverageSite{
					Node:         node,
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
	if write.Operation == runtimecontracts.WorkflowDataOperationClear {
		return nil
	}
	if write.IsContainedOperation() {
		out := make([]dataAccumulationPayloadSourceRef, 0)
		out = append(out, expressionPayloadSourceRefs(write.Key, "key")...)
		out = append(out, expressionPayloadSourceRefs(write.Index, "index")...)
		out = append(out, expressionPayloadSourceRefs(write.Value, "value")...)
		return out
	}
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

func expressionPayloadSourceRefs(expr runtimecontracts.ExpressionValue, label string) []dataAccumulationPayloadSourceRef {
	if expr.IsZero() || expr.HasLiteralValue() {
		return nil
	}
	if cel := strings.TrimSpace(expr.CEL); cel != "" {
		refs := payloadReferences(cel)
		out := make([]dataAccumulationPayloadSourceRef, 0, len(refs))
		for _, ref := range refs {
			out = append(out, dataAccumulationPayloadSourceRef{
				Field:       ref,
				Description: strings.TrimSpace(label) + " payload." + ref,
			})
		}
		return out
	}
	if ref := strings.TrimSpace(expr.Ref); strings.HasPrefix(ref, "payload.") {
		field := strings.TrimPrefix(ref, "payload.")
		return []dataAccumulationPayloadSourceRef{{
			Field:       field,
			Description: strings.TrimSpace(label) + " " + ref,
		}}
	}
	return nil
}

func policyValueMap(policy runtimecontracts.PolicyDocument) map[string]any {
	out := make(map[string]any, len(policy.Values))
	for key, value := range policy.Values {
		out[key] = value.Value
	}
	return out
}

var bootverifyPayloadReferencePattern = regexp.MustCompile(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`)

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

func executableNodeEventPayloadFields(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) (map[string]struct{}, bool) {
	if source == nil || !node.Valid() {
		return nil, false
	}
	structural, err := executablePayloadStructuralType(source, node, strings.TrimSpace(eventType))
	if err != nil || structural == nil {
		return nil, false
	}
	out := map[string]struct{}{}
	collectStructuralPayloadFields("", *structural, out)
	return out, true
}

func collectStructuralPayloadFields(prefix string, value runtimecontracts.ResolvedCatalogType, out map[string]struct{}) {
	if value.Kind != runtimecontracts.CatalogTypeObject {
		return
	}
	for _, field := range value.Fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			continue
		}
		full := name
		if prefix != "" {
			full = prefix + "." + name
		}
		out[full] = struct{}{}
		collectStructuralPayloadFields(full, field.Type, out)
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
