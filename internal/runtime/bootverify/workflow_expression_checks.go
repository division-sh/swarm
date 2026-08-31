package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func checkConditionExpressionValidation(c *checkerContext) []Finding { return c.conditionExpressions() }
func checkDataAccumulationExpressionValidation(c *checkerContext) []Finding {
	return c.dataAccumulationExpressions()
}
func checkEmitFieldExpressionValidation(c *checkerContext) []Finding {
	return c.emitFieldExpressions()
}
func checkExpressionFieldReferenceValidation(c *checkerContext) []Finding {
	return c.expressionFieldReferences()
}

func (c *checkerContext) conditionExpressions() []Finding {
	if c.conditionExprLoaded {
		return c.conditionExprFindings
	}
	c.conditionExprLoaded = true
	for _, record := range wave1ScopedNodeRecords(c.source) {
		nodeRef, _ := record.Identity()
		nodeID := nodeRef.Key()
		node := record.Entry
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			payloadType, payloadTypeErr := executablePayloadStructuralType(c.source, nodeRef, eventType)
			if handler.Guard != nil {
				if err := validateGuardOnFailLocal(handler.Guard); err != nil {
					c.conditionExprFindings = append(c.conditionExprFindings, Finding{
						CheckID:  "condition_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s guard %v", nodeID, eventType, err),
						Location: nodeID,
					})
				}
			}
			for _, cond := range handlerConditions(handler) {
				expr := cond.Expression
				options := workflowexpr.ValueExpressionOptions{PayloadType: payloadType}
				if source := conditionCollectionSource(handler, cond.Context); source != "" {
					itemType, err := executableCollectionItemStructuralType(c.source, nodeRef, eventType, source)
					if err != nil {
						c.conditionExprFindings = append(c.conditionExprFindings, Finding{
							CheckID: "condition_expression_validation", Severity: SeverityHardInvalidity,
							Message: fmt.Sprintf("node %s handler %s condition %q has no exact item schema: %v", nodeID, eventType, expr, err), Location: nodeID,
						})
						continue
					}
					options.ItemType = itemType
				}
				if conditionMissingRecognizedPrefixLocal(expr, cond.Context) {
					c.conditionExprFindings = append(c.conditionExprFindings, Finding{
						CheckID:  "condition_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s condition %q missing required prefix", nodeID, eventType, expr),
						Location: nodeID,
					})
				}
				if payloadTypeErr != nil && workflowexpr.ExpressionReferencesRoot(expr, "payload") {
					c.conditionExprFindings = append(c.conditionExprFindings, Finding{
						CheckID: "condition_expression_validation", Severity: SeverityHardInvalidity,
						Message: fmt.Sprintf("node %s handler %s condition %q has no exact payload schema: %v", nodeID, eventType, expr, payloadTypeErr), Location: nodeID,
					})
					continue
				}
				if err := validateConditionCELLocal(expr, cond.Context, options); err != nil {
					c.conditionExprFindings = append(c.conditionExprFindings, Finding{
						CheckID:  "condition_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s CEL parse failed for %q: %v", nodeID, eventType, expr, err),
						Location: nodeID,
					})
				}
			}
		}
	}
	return c.conditionExprFindings
}

func conditionCollectionSource(handler runtimecontracts.SystemNodeEventHandler, context runtimepipeline.WorkflowConditionContext) string {
	switch context {
	case runtimepipeline.WorkflowConditionContextFilter:
		if handler.Filter != nil {
			return firstNonEmptyLocal(handler.Filter.ItemsFrom, handler.Filter.Source)
		}
	case runtimepipeline.WorkflowConditionContextCount:
		if handler.Count != nil {
			return firstNonEmptyLocal(handler.Count.ItemsFrom, handler.Count.Source)
		}
	}
	return ""
}

func firstNonEmptyLocal(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func executableCollectionItemStructuralType(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType, itemsFrom string) (*runtimecontracts.ResolvedCatalogType, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, fmt.Errorf("collection item schema requires a loaded contract bundle")
	}
	item, err := bundle.ResolveWorkflowCollectionItemType(node, eventType, itemsFrom)
	if err != nil {
		return nil, err
	}
	if item.Kind == runtimecontracts.CatalogTypeDynamic {
		return nil, fmt.Errorf("collection item schema is dynamic")
	}
	return &item, nil
}

func (c *checkerContext) dataAccumulationExpressions() []Finding {
	if c.dataAccumulationExprLoaded {
		return c.dataAccumulationExprFindings
	}
	c.dataAccumulationExprLoaded = true
	for _, record := range wave1ScopedNodeRecords(c.source) {
		nodeRef, _ := record.Identity()
		nodeID := nodeRef.Key()
		node := record.Entry
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			payloadType, payloadTypeErr := executablePayloadStructuralType(c.source, nodeRef, eventType)
			for _, expr := range handlerExecutableReaderExpressionsForSource(c.source, nodeRef, eventType, handler) {
				if expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation {
					continue
				}
				if payloadTypeErr != nil && workflowexpr.ExpressionReferencesRoot(expr.Expression, "payload") {
					c.dataAccumulationExprFindings = append(c.dataAccumulationExprFindings, Finding{CheckID: "data_accumulation_expression_validation", Severity: SeverityHardInvalidity, Message: fmt.Sprintf("node %s handler %s %s has no exact payload schema: %v", nodeID, eventType, expr.Kind, payloadTypeErr), Location: nodeID})
					continue
				}
				options := executableReaderExpressionOptions(expr, payloadType)
				if err := workflowexpr.ValidateValueExpressionWithOptions(expr.Expression, options); err != nil {
					c.dataAccumulationExprFindings = append(c.dataAccumulationExprFindings, Finding{
						CheckID:  "data_accumulation_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s %s %q is invalid for data_accumulation.expression: %v", nodeID, eventType, expr.Kind, expr.Expression, err),
						Location: nodeID,
					})
				}
			}
		}
	}
	return c.dataAccumulationExprFindings
}

func (c *checkerContext) emitFieldExpressions() []Finding {
	if c.emitFieldExprLoaded {
		return c.emitFieldExprFindings
	}
	c.emitFieldExprLoaded = true
	for _, record := range wave1ScopedNodeRecords(c.source) {
		nodeRef, _ := record.Identity()
		nodeID := nodeRef.Key()
		node := record.Entry
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			payloadType, payloadTypeErr := executablePayloadStructuralType(c.source, nodeRef, eventType)
			for _, expr := range handlerExecutableReaderExpressionsForSource(c.source, nodeRef, eventType, handler) {
				if expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleEmitFields &&
					expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleGuardEscalation {
					continue
				}
				if payloadTypeErr != nil && workflowexpr.ExpressionReferencesRoot(expr.Expression, "payload") {
					c.emitFieldExprFindings = append(c.emitFieldExprFindings, Finding{CheckID: "emit_field_expression_validation", Severity: SeverityHardInvalidity, Message: fmt.Sprintf("node %s handler %s %s has no exact payload schema: %v", nodeID, eventType, expr.Kind, payloadTypeErr), Location: nodeID})
					continue
				}
				options := executableReaderExpressionOptions(expr, payloadType)
				if err := workflowexpr.ValidateValueExpressionWithOptions(expr.Expression, options); err != nil {
					c.emitFieldExprFindings = append(c.emitFieldExprFindings, Finding{
						CheckID:  "emit_field_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s %s %q is invalid for emit.fields: %v", nodeID, eventType, expr.Kind, expr.Expression, err),
						Location: nodeID,
					})
				}
			}
		}
	}
	return c.emitFieldExprFindings
}

func (c *checkerContext) expressionFieldReferences() []Finding {
	if c.entityRefLoaded {
		return c.entityRefFindings
	}
	c.entityRefLoaded = true

	seen := map[string]struct{}{}
	for _, record := range wave1ScopedNodeRecords(c.source) {
		nodeRef, _ := record.Identity()
		nodeID := nodeRef.Key()
		nodeLabel := executableNodeDiagnostic(nodeRef)
		flowID := nodeRef.FlowID()
		node := record.Entry
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, expr := range handlerExecutableReaderExpressionsForSource(c.source, nodeRef, eventType, handler) {
				for _, ref := range runtimepipeline.WorkflowEntityReferences(expr.Expression) {
					ref = strings.TrimSpace(ref)
					if ref == "" {
						continue
					}
					leaf, err := wave1ResolveEntityPath(c.source, flowID, ref)
					if err != nil {
						key := strings.Join([]string{flowID, nodeID, eventType, expr.Kind, ref}, "|")
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						c.entityRefFindings = append(c.entityRefFindings, Finding{
							CheckID:  "expression_field_reference_validation",
							Severity: SeverityHardInvalidity,
							Message:  fmt.Sprintf("%s handler %s references entity.%s in %s but %v", nodeLabel, eventType, ref, expr.Kind, err),
							Location: nodeID,
						})
						continue
					}
					if expr.RequireScalarEntityLeaf && leaf.Kind != "scalar" && leaf.Kind != "enum" {
						key := strings.Join([]string{flowID, nodeID, eventType, expr.Kind, ref, leaf.Kind}, "|")
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						c.entityRefFindings = append(c.entityRefFindings, Finding{
							CheckID:  "expression_field_reference_validation",
							Severity: SeverityHardInvalidity,
							Message:  fmt.Sprintf("%s handler %s query filter path entity.%s must resolve to scalar or enum leaf, got %s", nodeLabel, eventType, ref, leaf.Type),
							Location: nodeID,
						})
					}
				}
				for _, ref := range runtimepipeline.WorkflowPlatformEntityReferences(expr.Expression) {
					ref = strings.TrimSpace(ref)
					if ref == "" {
						continue
					}
					leaf, err := wave1ResolvePlatformEntityPath(ref)
					if err != nil {
						key := strings.Join([]string{flowID, nodeID, eventType, expr.Kind, "_entity." + ref}, "|")
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						c.entityRefFindings = append(c.entityRefFindings, Finding{
							CheckID:  "expression_field_reference_validation",
							Severity: SeverityHardInvalidity,
							Message:  fmt.Sprintf("flow %s node %s handler %s references _entity.%s in %s but %v", defaultFlowLabel(flowID), nodeID, eventType, ref, expr.Kind, err),
							Location: nodeID,
						})
						continue
					}
					if expr.RequireScalarEntityLeaf && leaf.Kind != "scalar" && leaf.Kind != "enum" {
						key := strings.Join([]string{flowID, nodeID, eventType, expr.Kind, "_entity." + ref, leaf.Kind}, "|")
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						c.entityRefFindings = append(c.entityRefFindings, Finding{
							CheckID:  "expression_field_reference_validation",
							Severity: SeverityHardInvalidity,
							Message:  fmt.Sprintf("flow %s node %s handler %s query filter path _entity.%s must resolve to scalar or enum leaf, got %s", defaultFlowLabel(flowID), nodeID, eventType, ref, leaf.Type),
							Location: nodeID,
						})
					}
				}
			}
		}
	}

	return c.entityRefFindings
}

type expressionReference struct {
	Kind                    string
	Expression              string
	Phase                   runtimepipeline.WorkflowEntityFieldLifecyclePhase
	HandlerField            string
	RuleCollection          string
	RuleField               string
	RuleIndex               int
	HasRuleIndex            bool
	RequireScalarEntityLeaf bool
	AllowBareItem           bool
	ItemAlias               string
	AllowJoin               bool
	ItemType                runtimecontracts.ResolvedCatalogType
	HasItemType             bool
	ResultType              runtimecontracts.ResolvedCatalogType
	HasResultType           bool
	ResultOptional          bool
}

type handlerCondition struct {
	Expression string
	Context    runtimepipeline.WorkflowConditionContext
}

func handlerConditions(handler runtimecontracts.SystemNodeEventHandler) []handlerCondition {
	out := make([]handlerCondition, 0, 10)
	if handler.Guard != nil {
		for _, item := range handler.Guard.EffectiveChecks() {
			if check := strings.TrimSpace(item.Check); check != "" {
				out = append(out, handlerCondition{
					Expression: check,
					Context:    runtimepipeline.WorkflowConditionContextGuard,
				})
			}
		}
	}
	for _, rule := range handler.Rules {
		if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			out = append(out, handlerCondition{
				Expression: condition,
				Context:    runtimepipeline.WorkflowConditionContextRule,
			})
		}
	}
	for _, rule := range handler.OnComplete {
		if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			out = append(out, handlerCondition{
				Expression: condition,
				Context:    runtimepipeline.WorkflowConditionContextOnComplete,
			})
		}
	}
	if handler.Filter != nil {
		if condition := strings.TrimSpace(handler.Filter.Condition); condition != "" {
			out = append(out, handlerCondition{
				Expression: condition,
				Context:    runtimepipeline.WorkflowConditionContextFilter,
			})
		}
	}
	if handler.Count != nil {
		if condition := strings.TrimSpace(handler.Count.Condition); condition != "" {
			out = append(out, handlerCondition{
				Expression: condition,
				Context:    runtimepipeline.WorkflowConditionContextCount,
			})
		}
	}
	return out
}

func handlerEmitExpressionsForSource(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler) []expressionReference {
	out := make([]expressionReference, 0, 8)
	appendSpec := func(kindPrefix, siteKey string, spec runtimecontracts.EmitSpec, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase, itemAlias string) {
		if spec.Empty() {
			return
		}
		if bundle, ok := semanticview.Bundle(source); ok && bundle != nil {
			lowered, err := bundle.LowerEmitSpecFields(runtimecontracts.EmitFieldLoweringContext{
				Node:             node,
				TriggerEventType: eventType,
				Site:             siteKey,
				SchemaProvider:   source,
			}, spec)
			if err != nil {
				return
			}
			spec = lowered
		}
		for key, value := range spec.Fields {
			expr := strings.TrimSpace(value.CEL)
			if expr == "" && value.Kind == runtimecontracts.ExpressionKindRef {
				expr = strings.TrimSpace(value.Ref)
			}
			if expr == "" {
				continue
			}
			ref := expressionReference{
				Kind:       kindPrefix + " emit field " + strings.TrimSpace(key),
				Expression: expr,
				Phase:      phase,
				ItemAlias:  strings.TrimSpace(itemAlias),
				AllowJoin:  strings.HasPrefix(strings.TrimSpace(kindPrefix), "handler.join."),
			}
			resolution := semanticview.ResolveEventSchema(source, node.FlowID(), spec.EventType())
			if resolution.HasStructural {
				if field, ok := resolution.StructuralType.FieldPath(key); ok {
					ref.ResultType = field.Type.Clone()
					ref.HasResultType = true
					ref.ResultOptional = field.IsOptional
				}
			}
			out = append(out, ref)
		}
	}
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler) {
		before := len(out)
		appendSpec(site.Source, site.SiteKey, site.Spec, runtimepipeline.WorkflowEntityFieldLifecycleEmitFields, site.ItemAlias)
		if plan, ok := fanOutPlanForEmitSite(source, node, eventType, site); ok {
			for index := before; index < len(out); index++ {
				out[index].ItemType = plan.ItemType.Clone()
				out[index].HasItemType = true
			}
		}
	}
	if handler.Guard != nil {
		if failureSpec, err := handler.Guard.FailureSpec(); err == nil {
			if parsed, err := runtimeengine.GuardFailureFromSpec(failureSpec); err == nil && parsed.Action == runtimeengine.GuardFailureEscalate {
				appendSpec("guard escalation", "guard.on_fail.escalate", failureSpec.EscalationEmitSpec(), runtimepipeline.WorkflowEntityFieldLifecycleGuardEscalation, "")
			}
		}
	}
	return out
}

func fanOutPlanForEmitSite(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, site runtimecontracts.HandlerDeclarativeEmitSite) (runtimecontracts.FanOutCompiledPlan, bool) {
	if source == nil {
		return runtimecontracts.FanOutCompiledPlan{}, false
	}
	var kind runtimecontracts.FanOutSiteKind
	index := site.RuleIndex
	switch strings.TrimSpace(site.Source) {
	case "handler.fan_out.emit":
		kind = runtimecontracts.FanOutSiteHandler
		index = -1
	case "handler.rules.fan_out.emit":
		kind = runtimecontracts.FanOutSiteRule
	case "handler.on_complete.fan_out.emit":
		kind = runtimecontracts.FanOutSiteOnComplete
	default:
		return runtimecontracts.FanOutCompiledPlan{}, false
	}
	ref, err := runtimecontracts.NewFanOutSiteRef(node, eventType, kind, index)
	if err != nil {
		return runtimecontracts.FanOutCompiledPlan{}, false
	}
	return source.FanOutPlanForSite(ref)
}

func handlerEntityFieldWriters(handler runtimecontracts.SystemNodeEventHandler) map[string]struct{} {
	out := map[string]struct{}{}
	addWriter := func(target string) {
		if field, ok := runtimepipeline.WorkflowEntityFieldNameFromTarget(target); ok {
			out[field] = struct{}{}
		}
	}
	var addRuleWriters func(rule runtimecontracts.HandlerRuleEntry)
	addRuleWriters = func(rule runtimecontracts.HandlerRuleEntry) {
		for _, write := range rule.DataAccumulation.Writes {
			addWriter(write.Target())
		}
		if rule.Compute != nil {
			addWriter(rule.Compute.StoreAs)
		}
	}
	if handler.Query != nil {
		addWriter(handler.Query.StoreAs)
	}
	if gateNameLocal(handler.SetsGate) != "" {
		out["gates"] = struct{}{}
	}
	for _, write := range handler.DataAccumulation.Writes {
		addWriter(write.Target())
	}
	if handler.Compute != nil {
		addWriter(handler.Compute.StoreAs)
	}
	if handler.Filter != nil {
		addWriter(handler.Filter.StoreAs)
	}
	if handler.GroupBy != nil {
		addWriter(handler.GroupBy.StoreAs)
	}
	if handler.Reduce != nil {
		addWriter(handler.Reduce.StoreAs)
	}
	if handler.Count != nil {
		addWriter(handler.Count.StoreAs)
	}
	if handler.Clear != nil {
		for _, target := range handler.Clear.Targets {
			addWriter(target)
		}
	}
	for _, rule := range handler.Rules {
		addRuleWriters(rule)
	}
	for _, rule := range handler.OnComplete {
		addRuleWriters(rule)
	}
	return out
}

func validateGuardOnFailLocal(spec *runtimecontracts.GuardSpec) error {
	failureSpec, err := spec.FailureSpec()
	if err != nil {
		return err
	}
	parsed, err := runtimeengine.GuardFailureFromSpec(failureSpec)
	if err != nil {
		return err
	}
	switch parsed.Action {
	case runtimeengine.GuardFailureReject,
		runtimeengine.GuardFailureDiscard,
		runtimeengine.GuardFailureKill:
		return nil
	case runtimeengine.GuardFailureEscalate:
		if strings.TrimSpace(parsed.EventType) == "" {
			return fmt.Errorf("on_fail escalate requires event type")
		}
		return nil
	default:
		return fmt.Errorf("on_fail %q is not supported", failureSpec.Action)
	}
}

func conditionMissingRecognizedPrefixLocal(expression string, context runtimepipeline.WorkflowConditionContext) bool {
	return runtimepipeline.WorkflowConditionMissingRecognizedPrefix(expression, context)
}

func validateConditionCELLocal(expression string, context runtimepipeline.WorkflowConditionContext, opts workflowexpr.ValueExpressionOptions) error {
	return runtimepipeline.ValidateConditionCELWithOptions(expression, context, opts)
}

func executablePayloadStructuralType(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) (*runtimecontracts.ResolvedCatalogType, error) {
	if source == nil || !node.Valid() {
		return nil, fmt.Errorf("executable event source is unavailable")
	}
	eventType = strings.TrimSpace(eventType)
	if strings.Contains(eventType, "*") {
		return executableWildcardPayloadStructuralType(source, node, eventType)
	}
	resolution := semanticview.ResolveEventSchema(source, node.FlowID(), eventType)
	if !resolution.HasStructural {
		return nil, fmt.Errorf("event %s has no exact structural schema", strings.TrimSpace(eventType))
	}
	value := resolution.StructuralType.Clone()
	return &value, nil
}

func executableWildcardPayloadStructuralType(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string) (*runtimecontracts.ResolvedCatalogType, error) {
	eventType = runtimeeventidentity.Normalize(eventType)
	relations := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveTypedPubSubRelations()
	for _, issue := range relations.Issues {
		if issue.Consumer.Node.Equal(node) && runtimeeventidentity.Normalize(issue.Consumer.HandlerEvent) == eventType {
			return nil, fmt.Errorf("wildcard event %s has ambiguous producer schema authority: %s", eventType, issue.Message())
		}
	}
	seen := map[string]struct{}{}
	var selected *runtimecontracts.ResolvedCatalogType
	for _, match := range relations.Matches {
		if !match.Consumer.Node.Equal(node) || runtimeeventidentity.Normalize(match.Consumer.HandlerEvent) != eventType {
			continue
		}
		key := strings.TrimSpace(match.Event.EventKey())
		if key == "" {
			continue
		}
		identity := strings.TrimSpace(match.Producer.FlowID) + "\x00" + key
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		resolution := semanticview.ResolveEventSchema(source, match.Producer.FlowID, key)
		if !resolution.HasStructural {
			return nil, fmt.Errorf("wildcard event %s producer %s has no exact structural schema", eventType, key)
		}
		candidate := resolution.StructuralType.Clone()
		if selected == nil {
			selected = &candidate
			continue
		}
		if !runtimecontracts.StructuralCatalogTypesEqual(*selected, candidate) {
			return nil, fmt.Errorf("wildcard event %s expands to incompatible producer schemas", eventType)
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("wildcard event %s has no finite producer schema expansion", eventType)
	}
	return selected, nil
}

func executableReaderExpressionOptions(expr expressionReference, payloadType *runtimecontracts.ResolvedCatalogType) workflowexpr.ValueExpressionOptions {
	options := workflowexpr.ValueExpressionOptions{
		AllowBareItem: expr.AllowBareItem, ItemAlias: expr.ItemAlias, AllowJoin: expr.AllowJoin, PayloadType: payloadType,
	}
	if expr.HasItemType {
		value := expr.ItemType.Clone()
		options.ItemType = &value
	}
	if expr.HasResultType {
		value := expr.ResultType.Clone()
		options.ResultType = &value
		options.ResultOptional = expr.ResultOptional
	}
	return options
}
