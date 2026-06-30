package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
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
				if conditionMissingRecognizedPrefixLocal(expr, cond.Context) {
					c.conditionExprFindings = append(c.conditionExprFindings, Finding{
						CheckID:  "condition_expression_validation",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s condition %q missing required prefix", nodeID, eventType, expr),
						Location: nodeID,
					})
				}
				if err := validateConditionCELLocal(expr, cond.Context); err != nil {
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

func (c *checkerContext) dataAccumulationExpressions() []Finding {
	if c.dataAccumulationExprLoaded {
		return c.dataAccumulationExprFindings
	}
	c.dataAccumulationExprLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, expr := range handlerEntityExpressions(handler) {
				if expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation {
					continue
				}
				if err := workflowexpr.ValidateValueExpressionWithOptions(expr.Expression, workflowexpr.ValueExpressionOptions{AllowBareItem: expr.AllowBareItem}); err != nil {
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
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, expr := range handlerEntityExpressions(handler) {
				if expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleEmitFields &&
					expr.Phase != runtimepipeline.WorkflowEntityFieldLifecycleGuardEscalation {
					continue
				}
				if err := workflowexpr.ValidateValueExpressionWithOptions(expr.Expression, workflowexpr.ValueExpressionOptions{AllowBareItem: expr.AllowBareItem}); err != nil {
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
	for nodeID, node := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		flowID := ""
		if sourceRef, ok := c.source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, expr := range handlerEntityExpressions(handler) {
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
							Message:  fmt.Sprintf("flow %s node %s handler %s references entity.%s in %s but %v", defaultFlowLabel(flowID), nodeID, eventType, ref, expr.Kind, err),
							Location: nodeID,
						})
						continue
					}
					if expr.Kind == "query filter" && leaf.Kind != "scalar" && leaf.Kind != "enum" {
						key := strings.Join([]string{flowID, nodeID, eventType, expr.Kind, ref, leaf.Kind}, "|")
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						c.entityRefFindings = append(c.entityRefFindings, Finding{
							CheckID:  "expression_field_reference_validation",
							Severity: SeverityHardInvalidity,
							Message:  fmt.Sprintf("flow %s node %s handler %s query filter path entity.%s must resolve to scalar or enum leaf, got %s", defaultFlowLabel(flowID), nodeID, eventType, ref, leaf.Type),
							Location: nodeID,
						})
					}
				}
			}
		}
	}

	return c.entityRefFindings
}

func createEntityFieldInitializationFinding(flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler, expr expressionReference, field string) *Finding {
	if createEntityTopLevelDataAccumulationWritesField(handler, field) && createEntityTopLevelDataAccumulationMakesFieldAvailable(expr.Phase) {
		return nil
	}
	if createEntityComputeStoresField(handler, field) && createEntityComputeMakesFieldAvailable(expr.Phase) {
		if createEntityDynamicExpectedFrom(handler.Accumulate) {
			return &Finding{
				CheckID:  "expression_field_reference_validation",
				Severity: "warning",
				Message:  fmt.Sprintf("flow %s node %s handler %s references entity.%s in %s through same-handler compute.store_as, but accumulate.expected_from %q is dynamic so initialization proof degrades on the create_entity slice", flowID, nodeID, eventType, field, expr.Kind, strings.TrimSpace(handler.Accumulate.ExpectedFrom)),
				Location: nodeID,
			}
		}
		return nil
	}
	return &Finding{
		CheckID:  "expression_field_reference_validation",
		Severity: "error",
		Message:  fmt.Sprintf("flow %s node %s handler %s references entity.%s in %s but the create_entity handler does not provably initialize %s before that position", flowID, nodeID, eventType, field, expr.Kind, field),
		Location: nodeID,
	}
}

func createEntityTopLevelDataAccumulationWritesField(handler runtimecontracts.SystemNodeEventHandler, field string) bool {
	for _, write := range handler.DataAccumulation.Writes {
		targetField, ok := runtimepipeline.WorkflowEntityFieldNameFromTarget(write.Target())
		if ok && targetField == field {
			return true
		}
	}
	return false
}

func createEntityComputeStoresField(handler runtimecontracts.SystemNodeEventHandler, field string) bool {
	if handler.Compute == nil {
		return false
	}
	targetField, ok := runtimepipeline.WorkflowEntityFieldNameFromTarget(handler.Compute.StoreAs)
	return ok && targetField == field
}

func createEntityTopLevelDataAccumulationMakesFieldAvailable(phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) bool {
	return phase == runtimepipeline.WorkflowEntityFieldLifecycleEmitFields
}

func createEntityComputeMakesFieldAvailable(phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) bool {
	switch phase {
	case runtimepipeline.WorkflowEntityFieldLifecycleOnComplete,
		runtimepipeline.WorkflowEntityFieldLifecycleRule,
		runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation,
		runtimepipeline.WorkflowEntityFieldLifecycleEmitFields:
		return true
	default:
		return false
	}
}

func createEntityDynamicExpectedFrom(spec *runtimecontracts.AccumulateSpec) bool {
	if spec == nil {
		return false
	}
	path := spec.ExpectedPath
	if path.IsZero() {
		path = runtimepaths.Parse(spec.ExpectedFrom)
	}
	return path.Root == runtimepaths.RootEntity
}

type expressionReference struct {
	Kind            string
	Expression      string
	Phase           runtimepipeline.WorkflowEntityFieldLifecyclePhase
	SelfTargetField string
	AllowBareItem   bool
}

type handlerCondition struct {
	Expression string
	Context    runtimepipeline.WorkflowConditionContext
}

func handlerConditions(handler runtimecontracts.SystemNodeEventHandler) []handlerCondition {
	out := make([]handlerCondition, 0, 10)
	if handler.Guard != nil {
		if check := strings.TrimSpace(handler.Guard.Check); check != "" {
			out = append(out, handlerCondition{
				Expression: check,
				Context:    runtimepipeline.WorkflowConditionContextGuard,
			})
		}
		for _, item := range handler.Guard.Checks {
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
				out = append(out, handlerCondition{
					Expression: condition,
					Context:    runtimepipeline.WorkflowConditionContextOnComplete,
				})
			}
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

func handlerEntityExpressions(handler runtimecontracts.SystemNodeEventHandler) []expressionReference {
	out := make([]expressionReference, 0, 16)
	if handler.Guard != nil {
		if check := strings.TrimSpace(handler.Guard.Check); check != "" {
			out = append(out, expressionReference{Kind: "guard", Expression: check, Phase: runtimepipeline.WorkflowEntityFieldLifecycleGuard})
		}
		for _, item := range handler.Guard.Checks {
			if check := strings.TrimSpace(item.Check); check != "" {
				out = append(out, expressionReference{Kind: "guard", Expression: check, Phase: runtimepipeline.WorkflowEntityFieldLifecycleGuard})
			}
		}
	}
	if condition := strings.TrimSpace(handler.Condition); condition != "" && !strings.EqualFold(condition, "else") {
		out = append(out, expressionReference{Kind: "condition", Expression: condition, Phase: runtimepipeline.WorkflowEntityFieldLifecycleRule})
	}
	appendWriteExpressions := func(kind string, writes []runtimecontracts.WorkflowDataWrite) {
		appendOperandExpression := func(operandKind string, exprValue runtimecontracts.ExpressionValue) {
			expr := strings.TrimSpace(exprValue.CEL)
			if expr == "" && exprValue.Kind == runtimecontracts.ExpressionKindRef {
				expr = strings.TrimSpace(exprValue.Ref)
			}
			if expr != "" {
				out = append(out, expressionReference{Kind: kind + " " + operandKind, Expression: expr, Phase: runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation})
			}
		}
		for _, write := range writes {
			if expr := strings.TrimSpace(write.Value.CEL); expr != "" {
				selfTarget, _ := runtimepipeline.WorkflowEntityFieldNameFromTarget(write.Target())
				out = append(out, expressionReference{
					Kind:            kind,
					Expression:      expr,
					Phase:           runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation,
					SelfTargetField: selfTarget,
				})
			}
			if write.IsContainedOperation() {
				appendOperandExpression("key", write.Key)
				appendOperandExpression("index", write.Index)
				if strings.TrimSpace(write.Value.CEL) == "" {
					appendOperandExpression("value", write.Value)
				}
			}
		}
	}
	appendRuleExpressions := func(kindPrefix string, rule runtimecontracts.HandlerRuleEntry) {
		if condition := strings.TrimSpace(rule.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			phase := runtimepipeline.WorkflowEntityFieldLifecycleRule
			if strings.Contains(kindPrefix, "on_complete") {
				phase = runtimepipeline.WorkflowEntityFieldLifecycleOnComplete
			}
			out = append(out, expressionReference{Kind: kindPrefix + " condition", Expression: condition, Phase: phase})
		}
		appendWriteExpressions(kindPrefix+" expression", rule.DataAccumulation.Writes)
		if rule.FanOut != nil {
			// Fan-out has no CEL-bearing fields today.
		}
	}

	appendWriteExpressions("expression", handler.DataAccumulation.Writes)
	for _, rule := range handler.Rules {
		appendRuleExpressions("rule", rule)
	}
	for _, rule := range handler.OnComplete {
		appendRuleExpressions("on_complete", rule)
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			appendRuleExpressions("accumulate.on_complete", rule)
		}
		if handler.Accumulate.OnTimeout != nil {
			appendRuleExpressions("accumulate.on_timeout", *handler.Accumulate.OnTimeout)
		}
	}
	if handler.Filter != nil {
		if predicate := strings.TrimSpace(handler.Filter.Predicate); predicate != "" {
			out = append(out, expressionReference{Kind: "filter predicate", Expression: predicate, Phase: runtimepipeline.WorkflowEntityFieldLifecycleFilter})
		}
		if condition := strings.TrimSpace(handler.Filter.Condition); condition != "" {
			out = append(out, expressionReference{Kind: "filter condition", Expression: condition, Phase: runtimepipeline.WorkflowEntityFieldLifecycleFilter})
		}
	}
	if handler.Count != nil {
		if condition := strings.TrimSpace(handler.Count.Condition); condition != "" {
			out = append(out, expressionReference{Kind: "count condition", Expression: condition, Phase: runtimepipeline.WorkflowEntityFieldLifecycleCount})
		}
	}
	appendEmitExpressions := func(kindPrefix string, spec runtimecontracts.EmitSpec, allowBareItem bool) {
		for key, value := range spec.Fields {
			expr := strings.TrimSpace(value.CEL)
			if expr == "" && value.Kind == runtimecontracts.ExpressionKindRef {
				expr = strings.TrimSpace(value.Ref)
			}
			if expr != "" {
				out = append(out, expressionReference{
					Kind:          kindPrefix + " emit field " + strings.TrimSpace(key),
					Expression:    expr,
					Phase:         runtimepipeline.WorkflowEntityFieldLifecycleEmitFields,
					AllowBareItem: allowBareItem,
				})
			}
		}
	}
	appendEmitExpressions("handler", handler.Emit, false)
	if handler.Guard != nil {
		if failureSpec, err := handler.Guard.FailureSpec(); err == nil {
			appendEmitExpressions("guard escalation", failureSpec.EscalationEmitSpec(), false)
			if len(out) > 0 {
				for i := range out {
					if strings.HasPrefix(out[i].Kind, "guard escalation emit field ") {
						out[i].Phase = runtimepipeline.WorkflowEntityFieldLifecycleGuardEscalation
					}
				}
			}
		}
	}
	if handler.FanOut != nil {
		appendEmitExpressions("fan_out", handler.FanOut.Emit, true)
	}
	for _, rule := range handler.Rules {
		appendEmitExpressions("rule", rule.Emit, false)
		if rule.FanOut != nil {
			appendEmitExpressions("rule fan_out", rule.FanOut.Emit, true)
		}
	}
	for _, rule := range handler.OnComplete {
		appendEmitExpressions("on_complete", rule.Emit, false)
		if rule.FanOut != nil {
			appendEmitExpressions("on_complete fan_out", rule.FanOut.Emit, true)
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			appendEmitExpressions("accumulate.on_complete", rule.Emit, false)
			if rule.FanOut != nil {
				appendEmitExpressions("accumulate.on_complete fan_out", rule.FanOut.Emit, true)
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			appendEmitExpressions("accumulate.on_timeout", handler.Accumulate.OnTimeout.Emit, false)
			if handler.Accumulate.OnTimeout.FanOut != nil {
				appendEmitExpressions("accumulate.on_timeout fan_out", handler.Accumulate.OnTimeout.FanOut.Emit, true)
			}
		}
	}
	if handler.Query != nil {
		appendQueryExpressions(&out, *handler.Query)
	}
	if handler.Reduce != nil {
		for key, value := range handler.Reduce.Params {
			if expr := strings.TrimSpace(value.CEL); expr != "" {
				out = append(out, expressionReference{Kind: "reduce param " + strings.TrimSpace(key), Expression: expr, Phase: runtimepipeline.WorkflowEntityFieldLifecycleReduce})
			}
		}
	}
	for _, branch := range handler.Branch {
		if condition := strings.TrimSpace(branch.Condition); condition != "" && !strings.EqualFold(condition, "else") {
			out = append(out, expressionReference{Kind: "branch condition", Expression: condition, Phase: runtimepipeline.WorkflowEntityFieldLifecycleRule})
		}
		if branch.Then != nil {
			appendRuleExpressions("branch.then", *branch.Then)
		}
		if branch.Else != nil {
			appendRuleExpressions("branch.else", *branch.Else)
		}
	}
	return out
}

func appendQueryExpressions(out *[]expressionReference, query runtimecontracts.QuerySpec) {
	if filter := strings.TrimSpace(query.Filter); filter != "" {
		*out = append(*out, expressionReference{Kind: "query filter", Expression: filter, Phase: runtimepipeline.WorkflowEntityFieldLifecycleGuard})
	}
	for _, nested := range query.Queries {
		appendQueryExpressions(out, nested)
	}
}

func availableEntityFieldsForExpression(handler runtimecontracts.SystemNodeEventHandler, expr expressionReference) map[string]struct{} {
	switch expr.Phase {
	case runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeDataAccumulation(handler)
	case runtimepipeline.WorkflowEntityFieldLifecycleGuard,
		runtimepipeline.WorkflowEntityFieldLifecycleGuardEscalation:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeCondition(handler, runtimepipeline.WorkflowConditionContextGuard)
	case runtimepipeline.WorkflowEntityFieldLifecycleFilter:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeCondition(handler, runtimepipeline.WorkflowConditionContextFilter)
	case runtimepipeline.WorkflowEntityFieldLifecycleCount:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeCondition(handler, runtimepipeline.WorkflowConditionContextCount)
	case runtimepipeline.WorkflowEntityFieldLifecycleOnComplete:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeCondition(handler, runtimepipeline.WorkflowConditionContextOnComplete)
	case runtimepipeline.WorkflowEntityFieldLifecycleRule:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeCondition(handler, runtimepipeline.WorkflowConditionContextRule)
	case runtimepipeline.WorkflowEntityFieldLifecycleReduce:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeDataAccumulation(handler)
	case runtimepipeline.WorkflowEntityFieldLifecycleEmitFields:
		return runtimepipeline.WorkflowEntityFieldsAvailableBeforeEmitFields(handler)
	default:
		return map[string]struct{}{}
	}
}

func flowEntityFieldWriters(nodes map[string]runtimecontracts.SystemNodeContract) map[string]struct{} {
	out := map[string]struct{}{}
	for _, node := range nodes {
		for _, handler := range node.EventHandlers {
			for field := range handlerEntityFieldWriters(handler) {
				out[field] = struct{}{}
			}
		}
	}
	return out
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
	var addQueryWriters func(query *runtimecontracts.QuerySpec)
	addQueryWriters = func(query *runtimecontracts.QuerySpec) {
		if query == nil {
			return
		}
		addWriter(query.StoreAs)
		for i := range query.Queries {
			addQueryWriters(&query.Queries[i])
		}
	}
	addQueryWriters(handler.Query)
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
		addWriter(handler.Clear.Target)
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
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			addRuleWriters(rule)
		}
		if handler.Accumulate.OnTimeout != nil {
			addRuleWriters(*handler.Accumulate.OnTimeout)
		}
	}
	for _, branch := range handler.Branch {
		if branch.Then != nil {
			addRuleWriters(*branch.Then)
		}
		if branch.Else != nil {
			addRuleWriters(*branch.Else)
		}
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

func validateConditionCELLocal(expression string, context runtimepipeline.WorkflowConditionContext) error {
	return runtimepipeline.ValidateConditionCEL(expression, context)
}
