package pipeline

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type handlerExecutableReaderCollector func(*[]WorkflowExecutableReader, executableReaderContext, runtimecontracts.SystemNodeEventHandler)
type handlerRuleExecutableReaderCollector func(*[]WorkflowExecutableReader, executableReaderContext, string, runtimecontracts.HandlerRuleEntry)

type executableReaderContext struct {
	source         semanticview.Source
	node           runtimeidentity.ExecutableNode
	eventType      string
	ruleCollection string
	ruleIndex      int
}

// Every executable handler field has one explicit reader disposition. This is
// deliberately a closed census rather than a generic reflection interpreter.
var systemNodeEventHandlerExecutableReaderCensus = map[string]handlerExecutableReaderCollector{
	"Activity": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendActivityExecutableReaders(out, ctx, "activity", handler.Activity)
	},
	"Description": noHandlerExecutableReaders,
	// Kept in the decoded shape solely for explicit retirement rejection.
	"CreateEntity": noHandlerExecutableReaders,
	// Emit readers are lowered once by HandlerDeclarativeEmitSites below so
	// namespace sugar, fan-out aliases, and join-result visibility stay exact.
	"Emit":      noHandlerExecutableReaders,
	"OnSuccess": noHandlerExecutableReaders,
	"Guard": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendGuardExecutableReaders(out, handler.Guard)
	},
	"AdvancesTo": noHandlerExecutableReaders,
	"Terminate":  noHandlerExecutableReaders,
	"SetsGate":   noHandlerExecutableReaders,
	"ClearGates": noHandlerExecutableReaders,
	"DataAccumulation": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendDataAccumulationExecutableReaders(out, "data_accumulation", handler.DataAccumulation)
	},
	"Condition": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendExecutableReader(out, "condition", handler.Condition, WorkflowEntityFieldLifecycleRule)
	},
	"Logic": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendExecutableReader(out, "logic", handler.Logic, WorkflowEntityFieldLifecycleRule)
	},
	"Loop": noHandlerExecutableReaders,
	"OnComplete": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendRulesExecutableReaders(out, ctx, "on_complete", handler.OnComplete)
	},
	"Rules": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendRulesExecutableReaders(out, ctx, "rules", handler.Rules)
	},
	"Accumulate": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		if handler.Accumulate != nil {
			appendExecutableReader(out, "accumulate.from", handler.Accumulate.From, WorkflowEntityFieldLifecycleAccumulate)
			appendExecutableReader(out, "accumulate.key", handler.Accumulate.Key, WorkflowEntityFieldLifecycleAccumulate)
		}
	},
	"Join": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendJoinExecutableReaders(out, ctx, handler.Join)
	},
	// Closure transport executes compiler-owned plans, not arrival readers.
	"JoinUntilPlans": noHandlerExecutableReaders,
	"Compute": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendComputeExecutableReaders(out, "compute", handler.Compute, WorkflowEntityFieldLifecycleCompute)
	},
	"Query": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendQueryExecutableReaders(out, "query", handler.Query)
	},
	"FanOut": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, _ runtimecontracts.SystemNodeEventHandler) {
		appendCompiledFanOutExecutableReaders(out, ctx, runtimecontracts.FanOutSiteHandler, -1, "fan_out", WorkflowEntityFieldLifecycleFanOut)
	},
	"GroupBy": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendGroupByExecutableReaders(out, handler.GroupBy)
	},
	"Filter": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendFilterExecutableReaders(out, handler.Filter)
	},
	"Reduce": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendReduceExecutableReaders(out, handler.Reduce)
	},
	"Count": func(out *[]WorkflowExecutableReader, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendCountExecutableReaders(out, handler.Count)
	},
	"Clear": noHandlerExecutableReaders,
}

var handlerRuleEntryExecutableReaderCensus = map[string]handlerRuleExecutableReaderCollector{
	"ID":          noHandlerRuleExecutableReaders,
	"Description": noHandlerRuleExecutableReaders,
	"Condition": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		before := len(*out)
		appendConditionExecutableReader(out, prefix+".condition", rule.Condition, WorkflowEntityFieldLifecycleRule, WorkflowConditionContextRule)
		if len(*out) > before && ctx.ruleCollection == "rules" {
			(*out)[before].SourceSlot = prefix + ".when"
		}
	},
	"PolicyRow":  noHandlerRuleExecutableReaders,
	"AdvancesTo": noHandlerRuleExecutableReaders,
	"Terminate":  noHandlerRuleExecutableReaders,
	"Emit":       noHandlerRuleExecutableReaders,
	"Activity": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendActivityExecutableReaders(out, ctx, prefix+".activity", rule.Activity)
	},
	"DataAccumulation": func(out *[]WorkflowExecutableReader, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendDataAccumulationExecutableReaders(out, prefix+".data_accumulation", rule.DataAccumulation)
	},
	"Compute": func(out *[]WorkflowExecutableReader, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendComputeExecutableReaders(out, prefix+".compute", rule.Compute, WorkflowEntityFieldLifecycleRule)
	},
	"FanOut": func(out *[]WorkflowExecutableReader, ctx executableReaderContext, prefix string, _ runtimecontracts.HandlerRuleEntry) {
		if ctx.ruleCollection != "rules" && ctx.ruleCollection != "on_complete" {
			return
		}
		kind := runtimecontracts.FanOutSiteRule
		if ctx.ruleCollection == "on_complete" {
			kind = runtimecontracts.FanOutSiteOnComplete
		}
		appendCompiledFanOutExecutableReaders(out, ctx, kind, ctx.ruleIndex, prefix+".fan_out", WorkflowEntityFieldLifecycleRule)
	},
	"declarationIdentity": noHandlerRuleExecutableReaders,
	"authored":            noHandlerRuleExecutableReaders,
	"admissionProvenance": noHandlerRuleExecutableReaders,
}

func WorkflowHandlerExecutableReaders(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler) []WorkflowExecutableReader {
	ctx := executableReaderContext{source: source, node: node, eventType: strings.TrimSpace(eventType)}
	out := make([]WorkflowExecutableReader, 0, 24)
	for _, field := range sortedExecutableReaderFields(systemNodeEventHandlerExecutableReaderCensus) {
		before := len(out)
		systemNodeEventHandlerExecutableReaderCensus[field](&out, ctx, handler)
		for index := before; index < len(out); index++ {
			out[index].HandlerField = field
		}
	}
	// Canonical emit lowering expands emit.from and namespace sugar into the
	// exact expressions executed at every declarative emit site.
	out = append(out, WorkflowHandlerEmitExpressions(source, node, eventType, handler)...)
	if handler.Join != nil {
		if plan, ok := semanticview.WorkflowJoinPlanForExecutionHandler(source, node, eventType, *handler.Join); ok {
			for i := range out {
				if !out[i].AllowJoin {
					continue
				}
				out[i].JoinResultType = plan.ResultType
				if plan.Mode == runtimecontracts.WorkflowJoinModeFanOutDelivery {
					out[i].JoinContext = workflowexpr.JoinContextFanOutDelivery
				} else if plan.Spec.Members.Count != nil {
					out[i].JoinContext = workflowexpr.JoinContextCountArrival
				}
			}
		}
	}
	return out
}

func sortedExecutableReaderFields[T any](census map[string]T) []string {
	fields := make([]string, 0, len(census))
	for field := range census {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func noHandlerExecutableReaders(*[]WorkflowExecutableReader, executableReaderContext, runtimecontracts.SystemNodeEventHandler) {
}

func noHandlerRuleExecutableReaders(*[]WorkflowExecutableReader, executableReaderContext, string, runtimecontracts.HandlerRuleEntry) {
}

func appendExecutableReader(out *[]WorkflowExecutableReader, kind, expression string, phase WorkflowEntityFieldLifecyclePhase) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return
	}
	*out = append(*out, WorkflowExecutableReader{Kind: strings.TrimSpace(kind), SourceSlot: strings.TrimSpace(kind), Expression: expression, Phase: phase})
}

func appendConditionExecutableReader(out *[]WorkflowExecutableReader, kind, expression string, phase WorkflowEntityFieldLifecyclePhase, context WorkflowConditionContext) {
	if strings.EqualFold(strings.TrimSpace(expression), "else") && (context == WorkflowConditionContextRule || context == WorkflowConditionContextOnComplete) {
		return // Only internal selection defaults are not executable expressions.
	}
	before := len(*out)
	appendExecutableReader(out, kind, expression, phase)
	if len(*out) > before {
		ref := &(*out)[len(*out)-1]
		ref.ConditionContext = context
		ref.HasConditionContext = true
	}
}

func appendExpressionValueExecutableReaders(out *[]WorkflowExecutableReader, kind string, value runtimecontracts.ExpressionValue, phase WorkflowEntityFieldLifecyclePhase) {
	before := len(*out)
	appendExecutableReader(out, kind+".ref", value.Ref, phase)
	appendExecutableReader(out, kind+".cel", value.CEL, phase)
	for index := before; index < len(*out); index++ {
		(*out)[index].SourceSlot = kind
	}
}

func appendActivityExecutableReaders(out *[]WorkflowExecutableReader, ctx executableReaderContext, kind string, activity runtimecontracts.ActivitySpec) {
	phase := WorkflowEntityFieldLifecycleRule
	keys := make([]string, 0, len(activity.Input))
	for key := range activity.Input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		before := len(*out)
		appendExpressionValueExecutableReaders(out, runtimecontracts.NodeProvenanceMapPath(kind+".input", key), activity.Input[key], phase)
		resultType, resultOptional, found, err := activityInputResultType(ctx.source, activity.Tool, key)
		for index := before; index < len(*out); index++ {
			if err != nil {
				(*out)[index].ResultTypeError = err.Error()
				continue
			}
			if found {
				(*out)[index].ResultType = resultType
				(*out)[index].HasResultType = true
				(*out)[index].ResultOptional = resultOptional
			}
		}
	}
	// Approval.Decision is an opaque stable identifier, not an expression.
}

func activityInputResultType(source semanticview.Source, toolID, field string) (runtimecontracts.ResolvedCatalogType, bool, bool, error) {
	toolID = strings.TrimSpace(toolID)
	field = strings.TrimSpace(field)
	if source == nil || toolID == "" || field == "" {
		return runtimecontracts.ResolvedCatalogType{}, false, false, nil
	}
	tool, ok := source.ToolEntries()[toolID]
	if !ok {
		return runtimecontracts.ResolvedCatalogType{}, false, false, nil
	}
	input := tool.InputSchema()
	property, ok := input.Property(field)
	if !ok {
		if additional, hasAdditional := input.AdditionalPropertiesSchema(); hasAdditional {
			property, ok = additional, true
		} else if allowed, declared := input.AdditionalPropertiesAllowed(); declared && allowed {
			property, ok = runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaAny), true
		}
	}
	if !ok {
		return runtimecontracts.ResolvedCatalogType{}, false, false, nil
	}
	resolved, err := runtimecontracts.ResolveJSONSchemaStructuralType(property.Projection(), "tool."+toolID+".input."+field)
	if err != nil {
		return runtimecontracts.ResolvedCatalogType{}, false, false, fmt.Errorf("resolve activity input %s for tool %s: %w", field, toolID, err)
	}
	return resolved, !input.IsRequired(field), true, nil
}
func appendGuardExecutableReaders(out *[]WorkflowExecutableReader, guard *runtimecontracts.GuardSpec) {
	if guard == nil {
		return
	}
	for i, check := range guard.EffectiveChecks() {
		kind := "guard.check"
		if len(guard.Checks) > 0 {
			kind = fmt.Sprintf("guard.checks[%d]", i)
		}
		before := len(*out)
		appendConditionExecutableReader(out, kind, check.Check, WorkflowEntityFieldLifecycleGuard, WorkflowConditionContextGuard)
		for index := before; index < len(*out); index++ {
			(*out)[index].GuardCheckIndex = i
			(*out)[index].HasGuardCheckIndex = true
		}
	}
}

func appendDataAccumulationExecutableReaders(out *[]WorkflowExecutableReader, kind string, spec runtimecontracts.WorkflowDataAccumulation) {
	phase := WorkflowEntityFieldLifecycleDataAccumulation
	for i, write := range spec.Writes {
		before := len(*out)
		prefix := fmt.Sprintf("%s.writes[%d]", kind, i)
		if paths := entityruntime.MutationRequiredPaths(write); len(paths) != 0 {
			*out = append(*out, WorkflowExecutableReader{Kind: prefix + ".target_parent", Phase: phase, RequiredEntityPaths: paths})
		}
		if write.Value.IsZero() {
			if source := strings.TrimSpace(write.Source()); source != "" {
				expression := source
				if !runtimepaths.Parse(source).HasExplicitRoot() {
					expression = "payload." + source
				}
				appendExecutableReader(out, prefix+".source", expression, phase)
			}
		}
		appendExpressionValueExecutableReaders(out, prefix+".value", write.Value, phase)
		appendExpressionValueExecutableReaders(out, prefix+".key", write.Key, phase)
		appendExpressionValueExecutableReaders(out, prefix+".index", write.Index, phase)
		for index := before; index < len(*out); index++ {
			(*out)[index].WriteIndex = i
			(*out)[index].HasWriteIndex = true
		}
	}
}

func appendRulesExecutableReaders(out *[]WorkflowExecutableReader, ctx executableReaderContext, kind string, rules []runtimecontracts.HandlerRuleEntry) {
	for i, rule := range rules {
		prefix := fmt.Sprintf("%s[%d]", kind, i)
		if strings.HasPrefix(kind, "join.") {
			prefix = kind
		}
		label := prefix
		if id := strings.TrimSpace(rule.ID); id != "" {
			label = kind + "[" + id + "]"
		}
		before := len(*out)
		for _, field := range sortedExecutableReaderFields(handlerRuleEntryExecutableReaderCensus) {
			if !handlerRuleExecutableReaderFieldIsActive(kind, field) {
				continue
			}
			fieldBefore := len(*out)
			ruleCtx := ctx
			ruleCtx.ruleCollection = kind
			ruleCtx.ruleIndex = i
			handlerRuleEntryExecutableReaderCensus[field](out, ruleCtx, prefix, rule)
			for index := fieldBefore; index < len(*out); index++ {
				(*out)[index].Kind = label + strings.TrimPrefix((*out)[index].Kind, prefix)
				(*out)[index].RuleCollection = kind
				(*out)[index].RuleField = field
				(*out)[index].RuleIndex = i
				(*out)[index].HasRuleIndex = true
			}
		}
		if strings.Contains(kind, "on_complete") {
			for index := before; index < len(*out); index++ {
				if (*out)[index].Phase == WorkflowEntityFieldLifecycleRule {
					(*out)[index].Phase = WorkflowEntityFieldLifecycleOnComplete
				}
				if (*out)[index].HasConditionContext {
					(*out)[index].ConditionContext = WorkflowConditionContextOnComplete
				}
			}
		}
	}
}

func handlerRuleExecutableReaderFieldIsActive(collection, field string) bool {
	if collection == "rules" {
		return true
	}
	switch field {
	case "Activity":
		return false
	default:
		return true
	}
}

func appendJoinExecutableReaders(out *[]WorkflowExecutableReader, ctx executableReaderContext, join *runtimecontracts.JoinSpec) {
	if join == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleRule
	beforeMembers := len(*out)
	membersFrom := join.Members.From
	if field, statePath := strings.CutPrefix(strings.TrimSpace(membersFrom), "state."); statePath && len(runtimepaths.Parse(field).Segments) == 1 {
		membersFrom = "entity." + field
	}
	appendExecutableReader(out, "join.members.from", membersFrom, phase)
	for i := beforeMembers; i < len(*out); i++ {
		(*out)[i].CommittedStage = join.Stage
	}
	appendExecutableReader(out, "join.members.by", join.Members.By, phase)
	before := len(*out)
	appendRulesExecutableReaders(out, ctx, "join.on_complete", []runtimecontracts.HandlerRuleEntry{join.OnComplete})
	if join.Deadline != nil {
		appendRulesExecutableReaders(out, ctx, "join.on_deadline", []runtimecontracts.HandlerRuleEntry{join.OnDeadline})
	}
	for index := before; index < len(*out); index++ {
		(*out)[index].AllowJoin = true
	}
}

func appendComputeExecutableReaders(out *[]WorkflowExecutableReader, kind string, compute *runtimecontracts.ComputeSpec, phase WorkflowEntityFieldLifecyclePhase) {
	if compute == nil {
		return
	}
	if compute.Lookup != nil {
		for i, value := range compute.Lookup.On {
			appendExecutableReader(out, fmt.Sprintf("%s.lookup.on[%d]", kind, i), value, phase)
		}
	}
	if compute.Validation != nil {
		appendStringMapExecutableReaders(out, kind+".validation.input", compute.Validation.Input, phase)
	}
	if compute.Module != nil {
		appendStringMapExecutableReaders(out, kind+".module.input", compute.Module.Input, phase)
	}
}

func appendStringMapExecutableReaders(out *[]WorkflowExecutableReader, kind string, values map[string]string, phase WorkflowEntityFieldLifecyclePhase) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appendExecutableReader(out, kind+"."+strings.TrimSpace(key), values[key], phase)
	}
}

func appendQueryExecutableReaders(out *[]WorkflowExecutableReader, kind string, query *runtimecontracts.QuerySpec) {
	if query == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleGuard
	appendExecutableReader(out, kind+".source", query.Source, phase)
	beforeFilter := len(*out)
	appendConditionExecutableReader(out, kind+".filter", query.Filter, phase, WorkflowConditionContextQueryFilter)
	if len(*out) > beforeFilter {
		ref := &(*out)[len(*out)-1]
		ref.RequireScalarEntityLeaf = true
		ref.ConditionCollectionSource = firstExecutableCollectionSource(query.Source, query.Entities)
	}
	// entities, group_by, and select are declarations admitted by the exact
	// collection plan, not executable value expressions.
	// Sequence rows in Queries are retained source data that stepQuery does not execute.
}

func appendCompiledFanOutExecutableReaders(out *[]WorkflowExecutableReader, ctx executableReaderContext, siteKind runtimecontracts.FanOutSiteKind, index int, kind string, phase WorkflowEntityFieldLifecyclePhase) {
	if ctx.source == nil {
		return
	}
	site, err := runtimecontracts.NewFanOutSiteRef(ctx.node, ctx.eventType, siteKind, index)
	if err != nil {
		return
	}
	plan, ok := ctx.source.FanOutPlanForSite(site)
	if !ok {
		return
	}
	beforeSource := len(*out)
	appendExecutableReader(out, kind+".items_from", plan.ItemsFrom, phase)
	for i := beforeSource; i < len(*out); i++ {
		(*out)[i].FanOutAfterWrites = plan.SourceAfterWrites
	}
	before := len(*out)
	appendExecutableReader(out, kind+".identity", plan.Identity, phase)
	if len(*out) > before {
		ref := &(*out)[len(*out)-1]
		ref.FanOutAfterWrites = true
		ref.ItemAlias = plan.ItemAlias
		ref.ItemType = plan.ItemType.Clone()
		ref.HasItemType = true
	}
}

func appendGroupByExecutableReaders(out *[]WorkflowExecutableReader, groupBy *runtimecontracts.GroupBySpec) {
	if groupBy == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleGroupBy
	appendExecutableReader(out, "group_by.items_from", groupBy.ItemsFrom, phase)
	// key is a schema-bound item selector, not an executable expression.
}

func appendFilterExecutableReaders(out *[]WorkflowExecutableReader, filter *runtimecontracts.FilterSpec) {
	if filter == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleFilter
	appendCollectionSourceExecutableReader(out, "filter", filter.Source, filter.ItemsFrom, phase)
	// Predicate is not evaluated by stepFilter; Condition is the executable filter expression.
	before := len(*out)
	appendConditionExecutableReader(out, "filter.condition", filter.Condition, phase, WorkflowConditionContextFilter)
	if len(*out) > before {
		(*out)[len(*out)-1].ConditionCollectionSource = firstExecutableCollectionSource(filter.ItemsFrom, filter.Source)
	}
}

func appendReduceExecutableReaders(out *[]WorkflowExecutableReader, reduce *runtimecontracts.ReduceSpec) {
	if reduce == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleReduce
	appendCollectionSourceExecutableReader(out, "reduce", reduce.Source, reduce.ItemsFrom, phase)
	// Params are not evaluated by stepReduce; Operation selects the reduction behavior.
}

func appendCountExecutableReaders(out *[]WorkflowExecutableReader, count *runtimecontracts.CountSpec) {
	if count == nil {
		return
	}
	phase := WorkflowEntityFieldLifecycleCount
	appendCollectionSourceExecutableReader(out, "count", count.Source, count.ItemsFrom, phase)
	before := len(*out)
	appendConditionExecutableReader(out, "count.condition", count.Condition, phase, WorkflowConditionContextCount)
	if len(*out) > before {
		(*out)[len(*out)-1].ConditionCollectionSource = firstExecutableCollectionSource(count.ItemsFrom, count.Source)
	}
}

func appendCollectionSourceExecutableReader(out *[]WorkflowExecutableReader, kind, source, itemsFrom string, phase WorkflowEntityFieldLifecyclePhase) {
	if strings.TrimSpace(itemsFrom) != "" {
		appendExecutableReader(out, kind+".items_from", itemsFrom, phase)
		return
	}
	appendExecutableReader(out, kind+".source", source, phase)
}

func firstExecutableCollectionSource(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

type WorkflowExecutableReader struct {
	RequiredEntityPaths       []string
	CommittedStage            string
	KnownPresence             []string
	AssignmentError           string
	WriteIndex                int
	HasWriteIndex             bool
	GuardCheckIndex           int
	HasGuardCheckIndex        bool
	EmitSite                  *runtimecontracts.HandlerDeclarativeEmitSite
	EmitField                 string
	FanOutAfterWrites         bool
	Kind                      string
	SourceSlot                string
	Expression                string
	Phase                     WorkflowEntityFieldLifecyclePhase
	HandlerField              string
	RuleCollection            string
	RuleField                 string
	RuleIndex                 int
	HasRuleIndex              bool
	RequireScalarEntityLeaf   bool
	AllowBareItem             bool
	ItemAlias                 string
	AllowJoin                 bool
	JoinResultType            runtimecontracts.CatalogTypeReference
	JoinContext               workflowexpr.JoinContext
	ItemType                  runtimecontracts.ResolvedCatalogType
	HasItemType               bool
	ResultType                runtimecontracts.ResolvedCatalogType
	HasResultType             bool
	ResultOptional            bool
	ResultTypeError           string
	ConditionContext          WorkflowConditionContext
	HasConditionContext       bool
	ConditionCollectionSource string
}

func WorkflowHandlerConditionExpressions(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler) []WorkflowExecutableReader {
	readers := WorkflowHandlerExecutableReaders(source, node, eventType, handler)
	out := make([]WorkflowExecutableReader, 0, 10)
	for _, reader := range readers {
		if reader.HasConditionContext {
			out = append(out, reader)
		}
	}
	return out
}

func WorkflowHandlerEmitExpressions(source semanticview.Source, node runtimeidentity.ExecutableNode, eventType string, handler runtimecontracts.SystemNodeEventHandler) []WorkflowExecutableReader {
	out := make([]WorkflowExecutableReader, 0, 8)
	appendSpec := func(kindPrefix, siteKey string, spec runtimecontracts.EmitSpec, phase WorkflowEntityFieldLifecyclePhase, itemAlias string) {
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
			ref := WorkflowExecutableReader{
				Kind:       kindPrefix + " emit field " + strings.TrimSpace(key),
				SourceSlot: runtimecontracts.NodeProvenanceMapPath(strings.TrimPrefix(strings.ReplaceAll(siteKey, ".emit_template", ".emit"), "handler.")+".fields", key),
				EmitField:  key,
				Expression: expr,
				Phase:      phase,
				ItemAlias:  strings.TrimSpace(itemAlias),
				AllowJoin:  strings.HasPrefix(strings.TrimSpace(kindPrefix), "handler.join."),
			}
			resolution := semanticview.ResolveEventSchema(source, node.FlowPath(), spec.EventType())
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
	var plans []runtimecontracts.FanOutCompiledPlan
	if source != nil {
		plans = source.FanOutPlansForHandler(node, eventType)
	}
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler, plans) {
		before := len(out)
		appendSpec(site.Source, site.SiteKey, site.Spec, WorkflowEntityFieldLifecycleEmitFields, site.ItemAlias)
		for index := before; index < len(out); index++ {
			out[index].EmitSite = &site
			if strings.HasSuffix(site.SiteKey, ".emit_template") {
				key := out[index].EmitField
				rows := handler.Rules
				if strings.HasPrefix(site.SiteKey, "handler.on_complete[") {
					rows = handler.OnComplete
				}
				if site.RuleIndex >= 0 && site.RuleIndex < len(rows) {
					if _, specialized := rows[site.RuleIndex].Emit.Fields[key]; !specialized {
						out[index].SourceSlot = runtimecontracts.NodeProvenanceMapPath("emit.fields", key)
					}
				}
			}
		}
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
				appendSpec("guard escalation", "guard.on_fail.escalate", failureSpec.EscalationEmitSpec(), WorkflowEntityFieldLifecycleGuardEscalation, "")
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

// WorkflowExecutableReaderCensusFields exposes closed field coverage without
// exposing the collectors used by verification and constructor admission.
func WorkflowExecutableReaderCensusFields() ([]string, []string) {
	return sortedExecutableReaderFields(systemNodeEventHandlerExecutableReaderCensus), sortedExecutableReaderFields(handlerRuleEntryExecutableReaderCensus)
}
