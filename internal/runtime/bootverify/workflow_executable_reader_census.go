package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type handlerExecutableReaderCollector func(*[]expressionReference, executableReaderContext, runtimecontracts.SystemNodeEventHandler)
type handlerRuleExecutableReaderCollector func(*[]expressionReference, executableReaderContext, string, runtimecontracts.HandlerRuleEntry)

type executableReaderContext struct {
	source    semanticview.Source
	flowID    string
	nodeID    string
	eventType string
}

// Every executable handler field has one explicit reader disposition. This is
// deliberately a closed census rather than a generic reflection interpreter.
var systemNodeEventHandlerExecutableReaderCensus = map[string]handlerExecutableReaderCollector{
	"Action": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendActionExecutableReaders(out, "action", handler.Action)
	},
	"Activity": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendActivityExecutableReaders(out, "activity", handler.Activity)
	},
	"CreateEntity": noHandlerExecutableReaders,
	"SelectEntity": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendSelectExecutableReaders(out, "select_entity", handler.SelectEntity)
	},
	"SelectOrCreateEntity": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendSelectOrCreateExecutableReaders(out, "select_or_create_entity", handler.SelectOrCreateEntity)
	},
	"Description":    noHandlerExecutableReaders,
	"EvidenceTarget": noHandlerExecutableReaders,
	// Emit readers are lowered once by HandlerDeclarativeEmitSites below so
	// namespace sugar, fan-out aliases, and join-result visibility stay exact.
	"Emit":      noHandlerExecutableReaders,
	"OnSuccess": noHandlerExecutableReaders,
	"Guard": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendGuardExecutableReaders(out, handler.Guard)
	},
	"AdvancesTo": noHandlerExecutableReaders,
	"SetsGate":   noHandlerExecutableReaders,
	"ClearGates": noHandlerExecutableReaders,
	"DataAccumulation": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendDataAccumulationExecutableReaders(out, "data_accumulation", handler.DataAccumulation)
	},
	"Condition": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendExecutableReader(out, "condition", handler.Condition, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	},
	"Logic": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendExecutableReader(out, "logic", handler.Logic, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	},
	"Loop": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		if handler.Loop != nil {
			appendExecutableReader(out, "loop.from", handler.Loop.From, runtimepipeline.WorkflowEntityFieldLifecycleGuard)
		}
	},
	"OnComplete": func(out *[]expressionReference, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendRulesExecutableReaders(out, ctx, "on_complete", handler.OnComplete)
	},
	"Rules": func(out *[]expressionReference, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendRulesExecutableReaders(out, ctx, "rules", handler.Rules)
	},
	"Accumulate": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		if handler.Accumulate != nil {
			appendExecutableReader(out, "accumulate.from", handler.Accumulate.From, runtimepipeline.WorkflowEntityFieldLifecycleAccumulate)
			appendExecutableReader(out, "accumulate.window", handler.Accumulate.Window, runtimepipeline.WorkflowEntityFieldLifecycleAccumulate)
			appendExecutableReader(out, "accumulate.dedup_by", handler.Accumulate.DedupBy, runtimepipeline.WorkflowEntityFieldLifecycleAccumulate)
		}
	},
	"Join": func(out *[]expressionReference, ctx executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendJoinExecutableReaders(out, ctx, handler.Join)
	},
	"Compute": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendComputeExecutableReaders(out, "compute", handler.Compute, runtimepipeline.WorkflowEntityFieldLifecycleCompute)
	},
	"Query": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendQueryExecutableReaders(out, "query", handler.Query)
	},
	"FanOut": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendFanOutExecutableReaders(out, "fan_out", handler.FanOut, runtimepipeline.WorkflowEntityFieldLifecycleFanOut)
	},
	"GroupBy": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendGroupByExecutableReaders(out, handler.GroupBy)
	},
	"Filter": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendFilterExecutableReaders(out, handler.Filter)
	},
	"Reduce": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendReduceExecutableReaders(out, handler.Reduce)
	},
	"Count": func(out *[]expressionReference, _ executableReaderContext, handler runtimecontracts.SystemNodeEventHandler) {
		appendCountExecutableReaders(out, handler.Count)
	},
	"Clear": noHandlerExecutableReaders,
}

var handlerRuleEntryExecutableReaderCensus = map[string]handlerRuleExecutableReaderCollector{
	"ID":          noHandlerRuleExecutableReaders,
	"Description": noHandlerRuleExecutableReaders,
	"Condition": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendExecutableReader(out, prefix+".condition", rule.Condition, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	},
	"PolicyRow":  noHandlerRuleExecutableReaders,
	"AdvancesTo": noHandlerRuleExecutableReaders,
	"Emit":       noHandlerRuleExecutableReaders,
	"Action": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendActionExecutableReaders(out, prefix+".action", rule.Action)
	},
	"Activity": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendActivityExecutableReaders(out, prefix+".activity", rule.Activity)
	},
	"DataAccumulation": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendDataAccumulationExecutableReaders(out, prefix+".data_accumulation", rule.DataAccumulation)
	},
	"Compute": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendComputeExecutableReaders(out, prefix+".compute", rule.Compute, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	},
	"FanOut": func(out *[]expressionReference, _ executableReaderContext, prefix string, rule runtimecontracts.HandlerRuleEntry) {
		appendFanOutExecutableReaders(out, prefix+".fan_out", rule.FanOut, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	},
}

func handlerExecutableReaderExpressionsForSource(source semanticview.Source, flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) []expressionReference {
	ctx := executableReaderContext{source: source, flowID: strings.TrimSpace(flowID), nodeID: strings.TrimSpace(nodeID), eventType: strings.TrimSpace(eventType)}
	out := make([]expressionReference, 0, 24)
	for _, field := range sortedExecutableReaderFields(systemNodeEventHandlerExecutableReaderCensus) {
		before := len(out)
		systemNodeEventHandlerExecutableReaderCensus[field](&out, ctx, handler)
		for index := before; index < len(out); index++ {
			out[index].HandlerField = field
		}
	}
	// Canonical emit lowering expands emit.from and namespace sugar into the
	// exact expressions executed at every declarative emit site.
	out = append(out, handlerEmitExpressionsForSource(source, flowID, nodeID, eventType, handler)...)
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

func noHandlerExecutableReaders(*[]expressionReference, executableReaderContext, runtimecontracts.SystemNodeEventHandler) {
}

func noHandlerRuleExecutableReaders(*[]expressionReference, executableReaderContext, string, runtimecontracts.HandlerRuleEntry) {
}

func appendExecutableReader(out *[]expressionReference, kind, expression string, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	expression = strings.TrimSpace(expression)
	if expression == "" || strings.EqualFold(expression, "else") {
		return
	}
	*out = append(*out, expressionReference{Kind: strings.TrimSpace(kind), Expression: expression, Phase: phase})
}

func appendExpressionValueExecutableReaders(out *[]expressionReference, kind string, value runtimecontracts.ExpressionValue, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	appendExecutableReader(out, kind+".ref", value.Ref, phase)
	appendExecutableReader(out, kind+".cel", value.CEL, phase)
}

func appendExpressionValueMapExecutableReaders(out *[]expressionReference, kind string, values map[string]runtimecontracts.ExpressionValue, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appendExpressionValueExecutableReaders(out, kind+"."+strings.TrimSpace(key), values[key], phase)
	}
}

func appendActionExecutableReaders(out *[]expressionReference, kind string, action runtimecontracts.ActionSpec) {
	phase := runtimepipeline.WorkflowEntityFieldLifecycleRule
	appendExecutableReader(out, kind+".instance_id_from", action.InstanceIDFrom, phase)
	if action.ConfigFrom != nil {
		for _, entry := range action.ConfigFrom.Entries {
			appendExecutableReader(out, kind+".config_from."+strings.TrimSpace(entry.Key), entry.Ref, phase)
		}
		keys := make([]string, 0, len(action.ConfigFrom.Bindings))
		for key := range action.ConfigFrom.Bindings {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			appendExecutableReader(out, kind+".config_from."+strings.TrimSpace(key), action.ConfigFrom.Bindings[key], phase)
		}
	}
	if mailbox := action.Mailbox; mailbox != nil {
		appendExpressionValueExecutableReaders(out, kind+".mailbox.item_type", mailbox.ItemType, phase)
		appendExpressionValueExecutableReaders(out, kind+".mailbox.severity", mailbox.Severity, phase)
		appendExpressionValueExecutableReaders(out, kind+".mailbox.summary", mailbox.Summary, phase)
		appendExpressionValueExecutableReaders(out, kind+".mailbox.entity_id", mailbox.EntityID, phase)
		appendExpressionValueExecutableReaders(out, kind+".mailbox.flow_instance", mailbox.FlowInstance, phase)
		appendExpressionValueMapExecutableReaders(out, kind+".mailbox.payload", mailbox.Payload, phase)
	}
	if artifact := action.ArtifactRepo; artifact != nil {
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.repo_id", artifact.RepoID, phase)
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.namespace", artifact.Namespace, phase)
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.partition_key", artifact.PartitionKey, phase)
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.display_slug", artifact.DisplaySlug, phase)
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.request_id", artifact.RequestID, phase)
		appendExpressionValueExecutableReaders(out, kind+".artifact_repo.author", artifact.Author, phase)
		appendExpressionValueMapExecutableReaders(out, kind+".artifact_repo.provenance", artifact.Provenance, phase)
		for i, file := range artifact.Files {
			prefix := fmt.Sprintf("%s.artifact_repo.files[%d]", kind, i)
			appendExpressionValueExecutableReaders(out, prefix+".path", file.Path, phase)
			appendExpressionValueExecutableReaders(out, prefix+".content", file.Content, phase)
		}
		// Success/failure payloads are declarative emit sites and are lowered by
		// the canonical emit pass in handlerExecutableReaderExpressionsForSource.
	}
}

func appendActivityExecutableReaders(out *[]expressionReference, kind string, activity runtimecontracts.ActivitySpec) {
	phase := runtimepipeline.WorkflowEntityFieldLifecycleRule
	appendExpressionValueMapExecutableReaders(out, kind+".input", activity.Input, phase)
	if activity.Approval != nil {
		appendExecutableReader(out, kind+".approval.decision", activity.Approval.Decision, phase)
	}
}

func appendSelectExecutableReaders(out *[]expressionReference, kind string, spec *runtimecontracts.SelectEntitySpec) {
	if spec == nil {
		return
	}
	for _, binding := range spec.Bindings {
		appendExecutableReader(out, kind+".by."+strings.TrimSpace(binding.Field), binding.Ref, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	}
}

func appendSelectOrCreateExecutableReaders(out *[]expressionReference, kind string, spec *runtimecontracts.SelectOrCreateEntitySpec) {
	if spec == nil {
		return
	}
	for _, binding := range spec.Bindings {
		appendExecutableReader(out, kind+".by."+strings.TrimSpace(binding.Field), binding.Ref, runtimepipeline.WorkflowEntityFieldLifecycleRule)
	}
}

func appendGuardExecutableReaders(out *[]expressionReference, guard *runtimecontracts.GuardSpec) {
	if guard == nil {
		return
	}
	appendExecutableReader(out, "guard.check", guard.Check, runtimepipeline.WorkflowEntityFieldLifecycleGuard)
	for i, check := range guard.Checks {
		appendExecutableReader(out, fmt.Sprintf("guard.checks[%d]", i), check.Check, runtimepipeline.WorkflowEntityFieldLifecycleGuard)
	}
}

func appendDataAccumulationExecutableReaders(out *[]expressionReference, kind string, spec runtimecontracts.WorkflowDataAccumulation) {
	phase := runtimepipeline.WorkflowEntityFieldLifecycleDataAccumulation
	for i, write := range spec.Writes {
		prefix := fmt.Sprintf("%s.writes[%d]", kind, i)
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
	}
}

func appendRulesExecutableReaders(out *[]expressionReference, ctx executableReaderContext, kind string, rules []runtimecontracts.HandlerRuleEntry) {
	for i, rule := range rules {
		prefix := fmt.Sprintf("%s[%d]", kind, i)
		if id := strings.TrimSpace(rule.ID); id != "" {
			prefix = kind + "[" + id + "]"
		}
		before := len(*out)
		for _, field := range sortedExecutableReaderFields(handlerRuleEntryExecutableReaderCensus) {
			fieldBefore := len(*out)
			handlerRuleEntryExecutableReaderCensus[field](out, ctx, prefix, rule)
			for index := fieldBefore; index < len(*out); index++ {
				(*out)[index].RuleCollection = kind
				(*out)[index].RuleField = field
				(*out)[index].RuleIndex = i
				(*out)[index].HasRuleIndex = true
			}
		}
		if strings.Contains(kind, "on_complete") {
			for index := before; index < len(*out); index++ {
				if (*out)[index].Phase == runtimepipeline.WorkflowEntityFieldLifecycleRule {
					(*out)[index].Phase = runtimepipeline.WorkflowEntityFieldLifecycleOnComplete
				}
			}
		}
	}
}

func appendJoinExecutableReaders(out *[]expressionReference, ctx executableReaderContext, join *runtimecontracts.JoinSpec) {
	if join == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleRule
	appendExecutableReader(out, "join.members.from", join.Members.From, phase)
	appendExecutableReader(out, "join.members.by", join.Members.By, phase)
	appendExecutableReader(out, "join.complete_when", join.CompleteWhen, phase)
	if join.Window != nil {
		appendExecutableReader(out, "join.window.from", join.Window.From, phase)
		appendExecutableReader(out, "join.window.by", join.Window.By, phase)
	}
	before := len(*out)
	appendRulesExecutableReaders(out, ctx, "join.on_complete", []runtimecontracts.HandlerRuleEntry{join.OnComplete})
	appendRulesExecutableReaders(out, ctx, "join.timeout", []runtimecontracts.HandlerRuleEntry{join.Timeout.Outcome})
	for index := before; index < len(*out); index++ {
		(*out)[index].AllowJoin = true
	}
}

func appendComputeExecutableReaders(out *[]expressionReference, kind string, compute *runtimecontracts.ComputeSpec, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
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

func appendStringMapExecutableReaders(out *[]expressionReference, kind string, values map[string]string, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appendExecutableReader(out, kind+"."+strings.TrimSpace(key), values[key], phase)
	}
}

func appendQueryExecutableReaders(out *[]expressionReference, kind string, query *runtimecontracts.QuerySpec) {
	if query == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleGuard
	appendExecutableReader(out, kind+".source", query.Source, phase)
	appendExecutableReader(out, kind+".entities", query.Entities, phase)
	beforeFilter := len(*out)
	appendExecutableReader(out, kind+".filter", query.Filter, phase)
	if len(*out) > beforeFilter {
		(*out)[len(*out)-1].RequireScalarEntityLeaf = true
	}
	appendExecutableReader(out, kind+".group_by", query.GroupBy, phase)
	// Select entries are literal object field names, not executable expressions.
	for i := range query.Queries {
		appendQueryExecutableReaders(out, fmt.Sprintf("%s.queries[%d]", kind, i), &query.Queries[i])
	}
}

func appendFanOutExecutableReaders(out *[]expressionReference, kind string, fanOut *runtimecontracts.FanOutSpec, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	if fanOut == nil {
		return
	}
	appendExecutableReader(out, kind+".items_from", fanOut.ItemsFrom, phase)
	appendExecutableReader(out, kind+".identity", fanOut.Identity, phase)
}

func appendGroupByExecutableReaders(out *[]expressionReference, groupBy *runtimecontracts.GroupBySpec) {
	if groupBy == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleGroupBy
	appendExecutableReader(out, "group_by.items_from", groupBy.ItemsFrom, phase)
	appendExecutableReader(out, "group_by.key", groupBy.Key, phase)
}

func appendFilterExecutableReaders(out *[]expressionReference, filter *runtimecontracts.FilterSpec) {
	if filter == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleFilter
	appendCollectionSourceExecutableReader(out, "filter", filter.Source, filter.ItemsFrom, phase)
	// Predicate is not evaluated by stepFilter; Condition is the executable filter expression.
	appendExecutableReader(out, "filter.condition", filter.Condition, phase)
}

func appendReduceExecutableReaders(out *[]expressionReference, reduce *runtimecontracts.ReduceSpec) {
	if reduce == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleReduce
	appendCollectionSourceExecutableReader(out, "reduce", reduce.Source, reduce.ItemsFrom, phase)
	// Params are not evaluated by stepReduce; Operation selects the reduction behavior.
}

func appendCountExecutableReaders(out *[]expressionReference, count *runtimecontracts.CountSpec) {
	if count == nil {
		return
	}
	phase := runtimepipeline.WorkflowEntityFieldLifecycleCount
	appendCollectionSourceExecutableReader(out, "count", count.Source, count.ItemsFrom, phase)
	appendExecutableReader(out, "count.condition", count.Condition, phase)
}

func appendCollectionSourceExecutableReader(out *[]expressionReference, kind, source, itemsFrom string, phase runtimepipeline.WorkflowEntityFieldLifecyclePhase) {
	if strings.TrimSpace(itemsFrom) != "" {
		appendExecutableReader(out, kind+".items_from", itemsFrom, phase)
		return
	}
	appendExecutableReader(out, kind+".source", source, phase)
}
