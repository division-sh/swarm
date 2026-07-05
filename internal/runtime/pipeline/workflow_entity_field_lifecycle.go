package pipeline

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type WorkflowEntityFieldLifecyclePhase string

const (
	WorkflowEntityFieldLifecycleGuard            WorkflowEntityFieldLifecyclePhase = "guard"
	WorkflowEntityFieldLifecycleGuardEscalation  WorkflowEntityFieldLifecyclePhase = "guard_escalation_fields"
	WorkflowEntityFieldLifecycleAccumulate       WorkflowEntityFieldLifecyclePhase = "accumulate"
	WorkflowEntityFieldLifecycleFilter           WorkflowEntityFieldLifecyclePhase = "filter"
	WorkflowEntityFieldLifecycleGroupBy          WorkflowEntityFieldLifecyclePhase = "group_by"
	WorkflowEntityFieldLifecycleReduce           WorkflowEntityFieldLifecyclePhase = "reduce"
	WorkflowEntityFieldLifecycleCount            WorkflowEntityFieldLifecyclePhase = "count"
	WorkflowEntityFieldLifecycleCompute          WorkflowEntityFieldLifecyclePhase = "compute"
	WorkflowEntityFieldLifecycleFanOut           WorkflowEntityFieldLifecyclePhase = "fan_out"
	WorkflowEntityFieldLifecycleOnComplete       WorkflowEntityFieldLifecyclePhase = "on_complete"
	WorkflowEntityFieldLifecycleRule             WorkflowEntityFieldLifecyclePhase = "rule"
	WorkflowEntityFieldLifecycleDataAccumulation WorkflowEntityFieldLifecyclePhase = "data_accumulation"
	WorkflowEntityFieldLifecycleEmitFields       WorkflowEntityFieldLifecyclePhase = "emit_fields"
)

func WorkflowEntityReferences(expression string) []string {
	return workflowexpr.EntityReferences(expression)
}

func WorkflowPlatformEntityReferences(expression string) []string {
	return workflowexpr.PlatformEntityReferences(expression)
}

func stripWorkflowExpressionStringLiterals(expression string) string {
	return workflowexpr.StripStringLiterals(expression)
}

func WorkflowEntityReferenceField(ref string) string {
	return workflowexpr.EntityReferenceField(ref)
}

func WorkflowBuiltinEntityField(field string) bool {
	return false
}

func WorkflowPresenceGuardedEntityFields(expression string) map[string]struct{} {
	return workflowexpr.PresenceGuardedEntityFields(expression)
}

func WorkflowEntityFieldsAvailableBeforeCondition(handler runtimecontracts.SystemNodeEventHandler, context WorkflowConditionContext) map[string]struct{} {
	switch context {
	case WorkflowConditionContextGuard:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleGuard)
	case WorkflowConditionContextFilter:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleFilter)
	case WorkflowConditionContextCount:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleCount)
	case WorkflowConditionContextOnComplete:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleOnComplete)
	case WorkflowConditionContextRule:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleRule)
	default:
		return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleGuard)
	}
}

func WorkflowEntityFieldsAvailableBeforeDataAccumulation(handler runtimecontracts.SystemNodeEventHandler) map[string]struct{} {
	return workflowEntityFieldsAvailableBeforePhase(handler, WorkflowEntityFieldLifecycleDataAccumulation)
}

func WorkflowEntityFieldsAvailableBeforeEmitFields(handler runtimecontracts.SystemNodeEventHandler) map[string]struct{} {
	available := workflowBuiltinEntityFields()
	addWriter := func(target string) {
		if field, ok := workflowEntityFieldNameFromTarget(target); ok {
			available[field] = struct{}{}
		}
	}
	var addQueryWriter func(query *runtimecontracts.QuerySpec)
	addQueryWriter = func(query *runtimecontracts.QuerySpec) {
		if query == nil {
			return
		}
		addWriter(query.StoreAs)
		for i := range query.Queries {
			addQueryWriter(&query.Queries[i])
		}
	}
	addQueryWriter(handler.Query)
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
	if handler.Compute != nil {
		addWriter(handler.Compute.StoreAs)
	}
	if handler.FanOut != nil {
		available["fan_out_count"] = struct{}{}
	}
	if handler.CreateEntity {
		for _, write := range handler.DataAccumulation.Writes {
			addWriter(write.Target())
		}
	}
	return available
}

func WorkflowEntityReadsPersistedStateBeforeHandlerWrites(phase WorkflowEntityFieldLifecyclePhase) bool {
	switch phase {
	case WorkflowEntityFieldLifecycleGuard,
		WorkflowEntityFieldLifecycleGuardEscalation,
		WorkflowEntityFieldLifecycleFilter,
		WorkflowEntityFieldLifecycleCount,
		WorkflowEntityFieldLifecycleOnComplete,
		WorkflowEntityFieldLifecycleRule:
		return true
	default:
		return false
	}
}

func WorkflowEntityFieldNameFromTarget(target string) (string, bool) {
	return workflowEntityFieldNameFromTarget(target)
}

func workflowEntityFieldsAvailableBeforePhase(handler runtimecontracts.SystemNodeEventHandler, phase WorkflowEntityFieldLifecyclePhase) map[string]struct{} {
	available := workflowBuiltinEntityFields()
	addWriter := func(target string) {
		if field, ok := workflowEntityFieldNameFromTarget(target); ok {
			available[field] = struct{}{}
		}
	}
	addRuleWriters := func(rule runtimecontracts.HandlerRuleEntry) {
		for _, write := range rule.DataAccumulation.Writes {
			addWriter(write.Target())
		}
		if rule.Compute != nil {
			addWriter(rule.Compute.StoreAs)
		}
	}
	var addQueryWriter func(query *runtimecontracts.QuerySpec)
	addQueryWriter = func(query *runtimecontracts.QuerySpec) {
		if query == nil {
			return
		}
		addWriter(query.StoreAs)
		for i := range query.Queries {
			addQueryWriter(&query.Queries[i])
		}
	}
	addQueryWriter(handler.Query)
	if phaseAfter(phase, WorkflowEntityFieldLifecycleFilter) {
		if handler.Filter != nil {
			addWriter(handler.Filter.StoreAs)
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleGroupBy) {
		if handler.GroupBy != nil {
			addWriter(handler.GroupBy.StoreAs)
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleReduce) {
		if handler.Reduce != nil {
			addWriter(handler.Reduce.StoreAs)
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleCount) {
		if handler.Count != nil {
			addWriter(handler.Count.StoreAs)
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleCompute) {
		if handler.Compute != nil {
			addWriter(handler.Compute.StoreAs)
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleFanOut) {
		if handler.FanOut != nil {
			available["fan_out_count"] = struct{}{}
		}
	}
	if phaseAfter(phase, WorkflowEntityFieldLifecycleRule) {
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
	}
	if handler.CreateEntity && phaseAfter(phase, WorkflowEntityFieldLifecycleDataAccumulation) {
		for _, write := range handler.DataAccumulation.Writes {
			addWriter(write.Target())
		}
	}
	return available
}

func workflowBuiltinEntityFields() map[string]struct{} {
	return map[string]struct{}{}
}

func workflowEntityFieldNameFromTarget(target string) (string, bool) {
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return "", false
	}
	field, _, _ := strings.Cut(path, ".")
	field = strings.TrimSpace(field)
	if field == "" {
		return "", false
	}
	return field, true
}

func phaseAfter(current, threshold WorkflowEntityFieldLifecyclePhase) bool {
	return workflowEntityFieldLifecycleOrder(current) > workflowEntityFieldLifecycleOrder(threshold)
}

func workflowEntityFieldLifecycleOrder(phase WorkflowEntityFieldLifecyclePhase) int {
	switch phase {
	case WorkflowEntityFieldLifecycleGuard:
		return 1
	case WorkflowEntityFieldLifecycleGuardEscalation:
		return 1
	case WorkflowEntityFieldLifecycleAccumulate:
		return 2
	case WorkflowEntityFieldLifecycleFilter:
		return 3
	case WorkflowEntityFieldLifecycleGroupBy:
		return 4
	case WorkflowEntityFieldLifecycleReduce:
		return 5
	case WorkflowEntityFieldLifecycleCount:
		return 6
	case WorkflowEntityFieldLifecycleCompute:
		return 7
	case WorkflowEntityFieldLifecycleFanOut:
		return 8
	case WorkflowEntityFieldLifecycleOnComplete:
		return 9
	case WorkflowEntityFieldLifecycleRule:
		return 10
	case WorkflowEntityFieldLifecycleDataAccumulation:
		return 11
	case WorkflowEntityFieldLifecycleEmitFields:
		return 12
	default:
		return 0
	}
}
