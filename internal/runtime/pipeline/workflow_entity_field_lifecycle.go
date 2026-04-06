package pipeline

import (
	"regexp"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
)

type WorkflowEntityFieldLifecyclePhase string

const (
	WorkflowEntityFieldLifecycleGuard            WorkflowEntityFieldLifecyclePhase = "guard"
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
)

var workflowExpressionEntityReferencePattern = regexp.MustCompile(`entity\.([a-zA-Z_][a-zA-Z0-9_.]*)`)
var workflowExpressionEntityPresencePattern = regexp.MustCompile(`["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+in\s+entity\b`)

func WorkflowEntityReferences(expression string) []string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	matches := workflowExpressionEntityReferencePattern.FindAllStringSubmatch(expression, -1)
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
	return out
}

func WorkflowEntityReferenceField(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if idx := strings.IndexByte(ref, '.'); idx >= 0 {
		ref = ref[:idx]
	}
	return strings.TrimSpace(ref)
}

func WorkflowBuiltinEntityField(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "current_state", "workflow_name", "workflow_version", "gates":
		return true
	default:
		return false
	}
}

func WorkflowPresenceGuardedEntityFields(expression string) map[string]struct{} {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	matches := workflowExpressionEntityPresencePattern.FindAllStringSubmatch(expression, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		field := strings.TrimSpace(match[1])
		if field == "" {
			continue
		}
		out[field] = struct{}{}
	}
	return out
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
	return available
}

func workflowBuiltinEntityFields() map[string]struct{} {
	return map[string]struct{}{
		"entity_id":        {},
		"current_state":    {},
		"workflow_name":    {},
		"workflow_version": {},
		"gates":            {},
	}
}

func workflowEntityFieldNameFromTarget(target string) (string, bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false
	}
	if strings.HasPrefix(target, "entity.") {
		target = strings.TrimSpace(strings.TrimPrefix(target, "entity."))
	} else if strings.HasPrefix(target, "metadata.") {
		target = strings.TrimSpace(strings.TrimPrefix(target, "metadata."))
	}
	if target == "" {
		return "", false
	}
	if idx := strings.IndexByte(target, '.'); idx >= 0 {
		target = target[:idx]
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false
	}
	return target, true
}

func phaseAfter(current, threshold WorkflowEntityFieldLifecyclePhase) bool {
	return workflowEntityFieldLifecycleOrder(current) > workflowEntityFieldLifecycleOrder(threshold)
}

func workflowEntityFieldLifecycleOrder(phase WorkflowEntityFieldLifecyclePhase) int {
	switch phase {
	case WorkflowEntityFieldLifecycleGuard:
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
	default:
		return 0
	}
}
