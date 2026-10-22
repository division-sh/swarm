package pipeline

import (
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type WorkflowEntityFieldLifecyclePhase string

const (
	WorkflowEntityFieldLifecycleGuard            WorkflowEntityFieldLifecyclePhase = "guard"
	WorkflowEntityFieldLifecycleGuardEscalation  WorkflowEntityFieldLifecyclePhase = "guard_escalation_fields"
	WorkflowEntityFieldLifecycleGate             WorkflowEntityFieldLifecyclePhase = "gate"
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
	normalized, _, err := normalizeWorkflowExpression(expression, workflowExpressionContext{AllowUnresolvedQueryOperands: true})
	if err != nil {
		return nil
	}
	return workflowexpr.EntityReferences(normalized)
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
