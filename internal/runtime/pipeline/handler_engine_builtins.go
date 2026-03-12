package pipeline

import (
	"context"
	"fmt"
	"strings"
)

type workflowBuiltinGuardFunc func(*handlerEngineExecution, string) (bool, bool, error)
type workflowBuiltinActionFunc func(context.Context, *FactoryPipelineCoordinator, WorkflowHookContext, string) (bool, error)

var workflowBuiltinGuardHandlers = map[string]workflowBuiltinGuardFunc{
	"has_entity_id":              builtinGuardHasEntityID,
	"has_human_decision":         builtinGuardHasHumanDecision,
	"not_in_terminal_state":      builtinGuardNotInTerminalState,
	"revision_count_below_limit": builtinGuardRevisionCountBelowLimit,
	"state_in_phase":             builtinGuardStateInPhase,
}

var workflowBuiltinActionHandlers = map[string]workflowBuiltinActionFunc{
	"increment_revision_count": builtinActionIncrementRevisionCount,
	"record_state_change":      builtinActionImplicitNoop,
	"update_stage":             builtinActionImplicitNoop,
	"cancel_stage_timers":      builtinActionImplicitNoop,
	"start_stage_timers":       builtinActionImplicitNoop,
}

func lookupWorkflowBuiltinGuard(id string) (workflowBuiltinGuardFunc, bool) {
	handler, ok := workflowBuiltinGuardHandlers[normalizeWorkflowBuiltinGuardID(id)]
	return handler, ok
}

func lookupWorkflowBuiltinAction(id string) (workflowBuiltinActionFunc, bool) {
	handler, ok := workflowBuiltinActionHandlers[normalizeWorkflowBuiltinActionID(id)]
	return handler, ok
}

func isSupportedWorkflowGuardBuiltin(id string) bool {
	_, ok := lookupWorkflowBuiltinGuard(id)
	return ok
}

func isSupportedWorkflowActionBuiltin(id string) bool {
	_, ok := lookupWorkflowBuiltinAction(id)
	return ok
}

func normalizeWorkflowBuiltinGuardID(id string) string {
	switch strings.TrimSpace(id) {
	case "has_vertical_id":
		return "has_entity_id"
	case "inner_revision_count_below_limit":
		return "revision_count_below_limit"
	case "not_in_operating_phase", "not_in_terminal_stage":
		return "not_in_terminal_state"
	default:
		return strings.TrimSpace(id)
	}
}

func normalizeWorkflowBuiltinActionID(id string) string {
	return strings.TrimSpace(id)
}

func builtinGuardHasEntityID(exec *handlerEngineExecution, _ string) (bool, bool, error) {
	return hasValidUUID(exec.entityID), true, nil
}

func builtinGuardHasHumanDecision(exec *handlerEngineExecution, _ string) (bool, bool, error) {
	source := strings.TrimSpace(exec.event.SourceAgent)
	if strings.EqualFold(source, "human") || strings.EqualFold(source, "mailbox") {
		return true, true, nil
	}
	if strings.EqualFold(strings.TrimSpace(asString(exec.payload["decision_path"])), "mailbox") {
		return true, true, nil
	}
	return strings.TrimSpace(asString(exec.payload["mailbox_decision_id"])) != "", true, nil
}

func builtinGuardNotInTerminalState(exec *handlerEngineExecution, _ string) (bool, bool, error) {
	pc := exec.coordinator()
	if pc == nil || pc.SemanticSource() == nil {
		return true, true, nil
	}
	currentState := strings.TrimSpace(string(exec.state.Stage))
	if currentState == "" {
		return true, true, nil
	}
	workflow := pc.WorkflowDefinition()
	if workflow != nil {
		if stage, ok := workflow.Stage(exec.state.Stage); ok {
			return !stage.Terminal, true, nil
		}
	}
	for _, terminal := range pc.SemanticSource().WorkflowTerminalStages() {
		if strings.EqualFold(strings.TrimSpace(terminal), currentState) {
			return false, true, nil
		}
	}
	return true, true, nil
}

func builtinGuardRevisionCountBelowLimit(exec *handlerEngineExecution, policyRef string) (bool, bool, error) {
	limit := 3
	for _, key := range []string{strings.TrimSpace(policyRef), "max_revisions"} {
		if key == "" {
			continue
		}
		if value, ok := workflowExpressionLookupPath(exec.policy, key); ok {
			if parsed := asInt(value); parsed > 0 {
				limit = parsed
				break
			}
		}
		if parsed := asInt(exec.policy[key]); parsed > 0 {
			limit = parsed
			break
		}
	}
	return asInt(exec.state.Metadata["revision_count"]) < limit, true, nil
}

func builtinGuardStateInPhase(exec *handlerEngineExecution, policyRef string) (bool, bool, error) {
	pc := exec.coordinator()
	if pc == nil || pc.WorkflowDefinition() == nil {
		return false, true, nil
	}
	stage, ok := pc.WorkflowDefinition().Stage(exec.state.Stage)
	if !ok {
		return false, true, nil
	}
	required := strings.TrimSpace(policyRef)
	if required != "" {
		if value, ok := workflowExpressionLookupPath(exec.policy, required); ok {
			required = strings.TrimSpace(asString(value))
		}
	}
	if required == "" {
		required = strings.TrimSpace(asString(exec.policy["required_phase"]))
	}
	if required == "" {
		return false, true, fmt.Errorf("state_in_phase requires policy.required_phase")
	}
	return strings.EqualFold(strings.TrimSpace(stage.Phase), required), true, nil
}

func builtinActionIncrementRevisionCount(ctx context.Context, pc *FactoryPipelineCoordinator, hookCtx WorkflowHookContext, _ string) (bool, error) {
	if pc == nil {
		return false, fmt.Errorf("increment_revision_count requires runtime coordinator")
	}
	applyRevisionMutation := func() {
		pc.mutateValidationState(context.Background(), hookCtx.VerticalID, func(st *validationPipelineState) {
			st.RevisionCount++
		})
	}
	if !queuePipelinePostCommitAction(ctx, applyRevisionMutation) {
		applyRevisionMutation()
	}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		_ = pc.workflowStore.Mutate(ctx, hookCtx.VerticalID, func(instance *WorkflowInstance) {
			metadata := workflowMutableMetadata(instance)
			metadata["revision_count"] = asInt(metadata["revision_count"]) + 1
			if bucket, ok := workflowValidationProjectionBucket(*instance); ok {
				bucket["revision_count"] = asInt(bucket["revision_count"]) + 1
				workflowSetStateBucket(instance, workflowStateBucketValidationOrchestrator, bucket)
			}
		})
	}
	return true, nil
}

func builtinActionImplicitNoop(_ context.Context, _ *FactoryPipelineCoordinator, _ WorkflowHookContext, _ string) (bool, error) {
	return true, nil
}
