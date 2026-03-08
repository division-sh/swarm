package empire

import (
	"context"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

var tier1ScoringDimensions = []string{
	"icp_crispness",
	"distribution_leverage",
	"time_to_value",
	"operational_drag",
}

func (module) EvaluateWorkflowGuard(ctx context.Context, runtime runtimepipeline.WorkflowHookRuntime, hookCtx runtimepipeline.WorkflowHookContext, guard runtimecontracts.GuardActionEntry) (bool, bool) {
	_ = ctx
	switch strings.TrimSpace(guard.ID) {
	case "signal_above_threshold":
		return payloadFloat(hookCtx.Payload, "signal_strength") >= runtime.ContractPolicyFloat("signal_threshold", 55), true
	case "composite_above_shortlist":
		if result := strings.TrimSpace(payloadString(hookCtx.Payload, "result")); result != "" {
			return strings.EqualFold(result, "shortlisted"), true
		}
		return payloadFloat(hookCtx.Payload, "composite_score") >= runtime.ContractPolicyFloat("composite_shortlist", 75), true
	case "composite_in_marginal_range":
		if result := strings.TrimSpace(payloadString(hookCtx.Payload, "result")); result != "" {
			return strings.EqualFold(result, "marginal"), true
		}
		composite := payloadFloat(hookCtx.Payload, "composite_score")
		low := runtime.ContractPolicyFloat("composite_marginal_low", 55)
		high := runtime.ContractPolicyFloat("composite_shortlist", 75)
		promotionEligible, _ := module{}.EvaluateWorkflowGuard(ctx, runtime, hookCtx, runtimecontracts.GuardActionEntry{ID: "marginal_promotion_eligible"})
		return composite >= low && composite < high && promotionEligible, true
	case "composite_below_marginal":
		if result := strings.TrimSpace(payloadString(hookCtx.Payload, "result")); result != "" {
			return strings.EqualFold(result, "rejected"), true
		}
		composite := payloadFloat(hookCtx.Payload, "composite_score")
		low := runtime.ContractPolicyFloat("composite_marginal_low", 55)
		high := runtime.ContractPolicyFloat("composite_shortlist", 75)
		if composite < low {
			return true, true
		}
		promotionEligible, _ := module{}.EvaluateWorkflowGuard(ctx, runtime, hookCtx, runtimecontracts.GuardActionEntry{ID: "marginal_promotion_eligible"})
		return composite >= low && composite < high && !promotionEligible, true
	case "both_hard_gates_pass":
		floor := runtime.ContractPolicyInt("hard_gate_floor", 50)
		return scoringDimensionScore(hookCtx.Payload, "build_complexity") >= floor &&
			scoringDimensionScore(hookCtx.Payload, "automation_completeness") >= floor, true
	case "marginal_promotion_eligible":
		threshold := runtime.ContractPolicyInt("marginal_tier1_dimensions_above_70", 2)
		count := 0
		for _, dim := range tier1ScoringDimensions {
			if scoringDimensionScore(hookCtx.Payload, dim) >= 70 {
				count++
			}
		}
		return count >= threshold, true
	case "pipeline_has_capacity":
		return runtime.PipelineHasCapacity(ctx, runtime.ContractPolicyInt("pipeline_capacity_max", 3)), true
	case "gate_g1_research":
		return metadataFlag(hookCtx.State.Metadata, "g1_research"), true
	case "gate_g2_spec":
		return metadataFlag(hookCtx.State.Metadata, "g2_spec"), true
	case "gate_g3_cto":
		return metadataFlag(hookCtx.State.Metadata, "g3_cto"), true
	case "gate_g4_brand":
		return metadataFlag(hookCtx.State.Metadata, "g4_brand"), true
	case "all_gates_met":
		return metadataFlag(hookCtx.State.Metadata, "g1_research") &&
			metadataFlag(hookCtx.State.Metadata, "g2_spec") &&
			metadataFlag(hookCtx.State.Metadata, "g3_cto") &&
			metadataFlag(hookCtx.State.Metadata, "g4_brand"), true
	case "spec_validation_passed":
		return strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "status")), "passed") ||
			strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "passed")), "true"), true
	case "qa_passed":
		return strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "qa_passed")), "true") ||
			strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "status")), "passed"), true
	case "deploy_approved":
		return strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "decision")), "approved") ||
			strings.EqualFold(strings.TrimSpace(payloadString(hookCtx.Payload, "deploy_approved")), "true"), true
	case "has_retention_primitive":
		return payloadSliceLen(hookCtx.Payload["retention_primitives"]) > 0, true
	case "evidence_sufficient":
		return payloadSliceLen(hookCtx.Payload["competitors"]) > 0 &&
			payloadSliceLen(hookCtx.Payload["pain_signals"]) > 0, true
	default:
		return false, false
	}
}

func (module) ExecuteWorkflowAction(ctx context.Context, runtime runtimepipeline.WorkflowHookRuntime, hookCtx runtimepipeline.WorkflowHookContext, action runtimecontracts.GuardActionEntry) (bool, bool) {
	payloadFactory := runtime.WorkflowPayloadFactory()
	if payloadFactory == nil {
		return false, false
	}
	switch strings.TrimSpace(action.ID) {
	case "select_rubric":
		runtime.PersistWorkflowMetadata(ctx, hookCtx.VerticalID, func(metadata map[string]any) {
			metadata["selected_rubric"] = strings.TrimSpace(payloadString(hookCtx.Payload, "rubric"))
		})
		return true, true
	case "emit_scoring_requested":
		return true, true
	case "emit_validation_started":
		runtime.PublishWorkflowEvent(ctx, "validation.started", hookCtx.VerticalID, payloadMap(payloadFactory.BuildValidationStartedPayload(ctx, hookCtx.VerticalID, hookCtx.Payload, nil)))
		return true, true
	case "emit_vertical_shortlisted":
		runtime.PublishWorkflowEvent(ctx, "vertical.shortlisted", hookCtx.VerticalID, payloadMap(payloadFactory.BuildVerticalShortlistedPayload(
			hookCtx.VerticalID,
			payloadFloat(hookCtx.Payload, "composite_score"),
			payloadFloat(hookCtx.Payload, "viability_score"),
			hookCtx.Payload,
		)))
		return true, true
	case "emit_vertical_marginal":
		runtime.PublishWorkflowEvent(ctx, "vertical.marginal", hookCtx.VerticalID, payloadMap(payloadFactory.BuildVerticalMarginalPayload(
			hookCtx.VerticalID,
			scoringCompositeFromPayload(hookCtx.Payload),
		)))
		return true, true
	case "emit_vertical_rejected":
		runtime.PublishWorkflowEvent(ctx, "vertical.rejected", hookCtx.VerticalID, payloadMap(payloadFactory.BuildVerticalRejectedPayload(
			hookCtx.VerticalID,
			scoringCompositeFromPayload(hookCtx.Payload),
		)))
		return true, true
	case "emit_vertical_resumed":
		return true, true
	case "emit_opco_spinup_requested":
		runtime.PersistWorkflowMetadata(ctx, hookCtx.VerticalID, func(metadata map[string]any) {
			metadata["opco_spinup_emitted"] = true
		})
		return true, true
	default:
		return false, false
	}
}

func payloadString(payload map[string]any, key string) string {
	raw, ok := payload[strings.TrimSpace(key)]
	if !ok {
		return ""
	}
	switch typed := raw.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

func payloadFloat(payload map[string]any, key string) float64 {
	raw, ok := payload[strings.TrimSpace(key)]
	if !ok {
		return 0
	}
	switch typed := raw.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	default:
		return 0
	}
}

func scoringDimensionScore(payload map[string]any, dimension string) int {
	rawDimensions, ok := payload["dimensions"].(map[string]any)
	if !ok {
		return 0
	}
	rawResult, ok := rawDimensions[strings.TrimSpace(dimension)].(map[string]any)
	if !ok {
		return 0
	}
	switch typed := rawResult["score"].(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func metadataFlag(metadata map[string]any, key string) bool {
	raw, ok := metadata[strings.TrimSpace(key)]
	if !ok {
		return false
	}
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func payloadSliceLen(raw any) int {
	switch typed := raw.(type) {
	case []any:
		return len(typed)
	case []string:
		return len(typed)
	default:
		return 0
	}
}

func payloadMap[T any](in T) map[string]any {
	return runtimepipeline.ToMap(in)
}

func scoringCompositeFromPayload(payload map[string]any) runtimepipeline.ScoringComposite {
	return runtimepipeline.ScoringCompositeFromPayload(payload)
}
