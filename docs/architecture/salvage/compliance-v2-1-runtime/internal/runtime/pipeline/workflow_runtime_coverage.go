//go:build ignore

package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func workflowGuardImplemented(guardID string) bool {
	module := defaultWorkflowModuleOrNil()
	switch strings.TrimSpace(guardID) {
	case "",
		"pipeline_has_capacity",
		"has_vertical_id",
		"has_entity_id",
		"has_human_decision",
		"inner_revision_count_below_limit",
		"revision_count_below_limit",
		"gate_g1_research",
		"gate_g2_spec",
		"gate_g3_cto",
		"gate_g4_brand",
		"all_gates_met",
		"not_in_operating_phase",
		"stage_in_phase",
		"spec_validation_passed",
		"qa_passed",
		"deploy_approved",
		"has_retention_primitive",
		"no_blocking_red_flags",
		"evidence_sufficient":
		return true
	default:
		for _, id := range workflowProductGuardIDs(module) {
			if strings.TrimSpace(id) == strings.TrimSpace(guardID) {
				return true
			}
		}
		return false
	}
}

func workflowActionImplemented(actionID string) bool {
	module := defaultWorkflowModuleOrNil()
	switch strings.TrimSpace(actionID) {
	case "",
		"increment_revision_count",
		"emit_validation_started",
		"emit_spec_validation_requested",
		"emit_validation_package_ready",
		"select_rubric",
		"emit_scoring_requested",
		"emit_vertical_shortlisted",
		"emit_vertical_marginal",
		"emit_vertical_rejected",
		"emit_vertical_resumed",
		"emit_opco_spinup_requested",
		"spinup_opco_org",
		"begin_teardown":
		return true
	default:
		for _, id := range workflowProductActionIDs(module) {
			if strings.TrimSpace(id) == strings.TrimSpace(actionID) {
				return true
			}
		}
		return false
	}
}

func workflowProductGuardIDs(module WorkflowModule) []string {
	if module != nil {
		return module.ProductWorkflowGuards()
	}
	return []string{
		"signal_above_threshold",
		"composite_above_shortlist",
		"composite_in_marginal_range",
		"composite_below_marginal",
		"both_hard_gates_pass",
		"marginal_promotion_eligible",
	}
}

func workflowProductActionIDs(module WorkflowModule) []string {
	if module != nil {
		return module.ProductWorkflowActions()
	}
	return nil
}

func workflowParticipantCanSeeEvent(bundle *runtimecontracts.WorkflowContractBundle, participant, eventType string) bool {
	participant = strings.TrimSpace(participant)
	eventType = strings.TrimSpace(eventType)
	if participant == "" || eventType == "" || bundle == nil {
		return false
	}
	if participant == "runtime" || participant == "human" {
		return true
	}
	if node, ok := bundle.Nodes[participant]; ok {
		return workflowStringSliceContains(node.SubscribesTo, eventType) || workflowStringSliceContains(node.Produces, eventType)
	}
	for _, agent := range bundle.Agents {
		if strings.TrimSpace(agent.ID) != participant && strings.TrimSpace(agent.Role) != participant {
			continue
		}
		if workflowStringSliceContains(agent.Subscriptions, eventType) ||
			workflowStringSliceContains(agent.SubscriptionsBootstrap, eventType) ||
			workflowStringSliceContains(agent.SubscribesTo, eventType) ||
			workflowStringSliceContains(agent.EmitEvents, eventType) {
			return true
		}
	}
	return false
}

func workflowStringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
