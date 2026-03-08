package pipeline

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func workflowGuardExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return builtin
	}
	return strings.TrimSpace(entry.ID)
}

func workflowActionExecutionKey(entry runtimecontracts.GuardActionEntry) string {
	if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
		return builtin
	}
	return strings.TrimSpace(entry.ID)
}

func isExecutableWorkflowGuardEntry(entry runtimecontracts.GuardActionEntry) bool {
	switch workflowGuardExecutionKey(entry) {
	case "has_entity_id",
		"has_human_decision",
		"revision_count_below_limit",
		"not_in_terminal_stage",
		"stage_in_phase",
		"signal_above_threshold",
		"composite_above_shortlist",
		"composite_in_marginal_range",
		"composite_below_marginal",
		"both_hard_gates_pass",
		"marginal_promotion_eligible",
		"pipeline_has_capacity",
		"gate_g1_research",
		"gate_g2_spec",
		"gate_g3_cto",
		"gate_g4_brand",
		"all_gates_met",
		"spec_validation_passed",
		"qa_passed",
		"deploy_approved",
		"has_retention_primitive",
		"evidence_sufficient":
		return true
	default:
		return false
	}
}

func isExecutableWorkflowActionEntry(entry runtimecontracts.GuardActionEntry) bool {
	switch workflowActionExecutionKey(entry) {
	case "increment_revision_count",
		"select_rubric",
		"emit_scoring_requested",
		"emit_validation_started",
		"emit_vertical_shortlisted",
		"emit_vertical_marginal",
		"emit_vertical_rejected",
		"emit_vertical_resumed",
		"emit_opco_spinup_requested",
		"spinup_opco_org",
		"begin_teardown":
		return true
	default:
		return false
	}
}
