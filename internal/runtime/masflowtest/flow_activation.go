package masflowtest

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

func LoadMASWorkflowBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
	)
	if err != nil {
		t.Fatalf("load workflow contract bundle: %v", err)
	}
	return bundle
}

func ActivateOperatingFlowInstance(t testing.TB, am *runtimemanager.AgentManager, verticalID string, config map[string]any) {
	t.Helper()
	if am == nil {
		t.Fatal("agent manager is required")
	}
	payload := map[string]any{
		"vertical_name":        "ClinicOps",
		"vertical_description": "billing automation for clinics",
		"geography":            "us",
		"mandate_document":     "Reduce claim denials",
		"founder_directives":   "Prioritize quick wins",
		"org_roster":           "CEO, Product, Growth",
		"monthly_api_cap":      "$500",
		"product_budget":       "$300",
		"tech_stack":           "go,postgres,react",
	}
	for key, value := range config {
		payload[key] = value
	}
	if err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(LoadMASWorkflowBundle(t)),
		TemplateID:     "operating",
		InstanceID:     verticalID,
		VerticalID:     verticalID,
		FlowPath:       "operating/" + verticalID,
		InitialState:   "approved",
		Config:         payload,
	}); err != nil {
		t.Fatalf("activate operating flow instance: %v", err)
	}
}
