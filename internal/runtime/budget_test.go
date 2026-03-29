package runtime

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestBudgetThresholdsFromSource_DisabledWhenThresholdsMissing(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{}},
	})

	thresholds := budgetThresholdsFromSource(source)
	if thresholds.Enabled {
		t.Fatal("expected budget thresholds to be disabled when policy keys are absent")
	}
}

func TestBudgetThresholdsFromSource_UsesConfiguredPercents(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"budget_warning_percent":   {Value: 80},
			"budget_throttle_percent":  {Value: 90},
			"budget_emergency_percent": {Value: 100},
		}},
	})

	thresholds := budgetThresholdsFromSource(source)
	if !thresholds.Enabled {
		t.Fatal("expected budget thresholds to be enabled")
	}
	if thresholds.Warning != 0.80 || thresholds.Throttle != 0.90 || thresholds.Emergency != 1.00 {
		t.Fatalf("thresholds = %#v, want 0.80/0.90/1.00", thresholds)
	}
}
