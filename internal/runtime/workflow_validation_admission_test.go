package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func TestWorkflowAdmissionAccountsForPostRegistryClauses(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	opts := StructuralWorkflowContractValidationOptions()
	opts.FatalBootWarnings = false
	result, err := ValidateWorkflowContractSurface(context.Background(), compiledRuntimeValidationSource(t, bundle), opts)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, observation := range result.BootReport.Observations {
		if observation.Owner != workflowAdmissionOwner {
			continue
		}
		if seen[observation.CheckID] {
			t.Fatalf("duplicate clause evidence: %#v", observation)
		}
		seen[observation.CheckID] = true
		if observation.Class == bootverify.AdmissionSourceObservation && observation.Status != bootverify.AdmissionPassed {
			t.Fatalf("executed declaration clause not recorded: %#v", observation)
		}
		if observation.Class == bootverify.AdmissionDeploymentObservation && observation.Status != bootverify.AdmissionNotRun {
			t.Fatalf("portable validation invented deployment evidence: %#v", observation)
		}
	}
	if len(seen) != len(workflowAdmissionObservations(opts.Purpose)) {
		t.Fatalf("missing post-registry clauses: %#v", seen)
	}
}

func TestWorkflowAdmissionEarlyRefusalNamesUnperformedTail(t *testing.T) {
	source := loadHarnessInjectionValidationSource(t)
	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil {
		t.Fatal("production harness dependency accepted")
	}
	if result.BootReport.Observations[0].Status != bootverify.AdmissionFailed {
		t.Fatal("source refusal was not recorded")
	}
	for _, observation := range result.BootReport.Observations[1:] {
		if observation.Status != bootverify.AdmissionNotRun || len(observation.Dependencies) != 1 || observation.Dependencies[0] != "source_validation" {
			t.Fatalf("unperformed tail was lost or passed: %#v", observation)
		}
	}
	if result.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{}).Complete {
		t.Fatal("early refusal produced complete admission")
	}
}
