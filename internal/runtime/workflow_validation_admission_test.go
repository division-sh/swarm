package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	source := invalidWorkflowAdmissionConnectorSource(t)
	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil {
		t.Fatal("invalid connector declaration accepted")
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

func TestWorkflowAdmissionEarlyRefusalRetainsCompleteRegistryTail(t *testing.T) {
	source := invalidWorkflowAdmissionConnectorSource(t)
	controlOpts := StructuralWorkflowContractValidationOptions()
	controlOpts.FatalBootWarnings = false
	control, err := ValidateWorkflowContractSurface(context.Background(), compiledRuntimeValidationSource(t, testRuntimeWorkflowValidationBundle()), controlOpts)
	if err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []bootverify.ValidationPurpose{bootverify.StructuralValidation, bootverify.ExecutionValidation} {
		t.Run(fmt.Sprint(purpose), func(t *testing.T) {
			opts := controlOpts
			opts.Purpose = purpose
			opts.ExecutionPosture = executionposture.Live
			result, err := ValidateWorkflowContractSurface(context.Background(), source, opts)
			if err == nil || !strings.Contains(err.Error(), "effect_class must be non_idempotent_write") {
				t.Fatalf("source refusal disappeared: %v", err)
			}
			want := make(map[string]bool)
			for _, observation := range control.BootReport.Observations {
				if observation.Owner != workflowAdmissionOwner {
					want[observation.CheckID+"/"+string(observation.Class)] = true
				}
			}
			for _, observation := range result.BootReport.Observations {
				if observation.Owner == workflowAdmissionOwner {
					continue
				}
				identity := observation.CheckID + "/" + string(observation.Class)
				if !want[identity] || observation.Subject != "selected_source" || observation.Status != bootverify.AdmissionNotRun ||
					len(observation.Dependencies) != 1 || observation.Dependencies[0] != "source_validation" || !observation.StartedAt.IsZero() || !observation.FinishedAt.IsZero() {
					t.Fatalf("blocked registry check lost exact scope or claimed execution: %#v", observation)
				}
				delete(want, identity)
			}
			if len(want) != 0 {
				t.Fatalf("early source refusal hid registry tail: %#v", want)
			}
		})
	}
}

func invalidWorkflowAdmissionConnectorSource(t *testing.T) semanticview.Source {
	t.Helper()
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"provider.send": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolCategory("provider_connector"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolEffect(runtimecontracts.ActivityEffectClassReadOnly),
			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid"}),
		),
	}
	return compiledRuntimeValidationSource(t, bundle)
}
