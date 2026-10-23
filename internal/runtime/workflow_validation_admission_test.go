package runtime

import (
	"context"

	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

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

func invalidWorkflowAdmissionConnectorSource(t *testing.T) semanticview.Source {
	t.Helper()
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"provider.send": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolCategory("provider_connector"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid"}),
		),
	}
	return compiledRuntimeValidationSource(t, bundle)
}
