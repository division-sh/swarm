package runtime

import (
	"context"
	"errors"
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
	if IsSoleWorkflowAdmissionReportRefusal(err) {
		t.Fatal("independent source refusal was marked as a report summary")
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
	t.Run("owned_report_summary", func(t *testing.T) {
		cause := errors.New("owned report text")
		var result WorkflowContractValidationResult
		err := result.settleWorkflowAdmission(t.Context(), cause, "", true, true, "report-blocker")
		if !IsSoleWorkflowAdmissionReportRefusal(err) || errors.Unwrap(err) != cause || err.Error() != cause.Error() {
			t.Fatalf("report settlement changed the summary or its cause: %v", err)
		}
	})
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
