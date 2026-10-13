package runfork

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestDisposablePlanComponentsDetectDriftWithoutChangingEvidence(t *testing.T) {
	plan := RunForkPlan{SourceRunID: "fixture-source", SourceRunStatus: "running",
		ForkPoint: RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 1}}.WithHistoricalEvents(1, nil)
	before, err := DiagnosticSelectedPreparationPlanComponents(plan, RunForkContractFrontierAdmission{}, RunForkSelectedContractRecipientPlanning{}, "declaration")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Plan", "History", "Inputs", "Frontier", "Recipients", "Declarations", "Plan.fork_point", "Plan.source_run_status"} {
		if len(before[name]) != 64 {
			t.Fatalf("missing component fingerprint: %s", name)
		}
	}
	plan.SourceRunStatus = "completed"
	after, err := DiagnosticSelectedPreparationPlanComponents(plan, RunForkContractFrontierAdmission{}, RunForkSelectedContractRecipientPlanning{}, "declaration")
	if err != nil {
		t.Fatal(err)
	}
	if before["Plan"] == after["Plan"] || before["Plan.source_run_status"] == after["Plan.source_run_status"] {
		t.Fatal("plan/status drift was not captured")
	}
	for _, name := range []string{"History", "Inputs", "Frontier", "Recipients", "Declarations", "Plan.fork_point"} {
		if before[name] != after[name] {
			t.Fatalf("unchanged component changed fingerprint: %s", name)
		}
	}
	binding := SelectedForkPreparation{}
	original, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	binding.DiagnosticPlanComponents = before
	diagnostic, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, diagnostic) || strings.Contains(string(diagnostic), "Diagnostic") {
		t.Fatal("diagnostic leaked into persisted preparation bytes")
	}
}
