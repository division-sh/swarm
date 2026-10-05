package bootverify

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func absentStore2567Report() Report {
	now := time.Now().UTC()
	return Report{Purpose: ExecutionValidation, Observations: []AdmissionObservation{
		{CheckID: "selected_store_access", Owner: SelectedStoreAccessOwner, Subject: "store:/safe/missing.db",
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "observed absent",
			StartedAt: now, FinishedAt: now, NotRunCause: &AdmissionNotRunCause{Kind: AdmissionAbsentSQLiteStore, Path: "/safe/missing.db"}},
		{CheckID: "selected_store_schema", Owner: "internal/store/selected.AdmissionInspection.Inspect", Subject: "store:/safe/missing.db",
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "store absent", Dependencies: []string{"selected_store_access"}},
	}}
}

func TestIssue2567AbsentStoreReductionIsIncompleteNotFailure(t *testing.T) {
	report := absentStore2567Report()
	for _, check := range SelectedStoreDependentAdmissionChecks() {
		report.Observations = append(report.Observations, AdmissionObservation{
			CheckID: check.ID, Owner: check.Owner, Subject: report.Observations[0].Subject,
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "store absent", Dependencies: []string{"selected_store_access"},
		})
	}
	if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != "" || got.Interrupted {
		t.Fatalf("absence reduction = %+v", got)
	}
}

func TestIssue2567AbsentStoreReductionRejectsForgedEvidence(t *testing.T) {
	for _, name := range []string{"unknown_cause", "relative_path", "unclean_path", "wrong_root_subject", "wrong_root_owner", "wrong_root_check", "wrong_status", "wrong_class", "missing_time", "root_failure", "blank_reason", "root_dependency", "structural", "orphan", "wrong_dependent_subject", "wrong_dependent_owner", "unknown_dependent", "provider_dependency", "wrong_dependency", "duplicate", "contradictory_pass", "contradictory_root_class"} {
		t.Run(name, func(t *testing.T) {
			report := absentStore2567Report()
			root, child := &report.Observations[0], &report.Observations[1]
			switch name {
			case "unknown_cause":
				root.NotRunCause.Kind = "optional"
			case "relative_path":
				root.NotRunCause.Path = "missing.db"
			case "unclean_path":
				root.NotRunCause.Path = "/safe/../missing.db"
			case "wrong_root_subject":
				root.Subject = "store:/other/missing.db"
			case "wrong_root_owner":
				root.Owner = "another.owner"
			case "wrong_root_check":
				root.CheckID = "provider_environment_admission"
			case "wrong_status":
				root.Status = AdmissionPassed
			case "wrong_class":
				root.Class = AdmissionSourceObservation
			case "missing_time":
				root.StartedAt = time.Time{}
			case "root_failure":
				root.FailureClass = failures.ClassAuthenticationNeeded
			case "blank_reason":
				root.Reason = ""
			case "root_dependency":
				root.Dependencies = []string{"another"}
			case "structural":
				report.Purpose = StructuralValidation
			case "orphan":
				report.Observations = report.Observations[1:]
			case "wrong_dependent_subject":
				child.Subject = "store:/other/missing.db"
			case "wrong_dependent_owner":
				child.Owner = "another.owner"
			case "unknown_dependent":
				child.CheckID = "unknown_check"
			case "provider_dependency":
				child.CheckID = "provider_credential_admission"
			case "wrong_dependency":
				child.Dependencies = []string{"unrelated"}
			case "duplicate":
				report.Observations = append(report.Observations, *root)
			case "contradictory_pass":
				child.Status = AdmissionPassed
				child.StartedAt = root.StartedAt
				child.FinishedAt = root.FinishedAt
			case "contradictory_root_class":
				contradiction := *root
				contradiction.NotRunCause = nil
				contradiction.Class = AdmissionSourceObservation
				contradiction.Status = AdmissionPassed
				report.Observations = append(report.Observations, contradiction)
			}
			got := report.AdmissionDecision(AdmissionFindingPolicy{})
			if got.Complete || got.FailureClass == "" {
				t.Fatalf("forged absence admitted: %+v", got)
			}
		})
	}
}

func TestIssue2567AbsentStoreDoesNotMaskOtherFailureOrInterruption(t *testing.T) {
	for _, class := range []failures.Class{failures.ClassSchemaInvalid, failures.ClassAuthenticationNeeded, failures.ClassLifecycleConflict, failures.ClassDependencyUnavailable} {
		report := absentStore2567Report()
		finding := NewHardInvalidityFinding("independent", "provider:exact", "independent failure", "fix it")
		finding.FailureClass = class
		report.Add(finding)
		if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.FailureClass != class || got.Complete {
			t.Fatalf("absence hid %s: %+v", class, got)
		}
	}
	report := absentStore2567Report()
	report.Interrupted = true
	if got := report.AdmissionDecision(AdmissionFindingPolicy{}); !got.Interrupted || got.Complete {
		t.Fatalf("absence hid interruption: %+v", got)
	}
	report = absentStore2567Report()
	report.Observations = append(report.Observations, AdmissionObservation{CheckID: "provider_credential_admission", Owner: "credential.owner", Subject: "provider:exact", Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "missing read port"})
	if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.FailureClass != failures.ClassDependencyUnavailable {
		t.Fatalf("generic not_run became optional: %+v", got)
	}
}
