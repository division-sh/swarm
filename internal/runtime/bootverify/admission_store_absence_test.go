package bootverify

import (
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func absentStore2567Report() Report {
	return absentStore2567ReportAt("/safe/missing.db")
}

func absentStore2567ReportAt(path string) Report {
	now := time.Now().UTC()
	subject := "store:" + path
	report := Report{Purpose: ExecutionValidation, Observations: []AdmissionObservation{
		{CheckID: "selected_store_access", Owner: SelectedStoreAccessOwner, Subject: subject,
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "observed absent",
			StartedAt: now, FinishedAt: now, NotRunCause: &AdmissionNotRunCause{Kind: AdmissionAbsentSQLiteStore, Path: path}},
		{CheckID: "selected_store_schema", Owner: "internal/store/selected.AdmissionInspection.Inspect", Subject: subject,
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "store absent", Dependencies: []string{"selected_store_access"}},
		{CheckID: "startup_process_possession", Owner: "internal/store/selected.AdmissionInspection.ProbePossession", Subject: subject,
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "store absent", Dependencies: []string{"selected_store_access"}},
	}}
	for _, check := range SelectedStoreDependentAdmissionChecks() {
		report.Observations = append(report.Observations, AdmissionObservation{
			CheckID: check.ID, Owner: check.Owner, Subject: report.Observations[0].Subject,
			Class: AdmissionDeploymentObservation, Status: AdmissionNotRun, Reason: "store absent", Dependencies: []string{"selected_store_access"},
		})
	}
	return report
}

func TestIssue2567AbsentStoreReductionIsIncompleteNotFailure(t *testing.T) {
	report := absentStore2567Report()
	if len(report.Observations) != 14 {
		t.Fatalf("valid ledger has %d rows, want root plus 13 dependents", len(report.Observations))
	}
	if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != "" || got.Interrupted {
		t.Fatalf("absence reduction = %+v", got)
	}
}

var absentStore2567RequiredIDs = []string{
	"selected_store_schema", "startup_process_possession", "pinned_source_admission",
	"retained_source_integrity", "startup_recovery_admission", "startup_authority_lineage",
	"pending_reset_admission", "retained_route_admission", "retained_channel_admission",
	"selected_fork_recovery_admission", "retained_actor_admission",
	"selected_fork_source_dependencies", "retained_actor_provider_dependencies",
}

func TestIssue2567AbsentStoreReductionRequiresEveryDependent(t *testing.T) {
	for _, id := range absentStore2567RequiredIDs {
		t.Run(id, func(t *testing.T) {
			report := absentStore2567Report()
			report.Observations = slices.DeleteFunc(report.Observations, func(o AdmissionObservation) bool { return o.CheckID == id })
			if len(report.Observations) != 13 {
				t.Fatalf("fixture must contain exactly one %s", id)
			}
			if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != failures.ClassSchemaInvalid {
				t.Fatalf("omitted %s admitted: %+v", id, got)
			}
		})
	}
	t.Run("absence_root_only", func(t *testing.T) {
		report := absentStore2567Report()
		report.Observations = report.Observations[:1]
		if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != failures.ClassSchemaInvalid {
			t.Fatalf("all dependent observations omitted: %+v", got)
		}
	})
}

func TestIssue2567AbsentStoreReductionIsOrderIndependent(t *testing.T) {
	for shift := 0; shift < 14; shift++ {
		report := absentStore2567Report()
		slices.Reverse(report.Observations)
		report.Observations = append(report.Observations[shift:], report.Observations[:shift]...)
		if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != "" || got.Interrupted {
			t.Fatalf("valid shuffled ledger shift %d: %+v", shift, got)
		}
	}
}

func TestIssue2567AbsentStoreReductionIsolatesSubjects(t *testing.T) {
	newReport := func() Report {
		first, second := absentStore2567Report(), absentStore2567ReportAt("/safe/other.db")
		first.Observations = append(first.Observations, second.Observations...)
		slices.Reverse(first.Observations)
		return first
	}
	if got := newReport().AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != "" || got.Interrupted {
		t.Fatalf("two complete subjects refused: %+v", got)
	}
	for _, subject := range []string{"store:/safe/missing.db", "store:/safe/other.db"} {
		t.Run(subject, func(t *testing.T) {
			for _, id := range absentStore2567RequiredIDs {
				t.Run(id, func(t *testing.T) {
					report := newReport()
					report.Observations = slices.DeleteFunc(report.Observations, func(o AdmissionObservation) bool { return o.Subject == subject && o.CheckID == id })
					if len(report.Observations) != 27 {
						t.Fatalf("fixture must contain exactly one %s for %s", id, subject)
					}
					if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != failures.ClassSchemaInvalid {
						t.Fatalf("complete peer hid omitted %s for %s: %+v", id, subject, got)
					}
				})
			}
		})
	}
}

func TestIssue2567AbsentStoreReductionRejectsForgedEvidence(t *testing.T) {
	for _, name := range []string{"unknown_cause", "relative_path", "unclean_path", "wrong_root_subject", "wrong_root_owner", "wrong_root_check", "wrong_status", "wrong_class", "missing_time", "root_failure", "blank_reason", "root_dependency", "structural", "orphan", "wrong_dependent_subject", "wrong_dependent_owner", "wrong_dependent_class", "blank_dependent_reason", "dependent_failure", "unknown_dependent", "provider_dependency", "wrong_dependency", "duplicate", "duplicate_dependent", "contradictory_pass", "contradictory_root_class"} {
		t.Run(name, func(t *testing.T) {
			report := absentStore2567Report()
			if got := report.AdmissionDecision(AdmissionFindingPolicy{}); got.Complete || got.FailureClass != "" || got.Interrupted {
				t.Fatalf("invalid unmodified baseline: %+v", got)
			}
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
			case "wrong_dependent_class":
				child.Class = AdmissionSourceObservation
			case "blank_dependent_reason":
				child.Reason = ""
			case "dependent_failure":
				child.FailureClass = failures.ClassAuthenticationNeeded
			case "unknown_dependent":
				child.CheckID = "unknown_check"
			case "provider_dependency":
				child.CheckID = "provider_credential_admission"
			case "wrong_dependency":
				child.Dependencies = []string{"unrelated"}
			case "duplicate":
				report.Observations = append(report.Observations, *root)
			case "duplicate_dependent":
				report.Observations = append(report.Observations, *child)
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
