package bootverify

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func admissionTestObservation(status AdmissionObservationStatus) AdmissionObservation {
	now := time.Now().UTC()
	return AdmissionObservation{
		CheckID: "credential_key_exists", Owner: registeredAdmissionOwner, Subject: "credential:required_key",
		Class: AdmissionDeploymentObservation, Status: status,
		StartedAt: now, FinishedAt: now,
	}
}

func TestAdmissionCompletenessReducerRequiresEvidence(t *testing.T) {
	for _, purpose := range []ValidationPurpose{ExecutionValidation, StructuralValidation} {
		decision := (Report{Purpose: purpose}).AdmissionDecision(AdmissionFindingPolicy{})
		if decision.Complete || decision.FailureClass != failures.ClassDependencyUnavailable {
			t.Fatalf("empty report purpose %d: %#v", purpose, decision)
		}
	}
	for _, status := range []AdmissionObservationStatus{AdmissionPassed, AdmissionFailed, AdmissionUnavailable, AdmissionNotRun, AdmissionNotApplicable, "unknown"} {
		t.Run(string(status), func(t *testing.T) {
			observation := admissionTestObservation(status)
			if status == AdmissionNotApplicable {
				observation.Reason = "the selected source has no live credential requirement"
			}
			decision := (Report{Observations: []AdmissionObservation{observation}}).AdmissionDecision(AdmissionFindingPolicy{})
			wantComplete := status == AdmissionPassed || status == AdmissionNotApplicable
			if decision.Complete != wantComplete || (decision.FailureClass == "") != wantComplete {
				t.Fatalf("decision=%#v, want complete=%t", decision, wantComplete)
			}
		})
	}
}

func TestAdmissionCompletenessReducerRejectsUnsupportedEvidence(t *testing.T) {
	for _, name := range []string{"missing_owner", "missing_subject", "missing_id", "unknown_class", "unproven_not_applicable", "passed_failure", "duplicate", "no_observation_time", "negative_observation_time", "blank_owner"} {
		t.Run(name, func(t *testing.T) {
			observation := admissionTestObservation(AdmissionPassed)
			switch name {
			case "missing_owner":
				observation.Owner = ""
			case "missing_subject":
				observation.Subject = ""
			case "missing_id":
				observation.CheckID = ""
			case "unknown_class":
				observation.Class = "unknown"
			case "unproven_not_applicable":
				observation.Status = AdmissionNotApplicable
			case "passed_failure":
				observation.FailureClass = failures.ClassAuthenticationNeeded
			case "no_observation_time":
				observation.StartedAt = time.Time{}
			case "negative_observation_time":
				observation.FinishedAt = observation.StartedAt.Add(-time.Second)
			case "blank_owner":
				observation.Owner = " "
			}
			report := Report{Observations: []AdmissionObservation{observation}}
			if name == "duplicate" {
				report.Observations = append(report.Observations, observation)
			}
			decision := report.AdmissionDecision(AdmissionFindingPolicy{})
			if decision.Complete || decision.FailureClass != failures.ClassSchemaInvalid {
				t.Fatalf("invalid evidence accepted: %#v", decision)
			}
		})
	}
}

func TestAdmissionCompletenessReducerPreservesPriorityAndFindingPolicy(t *testing.T) {
	classes := []failures.Class{failures.ClassDependencyUnavailable, failures.ClassLifecycleConflict, failures.ClassAuthenticationNeeded, failures.ClassSchemaInvalid}
	for _, left := range classes {
		for _, right := range classes {
			report := Report{}
			for i, class := range []failures.Class{left, right} {
				observation := admissionTestObservation(AdmissionFailed)
				observation.Subject += string(rune('a' + i))
				observation.FailureClass = class
				report.Observations = append(report.Observations, observation)
			}
			want := left
			if admissionFailurePriority(right) > admissionFailurePriority(left) {
				want = right
			}
			decision := report.AdmissionDecision(AdmissionFindingPolicy{})
			if decision.FailureClass != want || len(report.Observations) != 2 {
				t.Fatalf("%s/%s: decision=%#v", left, right, decision)
			}
			report.Interrupted = true
			decision = report.AdmissionDecision(AdmissionFindingPolicy{})
			if !decision.Interrupted || decision.FailureClass != want {
				t.Fatalf("interruption erased underlying evidence: %#v", decision)
			}
		}
	}
	for _, severity := range []string{SeveritySemanticDriftWarn, SeverityLintEvidence} {
		report := Report{Observations: []AdmissionObservation{admissionTestObservation(AdmissionPassed)}}
		report.Add(Finding{CheckID: "credential_key_exists", Severity: severity, FailureClass: failures.ClassAuthenticationNeeded, Message: "required credential is absent"})
		if decision := report.AdmissionDecision(AdmissionFindingPolicy{}); !decision.Complete {
			t.Fatalf("nonfatal finding became a refusal: %#v", decision)
		}
		decision := report.AdmissionDecision(AdmissionFindingPolicy{FatalWarnings: true})
		if severity == SeveritySemanticDriftWarn && decision.FailureClass != failures.ClassAuthenticationNeeded {
			t.Fatalf("fatal credential warning lost its class: %#v", decision)
		}
		if severity == SeverityLintEvidence && !decision.Complete {
			t.Fatalf("lint became fatal: %#v", decision)
		}
		decision = report.AdmissionDecision(AdmissionFindingPolicy{FatalWarnings: true, ExcludedFatalWarningChecks: []string{"credential_key_exists"}})
		if !decision.Complete {
			t.Fatalf("warning exclusion lost: %#v", decision)
		}
	}
}

func TestAdmissionReportSeparatesExecutionObligations(t *testing.T) {
	report := Report{
		Observations: []AdmissionObservation{admissionTestObservation(AdmissionPassed)},
		ExecutionObligations: []AdmissionExecutionObligation{{
			ID: "managed_provider_handshake", Owner: "internal/runtime.ValidateManagedProviderPreflight", Subject: "agent:worker",
			Reason: "requires production process authority and provider-owned visibility evidence",
		}},
	}
	if decision := report.AdmissionDecision(AdmissionFindingPolicy{}); !decision.Complete || decision.FailureClass != "" {
		t.Fatalf("execution obligation was confused with an admission observation: %#v", decision)
	}
	observation := admissionTestObservation(AdmissionNotRun)
	observation.Reason = "portable validation does not observe required credential state"
	report.Purpose, report.Observations = StructuralValidation, []AdmissionObservation{observation}
	if decision := report.AdmissionDecision(AdmissionFindingPolicy{}); decision.Complete || decision.FailureClass != "" {
		t.Fatalf("portable validation made a deployment claim: %#v", decision)
	}
	report.Purpose = ExecutionValidation
	if decision := report.AdmissionDecision(AdmissionFindingPolicy{}); decision.Complete || decision.FailureClass == "" {
		t.Fatalf("unperformed admission was relabeled as execution: %#v", decision)
	}
}

func TestAdmissionReportAccountsForEveryRegisteredClause(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{})
	report := Run(context.Background(), source, Options{Purpose: StructuralValidation})
	want := len(bootCheckRegistry) + len(supplementalChecks)
	for _, check := range bootCheckRegistry {
		if check.Mode == CheckMixed {
			want++
		}
	}
	if len(report.Observations) != want {
		t.Fatalf("observations=%d want=%d", len(report.Observations), want)
	}
	for _, observation := range report.Observations {
		if observation.Class == AdmissionDeploymentObservation && observation.Status != AdmissionNotRun {
			t.Fatalf("portable validation fabricated evidence: %#v", observation)
		}
		if observation.Owner == "" || observation.Subject == "" || observation.CheckID == "" {
			t.Fatalf("unattributed check: %#v", observation)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report = Run(ctx, source, Options{Purpose: StructuralValidation})
	if !report.AdmissionDecision(AdmissionFindingPolicy{}).Interrupted || len(report.Observations) != want {
		t.Fatalf("cancellation hid the unperformed tail: %#v", report)
	}
	for _, observation := range report.Observations {
		if observation.Status != AdmissionNotRun {
			t.Fatalf("canceled check claimed execution: %#v", observation)
		}
	}
}

func TestAdmissionReportRejectsUnclassifiedRegisteredCheck(t *testing.T) {
	c := newCheckerContext(context.Background(), semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}), Options{})
	report := Report{}
	called := false
	report.runAdmissionCheck(c, Check{ID: "unclassified", Run: func(*checkerContext) []Finding { called = true; return nil }})
	if called || !report.HasErrors() || report.AdmissionDecision(AdmissionFindingPolicy{}).Complete {
		t.Fatalf("unclassified check accepted: %#v", report)
	}
}

func TestAdmissionReportEarlyRejectionAccountsForRegisteredChecks(t *testing.T) {
	for _, purpose := range []ValidationPurpose{StructuralValidation, ExecutionValidation} {
		for _, input := range []string{"missing_source", "invalid_purpose", "canceled_missing_source"} {
			t.Run(fmt.Sprintf("%d/%s", purpose, input), func(t *testing.T) {
				ctx := context.Background()
				opts := Options{Purpose: purpose}
				if input == "invalid_purpose" {
					opts.Purpose = ValidationPurpose(255)
				}
				if input == "canceled_missing_source" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				report := Run(ctx, nil, opts)
				checks := append(append([]Check{}, bootCheckRegistry...), supplementalChecks...)
				want := make(map[string]bool)
				for _, check := range checks {
					class := AdmissionSourceObservation
					if check.Mode == CheckDeployment {
						class = AdmissionDeploymentObservation
					}
					want[check.ID+"/"+string(class)] = true
					if check.Mode == CheckMixed {
						want[check.ID+"/"+string(AdmissionDeploymentObservation)] = true
					}
				}
				for _, observation := range report.Observations {
					identity := observation.CheckID + "/" + string(observation.Class)
					if !want[identity] || observation.Owner != registeredAdmissionOwner || observation.Status != AdmissionNotRun || observation.Reason == "" ||
						len(observation.Dependencies) != 1 || observation.Dependencies[0] != "workflow_contract_validation" || !observation.StartedAt.IsZero() || !observation.FinishedAt.IsZero() {
						t.Fatalf("invalid blocked check: %#v", observation)
					}
					delete(want, identity)
				}
				if len(want) != 0 {
					t.Fatalf("early rejection hid registered checks: %#v", want)
				}
				decision := report.AdmissionDecision(AdmissionFindingPolicy{})
				if decision.Complete || decision.FailureClass != failures.ClassSchemaInvalid || decision.Interrupted != (input == "canceled_missing_source") {
					t.Fatalf("early refusal lost its category: %#v", decision)
				}
			})
		}
	}
}

func TestCredentialAdmissionRejectsMissingInspectionPort(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"remote": map[string]any{"prefix": "remote", "url": "http://127.0.0.1:1", "credentials_key": "remote_key"}}},
	}}})
	if _, err := MissingStaticCredentialRequirements(context.Background(), source, Options{}); err == nil {
		t.Fatal("nil required credential port was accepted as empty state")
	}
	c := newCheckerContext(context.Background(), source, Options{})
	report := Report{}
	report.runAdmissionCheck(c, Check{ID: "credential_key_exists", Mode: CheckDeployment, Run: checkCredentialKeyExists})
	decision := report.AdmissionDecision(AdmissionFindingPolicy{})
	if decision.Complete || decision.FailureClass != failures.ClassDependencyUnavailable {
		t.Fatalf("missing credential readback lost: %#v, report=%#v", decision, report)
	}
	if _, err := MissingStaticCredentialRequirements(context.Background(), semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}), Options{}); err != nil {
		t.Fatalf("source without credential requirements needs no port: %v", err)
	}
}

type failingAdmissionCredentialStore struct{ runtimecredentials.Store }

func (failingAdmissionCredentialStore) Get(context.Context, string) (string, bool, error) {
	return "", false, errors.New("read failure containing secret-token-for-redaction-test")
}

func (failingAdmissionCredentialStore) List(context.Context) ([]string, error) {
	panic("admission must not enumerate unrelated credentials")
}

func TestCredentialAdmissionDoesNotDiscloseUnderlyingReadError(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"mcp_servers": {Value: map[string]any{"remote": map[string]any{"prefix": "remote", "credentials_key": "remote_key"}}},
	}}})
	c := newCheckerContext(context.Background(), source, Options{Credentials: failingAdmissionCredentialStore{}})
	findings := c.credentials()
	if len(findings) != 1 || findings[0].FailureClass != failures.ClassDependencyUnavailable {
		t.Fatalf("read failure disappeared: %#v", findings)
	}
	if strings.Contains(findings[0].Message, "secret-token") || !strings.Contains(findings[0].Message, "remote_key") {
		t.Fatalf("unsafe or unscoped diagnostic: %#v", findings[0])
	}
}
