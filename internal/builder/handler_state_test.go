package builder

import (
	"context"
	"errors"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
)

type projectControlStub struct {
	current ProjectStatus
}

func (s projectControlStub) OpenProject(context.Context, string) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) ReloadProject(context.Context, string) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) CloseProject(context.Context) (ProjectStatus, error) {
	return s.current, nil
}
func (s projectControlStub) CurrentProject() ProjectStatus { return s.current }

func TestHandlerHealthSnapshot_ProjectsAndErrors(t *testing.T) {
	h := &handler{
		version: "builder-test",
		health: func(context.Context) (map[string]any, error) {
			return nil, errors.New("db unavailable")
		},
		projectControl: projectControlStub{current: ProjectStatus{
			ProjectDir:      "/tmp/project",
			Loaded:          true,
			WorkflowName:    "demo",
			WorkflowVersion: "v1",
		}},
	}

	snapshot := h.healthSnapshot(context.Background())
	if snapshot.Status != "degraded" {
		t.Fatalf("status = %q, want degraded", snapshot.Status)
	}
	if snapshot.DatabaseErr != "db unavailable" {
		t.Fatalf("database_error = %q", snapshot.DatabaseErr)
	}
	if snapshot.Version != "builder-test" {
		t.Fatalf("version = %q", snapshot.Version)
	}
}

func TestValidationIssueFromFindingCopiesRemediationAndEvidence(t *testing.T) {
	finding := runtimebootverify.Finding{
		CheckID:     "timer_validation",
		Severity:    runtimebootverify.SeverityHardInvalidity,
		Message:     "timer reminder start_on boot does not support cancel_on state:done",
		Remediation: "remove cancel_on from boot timer",
		Evidence:    []string{"timer: reminder", "cancel_on: state:done"},
	}

	issue := validationIssueFromFinding(finding)
	if issue.CheckID != finding.CheckID || issue.Severity != finding.Severity || issue.Message != finding.Message {
		t.Fatalf("issue = %#v, want core fields copied from %#v", issue, finding)
	}
	if issue.Remediation != finding.Remediation {
		t.Fatalf("remediation = %q, want %q", issue.Remediation, finding.Remediation)
	}
	if len(issue.Evidence) != 2 || issue.Evidence[0] != "timer: reminder" || issue.Evidence[1] != "cancel_on: state:done" {
		t.Fatalf("evidence = %#v", issue.Evidence)
	}

	finding.Evidence[0] = "mutated"
	if issue.Evidence[0] != "timer: reminder" {
		t.Fatalf("issue evidence aliases finding evidence: %#v", issue.Evidence)
	}
}

func TestValidationIssueFromFindingPreservesCredentialSuggestion(t *testing.T) {
	issue := validationIssueFromFinding(runtimebootverify.Finding{
		CheckID:     "credential_key_exists",
		Severity:    "warning",
		Message:     "credential missing",
		Remediation: "store the credential",
	})

	if issue.Remediation != "store the credential" {
		t.Fatalf("remediation = %q", issue.Remediation)
	}
	if issue.Suggestion == "" {
		t.Fatalf("suggestion missing for credential compatibility: %#v", issue)
	}
}
