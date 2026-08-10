package builder

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestValidationIssueFromFindingPreservesCredentialSuggestionForNormalizedWarning(t *testing.T) {
	report := runtimebootverify.Report{}
	report.Add(runtimebootverify.Finding{
		CheckID:     "credential_key_exists",
		Severity:    "warning",
		Message:     "credential missing",
		Remediation: "store the credential",
	})
	if len(report.Findings) != 1 {
		t.Fatalf("findings = %#v, want one normalized finding", report.Findings)
	}
	finding := report.Findings[0]
	if finding.Severity != runtimebootverify.SeveritySemanticDriftWarn {
		t.Fatalf("severity = %q, want normalized warning severity", finding.Severity)
	}
	if !validationFindingIsWarning(finding) {
		t.Fatalf("normalized warning was not classified as Builder warning: %#v", finding)
	}

	issue := validationIssueFromFinding(finding)
	if issue.Remediation != "store the credential" {
		t.Fatalf("remediation = %q", issue.Remediation)
	}
	if issue.Suggestion == "" {
		t.Fatalf("suggestion missing for credential compatibility: %#v", issue)
	}
}

func TestBuilderValidationConsumesCanonicalMockEffectReachability(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	for _, tc := range []struct {
		name           string
		includeLive    bool
		wantCredential bool
	}{
		{name: "all exact mocks waive unreachable connector", wantCredential: false},
		{name: "mixed source retains reachable connector", includeLive: true, wantCredential: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &handler{
				credentials:      credentialStore,
				executionPosture: executionposture.Live,
				llmProfile:       profile,
				semanticSource:   builderMockConnectorSource(tc.includeLive),
			}
			result := h.runFullValidation(context.Background())
			gotCredential := false
			for _, issue := range append(append([]ValidationIssue{}, result.Errors...), result.Warnings...) {
				if strings.Contains(issue.Message, "provider_credential") && strings.Contains(issue.Message, "tool provider.send") {
					gotCredential = true
				}
			}
			if gotCredential != tc.wantCredential {
				t.Fatalf("credential finding = %v, want %v; result=%#v", gotCredential, tc.wantCredential, result)
			}
		})
	}
}

func builderMockConnectorSource(includeLive bool) semanticview.Source {
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	connector := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))),
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://provider.example/messages"}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
		runtimecontracts.WithToolCredentials("provider_credential"),
	)
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"mock-agent": {
				ID:    "mock-agent",
				Model: llmselection.ModelAliasRegular,
				Mock: mockperformance.Performance{
					Kind:   "python",
					Module: "mocks/mock-agent.py",
					Source: []byte("def handle(input):\n    return {}\n"),
					Digest: "sha256:" + strings.Repeat("a", 64),
				},
			},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{"provider.send": connector},
	}
	if includeLive {
		bundle.Agents["live-agent"] = runtimecontracts.AgentRegistryEntry{ID: "live-agent", Model: llmselection.ModelAliasRegular}
	}
	return semanticview.Wrap(bundle)
}
