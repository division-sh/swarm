package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestBootVerifyReportsManagedCredentialState(t *testing.T) {
	source := managedCredentialBootSource([]string{"repo.read", "repo.write"})

	t.Run("missing store", func(t *testing.T) {
		report := Run(context.Background(), source, Options{})
		if !managedCredentialFindingContains(report.Findings, "managed credential github is missing") {
			t.Fatalf("findings = %#v, want missing managed credential", report.Findings)
		}
	})

	t.Run("unconnected", func(t *testing.T) {
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:    "github",
			Status: runtimemanagedcredentials.StatusPendingConsent,
		})
		report := Run(context.Background(), source, Options{ManagedCredentials: store})
		if !managedCredentialFindingContains(report.Findings, "managed credential github is pending_consent") {
			t.Fatalf("findings = %#v, want pending_consent managed credential", report.Findings)
		}
	})

	t.Run("refresh failed", func(t *testing.T) {
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:     "github",
			Status:  runtimemanagedcredentials.StatusRefreshFailed,
			Failure: "refresh failed",
		})
		report := Run(context.Background(), source, Options{ManagedCredentials: store})
		if !managedCredentialFindingContains(report.Findings, "refresh_failed") || !managedCredentialFindingContains(report.Findings, "refresh failed") {
			t.Fatalf("findings = %#v, want refresh failure managed credential", report.Findings)
		}
	})

	t.Run("scope insufficient", func(t *testing.T) {
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:         "github",
			Status:      runtimemanagedcredentials.StatusConnected,
			Scopes:      []string{"repo.read"},
			AccessToken: "access-token",
		})
		report := Run(context.Background(), source, Options{ManagedCredentials: store})
		if !managedCredentialFindingContains(report.Findings, "scope_insufficient") || !managedCredentialFindingContains(report.Findings, "repo.write") {
			t.Fatalf("findings = %#v, want scope-insufficient managed credential", report.Findings)
		}
	})

	t.Run("connected", func(t *testing.T) {
		store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
			Key:         "github",
			Status:      runtimemanagedcredentials.StatusConnected,
			Scopes:      []string{"repo.read", "repo.write"},
			AccessToken: "access-token",
		})
		report := Run(context.Background(), source, Options{ManagedCredentials: store})
		if managedCredentialFindingContains(report.Findings, "managed credential github") {
			t.Fatalf("findings = %#v, want no managed credential warning", report.Findings)
		}
	})
}

func managedCredentialBootSource(scopes []string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"send_provider": {
				HandlerType: "http",
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    "https://provider.example.test",
				},
				ManagedCredential: &runtimecontracts.ManagedCredentialRef{
					Key:    "github",
					Scopes: scopes,
				},
			},
		},
	})
}

func managedCredentialFindingContains(findings []Finding, want string) bool {
	for _, finding := range findings {
		if finding.CheckID == "managed_credential_state" && strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}
