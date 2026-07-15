package bootverify

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCredentialChecksConsumeExactMockConnectorAdmission(t *testing.T) {
	for _, credentialKind := range []string{"static", "managed"} {
		t.Run(credentialKind, func(t *testing.T) {
			source, plan := mockConnectorCredentialFixture(t, credentialKind, false)
			findings := newCheckerContext(context.Background(), source, Options{
				Credentials:            bootverifyCredentialStore{values: map[string]string{}},
				ExecutionMode:          executionmode.Mock,
				MockConnectorResponses: plan,
			}).credentials()
			for _, finding := range findings {
				if finding.Location == "provider_credential" {
					t.Fatalf("credential finding = %#v, want exact admitted mock connector to require no live credential", finding)
				}
			}
		})
	}
}

func TestCredentialChecksUseTypedModeRatherThanResponseArtifact(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", false)
	tests := []struct {
		name        string
		opts        Options
		wantMissing bool
	}{
		{
			name: "live retains requirement despite responder artifact",
			opts: Options{
				Credentials:            bootverifyCredentialStore{values: map[string]string{}},
				ExecutionMode:          executionmode.Live,
				MockConnectorResponses: plan,
			},
			wantMissing: true,
		},
		{
			name: "mock without exact responder never consults tool credential",
			opts: Options{
				Credentials:   bootverifyCredentialStore{values: map[string]string{}},
				ExecutionMode: executionmode.Mock,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := newCheckerContext(context.Background(), source, tc.opts).credentials()
			if got := credentialFindingContains(findings, "provider_credential", "tool provider.send"); got != tc.wantMissing {
				t.Fatalf("provider.send credential finding = %v, want %v; findings=%#v", got, tc.wantMissing, findings)
			}
		})
	}
}

func TestCredentialChecksRetainNonToolRequirementsSharingAKey(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", true)
	findings := newCheckerContext(context.Background(), source, Options{
		Credentials:            bootverifyCredentialStore{values: map[string]string{}},
		ExecutionMode:          executionmode.Mock,
		MockConnectorResponses: plan,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "mcp_server audit") {
		t.Fatalf("credential findings = %#v, want non-tool MCP requirement", findings)
	}
	if credentialFindingContains(findings, "provider_credential", "tool provider.send") {
		t.Fatalf("credential findings = %#v, admitted connector requirement survived filtering", findings)
	}
}

func mockConnectorCredentialFixture(t *testing.T, credentialKind string, includeSibling bool) (semanticview.Source, *providerconnectors.MockResponsePlan) {
	t.Helper()
	connector := runtimecontracts.ToolSchemaEntry{
		Category:    "provider_connector",
		HandlerType: "http",
		EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		HTTP:        &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://provider.example/messages"},
		OutputSchema: runtimecontracts.ToolInputSchema{
			Type:     "object",
			Required: []string{"message_id"},
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"message_id": {Type: "string"},
			},
		},
	}
	switch credentialKind {
	case "static":
		connector.Credentials = []string{"provider_credential"}
	case "managed":
		connector.ManagedCredential = &runtimecontracts.ManagedCredentialRef{Key: "provider_credential"}
	default:
		t.Fatalf("unsupported credential kind %q", credentialKind)
	}
	tools := map[string]runtimecontracts.ToolSchemaEntry{"provider.send": connector}
	if includeSibling {
		tools["audit.call"] = runtimecontracts.ToolSchemaEntry{
			HandlerType: "http",
			Credentials: []string{"provider_credential"},
			HTTP:        &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://audit.example/calls"},
		}
	}
	plan, err := providerconnectors.NewMockResponsePlan(map[string]map[string]any{
		"provider.send": {"message_id": "mock-message-1"},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{Tools: tools}
	if includeSibling {
		bundle.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"audit": map[string]any{"prefix": "audit", "credentials_key": "provider_credential"},
			}},
		}}
	}
	return semanticview.Wrap(bundle), plan
}

func credentialFindingContains(findings []Finding, location, fragment string) bool {
	for _, finding := range findings {
		if finding.Location == location && strings.Contains(finding.Message, fragment) {
			return true
		}
	}
	return false
}
