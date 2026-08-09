package bootverify

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCredentialChecksConsumeExactMockConnectorAdmission(t *testing.T) {
	for _, credentialKind := range []string{"static", "managed"} {
		t.Run(credentialKind, func(t *testing.T) {
			source, plan := mockConnectorCredentialFixture(t, credentialKind, false, false)
			reachability := mockConnectorEffectReachability(t, source, plan)
			findings := newCheckerContext(context.Background(), source, Options{
				Credentials:        bootverifyCredentialStore{values: map[string]string{}},
				EffectReachability: reachability,
			}).credentials()
			for _, finding := range findings {
				if finding.Location == "provider_credential" {
					t.Fatalf("credential finding = %#v, want exact admitted mock connector to require no live credential", finding)
				}
			}
		})
	}
}

func TestCredentialChecksUseTypedSelectionAndExactResponseAuthority(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", false, false)
	mixedSource, mixedPlan := mockConnectorCredentialFixture(t, "static", false, true)
	mixedReachability := mockConnectorEffectReachability(t, mixedSource, mixedPlan)
	if got := mixedReachability.LiveAgentIDs(); len(got) != 1 || got[0] != "live-agent" {
		t.Fatalf("mixed live agents = %#v, want live-agent", got)
	}
	tests := []struct {
		name        string
		source      semanticview.Source
		opts        Options
		wantMissing bool
	}{
		{
			name:   "all mock exact responder removes unreachable tool requirement",
			source: source,
			opts: Options{
				Credentials:        bootverifyCredentialStore{values: map[string]string{}},
				EffectReachability: mockConnectorEffectReachability(t, source, plan),
			},
		},
		{
			name:   "mixed source retains reachable tool requirement",
			source: mixedSource,
			opts: Options{
				Credentials:        bootverifyCredentialStore{values: map[string]string{}},
				EffectReachability: mixedReachability,
			},
			wantMissing: true,
		},
		{
			name:   "all mock without exact responder retains requirement",
			source: source,
			opts: Options{
				Credentials:        bootverifyCredentialStore{values: map[string]string{}},
				EffectReachability: mockConnectorEffectReachability(t, source, nil),
			},
			wantMissing: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := newCheckerContext(context.Background(), tc.source, tc.opts).credentials()
			if got := credentialFindingContains(findings, "provider_credential", "tool provider.send"); got != tc.wantMissing {
				t.Fatalf("provider.send credential finding = %v, want %v; findings=%#v", got, tc.wantMissing, findings)
			}
			if tc.name == "mixed source retains reachable tool requirement" && !credentialFindingContains(findings, "provider_credential", "live-agent") {
				t.Fatalf("mixed credential findings = %#v, want exact live actor", findings)
			}
		})
	}
}

func TestCredentialChecksRetainAgentFreeToolRequirement(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", false, false)
	bundle := &runtimecontracts.WorkflowContractBundle{Tools: source.ToolEntries()}
	agentFree := semanticview.Wrap(bundle)
	reachability := mockConnectorEffectReachability(t, agentFree, plan)
	findings := newCheckerContext(context.Background(), agentFree, Options{
		Credentials:        bootverifyCredentialStore{values: map[string]string{}},
		EffectReachability: reachability,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "tool provider.send") {
		t.Fatalf("agent-free credential findings = %#v, want outbound tool requirement retained", findings)
	}
}

func TestCredentialChecksRetainAllMockToolUsedByLiveWorkflowActivity(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", false, false)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("mock connector source has no canonical bundle")
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"provider-sender": {
			ID: "provider-sender",
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"provider.requested": {Activity: runtimecontracts.ActivitySpec{ID: "provider_send", Tool: "provider.send"}},
				"provider.rule_requested": {Rules: []runtimecontracts.HandlerRuleEntry{{
					ID: "send", Condition: "true", Activity: runtimecontracts.ActivitySpec{ID: "provider_rule_send", Tool: "provider.send"},
				}}},
			},
		},
	}
	source = semanticview.Wrap(bundle)
	reachability := mockConnectorEffectReachability(t, source, plan)
	if sites := reachability.LiveWorkflowActivitySites("provider.send"); len(sites) != 2 ||
		!strings.Contains(strings.Join(sites, "\n"), "node provider-sender handler provider.requested") ||
		!strings.Contains(strings.Join(sites, "\n"), "node provider-sender handler provider.rule_requested") {
		t.Fatalf("live workflow activity sites = %#v", sites)
	}
	findings := newCheckerContext(context.Background(), source, Options{
		Credentials:        bootverifyCredentialStore{values: map[string]string{}},
		EffectReachability: reachability,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "tool provider.send") ||
		!credentialFindingContains(findings, "provider_credential", "node provider-sender handler provider.requested") {
		t.Fatalf("credential findings = %#v, want live workflow activity requirement retained", findings)
	}
}

func TestCredentialChecksRetainNonToolRequirementsSharingAKey(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "static", true, false)
	findings := newCheckerContext(context.Background(), source, Options{
		Credentials:        bootverifyCredentialStore{values: map[string]string{}},
		EffectReachability: mockConnectorEffectReachability(t, source, plan),
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "mcp_server audit") {
		t.Fatalf("credential findings = %#v, want non-tool MCP requirement", findings)
	}
	if credentialFindingContains(findings, "provider_credential", "tool provider.send") {
		t.Fatalf("credential findings = %#v, admitted connector requirement survived filtering", findings)
	}
}

func TestManagedCredentialChecksRetainMixedSourceRequirement(t *testing.T) {
	source, plan := mockConnectorCredentialFixture(t, "managed", false, true)
	reachability := mockConnectorEffectReachability(t, source, plan)
	findings := newCheckerContext(context.Background(), source, Options{
		EffectReachability: reachability,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "tool provider.send") || !credentialFindingContains(findings, "provider_credential", "live-agent") {
		t.Fatalf("managed credential findings = %#v, want exact live actor and tool requirement", findings)
	}
}

func mockConnectorCredentialFixture(t *testing.T, credentialKind string, includeSibling, includeUnmocked bool) (semanticview.Source, *providerconnectors.MockResponsePlan) {
	t.Helper()
	connector := runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")),

		runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"message_id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("message_id"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://provider.example/messages"}), runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{
		Kind: "http_status_2xx",
	}))

	var err error
	switch credentialKind {
	case "static":
		connector, err = connector.WithStaticCredentials("provider_credential")
	case "managed":
		connector, err = connector.WithManagedCredential(runtimecontracts.ManagedCredentialRef{Key: "provider_credential"})
	default:
		t.Fatalf("unsupported credential kind %q", credentialKind)
	}
	if err != nil {
		t.Fatalf("derive connector credential contract: %v", err)
	}
	tools := map[string]runtimecontracts.ToolSchemaEntry{"provider.send": connector}
	if includeSibling {
		objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
		tools["audit.call"] = runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://audit.example/calls"}),
			runtimecontracts.WithToolCredentials("provider_credential"),
		)
	}
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
		Tools: tools,
	}
	if includeUnmocked {
		bundle.Agents["live-agent"] = runtimecontracts.AgentRegistryEntry{ID: "live-agent", Model: llmselection.ModelAliasRegular}
	}
	if includeSibling {
		bundle.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"audit": map[string]any{"prefix": "audit", "credentials_key": "provider_credential"},
			}},
		}}
	}
	source := semanticview.Wrap(bundle)
	plan, err := providerconnectors.CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	return source, plan
}

func mockConnectorEffectReachability(t *testing.T, source semanticview.Source, plan *providerconnectors.MockResponsePlan) SourceBootEffectReachability {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	reachability, err := ResolveSourceBootEffectReachability(source, profile, plan)
	if err != nil {
		t.Fatalf("ResolveSourceBootEffectReachability: %v", err)
	}
	return reachability
}

func credentialFindingContains(findings []Finding, location, fragment string) bool {
	for _, finding := range findings {
		if finding.Location == location && strings.Contains(finding.Message, fragment) {
			return true
		}
	}
	return false
}
