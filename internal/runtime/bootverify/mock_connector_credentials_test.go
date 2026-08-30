package bootverify

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
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
	bundle.FlowTree.Root.Nodes = bundle.Nodes
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

func TestMockOnlyPostureRequiresMockAgentsAndExactActivityResponses(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	liveSource, livePlan := mockConnectorCredentialFixture(t, "static", false, true)
	if _, err := ResolveSourceBootEffectReachability(liveSource, profile, livePlan, executionposture.MockOnly); err == nil || !strings.Contains(err.Error(), "live agents") {
		t.Fatalf("live-agent reachability error = %v, want mock-only actor rejection", err)
	}

	mockSource, exactPlan := mockConnectorCredentialFixture(t, "static", false, false)
	bundle, ok := semanticview.Bundle(mockSource)
	if !ok {
		t.Fatal("mock connector source has no canonical bundle")
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"provider-sender": {
			ID: "provider-sender",
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"provider.requested": {Activity: runtimecontracts.ActivitySpec{ID: "provider_send", Tool: "provider.send"}},
			},
		},
	}
	bundle.FlowTree.Root.Nodes = bundle.Nodes
	mockSource = semanticview.Wrap(bundle)
	reachability, err := ResolveSourceBootEffectReachability(mockSource, profile, exactPlan, executionposture.MockOnly)
	if err != nil {
		t.Fatalf("mock-only exact response reachability: %v", err)
	}
	if reachability.ToolCredentialRequired("provider.send") {
		t.Fatal("mock-only exact provider activity retained a live credential requirement")
	}
	if _, err := ResolveSourceBootEffectReachability(mockSource, profile, nil, executionposture.MockOnly); err == nil || !strings.Contains(err.Error(), "exact mock response") {
		t.Fatalf("missing-response reachability error = %v, want exact responder rejection", err)
	}
}

func TestCredentialChecksCensusScopedAgentsHiddenByAmbiguousAlias(t *testing.T) {
	source, plan := scopedAliasMockConnectorFixture(t, true)

	reachability := mockConnectorEffectReachability(t, source, plan)
	liveAgents := strings.Join(reachability.LiveAgentIDs(), "\n")
	for _, want := range []string{
		"flow project-live agent shared-worker",
		"flow flow-live agent shared-worker",
	} {
		if !strings.Contains(liveAgents, want) {
			t.Fatalf("scoped live agents = %q, want %q", liveAgents, want)
		}
	}
	findings := newCheckerContext(context.Background(), source, Options{
		Credentials:        bootverifyCredentialStore{values: map[string]string{}},
		EffectReachability: reachability,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "tool provider.send") ||
		!credentialFindingContains(findings, "provider_credential", "flow project-live agent shared-worker") ||
		!credentialFindingContains(findings, "provider_credential", "flow flow-live agent shared-worker") {
		t.Fatalf("credential findings = %#v, want both hidden scoped live declarations", findings)
	}
}

func TestCredentialChecksCensusScopedActivitiesHiddenByAmbiguousAlias(t *testing.T) {
	source, plan := scopedAliasMockConnectorFixture(t, false)
	sharedSenderCount := 0
	for _, record := range source.ExecutableNodeRecords() {
		if node, err := record.Identity(); err == nil && node.NodeID() == "shared-sender" {
			sharedSenderCount++
		}
	}
	if sharedSenderCount != 4 {
		t.Fatalf("exact shared-sender declaration count = %d, want four scoped declarations", sharedSenderCount)
	}
	addScopedAliasActivities(t, source)

	reachability := mockConnectorEffectReachability(t, source, plan)
	if got := reachability.LiveAgentIDs(); len(got) != 0 {
		t.Fatalf("live agents = %#v, want every scoped agent mocked", got)
	}
	sites := strings.Join(reachability.LiveWorkflowActivitySites("provider.send"), "\n")
	for _, want := range []string{
		"flow project-live flow project-live node shared-sender handler provider.requested",
		"flow flow-live flow flow-live node shared-sender handler provider.requested",
	} {
		if !strings.Contains(sites, want) {
			t.Fatalf("scoped activity sites = %q, want %q", sites, want)
		}
	}
	findings := newCheckerContext(context.Background(), source, Options{
		Credentials:        bootverifyCredentialStore{values: map[string]string{}},
		EffectReachability: reachability,
	}).credentials()
	if !credentialFindingContains(findings, "provider_credential", "tool provider.send") ||
		!credentialFindingContains(findings, "provider_credential", "flow project-live flow project-live node shared-sender") ||
		!credentialFindingContains(findings, "provider_credential", "flow flow-live flow flow-live node shared-sender") {
		t.Fatalf("credential findings = %#v, want both hidden scoped activity declarations", findings)
	}
}

func TestNativeToolChecksCensusScopedAgentsHiddenByAmbiguousAlias(t *testing.T) {
	source, _ := scopedAliasMockConnectorFixtureWithNativeTools(t, true)
	findings := newCheckerContext(context.Background(), source, Options{}).nativeTools()
	joined := fmt.Sprint(findings)
	for _, want := range []string{
		"flow project-live agent shared-worker",
		"flow project-mock agent shared-worker",
		"flow flow-live agent shared-worker",
		"flow flow-mock agent shared-worker",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("native findings = %#v, want %q", findings, want)
		}
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
				ID:             "mock-agent",
				Model:          llmselection.ModelAliasRegular,
				ResolvedIntent: bootverifyTestResolvedIntent("mock-agent"),
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
		bundle.Agents["live-agent"] = runtimecontracts.AgentRegistryEntry{
			ID:             "live-agent",
			Model:          llmselection.ModelAliasRegular,
			ResolvedIntent: bootverifyTestResolvedIntent("live-agent"),
		}
	}
	if includeSibling {
		bundle.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"audit": map[string]any{"prefix": "audit", "credentials_key": "provider_credential"},
			}},
		}}
	}
	source := semanticviewtest.WrapRootAgents(bundle)
	plan, err := providerconnectors.CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	return source, plan
}

func scopedAliasMockConnectorFixture(t *testing.T, includeLive bool) (semanticview.Source, *providerconnectors.MockResponsePlan) {
	return scopedAliasMockConnectorFixtureOptions(t, includeLive, false)
}

func scopedAliasMockConnectorFixtureWithNativeTools(t *testing.T, includeLive bool) (semanticview.Source, *providerconnectors.MockResponsePlan) {
	return scopedAliasMockConnectorFixtureOptions(t, includeLive, true)
}

func scopedAliasMockConnectorFixtureOptions(t *testing.T, includeLive, includeInvalidNativeTools bool) (semanticview.Source, *providerconnectors.MockResponsePlan) {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-alias-reachability\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "item:\n  item_id: string\n")
	writeScopedReachabilityAgentFile(t, filepath.Join(root, "agents.yaml"), "root-mock", "mocks/root-mock.py", false)
	writeBootverifyFixtureFile(t, filepath.Join(root, "mocks", "root-mock.py"), "def handle(input):\n    return {'text': 'mock'}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
root-node:
  id: root-node
  execution_type: system_node
  event_handlers: {}
`)

	for _, child := range []struct {
		name string
		live bool
	}{
		{name: "project-mock"},
		{name: "project-live", live: includeLive},
		{name: "flow-mock"},
		{name: "flow-live", live: includeLive},
	} {
		dir := filepath.Join(root, child.name)
		writeBootverifyFixtureFile(t, filepath.Join(dir, "schema.yaml"), "name: "+child.name+"\nmode: static\ninitial_state: active\nstates: [active]\n")
		module := "mocks/shared-worker.py"
		writeScopedReachabilityAgentFile(t, filepath.Join(dir, "agents.yaml"), "shared-worker", module, child.live, scopedReachabilityNativeTools(includeInvalidNativeTools))
		if !child.live {
			writeBootverifyFixtureFile(t, filepath.Join(dir, filepath.FromSlash(module)), "def handle(input):\n    return {'text': 'mock'}\n")
		}
		writeBootverifyFixtureFile(t, filepath.Join(dir, "nodes.yaml"), scopedReachabilityNodeYAML())
	}

	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	connectorSource, _ := mockConnectorCredentialFixture(t, "static", false, false)
	bundle.Tools = connectorSource.ToolEntries()
	source := semanticview.Wrap(bundle)
	plan, err := providerconnectors.CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	return source, plan
}

func writeScopedReachabilityAgentFile(t *testing.T, path, agentID, module string, live bool, extra ...string) {
	t.Helper()
	contents := agentID + ":\n  id: " + agentID + "\n  model: regular\n  memory: false\n  intent:\n    inline: Inspect provider reachability for this test fixture.\n"
	if !live {
		contents += "  mock:\n    kind: python\n    module: " + module + "\n"
	}
	contents += strings.Join(extra, "")
	writeBootverifyFixtureFile(t, path, contents)
}

func bootverifyTestResolvedIntent(agentID string) runtimeagentintent.Resolved {
	resolved, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(agentID)+".intent",
		"Inspect provider reachability for this test fixture.",
	)
	if err != nil {
		panic(err)
	}
	return resolved
}

func scopedReachabilityNativeTools(enabled bool) string {
	if !enabled {
		return ""
	}
	return "  native_tools:\n    hidden_capability: true\n"
}

func scopedReachabilityNodeYAML() string {
	return `
shared-sender:
  id: shared-sender
  execution_type: system_node
  event_handlers: {}
`
}

func addScopedAliasActivities(t *testing.T, source semanticview.Source) {
	t.Helper()
	found := map[string]bool{"project-live": false, "flow-live": false}
	for _, scope := range source.FlowScopes() {
		if _, wanted := found[scope.ID]; !wanted {
			continue
		}
		scope.Nodes["shared-sender"] = scopedReachabilityActivityNode()
		found[scope.ID] = true
	}
	if !found["project-live"] || !found["flow-live"] {
		t.Fatalf("scoped activity fixture missing child flows: %#v", found)
	}
}

func scopedReachabilityActivityNode() runtimecontracts.SystemNodeContract {
	return runtimecontracts.SystemNodeContract{
		ID:            "shared-sender",
		ExecutionType: "system_node",
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"provider.requested": {
				Activity: runtimecontracts.ActivitySpec{ID: "provider_send", Tool: "provider.send"},
			},
		},
	}
}

func mockConnectorEffectReachability(t *testing.T, source semanticview.Source, plan *providerconnectors.MockResponsePlan) SourceBootEffectReachability {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	reachability, err := ResolveSourceBootEffectReachability(source, profile, plan, executionposture.Live)
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
