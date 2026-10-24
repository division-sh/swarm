package providerconnectors

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestValidateSourceAcceptsTelegramProviderConnectorHTTPActivityTool(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
		},
	})

	if errs := ValidateSource(source); len(errs) != 0 {
		t.Fatalf("ValidateSource errors = %#v, want none", errs)
	}
}

func TestValidateSourceRejectsUnsupportedProviderConnectorShape(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram.send_message": testProviderConnectorTool(
				t,
				runtimecontracts.ActivityEffectClassReadOnly,
				nil,
				nil,
				&runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"},
				runtimecontracts.WithToolRateLimit("1/s", "1s"),
			),
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	for _, want := range []string{
		"must declare exactly one credential binding mode",
		"uses rate_limit",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ValidateSource errors = %q, want %q", joined, want)
		}
	}
}

func TestValidateSourceAcceptsSlackManagedCredentialProviderConnectorHTTPActivityTool(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": slackConnectorTool("https://slack.com/api/chat.postMessage"),
		},
	})

	if errs := ValidateSource(source); len(errs) != 0 {
		t.Fatalf("ValidateSource errors = %#v, want none", errs)
	}
}

func TestValidateSourceRejectsMixedStaticAndManagedProviderConnectorCredentials(t *testing.T) {
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	_, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		runtimecontracts.WithToolCredentials("slack_bot_token"),
		runtimecontracts.WithToolManagedCredential(runtimecontracts.ManagedCredentialRef{Key: "slack_oauth"}),
	)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("tool admission error = %v, want mixed auth rejection", err)
	}
}

func TestValidateSourceRejectsGitHubAppInstallationWithoutInputCarrier(t *testing.T) {
	_, err := runtimecontracts.NewToolSchemaEntry(testProviderConnectorToolOptions(runtimecontracts.ActivityEffectClassNonIdempotentWrite, nil, &runtimecontracts.ManagedCredentialRef{
		Key:        "github_app",
		GrantType:  runtimemanagedcredentials.GrantGitHubAppInstallation,
		GrantModel: managedcredentialmodel.GrantModelInstallation,
	}, &runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"})...)
	if err == nil || !strings.Contains(err.Error(), "managed_credential.installation_id_input is required") {
		t.Fatalf("tool admission error = %v, want installation_id_input requirement", err)
	}
}

func TestValidateSourceRejectsInstallationInputOnNonGitHubAppCredential(t *testing.T) {
	_, err := runtimecontracts.NewToolSchemaEntry(testProviderConnectorToolOptions(runtimecontracts.ActivityEffectClassNonIdempotentWrite, nil, &runtimecontracts.ManagedCredentialRef{
		Key:                 "slack_oauth",
		InstallationIDInput: "installation_id",
	}, &runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"})...)
	if err == nil || !strings.Contains(err.Error(), "managed_credential.installation_id_input requires grant_type github_app_installation") {
		t.Fatalf("tool admission error = %v, want grant_type guarded installation input", err)
	}
}

func TestValidateSourceRejectsConnectorWithoutResponseSuccess(t *testing.T) {
	tool := testProviderConnectorTool(t, runtimecontracts.ActivityEffectClassNonIdempotentWrite, nil, &runtimecontracts.ManagedCredentialRef{Key: "slack_oauth"}, nil)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "must declare exactly one response_success policy") {
		t.Fatalf("ValidateSource errors = %q, want required response_success rejection", joined)
	}
}

func TestValidateSourceRejectsMalformedProviderConnectorResponseSuccess(t *testing.T) {
	_, err := runtimecontracts.NewToolSchemaEntry(testProviderConnectorToolOptions(runtimecontracts.ActivityEffectClassNonIdempotentWrite, nil, &runtimecontracts.ManagedCredentialRef{Key: "slack_oauth"}, &runtimecontracts.HTTPResponseSuccess{
		Kind: "json_field_equals", Path: "body.ok", Equals: true,
	})...)
	if err == nil || !strings.Contains(err.Error(), "response_success.path") {
		t.Fatalf("tool admission error = %v, want response_success path rejection", err)
	}
}

func TestValidateSourceRejectsAgentExposureOfProviderConnector(t *testing.T) {
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"sender": {ID: "sender", Tools: []string{"telegram.send_message"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, `agent "sender" must not expose provider connector tool "telegram.send_message" directly`) {
		t.Fatalf("ValidateSource errors = %q, want direct exposure rejection", joined)
	}
}

func TestValidateSourceRejectsScopedDuplicateAgentExposureOfProviderConnector(t *testing.T) {
	connector := telegramConnectorTool("https://api.telegram.org")
	alpha := runtimecontracts.FlowContractView{
		Path:  "alpha",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "alpha"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"sender": {ID: "alpha-sender", Tools: []string{"telegram.send_message"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{"telegram.send_message": connector},
	}
	beta := runtimecontracts.FlowContractView{
		Path:  "beta",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "beta"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"sender": {ID: "beta-sender", Tools: []string{"telegram.send_message"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{"telegram.send_message": connector},
	}
	refs := map[string]runtimecontracts.ContractURIRef{
		"alpha/sender": {Kind: "agent", FlowID: "alpha", LocalID: "sender", Full: "test://alpha/sender"},
		"beta/sender":  {Kind: "agent", FlowID: "beta", LocalID: "sender", Full: "test://beta/sender"},
	}
	byURI := map[string]runtimecontracts.ContractURIRef{}
	for _, ref := range refs {
		byURI[ref.Full] = ref
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{alpha, beta}},
			ByID: map[string]*runtimecontracts.FlowContractView{"alpha": &alpha, "beta": &beta},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{Agents: refs, ByURI: byURI},
	})

	joined := joinErrors(ValidateSource(source))
	for _, agentID := range []string{"alpha-sender", "beta-sender"} {
		if !strings.Contains(joined, `agent "`+agentID+`" must not expose provider connector tool "telegram.send_message" directly`) {
			t.Fatalf("ValidateSource errors = %q, want scoped exposure rejection for %s", joined, agentID)
		}
	}
}

func TestValidateSourceEvaluatesNestedPhysicalAgentDeclarationExactlyOnce(t *testing.T) {
	source := loadNestedProviderConnectorAgentSource(t)
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("canonical declarations = %#v, want one physical declaration", declarations)
	}
	joined := joinErrors(ValidateSource(source))
	want := `agent "public-sender" must not expose provider connector tool "telegram.send_message" directly`
	if count := strings.Count(joined, want); count != 1 {
		t.Fatalf("provider-connector exposure count = %d in %q, want one physical-declaration error", count, joined)
	}
}

func loadNestedProviderConnectorAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	root := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
	write(filepath.Join(root, "schema.yaml"), "# fixture: nested-provider-agent\nstages: []\n")
	write(filepath.Join(root, "parent", "schema.yaml"), "# fixture: parent\nstages: []\n")
	write(filepath.Join(root, "parent", "child", "schema.yaml"), "# fixture: child\nstages: []\n")
	flowRoot := filepath.Join(root, "parent", "child", "support")
	write(filepath.Join(flowRoot, "schema.yaml"), "# fixture: support\nstages:\n  active: {}\n")
	for _, flow := range []string{".", "parent", "parent/child", "parent/child/support"} {
		write(filepath.Join(root, flow, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	}
	write(filepath.Join(flowRoot, "agents.yaml"), `
sender:
  id: public-sender
  role: sender
  intent: {inline: Exercise nested provider connector projection.}
  model: regular
  subscriptions: [work.requested]
`)
	write(filepath.Join(flowRoot, "events.yaml"), "work.requested:\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	const flowPath = "parent/child/support"
	entry := bundle.FlowTree.ByID[flowPath].Agents["sender"]
	entry.Tools = []string{"telegram.send_message"}
	bundle.FlowTree.ByID[flowPath].Agents["sender"] = entry
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
	}
	return semanticview.Wrap(bundle)
}

func TestSurfacesReportManagedCredentialRequirementsWithoutSecretValues(t *testing.T) {
	ctx := context.Background()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": slackConnectorTool("https://slack.com/api/chat.postMessage"),
		},
	})
	store := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "slack_oauth",
		Provider:     "slack",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     "https://slack.com/api/oauth.v2.access",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Scopes:       []string{"chat:write"},
		AccessToken:  "xoxb-access-secret",
		RefreshToken: "refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(time.Hour),
	})

	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: store})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	requires := surfaces[0].Requirements
	if len(requires) != 1 || requires[0].Kind != "managed_credential" || requires[0].Name != "slack_oauth" || !requirementSatisfied(requires[0]) || requires[0].Status != "CONNECTED" {
		t.Fatalf("requirements = %#v, want connected managed slack_oauth", requires)
	}
	rendered := packs.RenderSubject(surfaces[0], true)
	for _, secret := range []string{"xoxb-access-secret", "refresh-secret", "client-secret"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("surface leaked managed credential secret %q: %s", secret, rendered)
		}
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("CapabilitySubjects without store: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 || requirementSatisfied(unbound[0].Requirements[0]) || unbound[0].Requirements[0].Status != "UNCONNECTED" {
		t.Fatalf("unbound managed requirements = %#v", unbound)
	}
	for _, state := range []struct {
		status      string
		want        string
		remediation string
	}{
		{runtimemanagedcredentials.StatusPendingConsent, "PENDING_CONSENT", "swarm connections connect slack_oauth"},
		{runtimemanagedcredentials.StatusRefreshFailed, "REFRESH_FAILED", "swarm connections disconnect slack_oauth && swarm connections connect slack_oauth"},
	} {
		record := runtimemanagedcredentials.Record{
			Key: "slack_oauth", Provider: "slack", GrantType: runtimemanagedcredentials.GrantAuthorizationCodePKCE,
			Scopes: []string{"chat:write"}, Status: state.status,
		}
		got, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(record)})
		if err != nil {
			t.Fatalf("CapabilitySubjects status %s: %v", state.status, err)
		}
		if len(got) != 1 || len(got[0].Requirements) != 1 || got[0].Requirements[0].Status != state.want || requirementSatisfied(got[0].Requirements[0]) || got[0].Requirements[0].Remediation != state.remediation || got[0].Status != packs.StatusNotReady {
			t.Fatalf("status %s subject = %#v", state.status, got)
		}
	}

	insufficient := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "slack_oauth",
		Provider:     "slack",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     "https://slack.com/api/oauth.v2.access",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Scopes:       []string{"channels:read"},
		AccessToken:  "xoxb-access-secret",
		RefreshToken: "refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(time.Hour),
	})
	scopeSurfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: insufficient})
	if err != nil {
		t.Fatalf("CapabilitySubjects insufficient scopes: %v", err)
	}
	if len(scopeSurfaces) != 1 || len(scopeSurfaces[0].Requirements) != 1 || requirementSatisfied(scopeSurfaces[0].Requirements[0]) || scopeSurfaces[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("scope-insufficient managed requirements = %#v, want SCOPE_INSUFFICIENT unbound", scopeSurfaces)
	}
}

func TestSurfacesReportBoundAndUnboundRequirementsWithoutSecretValues(t *testing.T) {
	ctx := context.Background()
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
		},
	})
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(ctx, "telegram_bot_token", "provider-secret"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), StaticCredentials: store})
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	surface := surfaces[0]
	if surface.ID != "telegram.send_message" || surface.Provider != "telegram" || surface.Action != "send_message" {
		t.Fatalf("surface identity = %#v", surface)
	}
	if len(surface.Requirements) != 1 || surface.Requirements[0].Name != "telegram_bot_token" || !requirementSatisfied(surface.Requirements[0]) {
		t.Fatalf("requirements = %#v, want bound telegram_bot_token", surface.Requirements)
	}
	if strings.Contains(packs.RenderSubject(surface, true), "provider-secret") {
		t.Fatal("surface leaked credential value")
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("Surfaces without store: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 || requirementSatisfied(unbound[0].Requirements[0]) {
		t.Fatalf("unbound requirements = %#v", unbound)
	}
	t.Setenv("telegram_bot_token", "env-provider-secret")
	envBound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), StaticCredentials: runtimecredentials.NewEnvStore()})
	if err != nil {
		t.Fatalf("CapabilitySubjects env store: %v", err)
	}
	if len(envBound) != 1 || len(envBound[0].Requirements) != 1 || !requirementSatisfied(envBound[0].Requirements[0]) || envBound[0].Requirements[0].Source != runtimecredentials.SourceEnv || strings.Contains(packs.RenderSubject(envBound[0], true), "env-provider-secret") {
		t.Fatalf("env-bound subject = %#v", envBound)
	}
}

func TestEmbeddedPackInventoryLoadsTelegramFromVerifiedPlatformPack(t *testing.T) {
	tool, ok := testBuiltinTool(t, "telegram", "telegram.send_message")
	if !ok {
		t.Fatal("embedded connector tool telegram.send_message not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin telegram tool category = %q, want %q", tool.Category(), Category)
	}
	httpSpec, _ := tool.HTTP()
	if strings.Contains(httpSpec.URL, "provider-secret") {
		t.Fatal("builtin telegram tool leaked a concrete credential value")
	}
}

func TestCapabilitySubjectsEnumerateExactInstalledInventoryWithoutMakingToolsEffective(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: testPackRegistry(t), IncludeInstalled: true})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	want := []string{
		"github.add_labels_to_issue",
		"github.create_issue",
		"github.create_issue_comment",
		"microsoft_graph.send_mail",
		"notion.append_block_children",
		"slack.post_message",
		"telegram.answer_callback",
		"telegram.edit_message",
		"telegram.install_inbox_commands",
		"telegram.install_shared_inbox_commands",
		"telegram.send_interactive",
		"telegram.send_message",
		"whatsapp.send_text",
	}
	if len(subjects) != len(want) {
		t.Fatalf("subjects = %#v, want %d installed actions", subjects, len(want))
	}
	for i, subject := range subjects {
		if subject.ID != want[i] || subject.Status != packs.StatusAvailable || subject.Applicability != "installed" || subject.Source != "connector_pack" {
			t.Fatalf("subject[%d] = %#v, want AVAILABLE %s", i, subject, want[i])
		}
		if len(subject.Requirements) != 1 || subject.Requirements[0].Kind != packs.RequirementImport || requirementSatisfied(subject.Requirements[0]) {
			t.Fatalf("subject[%d] requirements = %#v, want unsatisfied import teaching row", i, subject.Requirements)
		}
	}
	if len(source.ToolEntries()) != 0 {
		t.Fatalf("installed inventory became effective tools: %#v", source.ToolEntries())
	}
}

func TestCapabilitySubjectsEffectiveFlowLocalIdentityReplacesAvailableTeachingRow(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
	}})
	subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: testPackRegistry(t), IncludeInstalled: true})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(subjects) != 13 {
		t.Fatalf("subjects = %#v, want one effective identity replacing its installed row", subjects)
	}
	for _, subject := range subjects {
		if subject.ID != "telegram.send_message" {
			continue
		}
		if subject.Source != "flow_local" || subject.Applicability != "effective" || subject.Status != packs.StatusNotReady {
			t.Fatalf("telegram subject = %#v, want effective flow-local NOT_READY", subject)
		}
		for _, requirement := range subject.Requirements {
			if requirement.Kind == packs.RequirementImport {
				t.Fatalf("effective telegram subject carries misleading import remediation: %#v", subject)
			}
		}
		return
	}
	t.Fatal("effective telegram subject missing")
}

func TestNotionConnectorPackReportsWorkspaceGrantAndTokenProfileRequirement(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}),
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "notion", "notion.append_block_children"),
		},
	}, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one Notion managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "notion.notion_oauth" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound notion.notion_oauth", requirement)
	}
	if requirement.GrantModel != managedcredentialmodel.GrantModelWorkspace || requirement.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthBasic || requirement.TokenRequest.Body != managedcredentialmodel.TokenBodyJSON {
		t.Fatalf("unbound requirement shape = %#v, want workspace Basic JSON", requirement)
	}

	matching := notionConnectedRecord()
	matching.Key = "notion.notion_oauth"
	matchingStore := runtimemanagedcredentials.NewMemoryStore(matching)
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected Notion credential", bound)
	}

	wrongProfile := notionConnectedRecord()
	wrongProfile.Key = matching.Key
	wrongProfile.TokenRequest = managedcredentialmodel.DefaultTokenRequestProfile()
	wrongProfileStore := runtimemanagedcredentials.NewMemoryStore(wrongProfile)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: wrongProfileStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requirements) != 1 || requirementSatisfied(mismatch[0].Requirements[0]) || mismatch[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed token profile mismatch", mismatch)
	}
}

func TestGitHubConnectorPackReportsInstallationGrantRequirement(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}),
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "github", "github.create_issue_comment"),
		},
	}, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one GitHub managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "github.github_app" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound github.github_app", requirement)
	}
	if requirement.GrantType != runtimemanagedcredentials.GrantGitHubAppInstallation ||
		requirement.GrantModel != managedcredentialmodel.GrantModelInstallation ||
		requirement.InstallationIDInput != "installation_id" {
		t.Fatalf("unbound requirement shape = %#v, want github app installation grant", requirement)
	}

	matching := githubAppConnectedRecord()
	matching.Key = "github.github_app"
	matchingStore := runtimemanagedcredentials.NewMemoryStore(matching)
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected GitHub App credential", bound)
	}

	wrongGrant := githubAppConnectedRecord()
	wrongGrant.Key = matching.Key
	wrongGrant.GrantType = runtimemanagedcredentials.GrantClientCredentials
	wrongGrantStore := runtimemanagedcredentials.NewMemoryStore(wrongGrant)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: wrongGrantStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requirements) != 1 || requirementSatisfied(mismatch[0].Requirements[0]) || mismatch[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed grant_type mismatch", mismatch)
	}
}

func TestMicrosoftGraphConnectorPackReportsDefaultScopeManagedCredentialRequirement(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}),
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "microsoft_graph", "microsoft_graph.send_mail"),
		},
	}, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one Microsoft Graph managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "microsoft_graph.microsoft_graph_app" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound microsoft_graph.microsoft_graph_app", requirement)
	}
	if requirement.GrantModel != managedcredentialmodel.GrantModelScope || requirement.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthPost || requirement.TokenRequest.Body != managedcredentialmodel.TokenBodyForm {
		t.Fatalf("unbound requirement shape = %#v, want scope_grant post/form", requirement)
	}
	if got := strings.Join(requirement.Scopes, " "); got != "https://graph.microsoft.com/.default" {
		t.Fatalf("unbound requirement scopes = %q, want Graph .default resource scope", got)
	}

	matching := microsoftGraphConnectedRecord()
	matching.Key = "microsoft_graph.microsoft_graph_app"
	matchingStore := runtimemanagedcredentials.NewMemoryStore(matching)
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected Microsoft Graph credential", bound)
	}

	wrongScope := microsoftGraphConnectedRecord()
	wrongScope.Key = matching.Key
	wrongScope.Scopes = []string{"Mail.Send"}
	wrongScopeStore := runtimemanagedcredentials.NewMemoryStore(wrongScope)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{Registry: testPackRegistry(t), ManagedCredentials: wrongScopeStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requirements) != 1 || requirementSatisfied(mismatch[0].Requirements[0]) || mismatch[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed .default scope mismatch", mismatch)
	}
}

func TestConnectorPackImportApplicationSurvivesSemanticSourceWrappers(t *testing.T) {
	explicit := providerConnectorScopedSource{
		Source: semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}),
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}
	imported, err := SourceWithConnectorPackImports(explicit, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}
	wrapper := providerConnectorSourceWrapper{Source: imported}
	reapplied, err := SourceWithConnectorPackImports(wrapper, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports wrapped: %v", err)
	}
	if _, ok := reapplied.ToolEntries()["telegram.send_message"]; !ok {
		t.Fatal("wrapped imported source lost telegram.send_message")
	}
}

func TestConnectorPackCapabilitiesRemainVisibleThroughRuntimeToolOverlay(t *testing.T) {
	explicit := providerConnectorScopedSource{
		Source:     semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}),
		flowScopes: []semanticview.FlowScope{{ID: "child", Path: "child"}},
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "github", "github.create_issue"),
		},
	}
	imported, err := SourceWithConnectorPackImports(explicit, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	overlaid, err := semanticview.WithRuntimeTools(imported, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolCategory("channel_operation"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
			runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		),
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	revalidated, err := SourceWithConnectorPackImports(overlaid, testPackRegistry(t))
	if err != nil {
		t.Fatalf("same-generation connector import through overlay: %v", err)
	}
	subjects, err := CapabilitySubjects(context.Background(), revalidated, CapabilityOptions{Registry: testPackRegistry(t)})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(subjects) != 1 || subjects[0].ID != "github.create_issue" || subjects[0].Source != "connector_pack_import" || subjects[0].SourcePath == "" {
		t.Fatalf("connector import provenance hidden through overlay: %#v", subjects)
	}
	generationVisible := false
	for _, evidence := range subjects[0].Evidence {
		if evidence.Kind == "generation" && evidence.Fields["manifest_hash"] != "" {
			generationVisible = true
		}
	}
	if !generationVisible {
		t.Fatalf("connector generation hidden through overlay: %#v", subjects[0].Evidence)
	}
	const toolID = "github.create_issue"
	capabilities := revalidated.SemanticCapabilities()
	importSource, present := capabilities.ConnectorImportSource("child", toolID)
	if !present {
		t.Fatal("inherited tool lost its exact import source")
	}
	provenance, present := capabilities.ConnectorPackProvenance("child", toolID)
	if !present {
		t.Fatal("inherited tool lost its exact pack identity")
	}
	generation, present := capabilities.ConnectorGeneration("child", toolID)
	if !present {
		t.Fatal("inherited tool lost its generation")
	}
	for _, fault := range []string{"source", "identity", "hash", "generation_absent", "provenance_absent"} {
		t.Run(fault, func(t *testing.T) {
			child := connectorPackSource{Source: revalidated,
				importedByFlowScope: map[string]map[string]runtimecontracts.ToolSchemaEntry{"child": {toolID: revalidated.ToolEntries()[toolID]}},
				importSources:       map[string]semanticview.ConnectorImportSource{toolID: importSource},
				importProvenance:    map[string]semanticview.ConnectorPackProvenance{toolID: provenance},
				importedGeneration:  map[string]GenerationSurface{toolID: generation},
			}
			switch fault {
			case "source":
				child.importSources[toolID] = semanticview.MustConnectorImportSource("foreign/connector.yaml")
			case "identity":
				identity := provenance
				identity.PackID = "foreign"
				child.importProvenance[toolID] = identity
			case "hash":
				identity := provenance
				identity.ManifestHash = "sha256:" + strings.Repeat("f", 64)
				child.importProvenance[toolID] = identity
			case "generation_absent":
				child.importedGeneration = nil
			case "provenance_absent":
				child.importProvenance = nil
			}
			if subjects, err := CapabilitySubjects(context.Background(), child, CapabilityOptions{Registry: testPackRegistry(t)}); err == nil || !strings.Contains(err.Error(), "ambiguous flow credential scopes") || len(subjects) != 0 {
				t.Fatalf("contradictory scoped evidence coalesced: subjects=%+v err=%v", subjects, err)
			}
		})
	}
}

func TestConnectorPackImportRejectsCollisionsAndNamesSources(t *testing.T) {
	source := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
			},
		}),
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}

	_, err := SourceWithConnectorPackImports(source, testPackRegistry(t))
	if err == nil {
		t.Fatal("SourceWithConnectorPackImports succeeded, want collision")
	}
	for _, want := range []string{`provider connector tool "telegram.send_message" collision`, "flow . schema.yaml imports.connector_packs", "merged tool source", "remove one"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("collision error = %q, want containing %q", err.Error(), want)
		}
	}
}

func TestValidateSourceRejectsDirectAgentExposureOfImportedProviderConnector(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"sender": {ID: "sender", Tools: []string{"telegram.send_message"}},
		},
	}
	base := semanticviewtest.WrapRootAgents(bundle)
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: base,
		importScopes: []connectorPackTestImportScope{
			flowScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}, testPackRegistry(t))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}
	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, `agent "sender" must not expose provider connector tool "telegram.send_message" directly`) {
		t.Fatalf("ValidateSource errors = %q, want imported direct exposure rejection", joined)
	}
}

type providerConnectorScopedSource struct {
	semanticview.Source
	importScopes []connectorPackTestImportScope
	flowScopes   []semanticview.FlowScope
}

type connectorPackTestImportScope struct {
	Key     string
	Imports []runtimecontracts.ConnectorPackImport
}

type providerConnectorSourceWrapper struct {
	semanticview.Source
}

func (s providerConnectorScopedSource) FlowScopes() []semanticview.FlowScope {
	out := append([]semanticview.FlowScope(nil), s.flowScopes...)
	for _, scope := range s.importScopes {
		flowPath := strings.TrimSpace(scope.Key)
		if flowPath == "" {
			flowPath = "."
		}
		out = append(out, semanticview.FlowScope{ID: flowPath, Path: flowPath})
	}
	return out
}

func (s providerConnectorScopedSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	id = strings.TrimSpace(id)
	for _, scope := range s.flowScopes {
		if strings.TrimSpace(scope.ID) == id {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}

func (s providerConnectorScopedSource) FlowSchemaByID(id string) (runtimecontracts.FlowSchemaDocument, bool) {
	id = strings.TrimSpace(id)
	for _, scope := range s.importScopes {
		flowPath := strings.TrimSpace(scope.Key)
		if flowPath == "" {
			flowPath = "."
		}
		if flowPath == id {
			return runtimecontracts.FlowSchemaDocument{
				Name: flowPath,
				Imports: runtimecontracts.FlowSchemaImports{
					ConnectorPacks: append([]runtimecontracts.ConnectorPackImport(nil), scope.Imports...),
				},
			}, true
		}
	}
	return s.Source.FlowSchemaByID(id)
}

func testProviderConnectorTool(
	t *testing.T,
	effect runtimecontracts.ActivityEffectClass,
	credentials []string,
	managed *runtimecontracts.ManagedCredentialRef,
	success *runtimecontracts.HTTPResponseSuccess,
	extra ...runtimecontracts.ToolSchemaEntryOption,
) runtimecontracts.ToolSchemaEntry {
	t.Helper()
	options := testProviderConnectorToolOptions(effect, credentials, managed, success, extra...)
	tool, err := runtimecontracts.NewToolSchemaEntry(options...)
	if err != nil {
		t.Fatalf("construct provider connector tool: %v", err)
	}
	return tool
}

func testProviderConnectorToolOptions(
	effect runtimecontracts.ActivityEffectClass,
	credentials []string,
	managed *runtimecontracts.ManagedCredentialRef,
	success *runtimecontracts.HTTPResponseSuccess,
	extra ...runtimecontracts.ToolSchemaEntryOption,
) []runtimecontracts.ToolSchemaEntryOption {
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	options := []runtimecontracts.ToolSchemaEntryOption{
		runtimecontracts.WithToolCategory(Category),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"}),
	}
	if len(credentials) > 0 {
		options = append(options, runtimecontracts.WithToolCredentials(credentials...))
	}
	if managed != nil {
		options = append(options, runtimecontracts.WithToolManagedCredential(*managed))
	}
	if success != nil {
		options = append(options, runtimecontracts.WithToolResponseSuccess(*success))
	}
	options = append(options, extra...)
	return options
}

func telegramConnectorTool(baseURL string) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory(Category),
		runtimecontracts.WithToolDescription("send Telegram messages"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"chat_id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
				"text":    runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
			}), runtimecontracts.ToolSchemaRequired("chat_id", "text")),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
		),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    strings.TrimRight(baseURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage",
			Body: map[string]any{
				"chat_id": "{{input.chat_id}}",
				"text":    "{{input.text}}",
			},
		}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
		runtimecontracts.WithToolCredentials("telegram_bot_token"),
	)
}

func slackConnectorTool(url string) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory(Category),
		runtimecontracts.WithToolDescription("post Slack messages"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"channel": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
				"text":    runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
			}), runtimecontracts.ToolSchemaRequired("channel", "text")),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
		),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    strings.TrimSpace(url),
			Body: map[string]any{
				"channel": "{{input.channel}}",
				"text":    "{{input.text}}",
			},
		}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{
			Kind:   "json_field_equals",
			Path:   "response.body.ok",
			Equals: true,
		}),
		runtimecontracts.WithToolManagedCredential(runtimecontracts.ManagedCredentialRef{
			Key:    "slack_oauth",
			Scopes: []string{"chat:write"},
		}),
	)
}

func notionConnectedRecord() runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:          "notion_oauth",
		Provider:     "notion",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCode,
		TokenURL:     "https://api.notion.com/v1/oauth/token",
		ClientID:     "notion-client",
		ClientSecret: "notion-secret",
		GrantModel:   managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
			StaticHeaders: map[string]string{
				"Notion-Version": "2026-03-11",
			},
		},
		AccessToken:  "notion-access-secret",
		RefreshToken: "notion-refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
}

func microsoftGraphConnectedRecord() runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:          "microsoft_graph_app",
		Provider:     "microsoft_graph",
		GrantType:    runtimemanagedcredentials.GrantClientCredentials,
		TokenURL:     "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
		ClientID:     "graph-client",
		ClientSecret: "graph-secret",
		Scopes:       []string{"https://graph.microsoft.com/.default"},
		GrantModel:   managedcredentialmodel.GrantModelScope,
		TokenRequest: managedcredentialmodel.DefaultTokenRequestProfile(),
		AccessToken:  "graph-access-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
}

func githubAppConnectedRecord() runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:            "github_app",
		Provider:       "github",
		GrantType:      runtimemanagedcredentials.GrantGitHubAppInstallation,
		APIBaseURL:     "https://api.github.com",
		ClientID:       "github-app-client-id",
		InstallationID: "1001",
		PrivateKey:     "redacted-test-private-key",
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		AccessToken:    "github-install-secret",
		Status:         runtimemanagedcredentials.StatusConnected,
		ExpiresAt:      time.Now().Add(time.Hour),
	}
}

func flowScopeWithConnectorPackImport(key, provider, tool string) connectorPackTestImportScope {
	return connectorPackTestImportScope{Key: key, Imports: []runtimecontracts.ConnectorPackImport{{Provider: provider, Tool: tool}}}
}

func joinErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "\n")
}

func requirementSatisfied(requirement packs.Requirement) bool {
	return requirement.Satisfied != nil && *requirement.Satisfied
}

func evidenceByKind(evidence []packs.Evidence, kind string) map[string]string {
	for _, item := range evidence {
		if item.Kind == kind {
			return item.Fields
		}
	}
	return nil
}
