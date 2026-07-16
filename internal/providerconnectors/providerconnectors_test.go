package providerconnectors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			"telegram.send_message": {
				Category:        Category,
				HandlerType:     "http",
				EffectClass:     string(runtimecontracts.ActivityEffectClassReadOnly),
				Credentials:     nil,
				HTTP:            &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://api.telegram.org"},
				RateLimit:       "1/s",
				ResponseMapping: map[string]any{"ok": "{{response.body.ok}}"},
			},
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	for _, want := range []string{
		"effect_class must be non_idempotent_write",
		"must declare exactly one credential binding mode",
		"uses response_mapping",
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
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.Credentials = []string{"slack_bot_token"}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "must not declare both static credentials and managed_credential") {
		t.Fatalf("ValidateSource errors = %q, want mixed auth rejection", joined)
	}
}

func TestValidateSourceRejectsGitHubAppInstallationWithoutInputCarrier(t *testing.T) {
	tool := slackConnectorTool("https://api.github.test/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/comments")
	tool.ManagedCredential = &runtimecontracts.ManagedCredentialRef{
		Key:        "github_app",
		GrantType:  runtimemanagedcredentials.GrantGitHubAppInstallation,
		GrantModel: managedcredentialmodel.GrantModelInstallation,
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"github.create_issue_comment": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "managed_credential.installation_id_input is required") {
		t.Fatalf("ValidateSource errors = %q, want installation_id_input requirement", joined)
	}
}

func TestValidateSourceRejectsInstallationInputOnNonGitHubAppCredential(t *testing.T) {
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.ManagedCredential.InstallationIDInput = "installation_id"
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "managed_credential.installation_id_input requires grant_type github_app_installation") {
		t.Fatalf("ValidateSource errors = %q, want grant_type guarded installation input", joined)
	}
}

func TestValidateSourceRejectsConnectorWithoutResponseSuccess(t *testing.T) {
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.ResponseSuccess = nil
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
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.ResponseSuccess = &runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "body.ok", Equals: true}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "response_success.path must start with response.") {
		t.Fatalf("ValidateSource errors = %q, want response_success path rejection", joined)
	}
}

func TestValidateSourceRejectsAgentExposureOfProviderConnector(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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

	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: store})
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

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
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
		got, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(record)})
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
	scopeSurfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: insufficient})
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

	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{StaticCredentials: store})
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

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("Surfaces without store: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 || requirementSatisfied(unbound[0].Requirements[0]) {
		t.Fatalf("unbound requirements = %#v", unbound)
	}
	t.Setenv("telegram_bot_token", "env-provider-secret")
	envBound, err := CapabilitySubjects(ctx, source, CapabilityOptions{StaticCredentials: runtimecredentials.NewEnvStore()})
	if err != nil {
		t.Fatalf("CapabilitySubjects env store: %v", err)
	}
	if len(envBound) != 1 || len(envBound[0].Requirements) != 1 || !requirementSatisfied(envBound[0].Requirements[0]) || envBound[0].Requirements[0].Source != runtimecredentials.SourceEnv || strings.Contains(packs.RenderSubject(envBound[0], true), "env-provider-secret") {
		t.Fatalf("env-bound subject = %#v", envBound)
	}
}

func TestSurfacesResolveImportedFlowCredentialBindings(t *testing.T) {
	ctx := context.Background()
	tool := telegramConnectorTool("https://api.telegram.org")
	source := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": tool,
			},
		}),
		projectScopes: []semanticview.ProjectScope{
			{
				Key: "",
				Manifest: runtimecontracts.ProjectPackageDocument{
					Flows: []runtimecontracts.ProjectFlowRef{
						{
							ID:   "worker",
							Flow: "worker",
							Bind: runtimecontracts.FlowPackageBind{
								Credentials: map[string]string{
									"telegram_bot_token": "tenant_telegram_bot_token",
								},
							},
						},
					},
				},
			},
			{
				Key: "flows/worker",
				Manifest: runtimecontracts.ProjectPackageDocument{
					Requires: runtimecontracts.FlowPackageRequires{
						Credentials: []string{"telegram_bot_token"},
					},
				},
			},
		},
		flowScopes: []semanticview.FlowScope{
			{
				ID:         "worker",
				PackageKey: "flows/worker",
				Tools: map[string]runtimecontracts.ToolSchemaEntry{
					"telegram.send_message": tool,
				},
			},
		},
	}
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(ctx, "tenant_telegram_bot_token", "provider-secret"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{StaticCredentials: store})
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	if len(surfaces[0].Requirements) != 1 || surfaces[0].Requirements[0].Name != "tenant_telegram_bot_token" || !requirementSatisfied(surfaces[0].Requirements[0]) {
		t.Fatalf("requirements = %#v, want package-local telegram_bot_token marked bound via deployment binding", surfaces[0].Requirements)
	}
}

func TestCapabilitySubjectsResolveImportedManagedCredentialBindings(t *testing.T) {
	ctx := context.Background()
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	source := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"slack.post_message": tool}}),
		projectScopes: []semanticview.ProjectScope{
			{Key: "", Manifest: runtimecontracts.ProjectPackageDocument{Flows: []runtimecontracts.ProjectFlowRef{{
				ID: "worker", Flow: "worker", Bind: runtimecontracts.FlowPackageBind{Credentials: map[string]string{"slack_oauth": "tenant_slack_oauth"}},
			}}}},
			{Key: "flows/worker", Manifest: runtimecontracts.ProjectPackageDocument{Requires: runtimecontracts.FlowPackageRequires{Credentials: []string{"slack_oauth"}}}},
		},
		flowScopes: []semanticview.FlowScope{{ID: "worker", PackageKey: "flows/worker", Tools: map[string]runtimecontracts.ToolSchemaEntry{"slack.post_message": tool}}},
	}
	record := runtimemanagedcredentials.Record{
		Key: "tenant_slack_oauth", Provider: "slack", GrantType: runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		Scopes: []string{"chat:write"}, AccessToken: "secret", Status: runtimemanagedcredentials.StatusConnected,
	}
	subjects, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: runtimemanagedcredentials.NewMemoryStore(record)})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(subjects) != 1 || len(subjects[0].Requirements) != 1 || subjects[0].Requirements[0].Name != "tenant_slack_oauth" || !requirementSatisfied(subjects[0].Requirements[0]) {
		t.Fatalf("managed import-boundary subjects = %#v", subjects)
	}
	if strings.Contains(packs.RenderSubject(subjects[0], true), "secret") {
		t.Fatalf("managed subject leaked token: %#v", subjects[0])
	}
}

func TestDefaultPackRegistryLoadsTelegramFromVerifiedPlatformPack(t *testing.T) {
	tool, ok := BuiltinTool("telegram", "telegram.send_message")
	if !ok {
		t.Fatal("BuiltinTool telegram.send_message not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin telegram tool category = %q, want %q", tool.Category, Category)
	}
	if strings.Contains(tool.HTTP.URL, "provider-secret") {
		t.Fatal("builtin telegram tool leaked a concrete credential value")
	}
}

func TestCapabilitySubjectsEnumerateExactInstalledInventoryWithoutMakingToolsEffective(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{IncludeInstalled: true})
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
		"telegram.send_message",
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
	subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{IncludeInstalled: true})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(subjects) != 7 {
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

func TestDefaultPackRegistryLoadsSlackManagedCredentialPack(t *testing.T) {
	tool, ok := BuiltinTool("slack", "slack.post_message")
	if !ok {
		t.Fatal("BuiltinTool slack.post_message not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin slack tool category = %q, want %q", tool.Category, Category)
	}
	if tool.ManagedCredential == nil || tool.ManagedCredential.Key != "slack_oauth" {
		t.Fatalf("builtin slack managed credential = %#v, want slack_oauth", tool.ManagedCredential)
	}
	if tool.ResponseSuccess == nil || tool.ResponseSuccess.Path != "response.body.ok" || tool.ResponseSuccess.Equals != true {
		t.Fatalf("builtin slack response_success = %#v, want response.body.ok true", tool.ResponseSuccess)
	}
	if len(tool.Credentials) != 0 {
		t.Fatalf("builtin slack static credentials = %#v, want none", tool.Credentials)
	}
	if strings.Contains(tool.HTTP.URL, "xoxb") || strings.Contains(tool.HTTP.URL, "secret") {
		t.Fatal("builtin slack tool leaked a concrete credential value")
	}
}

func TestDefaultPackRegistryLoadsNotionManagedCredentialPack(t *testing.T) {
	tool, ok := BuiltinTool("notion", "notion.append_block_children")
	if !ok {
		t.Fatal("BuiltinTool notion.append_block_children not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin Notion tool category = %q, want %q", tool.Category, Category)
	}
	if tool.ManagedCredential == nil || tool.ManagedCredential.Key != "notion_oauth" {
		t.Fatalf("builtin Notion managed credential = %#v, want notion_oauth", tool.ManagedCredential)
	}
	if tool.ManagedCredential.GrantModel != managedcredentialmodel.GrantModelWorkspace {
		t.Fatalf("builtin Notion grant_model = %q, want workspace_grant", tool.ManagedCredential.GrantModel)
	}
	wantProfile := managedcredentialmodel.TokenRequestProfile{
		ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
		Body:       managedcredentialmodel.TokenBodyJSON,
		StaticHeaders: map[string]string{
			"Notion-Version": "2026-03-11",
		},
	}
	if !managedcredentialmodel.TokenRequestProfileEqual(tool.ManagedCredential.TokenRequest, wantProfile) {
		t.Fatalf("builtin Notion token_request = %#v, want Basic JSON Notion-Version", tool.ManagedCredential.TokenRequest)
	}
	if tool.HTTP == nil || tool.HTTP.Method != "PATCH" || !strings.Contains(tool.HTTP.URL, "/v1/blocks/{{input.block_id}}/children") {
		t.Fatalf("builtin Notion http = %#v, want append block children PATCH endpoint", tool.HTTP)
	}
	if got := tool.HTTP.Headers["Notion-Version"]; got != "2026-03-11" {
		t.Fatalf("builtin Notion API header = %q, want Notion-Version", got)
	}
	if len(tool.Credentials) != 0 {
		t.Fatalf("builtin Notion static credentials = %#v, want none", tool.Credentials)
	}
}

func TestDefaultPackRegistryLoadsMicrosoftGraphManagedCredentialPack(t *testing.T) {
	tool, ok := BuiltinTool("microsoft_graph", "microsoft_graph.send_mail")
	if !ok {
		t.Fatal("BuiltinTool microsoft_graph.send_mail not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin Microsoft Graph tool category = %q, want %q", tool.Category, Category)
	}
	if tool.ManagedCredential == nil || tool.ManagedCredential.Key != "microsoft_graph_app" {
		t.Fatalf("builtin Microsoft Graph managed credential = %#v, want microsoft_graph_app", tool.ManagedCredential)
	}
	if got := strings.Join(tool.ManagedCredential.Scopes, " "); got != "https://graph.microsoft.com/.default" {
		t.Fatalf("builtin Microsoft Graph scopes = %q, want Graph .default resource scope", got)
	}
	if managedcredentialmodel.NormalizeGrantModel(tool.ManagedCredential.GrantModel) != managedcredentialmodel.GrantModelScope {
		t.Fatalf("builtin Microsoft Graph grant_model = %q, want scope_grant", tool.ManagedCredential.GrantModel)
	}
	if !managedcredentialmodel.TokenRequestProfileEqual(tool.ManagedCredential.TokenRequest, managedcredentialmodel.DefaultTokenRequestProfile()) {
		t.Fatalf("builtin Microsoft Graph token_request = %#v, want default post/form", tool.ManagedCredential.TokenRequest)
	}
	if tool.HTTP == nil || tool.HTTP.Method != "POST" || !strings.Contains(tool.HTTP.URL, "/v1.0/users/{{input.user_id}}/sendMail") {
		t.Fatalf("builtin Microsoft Graph http = %#v, want sendMail POST endpoint", tool.HTTP)
	}
	if len(tool.Credentials) != 0 {
		t.Fatalf("builtin Microsoft Graph static credentials = %#v, want none", tool.Credentials)
	}
	if strings.Contains(tool.HTTP.URL, "secret") || strings.Contains(tool.HTTP.URL, "token") {
		t.Fatal("builtin Microsoft Graph tool leaked a concrete credential value")
	}
}

func TestDefaultPackRegistryLoadsGitHubAppInstallationPack(t *testing.T) {
	tool, ok := BuiltinTool("github", "github.create_issue_comment")
	if !ok {
		t.Fatal("BuiltinTool github.create_issue_comment not found")
	}
	if !isProviderConnector(tool) {
		t.Fatalf("builtin GitHub tool category = %q, want %q", tool.Category, Category)
	}
	if tool.ManagedCredential == nil || tool.ManagedCredential.Key != "github_app" {
		t.Fatalf("builtin GitHub managed credential = %#v, want github_app", tool.ManagedCredential)
	}
	if tool.ManagedCredential.GrantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
		t.Fatalf("builtin GitHub grant_type = %q, want github_app_installation", tool.ManagedCredential.GrantType)
	}
	if tool.ManagedCredential.GrantModel != managedcredentialmodel.GrantModelInstallation {
		t.Fatalf("builtin GitHub grant_model = %q, want installation_grant", tool.ManagedCredential.GrantModel)
	}
	if tool.ManagedCredential.InstallationIDInput != "installation_id" {
		t.Fatalf("builtin GitHub installation_id_input = %q, want installation_id", tool.ManagedCredential.InstallationIDInput)
	}
	if tool.HTTP == nil || tool.HTTP.Method != "POST" || !strings.Contains(tool.HTTP.URL, "/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/comments") {
		t.Fatalf("builtin GitHub http = %#v, want create issue comment endpoint", tool.HTTP)
	}
	if len(tool.Credentials) != 0 {
		t.Fatalf("builtin GitHub static credentials = %#v, want none", tool.Credentials)
	}
	if strings.Contains(tool.HTTP.URL, "secret") || strings.Contains(tool.HTTP.URL, "token") {
		t.Fatal("builtin GitHub tool leaked a concrete credential value")
	}

	createIssue, ok := BuiltinTool("github", "github.create_issue")
	if !ok {
		t.Fatal("BuiltinTool github.create_issue not found")
	}
	if !isProviderConnector(createIssue) || createIssue.ManagedCredential == nil || createIssue.ManagedCredential.GrantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
		t.Fatalf("builtin GitHub create issue tool = %#v, want provider connector with github_app_installation", createIssue)
	}
	if createIssue.HTTP == nil || createIssue.HTTP.Method != "POST" || !strings.Contains(createIssue.HTTP.URL, "/repos/{{input.owner}}/{{input.repo}}/issues") {
		t.Fatalf("builtin GitHub create issue http = %#v, want create issue endpoint", createIssue.HTTP)
	}

	addLabels, ok := BuiltinTool("github", "github.add_labels_to_issue")
	if !ok {
		t.Fatal("BuiltinTool github.add_labels_to_issue not found")
	}
	if !isProviderConnector(addLabels) || addLabels.ManagedCredential == nil || addLabels.ManagedCredential.GrantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
		t.Fatalf("builtin GitHub add labels tool = %#v, want provider connector with github_app_installation", addLabels)
	}
	if addLabels.HTTP == nil || addLabels.HTTP.Method != "POST" || !strings.Contains(addLabels.HTTP.URL, "/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/labels") {
		t.Fatalf("builtin GitHub add labels http = %#v, want issue labels endpoint", addLabels.HTTP)
	}
	bodyMap, ok := addLabels.HTTP.Body.(map[string]any)
	if !ok {
		t.Fatalf("builtin GitHub add labels body = %#v, want object body", addLabels.HTTP.Body)
	}
	if body, ok := bodyMap["labels"].(string); !ok || body != "{{input.labels}}" {
		t.Fatalf("builtin GitHub add labels body = %#v, want whole-field labels template", addLabels.HTTP.Body)
	}
}

func TestNotionConnectorPackReportsWorkspaceGrantAndTokenProfileRequirement(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "notion", "notion.append_block_children"),
		},
	})
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one Notion managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "notion_oauth" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound notion_oauth", requirement)
	}
	if requirement.GrantModel != managedcredentialmodel.GrantModelWorkspace || requirement.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthBasic || requirement.TokenRequest.Body != managedcredentialmodel.TokenBodyJSON {
		t.Fatalf("unbound requirement shape = %#v, want workspace Basic JSON", requirement)
	}

	matchingStore := runtimemanagedcredentials.NewMemoryStore(notionConnectedRecord())
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected Notion credential", bound)
	}

	wrongProfile := notionConnectedRecord()
	wrongProfile.TokenRequest = managedcredentialmodel.DefaultTokenRequestProfile()
	wrongProfileStore := runtimemanagedcredentials.NewMemoryStore(wrongProfile)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: wrongProfileStore})
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
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "github", "github.create_issue_comment"),
		},
	})
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one GitHub managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "github_app" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound github_app", requirement)
	}
	if requirement.GrantType != runtimemanagedcredentials.GrantGitHubAppInstallation ||
		requirement.GrantModel != managedcredentialmodel.GrantModelInstallation ||
		requirement.InstallationIDInput != "installation_id" {
		t.Fatalf("unbound requirement shape = %#v, want github app installation grant", requirement)
	}

	matchingStore := runtimemanagedcredentials.NewMemoryStore(githubAppConnectedRecord())
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected GitHub App credential", bound)
	}

	wrongGrant := githubAppConnectedRecord()
	wrongGrant.GrantType = runtimemanagedcredentials.GrantClientCredentials
	wrongGrantStore := runtimemanagedcredentials.NewMemoryStore(wrongGrant)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: wrongGrantStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requirements) != 1 || requirementSatisfied(mismatch[0].Requirements[0]) || mismatch[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed grant_type mismatch", mismatch)
	}
}

func TestGitHubConnectorPackImportsMultipleActionsExplicitly(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			{
				Key: ".",
				Manifest: runtimecontracts.ProjectPackageDocument{
					ConnectorPacks: runtimecontracts.ConnectorPackImports{
						Imports: []runtimecontracts.ConnectorPackImport{
							{Provider: "github", Tool: "github.add_labels_to_issue"},
							{Provider: "github", Tool: "github.create_issue"},
							{Provider: "github", Tool: "github.create_issue_comment"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}
	tools := source.ToolEntries()
	for _, toolID := range []string{"github.add_labels_to_issue", "github.create_issue", "github.create_issue_comment"} {
		tool, ok := tools[toolID]
		if !ok {
			t.Fatalf("imported tool %s missing", toolID)
		}
		if !isProviderConnector(tool) || tool.ManagedCredential == nil || tool.ManagedCredential.Key != "github_app" {
			t.Fatalf("imported GitHub tool %s = %#v, want github_app provider connector", toolID, tool)
		}
	}
	surfaces, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(surfaces) != 3 {
		t.Fatalf("surfaces = %#v, want three GitHub action surfaces", surfaces)
	}
	seen := map[string]bool{}
	for _, surface := range surfaces {
		seen[surface.ID] = true
		if len(surface.Requirements) != 1 || surface.Requirements[0].Kind != "managed_credential" || surface.Requirements[0].Name != "github_app" || requirementSatisfied(surface.Requirements[0]) {
			t.Fatalf("surface %s requirements = %#v, want unbound github_app managed credential", surface.ID, surface.Requirements)
		}
		generation := evidenceByKind(surface.Evidence, "generation")
		if generation == nil || generation["operation"] == "" || generation["source"] == "" || generation["profile"] != "catalog/generator-profiles/github.yaml" || !strings.Contains(generation["permissions"], "issues:write") || !strings.HasSuffix(generation["fixture"], ":passing") || generation["review"] != "approved" {
			t.Fatalf("surface %s generation = %#v, want generated GitHub freshness evidence", surface.ID, generation)
		}
	}
	for _, toolID := range []string{"github.add_labels_to_issue", "github.create_issue", "github.create_issue_comment"} {
		if !seen[toolID] {
			t.Fatalf("surface for %s missing in %#v", toolID, surfaces)
		}
	}
}

func TestMicrosoftGraphConnectorPackReportsDefaultScopeManagedCredentialRequirement(t *testing.T) {
	ctx := context.Background()
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "microsoft_graph", "microsoft_graph.send_mail"),
		},
	})
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}

	unbound, err := CapabilitySubjects(ctx, source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("CapabilitySubjects unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requirements) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one Microsoft Graph managed credential requirement", unbound)
	}
	requirement := unbound[0].Requirements[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "microsoft_graph_app" || requirementSatisfied(requirement) || requirement.Status != "UNCONNECTED" {
		t.Fatalf("unbound requirement = %#v, want unbound microsoft_graph_app", requirement)
	}
	if requirement.GrantModel != managedcredentialmodel.GrantModelScope || requirement.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthPost || requirement.TokenRequest.Body != managedcredentialmodel.TokenBodyForm {
		t.Fatalf("unbound requirement shape = %#v, want scope_grant post/form", requirement)
	}
	if got := strings.Join(requirement.Scopes, " "); got != "https://graph.microsoft.com/.default" {
		t.Fatalf("unbound requirement scopes = %q, want Graph .default resource scope", got)
	}

	matchingStore := runtimemanagedcredentials.NewMemoryStore(microsoftGraphConnectedRecord())
	bound, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requirements) != 1 || !requirementSatisfied(bound[0].Requirements[0]) || bound[0].Requirements[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected Microsoft Graph credential", bound)
	}

	wrongScope := microsoftGraphConnectedRecord()
	wrongScope.Scopes = []string{"Mail.Send"}
	wrongScopeStore := runtimemanagedcredentials.NewMemoryStore(wrongScope)
	mismatch, err := CapabilitySubjects(ctx, source, CapabilityOptions{ManagedCredentials: wrongScopeStore})
	if err != nil {
		t.Fatalf("CapabilitySubjects mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requirements) != 1 || requirementSatisfied(mismatch[0].Requirements[0]) || mismatch[0].Requirements[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed .default scope mismatch", mismatch)
	}
}

func TestProviderConnectorPackVerificationFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, files fstest.MapFS)
		want   string
	}{
		{
			name: "platform pack bad exact byte hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				files["connector.yaml"].Data = append(files["connector.yaml"].Data, '\n')
			},
			want: "manifest_hash mismatch",
		},
		{
			name: "unknown connector manifest field",
			mutate: func(t *testing.T, files fstest.MapFS) {
				files["connector.yaml"].Data = append(files["connector.yaml"].Data, []byte("unexpected: true\n")...)
				rewriteConnectorPackHash(t, files)
			},
			want: "field unexpected not found",
		},
		{
			name: "capability declaration drift",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceConnectorPackFile(t, files, "pack.yaml", "- telegram.send_message", "- telegram.other")
			},
			want: "capabilities do not match",
		},
		{
			name: "requires declaration drift",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceConnectorPackFile(t, files, "pack.yaml", "telegram_bot_token", "other_token")
			},
			want: "requires do not match",
		},
		{
			name: "unknown envelope field",
			mutate: func(t *testing.T, files fstest.MapFS) {
				files["pack.yaml"].Data = append(files["pack.yaml"].Data, []byte("unexpected: true\n")...)
			},
			want: "field unexpected not found",
		},
		{
			name: "retired scalar action capability",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceConnectorPackFile(t, files, "pack.yaml", "call_provider_actions:\n      - telegram.send_message", "call_provider_action: telegram.send_message")
			},
			want: "field call_provider_action not found",
		},
		{
			name: "missing tests metadata",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceConnectorPackFile(t, files, "pack.yaml", "tests:\n  - providerconnectors/telegram_pack\n  - runtime/telegram_connector_supported_surface\n", "tests: []\n")
			},
			want: "tests are required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := connectorPackTestFS(t, "telegram")
			tc.mutate(t, files)
			_, err := LoadPackFS(files, ".", "0.7.0")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadPackFS error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestSlackConnectorPackRequiresManagedCredentialDriftFailsClosed(t *testing.T) {
	files := connectorPackTestFS(t, "slack")
	replaceConnectorPackFile(t, files, "pack.yaml", "slack_oauth", "other_oauth")
	_, err := LoadPackFS(files, ".", "0.7.0")
	if err == nil || !strings.Contains(err.Error(), "requires do not match") {
		t.Fatalf("LoadPackFS error = %v, want managed requires drift failure", err)
	}
}

func TestConnectorPackImportRequiresExplicitEnableAndReportsSurface(t *testing.T) {
	ambient := providerConnectorScopedSource{
		Source:        semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{{Key: "."}},
	}
	ambientSource, err := SourceWithConnectorPackImports(ambient)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports ambient: %v", err)
	}
	if _, exists := ambientSource.ToolEntries()["telegram.send_message"]; exists {
		t.Fatal("ambient source exposed telegram.send_message without explicit import")
	}

	explicit := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}
	source, err := SourceWithConnectorPackImports(explicit)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports explicit: %v", err)
	}
	tool, exists := source.ToolEntries()["telegram.send_message"]
	if !exists {
		t.Fatal("explicit import did not expose telegram.send_message")
	}
	if tool.Category != Category {
		t.Fatalf("imported tool category = %q, want %q", tool.Category, Category)
	}
	if errs := ValidateSource(source); len(errs) != 0 {
		t.Fatalf("ValidateSource imported connector errors = %#v", errs)
	}
	surfaces, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 || surfaces[0].ID != "telegram.send_message" {
		t.Fatalf("surfaces = %#v, want telegram.send_message", surfaces)
	}
	if surfaces[0].Source != "connector_pack_import" || surfaces[0].Applicability != "effective" || surfaces[0].Provenance != packs.ProvenancePlatform {
		t.Fatalf("surface source = %#v, want effective platform connector pack import", surfaces[0])
	}
	if len(surfaces[0].Requirements) != 1 || surfaces[0].Requirements[0].Name != "telegram_bot_token" || requirementSatisfied(surfaces[0].Requirements[0]) {
		t.Fatalf("surface requirements = %#v, want unbound telegram_bot_token", surfaces[0].Requirements)
	}
}

func TestConnectorPackImportApplicationSurvivesSemanticSourceWrappers(t *testing.T) {
	explicit := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}
	imported, err := SourceWithConnectorPackImports(explicit)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports: %v", err)
	}
	wrapper := providerConnectorSourceWrapper{Source: imported}
	reapplied, err := SourceWithConnectorPackImports(wrapper)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports wrapped: %v", err)
	}
	if _, ok := reapplied.ToolEntries()["telegram.send_message"]; !ok {
		t.Fatal("wrapped imported source lost telegram.send_message")
	}
}

func TestSlackConnectorPackImportRequiresExplicitEnableAndReportsManagedSurface(t *testing.T) {
	ambient := providerConnectorScopedSource{
		Source:        semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{{Key: "."}},
	}
	ambientSource, err := SourceWithConnectorPackImports(ambient)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports ambient: %v", err)
	}
	if _, exists := ambientSource.ToolEntries()["slack.post_message"]; exists {
		t.Fatal("ambient source exposed slack.post_message without explicit import")
	}

	explicit := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "slack", "slack.post_message"),
		},
	}
	source, err := SourceWithConnectorPackImports(explicit)
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImports explicit: %v", err)
	}
	tool, exists := source.ToolEntries()["slack.post_message"]
	if !exists {
		t.Fatal("explicit import did not expose slack.post_message")
	}
	if tool.Category != Category || tool.ManagedCredential == nil || tool.ManagedCredential.Key != "slack_oauth" {
		t.Fatalf("imported tool = %#v, want provider_connector managed slack_oauth", tool)
	}
	if errs := ValidateSource(source); len(errs) != 0 {
		t.Fatalf("ValidateSource imported Slack connector errors = %#v", errs)
	}
	surfaces, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{})
	if err != nil {
		t.Fatalf("CapabilitySubjects: %v", err)
	}
	if len(surfaces) != 1 || surfaces[0].ID != "slack.post_message" {
		t.Fatalf("surfaces = %#v, want slack.post_message", surfaces)
	}
	if len(surfaces[0].Requirements) != 1 || surfaces[0].Requirements[0].Kind != "managed_credential" || surfaces[0].Requirements[0].Name != "slack_oauth" || requirementSatisfied(surfaces[0].Requirements[0]) {
		t.Fatalf("surface requirements = %#v, want unbound managed slack_oauth", surfaces[0].Requirements)
	}
}

func TestConnectorPackImportRejectsCollisionsAndNamesSources(t *testing.T) {
	source := providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
			},
		}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	}

	_, err := SourceWithConnectorPackImports(source)
	if err == nil {
		t.Fatal("SourceWithConnectorPackImports succeeded, want collision")
	}
	for _, want := range []string{`provider connector tool "telegram.send_message" collision`, "package . connector_packs.imports", "merged tool source", "remove one"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("collision error = %q, want containing %q", err.Error(), want)
		}
	}
}

func TestValidateSourceRejectsDirectAgentExposureOfImportedProviderConnector(t *testing.T) {
	source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Agents: map[string]runtimecontracts.AgentRegistryEntry{
				"sender": {ID: "sender", Tools: []string{"telegram.send_message"}},
			},
		}),
		projectScopes: []semanticview.ProjectScope{
			projectScopeWithConnectorPackImport(".", "telegram", "telegram.send_message"),
		},
	})
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
	projectScopes []semanticview.ProjectScope
	flowScopes    []semanticview.FlowScope
}

type providerConnectorSourceWrapper struct {
	semanticview.Source
}

func (s providerConnectorSourceWrapper) BaseSemanticSource() semanticview.Source {
	return s.Source
}

func (s providerConnectorScopedSource) ProjectScopes() []semanticview.ProjectScope {
	return append([]semanticview.ProjectScope(nil), s.projectScopes...)
}

func (s providerConnectorScopedSource) FlowScopes() []semanticview.FlowScope {
	return append([]semanticview.FlowScope(nil), s.flowScopes...)
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

func telegramConnectorTool(baseURL string) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.ToolSchemaEntry{
		Category:    Category,
		Description: "send Telegram messages",
		HandlerType: "http",
		EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		Credentials: []string{"telegram_bot_token"},
		InputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"chat_id": {Type: "string"},
				"text":    {Type: "string"},
			},
			Required: []string{"chat_id", "text"},
		},
		OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
		ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{
			Kind: "http_status_2xx",
		},
		HTTP: &runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    strings.TrimRight(baseURL, "/") + "/bot{{credentials.telegram_bot_token}}/sendMessage",
			Body: map[string]any{
				"chat_id": "{{input.chat_id}}",
				"text":    "{{input.text}}",
			},
		},
	}
}

func slackConnectorTool(url string) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.ToolSchemaEntry{
		Category:    Category,
		Description: "post Slack messages",
		HandlerType: "http",
		EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		ManagedCredential: &runtimecontracts.ManagedCredentialRef{
			Key:    "slack_oauth",
			Scopes: []string{"chat:write"},
		},
		InputSchema: runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"channel": {Type: "string"},
				"text":    {Type: "string"},
			},
			Required: []string{"channel", "text"},
		},
		OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
		ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{
			Kind:   "json_field_equals",
			Path:   "response.body.ok",
			Equals: true,
		},
		HTTP: &runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    strings.TrimSpace(url),
			Body: map[string]any{
				"channel": "{{input.channel}}",
				"text":    "{{input.text}}",
			},
		},
	}
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

func projectScopeWithConnectorPackImport(key, provider, tool string) semanticview.ProjectScope {
	return semanticview.ProjectScope{
		Key: key,
		Manifest: runtimecontracts.ProjectPackageDocument{
			ConnectorPacks: runtimecontracts.ConnectorPackImports{
				Imports: []runtimecontracts.ConnectorPackImport{{Provider: provider, Tool: tool}},
			},
		},
	}
}

func connectorPackTestFS(t *testing.T, provider string) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	for _, file := range []string{"pack.yaml", "connector.yaml"} {
		body, err := fs.ReadFile(builtinConnectorPackFS, "packs/"+provider+"/"+file)
		if err != nil {
			t.Fatalf("read builtin connector pack file %s/%s: %v", provider, file, err)
		}
		out[file] = &fstest.MapFile{Data: append([]byte(nil), body...)}
	}
	return out
}

func rewriteConnectorPackHash(t *testing.T, files fstest.MapFS) {
	t.Helper()
	sum := sha256.Sum256(files["connector.yaml"].Data)
	hash := "sha256:" + hex.EncodeToString(sum[:])
	replaceConnectorPackHashLine(t, files, hash)
}

func replaceConnectorPackFile(t *testing.T, files fstest.MapFS, name, old, new string) {
	t.Helper()
	file := files[name]
	body := string(file.Data)
	if !strings.Contains(body, old) {
		t.Fatalf("%s missing %q", name, old)
	}
	body = strings.Replace(body, old, new, 1)
	file.Data = []byte(body)
}

func replaceConnectorPackHashLine(t *testing.T, files fstest.MapFS, hash string) {
	t.Helper()
	file := files["pack.yaml"]
	lines := strings.Split(string(file.Data), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "manifest_hash: ") {
			lines[i] = "manifest_hash: " + hash
			file.Data = []byte(strings.Join(lines, "\n"))
			return
		}
	}
	t.Fatal("pack.yaml missing manifest_hash line")
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

func joinRequirementStatuses(requirements []packs.Requirement) string {
	parts := make([]string, 0, len(requirements))
	for _, req := range requirements {
		parts = append(parts, req.Kind+":"+req.Name+"="+req.Status)
	}
	return strings.Join(parts, "; ")
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
