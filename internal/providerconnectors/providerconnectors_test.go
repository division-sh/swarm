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

func TestValidateSourceRejectsSlackPostMessageWithoutRequiredResponseSuccess(t *testing.T) {
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.ResponseSuccess = nil
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"slack.post_message": tool,
		},
	})

	errs := ValidateSource(source)
	joined := joinErrors(errs)
	if !strings.Contains(joined, "must declare response_success response.body.ok == true") {
		t.Fatalf("ValidateSource errors = %q, want required Slack response_success rejection", joined)
	}
}

func TestValidateSourceRejectsMalformedProviderConnectorResponseSuccess(t *testing.T) {
	tool := slackConnectorTool("https://slack.com/api/chat.postMessage")
	tool.ResponseSuccess = &runtimecontracts.HTTPResponseSuccess{Path: "body.ok", Equals: true}
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

	surfaces, err := SurfacesWithOptions(ctx, source, SurfaceOptions{ManagedCredentials: store})
	if err != nil {
		t.Fatalf("SurfacesWithOptions: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	requires := surfaces[0].Requires
	if len(requires) != 1 || requires[0].Kind != "managed_credential" || requires[0].Name != "slack_oauth" || !requires[0].Bound || requires[0].Status != "CONNECTED" {
		t.Fatalf("requirements = %#v, want connected managed slack_oauth", requires)
	}
	rendered := strings.Join(surfaces[0].Can, " ") + strings.Join(surfaces[0].Cannot, " ") + joinRequirementStatuses(requires)
	for _, secret := range []string{"xoxb-access-secret", "refresh-secret", "client-secret"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("surface leaked managed credential secret %q: %s", secret, rendered)
		}
	}

	unbound, err := SurfacesWithOptions(ctx, source, SurfaceOptions{})
	if err != nil {
		t.Fatalf("SurfacesWithOptions without store: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requires) != 1 || unbound[0].Requires[0].Bound || unbound[0].Requires[0].Status != "UNBOUND" {
		t.Fatalf("unbound managed requirements = %#v", unbound)
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
	scopeSurfaces, err := SurfacesWithOptions(ctx, source, SurfaceOptions{ManagedCredentials: insufficient})
	if err != nil {
		t.Fatalf("SurfacesWithOptions insufficient scopes: %v", err)
	}
	if len(scopeSurfaces) != 1 || len(scopeSurfaces[0].Requires) != 1 || scopeSurfaces[0].Requires[0].Bound || scopeSurfaces[0].Requires[0].Status != "SCOPE_INSUFFICIENT" {
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

	surfaces, err := Surfaces(ctx, source, store)
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	surface := surfaces[0]
	if surface.ToolID != "telegram.send_message" || surface.Provider != "telegram" || surface.Action != "send_message" {
		t.Fatalf("surface identity = %#v", surface)
	}
	if len(surface.Requires) != 1 || surface.Requires[0].Name != "telegram_bot_token" || !surface.Requires[0].Bound {
		t.Fatalf("requirements = %#v, want bound telegram_bot_token", surface.Requires)
	}
	if strings.Contains(strings.Join(surface.Can, " ")+strings.Join(surface.Cannot, " "), "provider-secret") {
		t.Fatal("surface leaked credential value")
	}

	unbound, err := Surfaces(ctx, source, nil)
	if err != nil {
		t.Fatalf("Surfaces without store: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requires) != 1 || unbound[0].Requires[0].Bound {
		t.Fatalf("unbound requirements = %#v", unbound)
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

	surfaces, err := Surfaces(ctx, source, store)
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 {
		t.Fatalf("surfaces = %#v, want one", surfaces)
	}
	if len(surfaces[0].Requires) != 1 || surfaces[0].Requires[0].Name != "telegram_bot_token" || !surfaces[0].Requires[0].Bound {
		t.Fatalf("requirements = %#v, want package-local telegram_bot_token marked bound via deployment binding", surfaces[0].Requires)
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

	unbound, err := SurfacesWithOptions(ctx, source, SurfaceOptions{})
	if err != nil {
		t.Fatalf("SurfacesWithOptions unbound: %v", err)
	}
	if len(unbound) != 1 || len(unbound[0].Requires) != 1 {
		t.Fatalf("unbound surfaces = %#v, want one Notion managed credential requirement", unbound)
	}
	requirement := unbound[0].Requires[0]
	if requirement.Kind != "managed_credential" || requirement.Name != "notion_oauth" || requirement.Bound || requirement.Status != "UNBOUND" {
		t.Fatalf("unbound requirement = %#v, want unbound notion_oauth", requirement)
	}
	if requirement.GrantModel != managedcredentialmodel.GrantModelWorkspace || requirement.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthBasic || requirement.TokenRequest.Body != managedcredentialmodel.TokenBodyJSON {
		t.Fatalf("unbound requirement shape = %#v, want workspace Basic JSON", requirement)
	}

	matchingStore := runtimemanagedcredentials.NewMemoryStore(notionConnectedRecord())
	bound, err := SurfacesWithOptions(ctx, source, SurfaceOptions{ManagedCredentials: matchingStore})
	if err != nil {
		t.Fatalf("SurfacesWithOptions bound: %v", err)
	}
	if len(bound) != 1 || len(bound[0].Requires) != 1 || !bound[0].Requires[0].Bound || bound[0].Requires[0].Status != "CONNECTED" {
		t.Fatalf("bound requirement = %#v, want connected Notion credential", bound)
	}

	wrongProfile := notionConnectedRecord()
	wrongProfile.TokenRequest = managedcredentialmodel.DefaultTokenRequestProfile()
	wrongProfileStore := runtimemanagedcredentials.NewMemoryStore(wrongProfile)
	mismatch, err := SurfacesWithOptions(ctx, source, SurfaceOptions{ManagedCredentials: wrongProfileStore})
	if err != nil {
		t.Fatalf("SurfacesWithOptions mismatch: %v", err)
	}
	if len(mismatch) != 1 || len(mismatch[0].Requires) != 1 || mismatch[0].Requires[0].Bound || mismatch[0].Requires[0].Status != "SCOPE_INSUFFICIENT" {
		t.Fatalf("mismatch requirement = %#v, want fail-closed token profile mismatch", mismatch)
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
				replaceConnectorPackFile(t, files, "pack.yaml", "call_provider_action: telegram.send_message", "call_provider_action: telegram.other")
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
	surfaces, err := Surfaces(context.Background(), source, nil)
	if err != nil {
		t.Fatalf("Surfaces: %v", err)
	}
	if len(surfaces) != 1 || surfaces[0].ToolID != "telegram.send_message" {
		t.Fatalf("surfaces = %#v, want telegram.send_message", surfaces)
	}
	if len(surfaces[0].Requires) != 1 || surfaces[0].Requires[0].Name != "telegram_bot_token" || surfaces[0].Requires[0].Bound {
		t.Fatalf("surface requirements = %#v, want unbound telegram_bot_token", surfaces[0].Requires)
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
	surfaces, err := SurfacesWithOptions(context.Background(), source, SurfaceOptions{})
	if err != nil {
		t.Fatalf("SurfacesWithOptions: %v", err)
	}
	if len(surfaces) != 1 || surfaces[0].ToolID != "slack.post_message" {
		t.Fatalf("surfaces = %#v, want slack.post_message", surfaces)
	}
	if len(surfaces[0].Requires) != 1 || surfaces[0].Requires[0].Kind != "managed_credential" || surfaces[0].Requires[0].Name != "slack_oauth" || surfaces[0].Requires[0].Bound {
		t.Fatalf("surface requirements = %#v, want unbound managed slack_oauth", surfaces[0].Requires)
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

func joinRequirementStatuses(requirements []RequirementStatus) string {
	parts := make([]string, 0, len(requirements))
	for _, req := range requirements {
		parts = append(parts, req.Kind+":"+req.Name+"="+req.Status)
	}
	return strings.Join(parts, "; ")
}
