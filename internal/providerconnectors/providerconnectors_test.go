package providerconnectors

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
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
		"must declare at least one static credential binding",
		"uses response_mapping",
		"uses rate_limit",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ValidateSource errors = %q, want %q", joined, want)
		}
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

func joinErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "\n")
}
