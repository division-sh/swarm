package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func testRuntimeWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func TestEnsureWorkflowBootWiring_RejectsTouchedValidationDriftThroughSharedPath(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	cases := []struct {
		name        string
		source      semanticview.Source
		errContains string
		wantErr     bool
	}{
		{
			name: "tool resolution warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"missing_tool"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			wantErr: false,
		},
		{
			name: "missing emitted event schema warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", EmitEvents: []string{"missing.event"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "'missing.event' emitted but no schema in events.yaml",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": {
						HandlerType: "api_call",
					},
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "tool implementation warnings",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureWorkflowBootWiring(RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: tc.source},
			})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("ensureWorkflowBootWiring error = %v, want substring %q", err, tc.errContains)
				}
			} else if err != nil {
				t.Fatalf("ensureWorkflowBootWiring error = %v, want nil", err)
			}
		})
	}
}

func TestValidateWorkflowContractSurface_DurableActivityHTTPToolRequiresEffectClass(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			InputSchema: runtimecontracts.ToolInputSchema{
				Type:     "object",
				Required: []string{"url"},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"url": {Type: "string"},
				},
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{
						Tool: "source_scrape",
						Input: map[string]runtimecontracts.ExpressionValue{
							"url": runtimecontracts.CELExpression("payload.url"),
						},
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "must declare effect_class") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want missing effect_class", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityFailsClosedForMCPTool(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"mcp_source_scrape": {
			HandlerType: "mcp",
			EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
			InputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
			},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{Tool: "mcp_source_scrape"},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "handler_type \"mcp\" is not supported for activities") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want MCP activity fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityMinimalHTTPAccepted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
			InputSchema: runtimecontracts.ToolInputSchema{
				Type:     "object",
				Required: []string{"url"},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"url": {Type: "string"},
				},
			},
			OutputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"title": {Type: "string"},
				},
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{
						Tool: "source_scrape",
						Input: map[string]runtimecontracts.ExpressionValue{
							"url": runtimecontracts.CELExpression("payload.url"),
						},
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want nil", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityNonIdempotentWriteAdmitted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			Credentials: []string{"provider_token"},
			InputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{Tool: "source_scrape"},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want non_idempotent_write admitted", err)
	}
}

func TestValidateWorkflowContractSurface_TelegramProviderConnectorToolAdmitted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": {
			Category:    "provider_connector",
			Description: "send Telegram messages",
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			Credentials: []string{"telegram_bot_token"},
			InputSchema: runtimecontracts.ToolInputSchema{
				Type:     "object",
				Required: []string{"chat_id", "text"},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"chat_id": {Type: "string"},
					"text":    {Type: "string"},
				},
			},
			OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
			ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{
				Kind: "http_status_2xx",
			},
			HTTP: &runtimecontracts.HTTPToolSpec{
				Method: "POST",
				URL:    "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage",
				Body: map[string]any{
					"chat_id": "{{input.chat_id}}",
					"text":    "{{input.text}}",
				},
			},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"responder": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"inbound.telegram": {
					Activity: runtimecontracts.ActivitySpec{
						Tool: "telegram.send_message",
						Input: map[string]runtimecontracts.ExpressionValue{
							"chat_id": runtimecontracts.CELExpression("payload.payload.message.chat.id"),
							"text":    runtimecontracts.CELExpression(`"hello"`),
						},
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want Telegram connector admitted", err)
	}
}

func TestValidateWorkflowContractSurface_SlackManagedCredentialProviderConnectorToolAdmitted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"slack.post_message": {
			Category:    "provider_connector",
			Description: "post Slack messages",
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			ManagedCredential: &runtimecontracts.ManagedCredentialRef{
				Key:    "slack_oauth",
				Scopes: []string{"chat:write"},
			},
			InputSchema: runtimecontracts.ToolInputSchema{
				Type:     "object",
				Required: []string{"channel", "text"},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"channel": {Type: "string"},
					"text":    {Type: "string"},
				},
			},
			OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
			ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{
				Kind:   "json_field_equals",
				Path:   "response.body.ok",
				Equals: true,
			},
			HTTP: &runtimecontracts.HTTPToolSpec{
				Method: "POST",
				URL:    "https://slack.com/api/chat.postMessage",
				Body: map[string]any{
					"channel": "{{input.channel}}",
					"text":    "{{input.text}}",
				},
			},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"responder": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"inbound.telegram": {
					Activity: runtimecontracts.ActivitySpec{
						Tool: "slack.post_message",
						Input: map[string]runtimecontracts.ExpressionValue{
							"channel": runtimecontracts.CELExpression(`"C123"`),
							"text":    runtimecontracts.CELExpression(`"hello"`),
						},
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want Slack managed connector admitted", err)
	}
}

func TestValidateWorkflowContractSurface_SlackManagedCredentialProviderConnectorRequiresResponseSuccess(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"slack.post_message": {
			Category:    "provider_connector",
			Description: "post Slack messages",
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
			ManagedCredential: &runtimecontracts.ManagedCredentialRef{
				Key:    "slack_oauth",
				Scopes: []string{"chat:write"},
			},
			InputSchema: runtimecontracts.ToolInputSchema{
				Type:     "object",
				Required: []string{"channel", "text"},
				Properties: map[string]runtimecontracts.ToolInputSchema{
					"channel": {Type: "string"},
					"text":    {Type: "string"},
				},
			},
			OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
			HTTP: &runtimecontracts.HTTPToolSpec{
				Method: "POST",
				URL:    "https://slack.com/api/chat.postMessage",
				Body: map[string]any{
					"channel": "{{input.channel}}",
					"text":    "{{input.text}}",
				},
			},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"responder": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"inbound.telegram": {
					Activity: runtimecontracts.ActivitySpec{
						Tool: "slack.post_message",
						Input: map[string]runtimecontracts.ExpressionValue{
							"channel": runtimecontracts.CELExpression(`"C123"`),
							"text":    runtimecontracts.CELExpression(`"hello"`),
						},
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "provider connector validation failed") || !strings.Contains(err.Error(), "must declare exactly one response_success policy") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want connector response_success fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_ProviderConnectorToolFailsClosedForUnsupportedShape(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": {
			Category:    "provider_connector",
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
			HTTP:        &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage"},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "provider connector validation failed") || !strings.Contains(err.Error(), "effect_class must be non_idempotent_write") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want provider connector fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityIdempotentWriteFailsClosed(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassIdempotentWrite),
			InputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{Tool: "source_scrape"},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "idempotency execution ownership") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want idempotent_write fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityResultEventsRejectAuthoredCollision(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"scanner_source_requested_source_scrape.succeeded": {
			Note: "authored event with generated activity result name",
		},
	}
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
			InputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{Tool: "source_scrape"},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "generated activity result event \"scanner_source_requested_source_scrape.succeeded\" collides with authored event") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want authored event collision", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityResultEventsRejectGeneratedCollision(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": {
			HandlerType: "http",
			EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
			InputSchema: runtimecontracts.ToolInputSchema{
				Type: "object",
			},
			HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"scanner": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.requested": {
					Activity: runtimecontracts.ActivitySpec{
						ID:   "shared_activity",
						Tool: "source_scrape",
					},
				},
			},
		},
		"reader": {
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"source.other_requested": {
					Activity: runtimecontracts.ActivitySpec{
						ID:   "/shared_activity/",
						Tool: "source_scrape",
					},
				},
			},
		},
	}
	_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "generated activity result event \"shared_activity.succeeded\" collides with generated result event") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want generated event collision", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityHTTPSubfeaturesFailClosed(t *testing.T) {
	cases := []struct {
		name        string
		mutateTool  func(*runtimecontracts.ToolSchemaEntry)
		errContains string
	}{
		{
			name: "response mapping",
			mutateTool: func(tool *runtimecontracts.ToolSchemaEntry) {
				tool.ResponseMapping = map[string]any{"title": "{{response.body.title}}"}
			},
			errContains: "uses response_mapping",
		},
		{
			name: "rate limit",
			mutateTool: func(tool *runtimecontracts.ToolSchemaEntry) {
				tool.RateLimit = "1/s"
				tool.RateLimitMaxWait = "0s"
			},
			errContains: "uses rate_limit",
		},
		{
			name: "read only static credentials",
			mutateTool: func(tool *runtimecontracts.ToolSchemaEntry) {
				tool.Credentials = []string{"provider_token"}
			},
			errContains: "static credential activity HTTP execution is supported only for non_idempotent_write",
		},
		{
			name: "managed credentials",
			mutateTool: func(tool *runtimecontracts.ToolSchemaEntry) {
				tool.EffectClass = string(runtimecontracts.ActivityEffectClassNonIdempotentWrite)
				tool.ManagedCredential = &runtimecontracts.ManagedCredentialRef{Key: "provider_oauth"}
			},
			errContains: "uses managed_credential",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := testRuntimeWorkflowValidationBundle()
			tool := runtimecontracts.ToolSchemaEntry{
				HandlerType: "http",
				EffectClass: string(runtimecontracts.ActivityEffectClassReadOnly),
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:     "object",
					Required: []string{"url"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"url": {Type: "string"},
					},
				},
				HTTP: &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"},
			}
			tc.mutateTool(&tool)
			bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{"source_scrape": tool}
			bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
				"scanner": {
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
						"source.requested": {
							Activity: runtimecontracts.ActivitySpec{
								Tool: "source_scrape",
								Input: map[string]runtimecontracts.ExpressionValue{
									"url": runtimecontracts.CELExpression("payload.url"),
								},
							},
						},
					},
				},
			}
			_, err := ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
				CheckMCPReachable:              false,
				StrictEmitSchemas:              false,
				FatalToolImplementationWarning: false,
				FatalBootWarnings:              false,
			})
			if err == nil || !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("ValidateWorkflowContractSurface error = %v, want substring %q", err, tc.errContains)
			}
		})
	}
}

func TestEnsureWorkflowBootWiringFailsClosedForIncompatiblePlatformVersion(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Platform.Platform.Version = "0.7.0"
	bundle.PackageTree = []runtimecontracts.LoadedProjectPackage{{
		Key: ".",
		Manifest: runtimecontracts.ProjectPackageDocument{
			Name:            "runtime-incompatible-platform",
			PlatformVersion: ">=0.8.0",
		},
	}}

	err := ensureWorkflowBootWiring(RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
	})
	if err == nil {
		t.Fatal("ensureWorkflowBootWiring error = nil, want platform_version compatibility failure")
	}
	for _, want := range []string{
		"platform_version_compatibility",
		`platform_version range ">=0.8.0" does not include running platform "0.7.0"`,
		"remediation: update package.yaml platform_version after re-verifying",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ensureWorkflowBootWiring error = %v, want substring %q", err, want)
		}
	}
}

func TestRuntimeDepsValidateOwnsRequiredBootInputs(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	validModule := semanticOnlyWorkflowRuntime{source: semanticview.Wrap(testRuntimeWorkflowValidationBundle())}

	cases := []struct {
		name        string
		deps        RuntimeDeps
		errContains string
	}{
		{
			name:        "nil config",
			deps:        RuntimeDeps{Options: RuntimeOptions{WorkflowModule: validModule}},
			errContains: "runtime config is required",
		},
		{
			name:        "missing workflow module",
			deps:        RuntimeDeps{Config: &config.Config{}},
			errContains: "workflow contract validation failed: workflow module is required",
		},
		{
			name: "retired llm runtime mode",
			deps: RuntimeDeps{
				Config: &config.Config{
					LLM: config.LLMConfig{RuntimeMode: "cli_test"},
				},
				Options: RuntimeOptions{WorkflowModule: validModule},
			},
			errContains: "llm.runtime_mode is retired",
		},
		{
			name: "valid dependency graph",
			deps: RuntimeDeps{
				Config:  &config.Config{},
				Options: RuntimeOptions{WorkflowModule: validModule},
			},
		},
		{
			name: "store boundary blocker",
			deps: RuntimeDeps{
				Config: &config.Config{},
				Stores: Stores{
					ConstructionBlocker: "sqlite selected runtime persistence remains blocked",
				},
				Options: RuntimeOptions{WorkflowModule: validModule},
			},
			errContains: "runtime store boundary is not construction-ready: sqlite selected runtime persistence remains blocked",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.deps.Validate()
			if tc.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("RuntimeDeps.Validate error = %v, want substring %q", err, tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("RuntimeDeps.Validate: %v", err)
			}
		})
	}
}

func TestRuntimeDepsValidatedDerivesCanonicalBootGraph(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	module := semanticOnlyWorkflowRuntime{source: semanticview.Wrap(testRuntimeWorkflowValidationBundle())}

	boot, err := (RuntimeDeps{
		Config: &config.Config{},
		Options: RuntimeOptions{
			WorkflowModule:    module,
			BundleFingerprint: "  fingerprint-1  ",
		},
	}).validated()
	if err != nil {
		t.Fatalf("RuntimeDeps.validated: %v", err)
	}
	if boot.Source == nil {
		t.Fatal("validated RuntimeDeps Source = nil")
	}
	if boot.PromptResolver == nil {
		t.Fatal("validated RuntimeDeps PromptResolver = nil")
	}
	if boot.Credentials == nil {
		t.Fatal("validated RuntimeDeps Credentials = nil")
	}
	if boot.Authority == nil {
		t.Fatal("validated RuntimeDeps Authority = nil")
	}
	if boot.EmitRegistry == nil {
		t.Fatal("validated RuntimeDeps EmitRegistry = nil")
	}
	if boot.TrimmedBundleFingerprint != "fingerprint-1" {
		t.Fatalf("TrimmedBundleFingerprint = %q, want fingerprint-1", boot.TrimmedBundleFingerprint)
	}
}

func TestValidateWorkflowContractSurface_AllowsExplicitEventSchemas(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"id": {Type: "string"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if len(result.MissingEmitSchemaEventTypes) != 0 {
		t.Fatalf("MissingEmitSchemaEventTypes = %#v, want none", result.MissingEmitSchemaEventTypes)
	}
	if len(result.BootReport.Warnings()) != 0 {
		t.Fatalf("BootReport warnings = %#v, want none", result.BootReport.Warnings())
	}
}

func TestValidateWorkflowContractSurfaceRejectsInvalidGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Role: "agent", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Role: "consumer", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"unsupported": {Type: "NotDeclared"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "generated_tool_schema_closure") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want boot generated schema closure failure", err)
	}
	if len(result.BootReport.Errors()) != 1 {
		t.Fatalf("BootReport errors = %#v, want one error", result.BootReport.Errors())
	}
	if got := result.BootReport.Errors()[0].Message; !strings.Contains(got, "unsupported JSON Schema type \"NotDeclared\"") {
		t.Fatalf("BootReport error = %q, want unsupported type", got)
	}
}

func TestValidateWorkflowContractSurfaceAllowsPrecisionQualifiedGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Types: map[string]runtimecontracts.NamedTypeDecl{
			"RequiredCapabilities": {
				Fields: map[string]runtimecontracts.TypeFieldSpec{
					"automation_with_unlock": {Type: "numeric(5,2)"},
				},
			},
		},
	}
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Role: "agent", EmitEvents: []string{"ready.event"}},
		"agent-2": {ID: "agent-2", Role: "consumer", Subscriptions: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"capabilities": {Type: "RequiredCapabilities"},
					"amounts":      {Type: "[numeric(10,2)]"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if len(result.GeneratedEmitSchemaErrors) != 0 {
		t.Fatalf("GeneratedEmitSchemaErrors = %#v, want none", result.GeneratedEmitSchemaErrors)
	}
}

func TestValidateWorkflowContractSurface_FatalToolImplementationWarningsFollowSharedOptions(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"legacy_call": {
			HandlerType: "api_call",
		},
	}
	source := semanticview.Wrap(bundle)

	_, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "tool implementation warnings") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want tool implementation warning failure", err)
	}
}

func TestValidateWorkflowContractSurface_RejectsCreateEntityWithAccumulate(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadRuntimeWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate")))

	_, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err == nil || !strings.Contains(err.Error(), "declares both create_entity and accumulate") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want create_entity/accumulate boot error", err)
	}
}

func loadRuntimeWorkflowValidationFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}
