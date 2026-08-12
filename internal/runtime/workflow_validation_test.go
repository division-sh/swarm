package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestDefaultWorkflowContractValidationRejectsHarnessInput(t *testing.T) {
	source := loadHarnessInjectionValidationSource(t)
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness at worker.work_requested") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want harness production rejection", err)
	}
	if result.HarnessInjectedInputCount != 1 || result.HarnessObservedOutputCount != 1 || result.ProductionValid {
		t.Fatalf("validation result = %#v, want one harness input, one harness output, and production_valid=false", result)
	}
}

func TestValidateWorkflowContractSurfaceAllowsHarnessOnlyForExplicitVerifyPolicy(t *testing.T) {
	source := loadHarnessInjectionValidationSource(t)
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	opts.AllowHarnessInputs = true
	opts.AllowHarnessOutputs = true
	opts.CheckMCPReachable = false
	opts.FatalBootWarnings = false
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts)
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if result.HarnessInjectedInputCount != 1 || result.HarnessObservedOutputCount != 1 || result.ProductionValid {
		t.Fatalf("validation result = %#v, want one harness input, one harness output, and production_valid=false", result)
	}
}

func TestProductionValidationRejectsHarnessOutputIndependently(t *testing.T) {
	source := loadWorkflowValidationSourceAt(t, canonicalrouting.CopyHarnessInjectionWithoutSource(t))
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	opts.AllowHarnessInputs = true
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only output sink: harness at worker.work_completed") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want harness output production rejection", err)
	}
	if result.HarnessInjectedInputCount != 0 || result.HarnessObservedOutputCount != 1 || result.ProductionValid {
		t.Fatalf("validation result = %#v, want one harness output and production_valid=false", result)
	}
}

func TestProductionValidationRejectsRootHarnessOutput(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{
			Outputs: runtimecontracts.FlowOutputPins{
				EventPins: []runtimecontracts.FlowOutputEventPin{{
					Name:  "root_completed",
					Event: "root.completed",
					Sink:  runtimecontracts.FlowOutputSinkHarness,
				}},
			},
		},
	}
	source := semanticview.Wrap(bundle)
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only output sink: harness at root_completed") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want root harness output production rejection", err)
	}
	if result.HarnessObservedOutputCount != 1 || result.ProductionValid {
		t.Fatalf("validation result = %#v, want one root harness output and production_valid=false", result)
	}
}

func TestValidateWorkflowContractSurfaceRejectsProgrammaticUnknownOutputSink(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{
			Name: "root_completed", Event: "root.completed", Sink: runtimecontracts.FlowOutputSink(255),
		}}}},
	}
	bundle.Semantics.FlowOutputEventPins = map[string][]runtimecontracts.FlowOutputEventPin{
		"": bundle.RootSchema.Pins.Outputs.EventPins,
	}
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil || !strings.Contains(err.Error(), "output pin sink is invalid at root_completed") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want invalid sink rejection", err)
	}
	if result.ProductionValid {
		t.Fatalf("validation result = %#v, want production_valid=false", result)
	}
}

func TestEnsureWorkflowBootWiringRejectsHarnessOutputWithoutInputHarness(t *testing.T) {
	_, _, err := ensureWorkflowBootWiring(RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: loadWorkflowValidationSourceAt(t, canonicalrouting.CopyHarnessInjectionWithoutSource(t))},
	}, workflowValidationTestProfile(t), executionposture.Live)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only output sink: harness") {
		t.Fatalf("ensureWorkflowBootWiring error = %v, want harness output production rejection", err)
	}
}

func TestEnsureWorkflowBootWiringRejectsHarnessInput(t *testing.T) {
	_, _, err := ensureWorkflowBootWiring(RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: loadHarnessInjectionValidationSource(t)},
	}, workflowValidationTestProfile(t), executionposture.Live)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness") {
		t.Fatalf("ensureWorkflowBootWiring error = %v, want harness production rejection", err)
	}
}

func TestHarnessInputCreatesNoStandingTargetProviderIngressOrTargetFreeRoute(t *testing.T) {
	source := loadHarnessInjectionValidationSource(t)
	declarations, err := ResolveStandingTargetDeclarations(source, nil)
	if err != nil {
		t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
	}
	if len(declarations) != 0 {
		t.Fatalf("standing targets = %#v, want none", declarations)
	}

	wrapped, err := SourceWithProviderTriggerEvents(source, nil)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	graph := runtimepinrouting.CompileConnectGraph(wrapped)
	plans, issues := graph.Plans(), graph.Issues()
	if len(plans) != 0 || len(issues) != 0 {
		t.Fatalf("target-free plans = %#v issues = %#v, want none", plans, issues)
	}
}

func testRuntimeWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func TestRetiredDynamicAgentToolsFailClosedAtVerifyAndBoot(t *testing.T) {
	for _, name := range []string{"agent_hire", "agent_fire", "agent_reconfigure"} {
		for _, manifestation := range []struct {
			name      string
			configure func(*runtimecontracts.WorkflowContractBundle)
		}{
			{
				name: "agent_reference",
				configure: func(bundle *runtimecontracts.WorkflowContractBundle) {
					bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
						"worker": {ID: "worker", Tools: []string{name}},
					}
				},
			},
			{
				name: "http_tool_entry",
				configure: func(bundle *runtimecontracts.WorkflowContractBundle) {
					bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
						name: runtimecontracts.MustToolSchemaEntry(
							runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
							runtimecontracts.WithToolSchemas(
								runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
								runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
							),
							runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid"}),
						),
					}
				},
			},
		} {
			t.Run(name+"/"+manifestation.name, func(t *testing.T) {
				bundle := testRuntimeWorkflowValidationBundle()
				manifestation.configure(bundle)
				source := semanticview.Wrap(bundle)

				_, err := ValidateWorkflowContractSurface(
					testAuthorActivityContext(context.Background()),
					source,
					DefaultWorkflowContractValidationOptions(nil, executionposture.Live),
				)
				assertRetiredDynamicAgentToolSurfaceError(t, "verify", name, err)

				_, _, err = ensureWorkflowBootWiring(RuntimeOptions{
					WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
				}, workflowValidationTestProfile(t), executionposture.Live)
				assertRetiredDynamicAgentToolSurfaceError(t, "boot", name, err)
			})
		}
	}
}

func assertRetiredDynamicAgentToolSurfaceError(t testing.TB, surface, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s admitted retired tool %s", surface, name)
	}
	for _, want := range []string{name, "RETIRED", "agents.yaml", "flow lifecycle/readiness", "typed fan-out"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s error = %v, want %q", surface, err, want)
		}
	}
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
				return semanticviewtest.WrapRootAgents(bundle)
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
				return semanticviewtest.WrapRootAgents(bundle)
			}(),
			errContains: "'missing.event' emitted but no schema in events.yaml",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "tool implementation warnings",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ensureWorkflowBootWiring(RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: tc.source},
			}, workflowValidationTestProfile(t), executionposture.Live)
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
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"url": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("url")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"mcp_source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("mcp")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"url": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("url")),

			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"title": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"}), runtimecontracts.WithToolCredentials([]string{"provider_token"}...)),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want non_idempotent_write admitted", err)
	}
}

func TestValidateWorkflowContractSurface_ActivityApprovalBoundary(t *testing.T) {
	for _, tc := range []struct {
		name            string
		effectClass     runtimecontracts.ActivityEffectClass
		decision        string
		includeConsumer bool
		wantError       string
	}{
		{name: "valid", effectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite, decision: "support_reply", includeConsumer: true},
		{name: "read only teaching error", effectClass: runtimecontracts.ActivityEffectClassReadOnly, decision: "support_reply", includeConsumer: true, wantError: "read-only activities don't need approval"},
		{name: "missing revision consumer", effectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite, decision: "support_reply", wantError: "has no consumer"},
		{name: "noncanonical programmatic decision", effectClass: runtimecontracts.ActivityEffectClassNonIdempotentWrite, decision: " support_reply ", includeConsumer: true, wantError: "canonical stable decision id is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := testRuntimeWorkflowValidationBundle()
			bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
				"provider_write": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(tc.effectClass))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"})),
			}
			handlers := map[string]runtimecontracts.SystemNodeEventHandler{
				"support.reply_drafted": {
					Activity: runtimecontracts.ActivitySpec{
						ID: "send_support_reply", Tool: "provider_write",
						Approval: &runtimecontracts.ActivityApprovalSpec{Decision: tc.decision},
					},
				},
			}
			if tc.includeConsumer {
				handlers["send_support_reply.revision_requested"] = runtimecontracts.SystemNodeEventHandler{}
			}
			bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
				"support": {ID: "support", EventHandlers: handlers},
			}
			_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
				ExecutionPosture:  executionposture.Live,
				CheckMCPReachable: false, StrictEmitSchemas: false, FatalToolImplementationWarning: false, FatalBootWarnings: false,
			})
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("validation error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

func TestValidateWorkflowContractSurface_TelegramProviderConnectorToolAdmitted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolDescription("send Telegram messages"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"chat_id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			"text":    runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("chat_id", "text")),

			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage",
			Body: map[string]any{
				"chat_id": "{{input.chat_id}}",
				"text":    "{{input.text}}",
			},
		}), runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{
			Kind: "http_status_2xx",
		}), runtimecontracts.WithToolCredentials([]string{"telegram_bot_token"}...)),
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
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want Telegram connector admitted", err)
	}
	if result.mockConnectorResponses == nil {
		t.Fatal("mock validation did not compile the effective flow-local connector response")
	}
	if _, err := result.mockConnectorResponses.Admit("telegram.send_message", bundle.Tools["telegram.send_message"]); err != nil {
		t.Fatalf("generated flow-local response admission: %v", err)
	}
	for _, finding := range result.BootReport.Findings {
		if finding.Location == "provider_credential" && strings.Contains(finding.Message, "telegram_bot_token") {
			t.Fatalf("mock validation retained live connector credential finding: %#v", finding)
		}
	}

}

func TestValidateWorkflowContractSurface_SlackManagedCredentialProviderConnectorToolAdmitted(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"slack.post_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolDescription("post Slack messages"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"channel": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			"text":    runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("channel", "text")),

			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    "https://slack.com/api/chat.postMessage",
			Body: map[string]any{
				"channel": "{{input.channel}}",
				"text":    "{{input.text}}",
			},
		}), runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{
			Kind:   "json_field_equals",
			Path:   "response.body.ok",
			Equals: true,
		}), runtimecontracts.WithToolManagedCredential(runtimecontracts.ManagedCredentialRef{
			Key:    "slack_oauth",
			Scopes: []string{"chat:write"},
		})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"slack.post_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolDescription("post Slack messages"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"channel": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			"text":    runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
		}), runtimecontracts.ToolSchemaRequired("channel", "text")),

			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    "https://slack.com/api/chat.postMessage",
			Body: map[string]any{
				"channel": "{{input.channel}}",
				"text":    "{{input.text}}",
			},
		}), runtimecontracts.WithToolManagedCredential(runtimecontracts.ManagedCredentialRef{
			Key:    "slack_oauth",
			Scopes: []string{"chat:write"},
		})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "provider connector mock response compilation failed") || !strings.Contains(err.Error(), "must declare exactly one response_success policy") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want connector response_success fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_ProviderConnectorToolFailsClosedForUnsupportedShape(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage"})),
	}
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	if err == nil || !strings.Contains(err.Error(), "provider connector mock response compilation failed") || !strings.Contains(err.Error(), "effect_class must be non_idempotent_write") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want provider connector fail-closed", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityIdempotentWriteFailsClosed(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		"source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"})),
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
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
		mutateTool  func(runtimecontracts.ToolSchemaEntry) runtimecontracts.ToolSchemaEntry
		errContains string
	}{
		{
			name: "rate limit",
			mutateTool: func(tool runtimecontracts.ToolSchemaEntry) runtimecontracts.ToolSchemaEntry {
				updated, err := tool.WithRateLimit("1/s", "0s")
				if err != nil {
					panic(err)
				}
				return updated
			},
			errContains: "uses rate_limit",
		},
		{
			name: "read only static credentials",
			mutateTool: func(tool runtimecontracts.ToolSchemaEntry) runtimecontracts.ToolSchemaEntry {
				updated, err := tool.WithStaticCredentials("provider_token")
				if err != nil {
					panic(err)
				}
				return updated
			},
			errContains: "static credential activity HTTP execution is supported only for non_idempotent_write",
		},
		{
			name: "managed credentials",
			mutateTool: func(tool runtimecontracts.ToolSchemaEntry) runtimecontracts.ToolSchemaEntry {
				updated, err := tool.WithEffect(runtimecontracts.ActivityEffectClassNonIdempotentWrite)
				if err != nil {
					panic(err)
				}
				updated, err = updated.WithManagedCredential(runtimecontracts.ManagedCredentialRef{Key: "provider_oauth"})
				if err != nil {
					panic(err)
				}
				return updated
			},
			errContains: "uses managed_credential",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := testRuntimeWorkflowValidationBundle()
			tool := runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassReadOnly))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"url": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}), runtimecontracts.ToolSchemaRequired("url")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test?url={{input.url}}"}))

			tool = tc.mutateTool(tool)
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
			_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), WorkflowContractValidationOptions{
				ExecutionPosture:               executionposture.Live,
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

	_, _, err := ensureWorkflowBootWiring(RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
	}, workflowValidationTestProfile(t), executionposture.Live)
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

func workflowValidationTestProfile(t *testing.T) llmselection.Profile {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	return profile
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
			name: "missing workflow module",
			deps: RuntimeDeps{
				Config:  &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}},
				Options: RuntimeOptions{BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA)},
			},
			errContains: "workflow contract validation failed: workflow module is required",
		},
		{
			name: "retired llm runtime mode",
			deps: RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live},
					LLM:     config.LLMConfig{RuntimeMode: "cli_test"},
				},
				Options: RuntimeOptions{
					WorkflowModule:   validModule,
					BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA),
				},
			},
			errContains: "llm.runtime_mode is retired",
		},
		{
			name: "valid dependency graph",
			deps: RuntimeDeps{
				Config: &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}},
				Options: RuntimeOptions{
					WorkflowModule:   validModule,
					BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA),
				},
			},
		},
		{
			name: "inbound store without admitted provider registry",
			deps: RuntimeDeps{
				Config:       &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}},
				InboundStore: &recordingInboundStore{},
				Options: RuntimeOptions{
					WorkflowModule:   validModule,
					BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA),
				},
			},
			errContains: "provider trigger catalog snapshot is required when inbound store is configured",
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
		Config: &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live}},
		Options: RuntimeOptions{
			WorkflowModule:   module,
			BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA),
		},
	}).validated()
	if err != nil {
		t.Fatalf("RuntimeDeps.validated: %v", err)
	}
	if boot.Source == nil {
		t.Fatal("validated RuntimeDeps Source = nil")
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
	if boot.BundleSourceFact.BundleHash() != runtimeContextTestHashA {
		t.Fatalf("BundleSourceFact bundle_hash = %q, want %q", boot.BundleSourceFact.BundleHash(), runtimeContextTestHashA)
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
	source := semanticviewtest.WrapRootAgents(bundle)

	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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
	source := semanticviewtest.WrapRootAgents(bundle)

	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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
	source := semanticviewtest.WrapRootAgents(bundle)

	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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
		"legacy_call": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
	}
	source := semanticview.Wrap(bundle)

	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil || !strings.Contains(err.Error(), "tool implementation warnings") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want tool implementation warning failure", err)
	}
}

func TestValidateWorkflowContractSurface_RejectsCreateEntityWithAccumulate(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")

	source := semanticview.Wrap(loadRuntimeWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-create-entity-plus-accumulate")))

	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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

func loadHarnessInjectionValidationSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadWorkflowValidationSourceAt(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load workflow validation source: %v", err)
	}
	return semanticview.Wrap(bundle)
}
