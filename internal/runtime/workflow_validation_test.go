package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
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
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness at worker.work.requested") {
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
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only output sink: harness at worker.work.completed") {
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
					Event: "root.completed",
					Sink:  runtimecontracts.FlowOutputSinkHarness,
				}},
			},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile root harness output semantics: %v", err)
	}
	source := semanticviewtest.WrapRootAgents(bundle)
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only output sink: harness at root.completed") {
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
			Event: "root.completed", Sink: runtimecontracts.FlowOutputSink(255),
		}}}},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), "invalid sink") {
		t.Fatalf("CompileWorkflowSemantics error = %v, want invalid sink rejection", err)
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

func testRuntimeWorkflowValidationBundle(localEvents ...string) *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"test.input": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"}},
	}
	for _, eventName := range localEvents {
		bundle.Events[eventName] = runtimecontracts.EventCatalogEntry{}
	}
	return bundle
}

func compiledRuntimeValidationSource(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	t.Helper()
	source := semanticviewtest.WrapRootAgents(bundle)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile validation fixture: %v", err)
	}
	return source
}

func testRuntimeWorkflowValidationAgent(id string) runtimecontracts.AgentRegistryEntry {
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(id)+".intent",
		"Perform the workflow validation test operation.",
	)
	if err != nil {
		panic(err)
	}
	return runtimecontracts.AgentRegistryEntry{
		ID: id, Role: "test", Model: "regular", ResolvedIntent: intent,
		Subscriptions: []string{"test.input"},
	}
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
				source := semanticviewtest.WrapRootAgents(bundle)

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
			name: "unfulfillable authored tool rejection",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": func() runtimecontracts.AgentRegistryEntry {
						entry := testRuntimeWorkflowValidationAgent("agent-1")
						entry.Tools = []string{"missing_tool"}
						return entry
					}(),
				}
				return semanticviewtest.WrapRootAgents(bundle)
			}(),
			errContains: "agent agent-1 references unfulfillable tool missing_tool",
			wantErr:     true,
		},
		{
			name: "missing emitted event schema warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": func() runtimecontracts.AgentRegistryEntry {
						entry := testRuntimeWorkflowValidationAgent("agent-1")
						entry.EmitEvents = []string{"missing.event"}
						return entry
					}(),
				}
				return semanticviewtest.WrapRootAgents(bundle)
			}(),
			errContains: "agent agent-1 emit missing.event has no exact schema in .",
			wantErr:     true,
		},
		{
			name: "tool implementation warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
					"legacy_call": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
				}
				return semanticviewtest.WrapRootAgents(bundle)
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
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
	declareRuntimeValidationSourceRequestedEvent(bundle)
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
	declareRuntimeValidationSourceRequestedEvent(bundle)
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
			bundle := testRuntimeWorkflowValidationBundle("support.reply_drafted")
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
				"support": {EventHandlers: handlers},
			}
			_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("inbound.telegram")
	declareRuntimeValidationTelegramEvent(bundle)
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
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("inbound.telegram")
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("inbound.telegram")
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
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
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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
	root := canonicalrouting.CopyPublicationActivity(t, "root", "http://127.0.0.1:1/send", false)
	path := filepath.Join(root, "events.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, []byte("\nsend.succeeded: {activity_id: string}\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	_, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "compiled event .:send.succeeded has multiple declaration owners") {
		t.Fatalf("admission error = %v, want authored/generated collision before runtime boot", err)
	}
}

func TestValidateWorkflowContractSurface_DurableActivityResultEventsRejectGeneratedCollision(t *testing.T) {
	root := canonicalrouting.CopyPublicationActivity(t, "root", "http://127.0.0.1:1/send", false)
	path := filepath.Join(root, "nodes.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := "\nother:\n  execution_type: system_node\n  subscribes_to: [activity.requested]\n  event_handlers:\n    activity.requested:\n      activity:\n        id: send\n        tool: send\n        input: {message: {ref: payload.message}}\n"
	if err := os.WriteFile(path, append(raw, []byte(duplicate)...), 0600); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	_, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "compiled event .:send.failed has multiple declaration owners") {
		t.Fatalf("admission error = %v, want two generated owners rejected before runtime boot", err)
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
			bundle := testRuntimeWorkflowValidationBundle("source.requested")
			declareRuntimeValidationSourceRequestedEvent(bundle)
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
			_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
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

func declareRuntimeValidationSourceRequestedEvent(bundle *runtimecontracts.WorkflowContractBundle) {
	if bundle.Events == nil {
		bundle.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	bundle.Events["source.requested"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{"url": {Type: "text"}},
		Required:   []string{"url"},
	}}
}

func declareRuntimeValidationTelegramEvent(bundle *runtimecontracts.WorkflowContractBundle) {
	if bundle.Events == nil {
		bundle.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	if bundle.RootTypes.Types == nil {
		bundle.RootTypes.Types = map[string]runtimecontracts.NamedTypeDecl{}
	}
	bundle.RootTypes.Types["RuntimeValidationTelegramPayload"] = runtimecontracts.NamedTypeDecl{Fields: map[string]runtimecontracts.TypeFieldSpec{
		"message": {Type: "RuntimeValidationTelegramMessage"},
	}}
	bundle.RootTypes.Types["RuntimeValidationTelegramMessage"] = runtimecontracts.NamedTypeDecl{Fields: map[string]runtimecontracts.TypeFieldSpec{
		"chat": {Type: "RuntimeValidationTelegramChat"},
	}}
	bundle.RootTypes.Types["RuntimeValidationTelegramChat"] = runtimecontracts.NamedTypeDecl{Fields: map[string]runtimecontracts.TypeFieldSpec{
		"id": {Type: "text"},
	}}
	bundle.Events["inbound.telegram"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{"payload": {Type: "RuntimeValidationTelegramPayload"}},
		Required:   []string{"payload"},
	}}
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
			deps:        RuntimeDeps{Options: RuntimeOptions{ExecutionPosture: executionposture.Live, WorkflowModule: validModule}},
			errContains: "runtime config is required",
		},
		{
			name: "missing workflow module",
			deps: RuntimeDeps{
				Config:  &config.Config{Runtime: config.RuntimeConfig{}},
				Options: RuntimeOptions{ExecutionPosture: executionposture.Live, SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)},
			},
			errContains: "workflow contract validation failed: workflow module is required",
		},
		{
			name: "retired llm runtime mode",
			deps: RuntimeDeps{
				Config: &config.Config{
					Runtime: config.RuntimeConfig{},
					LLM:     config.LLMConfig{RuntimeMode: "cli_test"},
				},
				Options: RuntimeOptions{ExecutionPosture: executionposture.Live,
					WorkflowModule:     validModule,
					SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA),
				},
			},
			errContains: "llm.runtime_mode is retired",
		},
		{
			name: "valid dependency graph",
			deps: RuntimeDeps{
				Config: &config.Config{Runtime: config.RuntimeConfig{}},
				Options: RuntimeOptions{ExecutionPosture: executionposture.Live,
					WorkflowModule:     validModule,
					SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA),
				},
			},
		},
		{
			name: "inbound store without admitted provider registry",
			deps: RuntimeDeps{
				Config:       &config.Config{Runtime: config.RuntimeConfig{}},
				InboundStore: &recordingInboundStore{},
				Options: RuntimeOptions{ExecutionPosture: executionposture.Live,
					WorkflowModule:     validModule,
					SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA),
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
		Config: &config.Config{Runtime: config.RuntimeConfig{}},
		Options: RuntimeOptions{ExecutionPosture: executionposture.Live,
			WorkflowModule:     module,
			SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA),
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
	if boot.SourceArtifactFact.BundleHash() != runtimeContextTestHashA {
		t.Fatalf("SourceArtifactFact bundle_hash = %q, want %q", boot.SourceArtifactFact.BundleHash(), runtimeContextTestHashA)
	}
}

func TestValidateWorkflowContractSurface_AllowsExplicitEventSchemas(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-1")
			entry.Subscriptions = []string{"ready.event"}
			entry.EmitEvents = []string{"ready.event"}
			return entry
		}(),
		"agent-2": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-2")
			entry.Subscriptions = []string{"ready.event"}
			return entry
		}(),
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
	source := compiledRuntimeValidationSource(t, bundle)

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

func TestWorkflowContractAdmissionRejectsInvalidGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-1")
			entry.Role = "agent"
			entry.Subscriptions = []string{"ready.event"}
			entry.EmitEvents = []string{"ready.event"}
			return entry
		}(),
		"agent-2": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-2")
			entry.Role = "consumer"
			entry.Subscriptions = []string{"ready.event"}
			return entry
		}(),
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
	semanticviewtest.WrapRootAgents(bundle)
	err := runtimecontracts.CompileWorkflowSemantics(bundle)
	if err == nil || !strings.Contains(err.Error(), `compile event .:ready.event: compiled structural schema: structural schema field unsupported: unsupported structural schema type "NotDeclared"`) {
		t.Fatalf("compiled event admission = %v, want exact invalid-type refusal before boot", err)
	}
	if _, ok, err := bundle.ResolveCompiledFlowEventSchema(".", "ready.event"); err != nil || ok {
		t.Fatalf("failed admission retained an executable schema: present=%v err=%v", ok, err)
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
		"agent-1": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-1")
			entry.Role = "agent"
			entry.Subscriptions = []string{"ready.event"}
			entry.EmitEvents = []string{"ready.event"}
			return entry
		}(),
		"agent-2": func() runtimecontracts.AgentRegistryEntry {
			entry := testRuntimeWorkflowValidationAgent("agent-2")
			entry.Role = "consumer"
			entry.Subscriptions = []string{"ready.event"}
			return entry
		}(),
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
	source := compiledRuntimeValidationSource(t, bundle)

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
	source := semanticviewtest.WrapRootAgents(bundle)

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
	return semanticviewtest.WrapRootAgents(bundle)
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
	return semanticviewtest.WrapRootAgents(bundle)
}
