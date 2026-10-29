package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestNamesOnlyRootInterfaceUsesOrdinaryValidationPolicy(t *testing.T) {
	source := loadWorkflowValidationSourceAt(t, canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress))
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	opts.CheckMCPReachable = false
	result, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts)
	if err != nil || len(result.BootReport.Errors()) != 0 {
		t.Fatalf("ordinary root validation: result=%#v error=%v", result, err)
	}
}

func TestUnconnectedPrivateInputCreatesNoStandingTargetOrRoute(t *testing.T) {
	source := loadWorkflowValidationSourceAt(t, canonicalrouting.ExampleRoot(t, canonicalrouting.ParentConnect))
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fixture bundle missing")
	}
	bundle.RootSchema.Connect = nil
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	opts := DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
	opts.CheckMCPReachable = false
	if _, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, opts); err == nil || !strings.Contains(err.Error(), "input_pin_wiring") {
		t.Fatalf("unconnected private input validation = %v, want missing producer rejection", err)
	}
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
	bundle := &runtimecontracts.WorkflowContractBundle{RootSchema: &runtimecontracts.FlowSchemaDocument{
		Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "test.input"}}}},
	}}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"test.input": {},
	}
	for _, eventName := range localEvents {
		bundle.Events[eventName] = runtimecontracts.EventCatalogEntry{}
	}
	semanticviewtest.WrapRootAgents(bundle)
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
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
			location  string
			configure func(*runtimecontracts.WorkflowContractBundle)
		}{
			{
				name:     "agent_reference",
				location: "flow root agent worker tools",
				configure: func(bundle *runtimecontracts.WorkflowContractBundle) {
					bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
						"worker": {ID: "worker", Tools: []string{name}},
					}
				},
			},
			{
				name:     "http_tool_entry",
				location: "root tool entry",
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
				assertRetiredDynamicAgentToolSurfaceError(t, "verify", name, manifestation.location, err)

				_, _, err = ensureWorkflowBootWiring(RuntimeOptions{
					WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
				}, workflowValidationTestProfile(t), executionposture.Live)
				assertRetiredDynamicAgentToolSurfaceError(t, "boot", name, manifestation.location, err)
			})
		}
	}
}

func assertRetiredDynamicAgentToolSurfaceError(t testing.TB, surface, name, location string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s admitted retired tool %s", surface, name)
	}
	for _, want := range []string{location, `tool "` + name + `" is unsupported`} {
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

func TestValidateWorkflowContractSurface_DurableActivityFailsClosedForMCPTool(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle("source.requested")
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"mcp_source_scrape": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("mcp")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
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

// USER6220888538: GET is not a different approval or retry policy.

func TestRuleActivityValidationRequiresExplicitIDAndPreservesCombinationRefusal(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle("request")
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"send": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://proof.invalid"}),
		),
	}
	source := compiledRuntimeValidationSource(t, bundle)
	node := identitytest.RootNode(t, "worker")
	for _, id := range []string{"", "  "} {
		handler := runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{ID: id, Tool: "send"}}}}
		errs := validateHandlerActivitySurface(source, node, "request", handler)
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), "rules[0].activity.id is required") {
			t.Fatalf("unnamed typed rule activity: %v", errs)
		}
	}
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{ID: "direct", Tool: "send"},
		Rules:    []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{ID: "rule_send", Tool: "send"}}},
	}
	errs := validateHandlerActivitySurface(source, node, "request", handler)
	if len(errs) != 2 || !strings.Contains(errs[0].Error(), "only allowed on handlers without rules") || !strings.Contains(errs[1].Error(), "cannot be combined with rule activities") {
		t.Fatalf("invalid combined ownership lost its specific refusals: %v", errs)
	}
}

func TestActivityDiagnosticsUseRulePresentationAndPreserveRefusal(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle("request")
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"send": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://proof.invalid"}),
		),
	}
	bundle.FlowTree.Root.Children = append(bundle.FlowTree.Root.Children, runtimecontracts.FlowContractView{
		Path: "child", Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
	})
	bundle.FlowTree.ByID["child"] = &bundle.FlowTree.Root.Children[len(bundle.FlowTree.Root.Children)-1]
	source := compiledRuntimeValidationSource(t, bundle)
	for _, node := range []runtimeidentity.ExecutableNode{identitytest.RootNode(t, "worker"), identitytest.FlowNode(t, "child", "worker")} {
		t.Run(node.Key(), func(t *testing.T) {
			row := runtimecontracts.HandlerRuleEntry{
				Condition: "payload.ready &&\npayload.allowed",
				Activity:  runtimecontracts.ActivitySpec{ID: "rule_send", Tool: "send"},
				Emit:      runtimecontracts.EmitSpec{Event: "done"},
			}
			errs := validateHandlerActivitySurface(source, node, "request", runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{row}})
			want := "node " + node.Key() + " handler request.rules[0]: when payload.ready &&\npayload.allowed activity: activity and authored emit/fan_out emit are mutually exclusive in Stage 1; use generated activity result events"
			if len(errs) != 1 || errs[0].Error() != want {
				t.Fatalf("rule diagnostic/refusal = %v, want %q", errs, want)
			}
			row.Emit = runtimecontracts.EmitSpec{}
			errs = validateHandlerActivitySurface(source, node, "request", runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{row}})
			want = "node " + node.Key() + " handler request.on_complete[0]: condition payload.ready &&\npayload.allowed.activity: durable activity is not supported in on_complete in Stage 1"
			if len(errs) != 1 || errs[0].Error() != want {
				t.Fatalf("completion diagnostic/refusal = %v, want %q", errs, want)
			}
		})
	}
}

func TestValidateWorkflowContractSurface_SlackManagedCredentialProviderConnectorRequiresResponseSuccess(t *testing.T) {
	bundle := testRuntimeWorkflowValidationBundle("inbound.telegram")
	bundle.Tools = map[string]runtimecontracts.ToolSchemaEntry{
		"slack.post_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolDescription("post Slack messages"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
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
		"telegram.send_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage"})),
	}
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), compiledRuntimeValidationSource(t, bundle), WorkflowContractValidationOptions{
		ExecutionPosture:               executionposture.Live,
		CheckMCPReachable:              false,
		StrictEmitSchemas:              false,
		FatalToolImplementationWarning: false,
		FatalBootWarnings:              false,
	})
	for _, want := range []string{
		"provider connector mock response compilation failed",
		"must declare exactly one credential binding mode: static credentials or managed_credential",
		"must declare exactly one response_success policy",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("ValidateWorkflowContractSurface error = %v, want provider connector refusal %q", err, want)
		}
	}
}

func TestValidateWorkflowContractSurface_DurableActivityIdempotentWriteFailsClosed(t *testing.T) {
	// USER6220888538 retires the declaration, not the no-redispatch guarantee.
	// Refuse it at shared source admission, before late runtime validation.
	const teaching = "effect_class is not a tool key; the runtime attempts each HTTP call once, and an interrupted call is outcome-uncertain"
	for _, value := range []string{"idempotent_write", "read_only", "non_idempotent_write", "long_running"} {
		t.Run(value, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, "root", "http://127.0.0.1:1/send", false)
			path := filepath.Join(root, "tools.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(raw), "send:\n") {
				t.Fatalf("activity fixture tool declaration changed: %s", raw)
			}
			body := strings.Replace(string(raw), "send:\n", "send:\n  effect_class: "+value+"\n", 1)
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			_, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), teaching) || !strings.Contains(err.Error(), "tools.yaml:2:") || !strings.Contains(err.Error(), `["send"]["effect_class"]`) {
				t.Fatalf("retired activity selector did not refuse through shared source admission: %v", err)
			}
		})
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
	duplicate := "\nother:\n  execution_type: system_node\n  subscribes_to: [activity.requested]\n  event_handlers:\n    activity.requested:\n      activity:\n        id: send\n        tool: send\n        input: {message: payload.message}\n"
	if err := os.WriteFile(path, append(raw, []byte(duplicate)...), 0600); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	_, err = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), `duplicate activity id "send"`) ||
		!strings.Contains(err.Error(), `handlers "producer/activity.requested" and "other/activity.requested"`) {
		t.Fatalf("admission error = %v, want two generated owners rejected before runtime boot", err)
	}
}

// USER6220888538 gives key-free GET the same fixed conservative
// policy as POST, including the existing static credential owner.

func workflowValidationTestProfile(t *testing.T) llmselection.Profile {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	return profile
}

func TestWorkflowContractAdmissionRejectsInvalidGeneratedEmitToolSchema(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	bundle := testRuntimeWorkflowValidationBundle("ready.event")
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
	bundle.FlowTree.Root.Events = bundle.Events
	semanticviewtest.WrapRootAgents(bundle)
	err := runtimecontracts.CompileWorkflowSemantics(bundle)
	if err == nil || err.Error() != `compile event .:ready.event: event ready.event field unsupported: unknown catalog type "NotDeclared"` {
		t.Fatalf("compiled event admission = %v, want exact invalid-type refusal before boot", err)
	}
	if _, ok, err := bundle.ResolveCompiledFlowEventSchema(".", "ready.event"); err != nil || ok {
		t.Fatalf("failed admission retained an executable schema: present=%v err=%v", ok, err)
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

	bundle := loadRuntimeWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	node := bundle.Nodes["complete-task"]
	handler := node.EventHandlers["task.requested"]
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{Into: "items"}
	node.EventHandlers["task.requested"] = handler
	bundle.Nodes["complete-task"] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers["complete-task"] == nil {
		bundle.Semantics.NodeHandlers["complete-task"] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers["complete-task"]["task.requested"] = handler
	_, err := ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
	if err == nil || !strings.Contains(err.Error(), "uses retired create_entity; construction belongs to the canonical flow constructor") {
		t.Fatalf("ValidateWorkflowContractSurface error = %v, want retired-constructor refusal", err)
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
