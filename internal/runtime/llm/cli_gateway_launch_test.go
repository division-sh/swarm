package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

// Simulate only Docker process selection. The probe runs the native worker and
// real HTTP client; this remains a provider-framing fixture, not Docker proof.
func runCLIProbeDockerFixture() (int, bool) {
	if i := slices.Index(os.Args, "inspect"); i >= 0 && len(os.Args[i:]) == 4 && os.Args[i+1] == "--format" && os.Args[i+2] == "{{.Id}}" {
		fmt.Fprintln(os.Stdout, strings.Repeat("a", 64))
		return 0, true
	}
	if i := slices.Index(os.Args, "top"); i >= 0 && len(os.Args[i:]) == 4 && os.Args[i+2] == "-eo" && os.Args[i+3] == "pid,args" {
		fmt.Fprintln(os.Stdout, "PID COMMAND\n1 sleep infinity")
		return 0, true
	}
	return worker.RunArgument(context.Background(), os.Args[len(os.Args)-1], os.Stdin, os.Stdout)
}

// This supplies real native-child HTTP observation to provider protocol tests;
// its registry fixture is not durable authority or Docker transport proof.
func nativeCLIProbeFixture(t *testing.T, tools []ToolDefinition) (toolgateway.Binding, mcpTurnContextStoreStub) {
	t.Helper()
	var definitions []map[string]any
	for _, tool := range tools {
		schema := tool.Schema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		definitions = append(definitions, map[string]any{"name": tool.Name, "description": DeliveredToolDescription(tool), "inputSchema": schema})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer native-cli-probe" || r.Header.Get("X-SWARM-Context-Token") != "native-cli-context" {
			http.Error(w, "foreign fixture authority", http.StatusUnauthorized)
			return
		}
		var request struct{ Method string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26"}
		case "tools/list":
			result = map[string]any{"tools": definitions}
		default:
			t.Errorf("provider observation dispatched business method %s", request.Method)
			http.Error(w, "observation only", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	t.Cleanup(server.Close)
	var listed managedcapabilities.Surface
	turns := mcpTurnContextStoreStub{
		registerSurface: func(_ context.Context, _ time.Duration, surface managedcapabilities.Surface) string {
			var evidence []managedcapabilities.DeliveryEvidence
			for _, name := range surface.PlannedBindingNames(managedcapabilities.BindingMCPTool) {
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingMCPTool, ExactName: name, Kind: evidenceMCPListed, Status: managedcapabilities.EvidenceConfirmed})
			}
			var err error
			listed, err = surface.Observe(evidence...)
			if err != nil {
				t.Fatal(err)
			}
			return "native-cli-context"
		},
		resolve: func(string) (managedcapabilities.Surface, bool) { return listed, true },
	}
	return testToolGatewayBinding(server.URL, server.URL, "native-cli-probe"), turns
}

// This tests the real launch owner and native observation child, not paid Claude
// or durable workspace backing. Those receive separate supported-path credit.
func TestClaudeEveryLaunchRechecksGatewayBeforeModelInvocation(t *testing.T) {
	for _, format := range []string{"stream-json", "json"} {
		for _, phase := range []string{"first", "resumed", "tool_result"} {
			t.Run(format+"/"+phase, func(t *testing.T) {
				tool := ToolDefinition{Name: "fork_snapshot_read_entities", Description: "frozen snapshot", Schema: map[string]any{"type": "object"}}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "Bearer launch-proof" || r.Header.Get("X-SWARM-Context-Token") != "launch-context" {
						http.Error(w, "unauthorized", http.StatusUnauthorized)
						return
					}
					var request struct{ Method string }
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						return
					}
					var result any = map[string]any{"protocolVersion": "2025-03-26"}
					if request.Method == "tools/list" {
						result = map[string]any{"tools": []map[string]any{{"name": tool.Name, "description": tool.Description, "inputSchema": tool.Schema}}}
					} else if request.Method != "initialize" {
						t.Errorf("prelaunch observation dispatched %s", request.Method)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
				}))
				defer server.Close()
				cfg := &config.Config{LLM: config.LLMConfig{Models: selection.ModelAliases{selection.ModelAliasRegular: {selection.BackendClaudeCLI: "test-model"}}}}
				cfg.LLM.ClaudeCLI.OutputFormat = format
				root := t.TempDir()
				marker := filepath.Join(root, "model-launched")
				cfg.LLM.ClaudeCLI.Command = filepath.Join(root, "claude-launch-sentinel")
				if err := os.WriteFile(cfg.LLM.ClaudeCLI.Command, []byte("#!/bin/sh\nprintf launched > "+shellQuote(marker)+"\nexit 99\n"), 0o700); err != nil {
					t.Fatal(err)
				}
				harness := effecttest.New()
				target := &workspace.Target{Backend: workspace.BackendHost, Workdir: root}
				registered, released := 0, 0
				runtime := NewClaudeCLIRuntimeWithOptions(cfg, sessions.NewInMemoryRegistry(time.Minute), "launch-owner", workspaceResolverStub{target: target}, nil, nil,
					ClaudeCLIRuntimeOptions{CompletionController: liveTestCompletionController(harness, harness, harness, harness)})
				runtime.toolGateway = testToolGatewayBinding(server.URL, server.URL, "launch-proof")
				runtime.mcpTurns = mcpTurnContextStoreStub{
					register:   func(context.Context, time.Duration, []string) string { registered++; return "launch-context" },
					unregister: func(string) { released++ },
				}
				authority := testConversationForkAuthority()
				identity := testMemoryIdentity("fork-agent", "fork/one")
				identity.RunID = authority.ForkChat.SourceRunID
				actor := actors.AgentConfig{ID: "fork-agent", Identity: identity, ExecutionMode: effects.ExecutionModeLive, Model: selection.ModelAliasRegular}
				ctx := actors.WithActor(effects.WithAuthority(context.Background(), authority), actor)
				ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeLive)
				ctx = effects.WithLogicalOperationIdentity(ctx, authority.ForkChat.RequestOccurrenceID)
				ctx = WithConversationForkSandboxInvocationPolicy(ctx, []string{tool.Name})
				ctx = llmTestWorkContext(t, ctx)
				session := &Session{ID: "9f1fd109-3e42-4b02-8234-0e0e75c7b7bf", AgentID: actor.ID, Tools: []ToolDefinition{tool}}
				message := Message{Role: "user", Content: "inspect snapshot"}
				if phase != "first" {
					session.TurnCount, session.ProviderSessionID = 1, "private-confirmed-head"
				}
				if phase == "tool_result" {
					message = Message{Role: "tool", Content: "frozen snapshot returned"}
				}
				// A genuinely successful earlier target observation cannot be cached
				// as permission for any later provider launch.
				if _, err := runtime.probeWorkspaceMCP(ctx, session, target); err != nil {
					t.Fatal(err)
				}
				server.Close()
				before := *session
				var lease *sessions.Lease
				response, err := runtime.continueClaudeTurn(ctx, session, message, nil, actor, "", resolvedMemoryExecution{}, &lease)
				failure, ok := failures.EnvelopeFromError(err)
				var observed *workspace.WorkerExecutionError
				if response != nil || !ok || failure.Class != failures.ClassDependencyUnavailable || failure.Detail.Code != "workspace_gateway_unreachable" {
					t.Fatalf("pre-model refusal: response=%+v err=%v", response, err)
				}
				if !errors.As(err, &observed) || !observed.Observed || observed.ModelStarted {
					t.Fatalf("lost pre-model dispatch evidence: %v", err)
				}
				if _, err := os.Stat(marker); !os.IsNotExist(err) || len(harness.Attempts) != 0 || registered != 2 || released != 2 {
					t.Fatalf("refusal launched/leaked authority: marker=%v attempts=%d tokens=%d/%d", err, len(harness.Attempts), registered, released)
				}
				before.claudeState = session.claudeState
				if !reflect.DeepEqual(before, *session) {
					t.Fatalf("refusal changed session outcome: before=%+v after=%+v", before, *session)
				}
			})
		}
	}
}
