package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	actors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	effects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestClaudeContinuationCharacterization(t *testing.T) {
	for _, tc := range []struct {
		name   string
		native bool
		mcp    bool
		mode   string
	}{
		{"native_only", true, false, "success"},
		{"mcp_only", false, true, "success"},
		{"mixed", true, true, "success"},
		{"empty", false, false, "success"},
		{"process_failure", false, true, "failure"},
		{"child_identity", false, true, "wrong_child"},
		{"started_timeout", false, true, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("CLAUDE_CHARACTERIZATION_ROOT", dir)
			t.Setenv("CLAUDE_CHARACTERIZATION_MODE", tc.mode)
			script := filepath.Join(dir, "docker")
			if err := os.WriteFile(script, []byte("#!/bin/sh\nCLAUDE_CHARACTERIZATION_HELPER=1 exec "+shellQuote(os.Args[0])+" -test.run=^TestClaudeContinuationSubprocess$ -- \"$@\"\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{}
			cfg.Workspace.DockerBin = script
			cfg.LLM.ClaudeCLI.Command = "claude"
			cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
			if tc.mode == "timeout" {
				cfg.LLM.ClaudeCLI.Timeout = 500 * time.Millisecond
			}
			base := effecttest.New()
			var registered, unregistered int
			var listed managedcapabilities.Surface
			runtime := NewClaudeCLIRuntimeWithOptions(cfg,
				claudeSettledTestRegistry{Registry: sessions.NewInMemoryRegistry(0), effects: base}, "characterization",
				workspaceResolverStub{target: &workspace.Target{Container: "characterization", Workdir: "/workspace"}}, nil, nil,
				ClaudeCLIRuntimeOptions{
					CompletionController: liveTestCompletionController(base, base, base, base),
					ProviderCredentials:  testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "test-token"),
					ToolGateway:          testToolGatewayBinding("http://127.0.0.1:8081", "http://host.docker.internal:8081", "gateway-token"),
					MCPTurnContextStore: mcpTurnContextStoreStub{
						registerSurface: func(_ context.Context, _ time.Duration, surface managedcapabilities.Surface) string {
							registered++
							var evidence []managedcapabilities.DeliveryEvidence
							for _, tool := range surface.Tools {
								for _, binding := range tool.Bindings {
									if binding.Kind == managedcapabilities.BindingMCPTool {
										evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: evidenceMCPListed, Status: managedcapabilities.EvidenceConfirmed})
									}
								}
							}
							var err error
							listed, err = surface.Observe(evidence...)
							if err != nil {
								t.Fatal(err)
							}
							return "characterization-token"
						},
						resolve:    func(string) (managedcapabilities.Surface, bool) { return listed, true },
						unregister: func(string) { unregistered++ },
					},
				})
			var tools []ToolDefinition
			if tc.native {
				tools = append(tools, ToolDefinition{Name: "read_file"}, ToolDefinition{Name: "write_file"})
			}
			if tc.mcp {
				tools = append(tools, ToolDefinition{Name: "read_flow_data"})
			}
			conv := newTestManagedConversation(t, "characterization-agent", "support/one", "reader", tools, testMemory(), 4, runtime)
			conv.SetToolExecutor(&firstTurnWorkflowToolExec{})
			ctx := testManagedConversationContext(t, base, "characterization-agent", "support/one", "reader")
			actor, _ := actors.ActorFromContext(ctx)
			actor.NativeTools.FileIO = tc.native
			ctx = actors.WithActor(ctx, actor)
			if tc.mode == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
			}
			response, err := conv.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent(actor.ID)})
			settlements := base.CompletionSettlementsForAdapter("claude_cli")
			if tc.mode != "success" {
				if response != nil || err == nil || engine.FailureDispositionFor(err) != engine.FailureDispositionTerminal || len(settlements) != 1 || settlements[0].Settlement.State != effects.StateOutcomeUncertain {
					t.Fatalf("failure response=%+v err=%v settlements=%+v", response, err, settlements)
				}
			} else {
				if err != nil || response == nil || response.Message.Content != "characterized" || conv.Session.TurnCount != 1 || len(settlements) != 1 || settlements[0].Settlement.State != effects.StateSettled {
					t.Fatalf("success response=%+v err=%v session=%+v settlements=%+v", response, err, conv.Session, settlements)
				}
				firstHead := conv.Session.ProviderSessionID
				response, err = conv.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent(actor.ID)})
				if err != nil || response == nil || conv.Session.TurnCount != 2 || conv.Session.ProviderSessionID == firstHead {
					t.Fatalf("resumed response=%+v err=%v session=%+v", response, err, conv.Session)
				}
				first := characterizationArgs(t, dir, 1)
				second := characterizationArgs(t, dir, 2)
				if argValue(first, "--resume") != "" || slices.Contains(first, "--fork-session") || argValue(second, "--resume") != firstHead || !slices.Contains(second, "--fork-session") || argValue(second, "--system-prompt") != "" {
					t.Fatalf("fresh/resumed arguments first=%v second=%v", first, second)
				}
				for _, args := range [][]string{first, second} {
					wantNative := "ExitPlanMode"
					if tc.native {
						wantNative = "Edit,ExitPlanMode,Read,Write"
					}
					if argValue(args, "--tools") != wantNative || slices.Contains(args, "--disallowedTools") || argValue(args, "--model") != "test-model" || (argValue(args, "--mcp-config") != "") != tc.mcp {
						t.Fatalf("selected tool/model arguments=%v", args)
					}
				}
			}
			if registered != unregistered || (registered != 0) != tc.mcp {
				t.Fatalf("MCP lifetime registered=%d unregistered=%d", registered, unregistered)
			}
		})
	}
}

func characterizationArgs(t *testing.T, root string, index int) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("%d.args", index)))
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	return args
}

func TestClaudeContinuationSubprocess(t *testing.T) {
	if os.Getenv("CLAUDE_CHARACTERIZATION_HELPER") != "1" {
		return
	}
	_, _ = io.ReadAll(os.Stdin)
	root := os.Getenv("CLAUDE_CHARACTERIZATION_ROOT")
	files, _ := filepath.Glob(filepath.Join(root, "*.args"))
	raw, _ := json.Marshal(os.Args)
	if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%d.args", len(files)+1)), raw, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if os.Getenv("CLAUDE_CHARACTERIZATION_MODE") == "timeout" {
		time.Sleep(30 * time.Second)
		os.Exit(2)
	}
	var inventory []string
	for _, tool := range strings.Split(argValue(os.Args, "--tools"), ",") {
		if tool != "" {
			inventory = append(inventory, tool)
		}
	}
	if argValue(os.Args, "--mcp-config") != "" {
		inventory = append(inventory, "mcp__runtime-tools__read_flow_data")
	}
	if inventory == nil {
		inventory = []string{}
	}
	head := argValue(os.Args, "--session-id")
	if os.Getenv("CLAUDE_CHARACTERIZATION_MODE") == "wrong_child" {
		head = "foreign-child"
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"type": "system", "subtype": "init", "session_id": head, "tools": inventory, "mcp_servers": []any{map[string]any{"name": "runtime-tools", "status": "connected"}}})
	if os.Getenv("CLAUDE_CHARACTERIZATION_MODE") == "failure" {
		fmt.Fprintln(os.Stderr, "characterized provider failure")
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"type": "result", "result": "characterized", "session_id": head})
	os.Exit(0)
}
