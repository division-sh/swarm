package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type firstTurnWorkflowToolExec struct {
	readPayload any
	calls       []string
}

func (e *firstTurnWorkflowToolExec) Execute(ctx context.Context, name string, input any) (any, error) {
	e.calls = append(e.calls, strings.TrimSpace(name))
	switch strings.TrimSpace(name) {
	case "read_file":
		return e.readPayload, nil
	case "emit_category_assessed":
		if recorder, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok {
			recorder.Append(eventtest.RunCreatingRootIngress("", events.EventType("discovery/category.assessed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
		}
		return map[string]any{"emitted": true, "input": input}, nil
	default:
		return map[string]any{"name": name, "input": input}, nil
	}
}

func (e *firstTurnWorkflowToolExec) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		kind := toolcapabilities.KindStandard
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Kind:     kind,
			Visible:  true,
			Callable: true,
		})
	}
	return toolcapabilities.NewSet(caps)
}

func TestConversationStep_ClaudeCLIFirstTurnPreservesSupportedReadFileSurface(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")

	tempDir := t.TempDir()
	captureDir := filepath.Join(tempDir, "captures")
	if err := os.MkdirAll(captureDir, 0o755); err != nil {
		t.Fatalf("mkdir capture dir: %v", err)
	}
	scriptPath := filepath.Join(tempDir, "fake-docker.sh")
	script := "#!/bin/sh\n" +
		"SWARM_LLM_FIRST_TURN_FAKE_DOCKER=1 exec " + shellQuote(os.Args[0]) + " -test.run=TestClaudeCLIFirstTurnFakeDockerHelper -- \"$@\"\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker script: %v", err)
	}
	t.Setenv("FAKE_DOCKER_CAPTURE_DIR", captureDir)

	cfg := &config.Config{}
	cfg.Workspace.DockerBin = scriptPath
	cfg.LLM.ClaudeCLI.OutputFormat = "stream-json"
	cfg.LLM.ClaudeCLI.Command = "claude"

	var allowedTools []string
	var listedSurface managedcapabilities.Surface
	effects := effecttest.New()
	effects.Token.AgentID = "market-research-agent"
	runtime := NewClaudeCLIRuntimeWithOptions(
		cfg,
		sessions.NewInMemoryRegistry(0),
		"worker-1",

		workspaceResolverStub{target: &workspace.Target{Container: "swarm-agent-market-research", Workdir: "/workspace"}},
		nil,
		nil,
		ClaudeCLIRuntimeOptions{
			CompletionController: runtimeeffects.NewCompletionController(effects, effects),
			ToolGateway:          testToolGatewayBinding("http://127.0.0.1:8081", "http://host.docker.internal:8081", "gateway-token"),
			ProviderCredentials:  testProviderCredentialResolver(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token"),
			MCPTurnContextStore: mcpTurnContextStoreStub{
				registerSurface: func(registerCtx context.Context, _ time.Duration, surface managedcapabilities.Surface) string {
					authority, ok := runtimeeffects.AuthorityFromContext(registerCtx)
					if !ok || !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(authority.Target, surface) {
						t.Fatalf("Claude CLI registered MCP context without exact provider-turn target: authority=%#v surface=%#v", authority, surface)
					}
					var evidence []managedcapabilities.DeliveryEvidence
					var currentAllowed []string
					for _, tool := range surface.Tools {
						for _, binding := range tool.Bindings {
							if binding.Kind != managedcapabilities.BindingMCPTool {
								continue
							}
							currentAllowed = append(currentAllowed, tool.Name)
							evidence = append(evidence, managedcapabilities.DeliveryEvidence{
								BindingKind: binding.Kind, ExactName: binding.ExactName,
								Kind: evidenceMCPListed, Status: managedcapabilities.EvidenceConfirmed,
							})
						}
					}
					var err error
					listedSurface, err = surface.Observe(evidence...)
					if err != nil {
						t.Fatalf("observe MCP tools/list evidence: %v", err)
					}
					allowedTools = currentAllowed
					return "ctx-token-368"
				},
				resolve:    func(string) (managedcapabilities.Surface, bool) { return listedSurface, true },
				unregister: func(string) {},
			},
		})

	relayWrites := 0
	runtime.execWorkspaceFn = func(context.Context, *workspace.Target, string, ...string) ([]byte, []byte, int, error) {
		relayWrites++
		return nil, nil, 0, nil
	}

	huge := strings.Repeat("x", maxToolResultBytes+1024)
	exec := &firstTurnWorkflowToolExec{
		readPayload: map[string]any{
			"content":    huge,
			"size_bytes": len(huge),
		},
	}
	conv := NewConversation(
		"market-research-agent",
		"task-1",
		"system prompt",
		[]ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "read_file"},
		},
		testMemory(),
		4,
		runtime,
	)
	conv.SetToolExecutor(exec)

	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx := llmTestWorkContext(t, runtimebus.WithEmittedEventsRecorder(
		withTestMemory(runtimeactors.WithActor(effects.CompletionContext("claude-supported-read-file"), runtimeactors.AgentConfig{
			ID:       "market-research-agent",
			FlowPath: "market/inst-1",
			Memory:   testMemory(),
			NativeTools: runtimeactors.NativeToolConfig{
				FileIO: true,
			},
		}), "market-research-agent", "market/inst-1"),
		recorder,
	))

	resp, err := conv.Step(ctx, "scan the file")
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp == nil {
		t.Fatal("expected final response")
	}
	if relayWrites != 0 {
		t.Fatalf("relay writes = %d, want 0 on supported read_file turn", relayWrites)
	}
	if !slices.Equal(allowedTools, []string{"emit_category_assessed"}) {
		t.Fatalf("allowed tools = %#v", allowedTools)
	}
	settlements := effects.CompletionSettlementsForAdapter("claude_cli")
	if len(settlements) != 2 || settlements[0].AgentTurn == nil || settlements[1].AgentTurn == nil {
		t.Fatalf("completion settlements = %#v, want two atomic agent turns", settlements)
	}
	first := settlements[0].AgentTurn
	var firstSurface managedcapabilities.Surface
	if err := json.Unmarshal(first.CapabilitySurface, &firstSurface); err != nil {
		t.Fatalf("decode first turn capability surface: %v", err)
	}
	firstAvailableTools := firstSurface.EffectiveNames()
	if !slices.Equal(firstAvailableTools, []string{"emit_category_assessed", "read_file", "write_file"}) {
		t.Fatalf("first turn available_tools = %#v", firstAvailableTools)
	}
	if firstMCPToolsListed := firstSurface.BindingNames(managedcapabilities.BindingMCPTool); !slices.Equal(firstMCPToolsListed, []string{"mcp__runtime-tools__emit_category_assessed"}) {
		t.Fatalf("first turn MCP bindings = %#v, want exact authored emit binding", firstMCPToolsListed)
	}
	if !slices.Equal(exec.calls, []string{"read_file"}) {
		t.Fatalf("tool exec calls = %#v", exec.calls)
	}
	if len(recorder.Snapshot()) != 0 {
		t.Fatalf("emitted events = %#v", recorder.Snapshot())
	}

	secondInput, err := capturedToolResultInput(captureDir)
	if err != nil {
		t.Fatalf("read tool-result stdin: %v", err)
	}
	var payload []map[string]any
	if err := json.Unmarshal(secondInput, &payload); err != nil {
		t.Fatalf("unmarshal second stdin: %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("tool payload entries = %d, want 1 (%#v)", len(payload), payload)
	}
	readEntry := payload[0]
	if readEntry["name"] != "read_file" || readEntry["ok"] != true {
		t.Fatalf("read entry = %#v", readEntry)
	}
	readResult, _ := readEntry["result"].(map[string]any)
	if readResult == nil {
		t.Fatalf("read result = %#v", readEntry["result"])
	}
	if _, ok := readResult["follow_up"]; ok {
		t.Fatalf("read result follow_up = %#v, want absent on supported turn", readResult["follow_up"])
	}
	if truncated, _ := readResult["truncated"].(bool); truncated {
		t.Fatalf("read result = %#v, want full inline content", readResult)
	}
	content, _ := readResult["content"].(string)
	if len(content) != len(huge) {
		t.Fatalf("read result content len = %d, want %d", len(content), len(huge))
	}
	firstArgs := mustReadCapturedArgs(t, captureDir, 1)
	secondArgs := mustReadCapturedArgs(t, captureDir, 2)
	firstChild := argValue(firstArgs, "--session-id")
	secondChild := argValue(secondArgs, "--session-id")
	if firstChild == "" || secondChild == "" || firstChild == secondChild {
		t.Fatalf("provider children first=%q second=%q, want distinct attempt identities", firstChild, secondChild)
	}
	if argValue(firstArgs, "--resume") != "" || slices.Contains(firstArgs, "--fork-session") {
		t.Fatalf("first args = %#v, want fresh session", firstArgs)
	}
	if argValue(secondArgs, "--resume") != firstChild || !slices.Contains(secondArgs, "--fork-session") {
		t.Fatalf("second args = %#v, want resume %q with fork", secondArgs, firstChild)
	}
}

func mustReadCapturedArgs(t *testing.T, dir string, invocation int) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, strconv.Itoa(invocation)+".args"))
	if err != nil {
		t.Fatalf("read invocation %d args: %v", invocation, err)
	}
	var args []string
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatalf("decode invocation %d args: %v", invocation, err)
	}
	return args
}

func argValue(args []string, name string) string {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return strings.TrimSpace(args[i+1])
		}
	}
	return ""
}

func TestClaudeCLIFirstTurnFakeDockerHelper(t *testing.T) {
	if os.Getenv("SWARM_LLM_FIRST_TURN_FAKE_DOCKER") != "1" {
		return
	}
	os.Exit(runFirstTurnFakeDockerHelper())
}

func runFirstTurnFakeDockerHelper() int {
	captureDir := strings.TrimSpace(os.Getenv("FAKE_DOCKER_CAPTURE_DIR"))
	if captureDir == "" {
		fmt.Fprintln(os.Stderr, "FAKE_DOCKER_CAPTURE_DIR is required")
		return 2
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		return 2
	}
	if err := os.MkdirAll(captureDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir capture dir: %v\n", err)
		return 2
	}
	countFile := filepath.Join(captureDir, "invocations")
	count := 0
	if raw, err := os.ReadFile(countFile); err == nil {
		count, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
	}
	count++
	if err := os.WriteFile(countFile, []byte(strconv.Itoa(count)), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write invocation count: %v\n", err)
		return 2
	}
	if err := os.WriteFile(filepath.Join(captureDir, strconv.Itoa(count)+".stdin"), input, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write captured stdin: %v\n", err)
		return 2
	}
	argsRaw, _ := json.Marshal(os.Args)
	if err := os.WriteFile(filepath.Join(captureDir, strconv.Itoa(count)+".args"), argsRaw, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write captured args: %v\n", err)
		return 2
	}
	providerSessionID := ""
	for i, arg := range os.Args {
		if arg == "--session-id" && i+1 < len(os.Args) {
			providerSessionID = strings.TrimSpace(os.Args[i+1])
		}
	}
	if providerSessionID == "" {
		fmt.Fprintln(os.Stderr, "--session-id is required")
		return 2
	}
	if isReadFileToolResultPayload(input) {
		fmt.Fprintf(os.Stdout, "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":%q,\"mcp_servers\":[{\"name\":\"runtime-tools\",\"status\":\"connected\"}],\"tools\":[\"mcp__runtime-tools__emit_category_assessed\",\"Read\",\"Write\",\"Edit\"]}\n", providerSessionID)
		fmt.Fprintf(os.Stdout, "{\"type\":\"result\",\"result\":\"done\",\"session_id\":%q}\n", providerSessionID)
		return 0
	}
	fmt.Fprintf(os.Stdout, "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":%q,\"mcp_servers\":[{\"name\":\"runtime-tools\",\"status\":\"connected\"}],\"tools\":[\"mcp__runtime-tools__emit_category_assessed\",\"Read\",\"Write\",\"Edit\"]}\n", providerSessionID)
	fmt.Fprintf(os.Stdout, "{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool-read-1\",\"name\":\"Read\",\"input\":{\"path\":\"/workspace/corpus.json\"}}},\"session_id\":%q}\n", providerSessionID)
	fmt.Fprintf(os.Stdout, "{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_stop\",\"index\":0},\"session_id\":%q}\n", providerSessionID)
	return 0
}

func isReadFileToolResultPayload(input []byte) bool {
	var payload []map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(input), &payload); err != nil {
		return false
	}
	for _, entry := range payload {
		ok, _ := entry["ok"].(bool)
		if ok && strings.TrimSpace(asString(entry["name"])) == "read_file" {
			return true
		}
	}
	return false
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func capturedToolResultInput(captureDir string) ([]byte, error) {
	matches, err := filepath.Glob(filepath.Join(captureDir, "*.stdin"))
	if err != nil {
		return nil, err
	}
	slices.Sort(matches)
	for _, match := range matches {
		data, err := os.ReadFile(match)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(data, []byte(`"name":"read_file"`)) && bytes.Contains(data, []byte(`"ok":true`)) && bytes.Contains(data, []byte(`"size_bytes":`)) {
			return data, nil
		}
	}
	return nil, os.ErrNotExist
}
