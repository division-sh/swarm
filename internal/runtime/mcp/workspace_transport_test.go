package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/google/uuid"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		if code, handled := worker.RunArgument(ctx, os.Args[1], os.Stdin, os.Stdout); handled {
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

func TestWorkspaceNativeChildUsesRealMCPTransportAndExactOccurrence(t *testing.T) {
	ctx, surface, _ := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	if token == "" {
		t.Fatal("register exact turn")
	}
	defer registry.UnregisterTurnContext(token)
	var calls atomic.Int32
	gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
		calls.Add(1)
		return map[string]any{"executed": true}, nil
	}), testGatewayToken, managedCLIGatewayHooks(registry))
	server := httptest.NewServer(gateway.Handler())
	defer server.Close()
	target := &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}
	observation := toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{"Authorization": "Bearer " + testGatewayToken, "X-SWARM-Context-Token": token}}
	probe, err := workspace.RunWorker(ctx, target, "", worker.Request{Mode: "probe", Gateway: observation})
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.Definitions) == 0 || calls.Load() != 0 {
		t.Fatalf("probe definitions=%v calls=%d", probe.Definitions, calls.Load())
	}
	request := worker.Request{Mode: "call", Gateway: observation, Tool: "write_file", Occurrence: "same-workspace-occurrence", Arguments: json.RawMessage(`{"path":"/workspace/result.txt","content":"once"}`)}
	result, err := workspace.RunWorker(ctx, target, "", request)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result.ToolResult, &wire); err != nil || wire.IsError || calls.Load() != 1 {
		t.Fatalf("first call=%s calls=%d err=%v", result.ToolResult, calls.Load(), err)
	}
	result, err = workspace.RunWorker(ctx, target, "", request)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(result.ToolResult, &wire); err != nil || !wire.IsError || calls.Load() != 1 {
		t.Fatalf("replay=%s calls=%d err=%v", result.ToolResult, calls.Load(), err)
	}
}

func TestWorkspaceNativeChildRefusesBootAndContextAuthBeforeExecution(t *testing.T) {
	ctx, surface, _ := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	defer registry.UnregisterTurnContext(token)
	var calls atomic.Int32
	gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) { calls.Add(1); return nil, nil }), testGatewayToken, managedCLIGatewayHooks(registry))
	server := httptest.NewServer(gateway.Handler())
	defer server.Close()
	for _, tc := range []struct{ name, auth, token, code string }{
		{"boot", "Bearer foreign", token, "workspace_gateway_unreachable"},
		{"context", "Bearer " + testGatewayToken, "foreign", "workspace_gateway_unreachable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := workspace.RunWorker(ctx, &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}, "", worker.Request{Mode: "probe", Gateway: toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{"Authorization": tc.auth, "X-SWARM-Context-Token": tc.token}}})
			if err == nil || failures.Normalize(err, "test", "probe").Detail.Code != tc.code || calls.Load() != 0 {
				t.Fatalf("error=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestWorkspaceNativeChildRechecksGatewayAfterSuccessfulObservation(t *testing.T) {
	ctx, surface, _ := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	if token == "" {
		t.Fatal("register exact turn")
	}
	defer registry.UnregisterTurnContext(token)
	var calls atomic.Int32
	gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
		calls.Add(1)
		return nil, nil
	}), testGatewayToken, managedCLIGatewayHooks(registry))
	server := httptest.NewServer(gateway.Handler())
	defer server.Close()
	target := &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}
	request := worker.Request{Mode: "probe", Gateway: toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{
		"Authorization": "Bearer " + testGatewayToken, "X-SWARM-Context-Token": token,
	}}}
	first, err := workspace.RunWorker(ctx, target, "", request)
	if err != nil || len(first.Definitions) == 0 || calls.Load() != 0 {
		t.Fatalf("initial observation = %+v, calls=%d err=%v", first, calls.Load(), err)
	}
	server.Close()
	_, err = workspace.RunWorker(ctx, target, "", request)
	var execution *workspace.WorkerExecutionError
	if !errors.As(err, &execution) || !execution.Observed || execution.ModelStarted || failures.Normalize(err, "test", "probe").Detail.Code != "workspace_gateway_unreachable" || calls.Load() != 0 {
		t.Fatalf("stale observation admitted work: calls=%d err=%v", calls.Load(), err)
	}
}

type preparedContextWorkspace struct{ target *workspace.Target }

func (w preparedContextWorkspace) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func (w preparedContextWorkspace) ResolveWorkspaceForCapabilityAdmission(context.Context, models.AgentConfig) (*workspace.Target, error) {
	return w.target, nil
}

func TestMockPreparedContextsUseOwnNativeGateway(t *testing.T) {
	// These are two internal prepared-provider compositions, not a new public
	// multi-context serve topology or workspace-owner isolation proof.
	type prepared struct {
		ctx      context.Context
		actor    models.AgentConfig
		surface  managedcapabilities.Surface
		registry *TurnContextRegistry
		server   *httptest.Server
		factory  llm.RuntimeFactory
	}
	profile, err := selection.ResolveLiveBackend(selection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	tool := llmToolDefinitionForMCP(mcpToolDefinition("write_file", llm.ToolDefinition{Name: "write_file"}))
	var businessCalls atomic.Int32
	contexts := make([]prepared, 2)
	for i := range contexts {
		identity := agentidentitytest.RootRuntime(t, "same-mock", fmt.Sprintf("owned-context-%d", i))
		plan, err := identity.Plan()
		if err != nil {
			t.Fatal(err)
		}
		source := []byte("def handle(input):\n    return {'text': 'not run during preparation'}\n")
		actor := models.AgentConfig{
			ID: plan.AgentID(), FlowPath: plan.FlowInstance(), LLMBackend: profile.ID,
			ResolvedLLMBackend: selection.BackendMock, ResolvedLLMProvider: selection.ProviderMock,
			ResolvedLLMTransport: selection.TransportMock, ExecutionMode: "mock",
			Mock: mockperformance.Performance{Kind: mockperformance.KindPython, Module: "mocks/model.py", Source: source, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(source))},
		}
		surface, err := managedcapabilities.New(managedcapabilities.Plan{
			ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "mock", Transport: "cli", ProviderContract: "mock-context-proof",
			Authority: managedcapabilities.Authority{
				Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
				ExecutionKind:        managedcapabilities.ExecutionNormalAgent,
				ExecutionAuthorityID: fmt.Sprintf("context-%d-startup", i), StartupOwnerID: fmt.Sprintf("context-%d-owner", i), StartupGeneration: 1,
			},
			Tools: []managedcapabilities.PlannedTool{{
				Name: tool.Name, DefinitionHash: llm.ToolDefinitionIdentity(tool),
				Capability: toolcapabilities.Capability{Name: tool.Name, Visible: true, Callable: true},
				Bindings: []managedcapabilities.DeliveryBinding{
					{Kind: managedcapabilities.BindingMCPTool, ExactName: "mcp__runtime-tools__write_file", RequiredEvidenceKind: "mcp_listed"},
					{Kind: managedcapabilities.BindingMCPProvider, ExactName: "mcp__runtime-tools__write_file", RequiredEvidenceKind: "mcp_visible"},
				},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		registry := NewTurnContextRegistry(models.ActorFromContext)
		bootToken := fmt.Sprintf("context-%d-boot", i)
		gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
			businessCalls.Add(1)
			return nil, errors.New("business call forbidden during preparation")
		}), bootToken, managedCLIGatewayHooks(registry))
		server := httptest.NewServer(gateway.Handler())
		t.Cleanup(server.Close)
		binding, err := toolgateway.NewRuntimeOwnedBinding(toolgateway.TransportHTTP, server.URL, server.URL, bootToken, toolgateway.LifecycleOwnerServeBoot, toolgateway.SourceBoundMCPListener)
		if err != nil {
			t.Fatal(err)
		}
		contexts[i] = prepared{
			ctx:   managedcapabilities.WithContext(models.WithActor(context.Background(), actor), surface),
			actor: actor, surface: surface, registry: registry, server: server,
			factory: llm.RuntimeFactory{
				Cfg: &config.Config{LLM: config.LLMConfig{Backend: profile.ID}}, MCPTurns: registry, ToolGateway: binding,
				Workspaces: preparedContextWorkspace{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}},
			},
		}
	}
	probe := func(p prepared, factory llm.RuntimeFactory) error {
		set, err := llm.NewPreparedAgentRuntimeSet(profile, factory)
		if err != nil {
			return err
		}
		resolved, err := set.ResolveAgentRuntime(p.actor)
		if err != nil {
			return err
		}
		mock, ok := resolved.Runtime.(*llm.MockRuntime)
		if !ok {
			return fmt.Errorf("prepared model selection changed: %T", resolved.Runtime)
		}
		response, err := mock.ProbeStartupVisibleToolSurface(p.ctx, p.actor, "", []llm.ToolDefinition{tool})
		if err != nil {
			return err
		}
		if response == nil || response.CapabilitySurface == nil || response.CapabilitySurface.CanAdvanceFrom(p.surface) != nil || response.CapabilitySurface.ValidateEffective() != nil {
			return errors.New("prepared surface was missing, foreign or unavailable")
		}
		return nil
	}
	for _, p := range contexts {
		if err := probe(p, p.factory); err != nil {
			t.Fatal(err)
		}
	}
	foreign := contexts[0].factory
	foreign.MCPTurns = contexts[1].registry
	if err := probe(contexts[0], foreign); err == nil || failures.Normalize(err, "test", "probe").Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("foreign context registry borrowed primary transport: %v", err)
	}
	foreign = contexts[1].factory
	foreign.ToolGateway, err = toolgateway.NewRuntimeOwnedBinding(toolgateway.TransportHTTP, contexts[0].server.URL, contexts[0].server.URL, contexts[1].factory.ToolGateway.AuthToken(), toolgateway.LifecycleOwnerServeBoot, toolgateway.SourceBoundMCPListener)
	if err != nil {
		t.Fatal(err)
	}
	if err := probe(contexts[1], foreign); err == nil || failures.Normalize(err, "test", "probe").Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("foreign boot binding borrowed primary transport: %v", err)
	}
	contexts[0].server.Close()
	if err := probe(contexts[0], contexts[0].factory); err == nil || failures.Normalize(err, "test", "probe").Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("closed primary fell back to another gateway: %v", err)
	}
	if err := probe(contexts[1], contexts[1].factory); err != nil {
		t.Fatalf("secondary depended on primary lifetime: %v", err)
	}
	if businessCalls.Load() != 0 {
		t.Fatalf("preparation dispatched %d business calls", businessCalls.Load())
	}
	for i, p := range contexts {
		p.registry.mu.Lock()
		remaining := len(p.registry.data)
		p.registry.mu.Unlock()
		if remaining != 0 {
			t.Errorf("context %d retained %d probe tokens", i, remaining)
		}
	}
}
