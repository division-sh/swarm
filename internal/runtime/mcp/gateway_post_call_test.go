package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

func TestGatewayPostSuccessProjectionFailureIsUncertain(t *testing.T) {
	for _, knownFailure := range []bool{false, true} {
		name := "executed_then_relay_failed"
		if knownFailure {
			name = "known_execution_failure"
		}
		t.Run(name, func(t *testing.T) {
			calls, effects := 0, 0
			executor := &relayAwareToolExecutorStub{
				execFn: func(context.Context, string, any) (any, error) {
					calls++
					if knownFailure {
						return nil, failures.New(failures.ClassAuthorizationDenied, "known_tool_refusal", "test", "execute", map[string]any{"action": "tool_execute"})
					}
					effects++
					return map[string]any{"blob": strings.Repeat("committed-effect", maxToolResultBytes)}, nil
				},
				relayErr: errors.New("relay failed after tool committed"),
			}
			registry := newTestTurnContextRegistry()
			putTestTurnContext(t, registry, "projection-call", TurnContext{
				Actor:             models.AgentConfig{ID: "projection-agent"},
				CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "projection-agent"}, "query_entities", "read_file"),
				CreatedAt:         time.Now(), ExpiresAt: time.Now().Add(time.Minute),
			})
			gateway := NewGateway(executor, testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})
			server := httptest.NewServer(gateway.Handler())
			defer server.Close()
			result, err := (toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{
				"Authorization": "Bearer " + testGatewayToken, "X-SWARM-Context-Token": "projection-call",
			}}).Call(context.Background(), "query_entities", map[string]any{}, "one-call")
			server.Close()
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				IsError      bool                 `json:"isError"`
				RuntimeError *RuntimeErrorPayload `json:"runtimeError"`
			}
			if err := json.Unmarshal(result, &wire); err != nil || !wire.IsError || wire.RuntimeError == nil || wire.RuntimeError.Failure == nil {
				t.Fatalf("gateway lost typed post-call error: %s %v", result, err)
			}
			failure := wire.RuntimeError.Failure
			if calls != 1 {
				t.Fatalf("tool dispatched %d times", calls)
			}
			if knownFailure {
				if failure.Class != failures.ClassAuthorizationDenied || effects != 0 || executor.relayTool != "" {
					t.Fatalf("known pre-effect failure was changed: %+v effects=%d relay=%s", failure, effects, executor.relayTool)
				}
			} else if failure.Class != failures.ClassOutcomeUncertain || failure.Retryable || effects != 1 || executor.relayTool != "query_entities" {
				t.Fatalf("post-success projection became ordinary feedback: %+v effects=%d relay=%s", failure, effects, executor.relayTool)
			}
		})
	}
}

type projectionForkRuntime struct {
	ctx context.Context
}

func (r *projectionForkRuntime) StartSession(_ context.Context, agentID, _ string, tools []llm.ToolDefinition) (*llm.Session, error) {
	return &llm.Session{ID: "projection-session", AgentID: agentID, Tools: tools}, nil
}

func (r *projectionForkRuntime) ContinueForkChatSession(ctx context.Context, _ *llm.Session, _ llm.ForkChatCall) (*llm.Response, error) {
	r.ctx = ctx
	return &llm.Response{}, nil
}

func TestGatewayOversizedForkChatProjectionNeverUsesLiveExecutor(t *testing.T) {
	actor := models.AgentConfig{ID: "fork-projection-agent"}
	forkTurnID := uuid.NewString()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: forkTurnID, ExecutionOwner: "projection-owner",
		LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: runtimeeffects.ExecutionModeLive,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{ForkTurnID: forkTurnID, ForkID: uuid.NewString(),
			SourceRunID: uuid.NewString(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			ActorTokenID: "projection-actor", RequestOccurrenceID: uuid.NewString(), RequestHash: "projection-request"},
	}
	ctx := models.WithActor(runtimeeffects.WithAuthority(context.Background(), authority), actor)
	runtime := &projectionForkRuntime{}
	sandboxCalls, liveCalls := 0, 0
	sandbox := testToolExecutor(func(context.Context, string, any) (any, error) {
		sandboxCalls++
		return map[string]any{"sandbox_blob": strings.Repeat("sandbox-only", maxToolResultBytes)}, nil
	})
	definitions := sandbox.ToolDefinitionsForActor(actor)
	conversation, err := llm.NewForkChatConversation(actor.ID, "", "", definitions, agentmemory.Plan{}, 2, runtime)
	if err != nil {
		t.Fatal(err)
	}
	conversation.SetToolExecutor(sandbox)
	if _, err := conversation.RunForkChat(ctx, "capture exact sandbox dispatch"); err != nil {
		t.Fatal(err)
	}
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterConversationForkSandboxTurnContext(runtime.ctx, time.Minute, []string{"query_entities", "read_file"})
	if token == "" {
		t.Fatal("real conversation failed to register its sandbox dispatch")
	}
	defer registry.UnregisterTurnContext(token)
	live := &relayAwareToolExecutorStub{execFn: func(context.Context, string, any) (any, error) {
		liveCalls++
		return nil, errors.New("live execution must not run")
	}, relayErr: errors.New("live relay must not run")}
	server := httptest.NewServer(NewGateway(live, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext, WithActor: models.WithActor, ActorFromContext: models.ActorFromContext,
	}).Handler())
	defer server.Close()
	result, err := (toolgateway.HTTPObservation{URL: server.URL + "/mcp", Headers: map[string]string{
		"Authorization": "Bearer " + testGatewayToken, "X-SWARM-Context-Token": token,
	}}).Call(ctx, "query_entities", map[string]any{}, "one-sandbox-call")
	server.Close()
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(result, &wire) != nil || wire.IsError || len(wire.Content) != 1 || sandboxCalls != 1 || liveCalls != 0 || live.relayTool != "" {
		t.Fatalf("sandbox projection accessed the live owner: result=%s sandbox=%d live=%d relay=%s", result, sandboxCalls, liveCalls, live.relayTool)
	}
	var value map[string]any
	if json.Unmarshal([]byte(wire.Content[0].Text), &value) != nil || value["truncated"] != true || value["follow_up"] != nil || len(wire.Content[0].Text) > maxToolResultBytes {
		t.Fatalf("no-relay sandbox result is not bounded or invents a file: %s", wire.Content[0].Text)
	}
}
