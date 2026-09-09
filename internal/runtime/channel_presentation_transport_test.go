package runtime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestChannelPresentationExecutorAndRegisteredForkCatalogUnderReplacement(t *testing.T) {
	predecessor := configuredTelegramChannelBindingWithTextLimit(t, "http://127.0.0.1", false)
	successor := configuredTelegramChannelBindingWithTextLimit(t, "http://127.0.0.1", true)
	owner := testChannelActivationOwner(t, predecessor)
	executor := configuredChannelExecutor(semanticview.Wrap(configuredChannelAgentBundle(t)), owner, nil, unusedChannelRuntimeActivityExecutor{})
	actor := models.AgentConfig{ID: "channel-sender", Role: "worker", FlowID: "global", Tools: []string{"channel.ops.deliver"}, ExecutionMode: "live"}
	ctx := models.WithActor(context.Background(), actor)
	pinned, definitions, release, err := executor.AcquireToolDefinitionsForActorInContext(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	maximum := channelToolTextMaximum(t, definitions, "channel.ops.deliver")
	registry := runtimemcp.NewTurnContextRegistry(models.ActorFromContext)
	forkTurn := uuid.NewString()
	pinned = runtimeeffects.WithAuthority(pinned, runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: forkTurn,
		ExecutionOwner: "catalog-test", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID: forkTurn, ForkID: uuid.NewString(), SourceRunID: uuid.NewString(),
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), ActorTokenID: "catalog-test",
			RequestOccurrenceID: uuid.NewString(), RequestHash: "catalog-test",
		},
	})
	policy := runfork.CanonicalConversationForkSandboxPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	token := registry.RegisterConversationForkSandboxTurnContext(pinned, time.Minute, policy.AvailableToolNames())
	if token == "" {
		t.Fatal("real fork registry refused valid turn")
	}
	done := make(chan error, 1)
	completionObserved := false
	publication := testChannelActivationPublication(t, successor)
	go func() { done <- owner.Replace(publication) }()
	defer func() {
		registry.UnregisterTurnContext(token)
		release()
		if !completionObserved {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		probe, admitted := owner.AcquireRuntimeOperation(predecessor.RuntimeToolID("deliver"))
		if !admitted {
			break
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("replacement never fenced")
		}
		time.Sleep(time.Millisecond)
	}
	childCtx, cancel := context.WithTimeout(pinned, time.Second)
	defer cancel()
	_, nestedDefs, nestedRelease, err := executor.AcquireToolDefinitionsForActorInContext(childCtx, actor)
	if err != nil {
		t.Fatal(err)
	}
	if channelToolTextMaximum(t, nestedDefs, "channel.ops.deliver") != maximum {
		t.Fatal("nested catalog changed publication")
	}
	nestedRelease()
	gateway := runtimemcp.NewGateway(executor, "offline-token", runtimemcp.GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext, WithActor: models.WithActor, ActorFromContext: models.ActorFromContext,
	})
	// A fresh HTTP context deliberately has none of the parent's Go context values.
	requestCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)).WithContext(requestCtx)
	req.Header.Set("X-Swarm-Context-Token", token)
	req.Header.Set("Authorization", "Bearer offline-token")
	response := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"error"`) || !strings.Contains(response.Body.String(), `"tools":[]`) || strings.Contains(response.Body.String(), "channel.ops.deliver") {
		t.Fatalf("registered fork catalog lost parent: %d %s", response.Code, response.Body.String())
	}
	select {
	case err := <-done:
		completionObserved = true
		t.Fatalf("replacement overtook turn: %v", err)
	default:
	}
	registry.UnregisterTurnContext(token)
	response = httptest.NewRecorder()
	gateway.Handler().ServeHTTP(response, req)
	if !strings.Contains(response.Body.String(), `"error"`) {
		t.Fatal("revoked token acquired a successor")
	}
}
