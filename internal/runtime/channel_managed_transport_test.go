package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/channelactivation"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Called by the real dual-store channel/private-activity fixture. Only the
// provider protocol is simulated; lifecycle, claim, completion admission,
// registry, HTTP gateway, catalog hashes, executor, journal and connector run.
func proveRegisteredManagedChannelTransport(t *testing.T, ctx context.Context, selected any, executor *tools.Executor, owner *channelactivation.Owner, predecessor packs.OutboundBindingPlan, actor models.AgentConfig, calls func() int32) {
	t.Helper()
	store := selected.(interface {
		storetest.AgentFixtureStore
		storetest.ManagedAgentTurnFixtureStore
	})
	actor.Identity.RunID = correlation.RunIDFromContext(ctx)
	actor.Model = "regular"
	actor.ResolvedLLMBackend = "claude_cli"
	actor.Type = "generic"
	var intentErr error
	actor.Intent, intentErr = agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.channel-sender.intent", "Exercise exact managed channel transport.")
	if intentErr != nil {
		t.Fatal(intentErr)
	}
	source, _ := correlation.SourceArtifactFactFromContext(ctx)
	if err := storetest.UpsertStaticAgentFixtureForSource(t, ctx, store, manager.PersistedAgent{Config: actor, Status: "active"}, source); err != nil {
		t.Fatal(err)
	}
	state, found, err := store.LoadAgentLifecycleState(ctx, actor.Identity)
	if err != nil || !found {
		t.Fatalf("managed lifecycle: found=%v err=%v", found, err)
	}
	for _, transport := range []string{"mcp", "direct"} {
		t.Run("registered_managed_"+transport, func(t *testing.T) {
			inputCtx := configuredChannelCallContext(t, ctx, selected, actor, actor.Identity.RunID, actor.EntityID, actor.CanonicalFlowPath(), "managed-"+transport)
			input, _ := correlation.InboundEventFromContext(inputCtx)
			pinned, definitions, releaseRoot, err := executor.AcquireToolDefinitionsForActorInContext(inputCtx, actor)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseRoot()
			var definition llm.ToolDefinition
			for _, candidate := range definitions {
				if candidate.Name == "channel.ops.deliver" {
					definition = candidate
				}
			}
			if definition.Name == "" {
				t.Fatal("channel catalog missing")
			}
			token := effects.LifecycleToken{RuntimeEpoch: state.RuntimeEpoch, Identity: actor.Identity, AgentID: actor.ID, Generation: state.Generation}
			authority := effects.NormalAgentAuthority(token, "managed-channel-transport", time.Now().Add(time.Minute))
			authority.Target = effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: actor.Identity.RunID, AgentID: actor.ID, AgentIdentity: actor.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: actor.CanonicalFlowPath(), EntityID: actor.EntityID}
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorIdentity: actor.Identity, RuntimeMode: "task", Provider: "claude", Transport: "cli", ProviderContract: "channel-transport-protocol-proof",
				Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID, ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: authority.ID, RunID: authority.Target.RunID, SessionID: authority.Target.SessionID, TurnOrdinal: 1},
				Tools: []managedcapabilities.PlannedTool{{Name: definition.Name, DefinitionHash: llm.ToolDefinitionIdentity(definition), Capability: toolcapabilities.Capability{Name: definition.Name, Visible: true, Callable: true}, Bindings: []managedcapabilities.DeliveryBinding{
					{Kind: managedcapabilities.BindingMCPTool, ExactName: "mcp__runtime-tools__channel.ops.deliver", RequiredEvidenceKind: "mcp_listed"},
					{Kind: managedcapabilities.BindingMCPProvider, ExactName: "mcp__runtime-tools__channel.ops.deliver", RequiredEvidenceKind: "mcp_visible"},
				}}}, CreatedAt: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			surface, err = llm.ObserveCLIResponseCapabilitySurface(surface, &llm.Response{CLIInventory: llm.CLIInventoryValid, MCPServers: map[string]string{"runtime-tools": "connected"}, MCPVisibleTools: []string{"mcp__runtime-tools__channel.ops.deliver"}})
			if err != nil {
				t.Fatal(err)
			}
			intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.channel-sender.intent", "Exercise exact managed channel transport.")
			if err != nil {
				t.Fatal(err)
			}
			prompt, err := agentintent.IntentOnlyPrompt(intent)
			if err != nil {
				t.Fatal(err)
			}
			providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
			if err != nil {
				t.Fatal(err)
			}
			frame, err := agentframe.Complete(agentframe.SessionSeed{AgentIdentity: actor.Identity, Role: actor.Role, Intent: intent, ProviderPrompt: providerPrompt, RuntimeMode: "task", Provider: "claude", Transport: "cli", ModelAlias: "regular", Model: "protocol-proof"}, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: input}, agentframe.Completion{BundleHash: source.BundleHash(), Surface: surface})
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.ID), AgentIdentity: actor.Identity}
			storetest.CommitDeliveryObligationsForPersistedEvent(t, ctx, selected, input, []events.DeliveryRoute{route})
			claimed, err := storetest.ClaimDelivery(ctx, store, input, route)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := managedexecution.New(managedexecution.KindNormalRuntime, authority.ID, authority.FenceGeneration, "", "channel-transport", source.BundleHash(), []string{surface.ID})
			if err != nil {
				t.Fatal(err)
			}
			pinned = effects.WithAuthority(pinned, authority)
			pinned = effects.WithController(pinned, effects.NewCompletionController(store, store, store, nil).WithExecutionPosture(executionposture.Live))
			pinned = effects.WithExecutionMode(pinned, effects.ExecutionModeLive)
			pinned = deliverylifecycle.WithClaim(pinned, claimed.Claim)
			pinned = managedexecution.WithAdmission(pinned, admission)
			pinned = managedcapabilities.WithContext(pinned, surface)
			completion, err := effects.BeginManagedCompletion(pinned, "claude_cli", []byte(`{"protocol":"channel"}`), frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := completion.MarkLaunched(pinned); err != nil {
				t.Fatal(err)
			}

			registry := mcp.NewTurnContextRegistry(models.ActorFromContext)
			contextToken := registry.RegisterTurnContextWithCapabilitySurface(pinned, time.Minute, surface)
			if contextToken == "" {
				t.Fatal("real managed registration refused")
			}
			defer registry.UnregisterTurnContext(contextToken)
			gateway := mcp.NewGateway(executor, "transport-secret", mcp.GatewayHooks{
				ResolveTurnContext: registry.ResolveTurnContext, WithActor: models.WithActor, ActorFromContext: models.ActorFromContext,
				WithInboundEvent:          correlation.WithInboundEvent,
				ObserveCapabilityEvidence: registry.ObserveCapabilityEvidence, ObserveCapabilityMismatch: registry.ObserveCapabilityMismatch, ObserveMCPProviderCall: registry.ObserveMCPProviderCall,
			})
			replaceCtx, cancelReplace := context.WithCancel(context.Background())
			done := make(chan error, 1)
			successor := configuredTelegramChannelBindingWithTextLimit(t, "http://127.0.0.1:1", true)
			publication := testChannelActivationPublication(t, successor)
			go func() { done <- owner.ReplaceContext(replaceCtx, publication) }()
			defer func() {
				cancelReplace()
				if err := <-done; err == nil {
					t.Error("successor published before root completion")
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				probe, open := owner.AcquireRuntimeOperation(predecessor.RuntimeToolID("deliver"))
				if !open {
					break
				}
				probe.Release()
				if time.Now().After(deadline) {
					t.Fatal("replacement did not fence")
				}
				time.Sleep(time.Millisecond)
			}
			request := func(path string, body any) *httptest.ResponseRecorder {
				t.Helper()
				raw, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				requestCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw))).WithContext(requestCtx)
				req.Header.Set("Authorization", "Bearer transport-secret")
				req.Header.Set("X-Swarm-Context-Token", contextToken)
				response := httptest.NewRecorder()
				gateway.Handler().ServeHTTP(response, req)
				return response
			}
			listed := request("/mcp", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": map[string]any{}})
			if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), `"error"`) || !strings.Contains(listed.Body.String(), definition.Name) {
				t.Fatalf("managed list: %s", listed.Body.String())
			}
			before := calls()
			arguments := map[string]any{"presentation": map[string]any{"text": "Exact predecessor"}, "actions": []any{}}
			path := "/tools/channel.ops.deliver"
			body := map[string]any{"input": arguments}
			if transport == "mcp" {
				path = "/mcp"
				body = map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": definition.Name, "arguments": arguments, "_meta": map[string]any{"claudecode/toolUseId": "toolu-channel-proof"}}}
			}
			response := request(path, body)
			if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"error"`) || strings.Contains(response.Body.String(), `"isError":true`) || !strings.Contains(response.Body.String(), "delivery_reference") {
				t.Fatalf("managed %s call: %d %s", transport, response.Code, response.Body.String())
			}
			if calls() != before+1 {
				t.Fatalf("predecessor connector calls=%d, want %d", calls(), before+1)
			}
			originalToken := contextToken
			originalTurn, ok := registry.ResolveTurnContext(originalToken)
			if !ok {
				t.Fatal("registered managed turn disappeared")
			}
			for _, mismatch := range []string{"source", "actor", "run", "input", "expired"} {
				t.Run("refuse_"+mismatch, func(t *testing.T) {
					turn := originalTurn
					turn.Presentation = channelactivation.BindPresentation(pinned, time.Now().Add(time.Minute))
					switch mismatch {
					case "source":
						turn.SourceArtifactFact, err = correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
						if err != nil {
							t.Fatal(err)
						}
					case "actor":
						turn.Actor.Identity.RunID = uuid.NewString()
					case "run":
						turn.RunID = uuid.NewString()
					case "input":
						turn.HasInbound = false
					case "expired":
						turn.Presentation.Close()
						turn.Presentation = channelactivation.BindPresentation(pinned, time.Now().Add(-time.Second))
					}
					contextToken = uuid.NewString()
					registry.PutTurnContextForTest(contextToken, turn)
					defer registry.UnregisterTurnContext(contextToken)
					denied := request(path, body)
					if calls() != before+1 || (!strings.Contains(denied.Body.String(), "error") && !strings.Contains(denied.Body.String(), `"isError":true`) && denied.Code == http.StatusOK) {
						t.Fatalf("%s binding reached effects: %s", mismatch, denied.Body.String())
					}
				})
			}
			contextToken = originalToken
			registry.UnregisterTurnContext(contextToken)
			denied := request(path, body)
			if calls() != before+1 || (!strings.Contains(denied.Body.String(), "error") && denied.Code == http.StatusOK) {
				t.Fatal("revoked token reached connector")
			}
			if err := completion.MarkResponseObserved(pinned, map[string]any{"transport": transport}); err != nil {
				t.Fatal(err)
			}
			if err := completion.Succeed(pinned, map[string]any{"transport": transport}); err != nil {
				t.Fatal(fmt.Errorf("settle managed proof: %w", err))
			}
			if _, err := store.SettleSuccess(ctx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
