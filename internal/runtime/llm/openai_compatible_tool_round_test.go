package llm

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestOpenAICompatibleToolRoundReplaysAdmittedRecoveryWithoutChangingMemory(t *testing.T) {
	t.Run("selected_invocation", func(t *testing.T) {
		ctx, request, session, call := selectedCompatibleToolRoundFixture(t)
		before := providerSchemaJSON(t, session)
		if err := replayOpenAICompatibleToolRound(ctx, &request, session, &call); err != nil {
			t.Fatal(err)
		}
		if len(request.Messages) != 5 || string(request.Messages[3].ReasoningContent) != `"private reasoning"` ||
			len(request.Messages[3].ToolCalls) != 1 || request.Messages[3].ToolCalls[0].ID != "current-call" || request.Messages[4].ToolCallID != "current-call" {
			t.Fatalf("selected invocation lost its exact provider round: %+v", request.Messages)
		}
		if providerSchemaJSON(t, session) != before || call.toolRound != nil || call.selectedToolRound.completionHandle != nil {
			t.Fatal("selected replay changed memory or acquired normal recovery authority")
		}
	})
	request, session, call := compatibleToolRoundFixture(t)
	before := providerSchemaJSON(t, session)
	if err := replayOpenAICompatibleToolRound(context.Background(), &request, session, &call); err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 5 {
		t.Fatalf("messages=%+v, want both rounds and the current result", request.Messages)
	}
	for _, i := range []int{1, 3} {
		message := request.Messages[i]
		if message.Role != "assistant" || len(message.ToolCalls) != 1 || string(message.ReasoningContent) != `"private reasoning"` {
			t.Fatalf("round %d lost its assistant/tool/reasoning: %+v", i, message)
		}
	}
	if request.Messages[2].ToolCallID != "earlier-call" || request.Messages[4].ToolCallID != "current-call" {
		t.Fatalf("tool results lost their exact round linkage: %+v", request.Messages)
	}
	if providerSchemaJSON(t, session) != before || len(session.Messages[3].ToolCalls) != 0 {
		t.Fatal("request replay changed acquired conversation retention")
	}
	// A new user turn builds from the same acquired role/content snapshot,
	// not from the admitted predecessor request or its private reasoning.
	next, err := (&OpenAICompatibleRuntime{}).buildRequest(session, Message{Role: "user", Content: "next input"}, "model")
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range next.Messages {
		if len(message.ToolCalls) != 0 || len(message.ReasoningContent) != 0 {
			t.Fatal("completed-round tool calls/reasoning changed retained memory")
		}
	}
}

func TestOpenAICompatibleToolRoundRefusesMismatchedPredecessors(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Response, *Session, *managedProviderCall)
	}{
		{"acknowledgement", func(r *Response, _ *Session, _ *managedProviderCall) { r.completionAttempt = nil }},
		{"request_fingerprint", func(r *Response, _ *Session, _ *managedProviderCall) { r.completionDispatch.request = []byte(`{}`) }},
		{"attempt", func(r *Response, _ *Session, _ *managedProviderCall) {
			attempt := *r.completionAttempt
			attempt.AttemptID = uuid.NewString()
			r.completionAttempt = &attempt
		}},
		{"execution", func(r *Response, _ *Session, _ *managedProviderCall) {
			attempt := *r.completionAttempt
			attempt.Authority.SelectedFork.ExecutionID = uuid.NewString()
			r.completionAttempt = &attempt
		}},
		{"fork", func(r *Response, _ *Session, _ *managedProviderCall) {
			changed := *r.CapabilitySurface
			changed.Authority.RunID = uuid.NewString()
			r.CapabilitySurface = &changed
		}},
		{"session", func(_ *Response, s *Session, _ *managedProviderCall) { s.ID = uuid.NewString() }},
		{"parent", func(_ *Response, _ *Session, c *managedProviderCall) { c.frame.Turn.ParentFrameID = "foreign" }},
		{"provider", func(_ *Response, _ *Session, c *managedProviderCall) { c.provider.Provider = "foreign" }},
		{"normal_recovery", func(_ *Response, _ *Session, c *managedProviderCall) {
			c.toolRound = &runtimeeffects.CompletionContinuationSnapshot{}
		}},
	} {
		t.Run("selected/"+test.name, func(t *testing.T) {
			ctx, request, session, call := selectedCompatibleToolRoundFixture(t)
			test.edit(call.selectedToolRound, session, &call)
			before := providerSchemaJSON(t, request)
			if err := replayOpenAICompatibleToolRound(ctx, &request, session, &call); err == nil {
				t.Fatal("mismatched selected predecessor reached request dispatch")
			}
			if providerSchemaJSON(t, request) != before {
				t.Fatal("refused selected replay changed its request")
			}
		})
	}
	for _, test := range []struct {
		name string
		edit func(*openAICompatibleRequest, *Session, *managedProviderCall)
	}{
		{"missing", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) { c.toolRound = nil }},
		{"session", func(_ *openAICompatibleRequest, s *Session, _ *managedProviderCall) { s.ID = uuid.NewString() }},
		{"identity", func(_ *openAICompatibleRequest, s *Session, _ *managedProviderCall) {
			s.MemoryIdentity.RunID = uuid.NewString()
		}},
		{"memory", func(_ *openAICompatibleRequest, s *Session, _ *managedProviderCall) {
			s.Memory.Enabled = !s.Memory.Enabled
		}},
		{"turn", func(_ *openAICompatibleRequest, s *Session, _ *managedProviderCall) { s.TurnCount++ }},
		{"suffix", func(_ *openAICompatibleRequest, s *Session, _ *managedProviderCall) { s.Messages = s.Messages[:1] }},
		{"parent", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) {
			c.frame.Turn.ParentFrameID = "foreign"
		}},
		{"successor", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) {
			c.frame.Turn.ToolResult = json.RawMessage(`[]`)
		}},
		{"new_user_turn", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) {
			c.frame.Turn.Kind = agentframe.TurnInitial
		}},
		{"provider", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) { c.provider.Provider = "foreign" }},
		{"model", func(r *openAICompatibleRequest, _ *Session, _ *managedProviderCall) { r.Model = "foreign" }},
		{"request", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) {
			c.toolRound.Request = []byte(`{"model":"model","messages":[{"role":"user","content":"foreign"}]}`)
		}},
		{"payload", func(_ *openAICompatibleRequest, _ *Session, c *managedProviderCall) {
			c.toolRound.Payload = json.RawMessage(`{}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, session, call := compatibleToolRoundFixture(t)
			test.edit(&request, session, &call)
			before := providerSchemaJSON(t, request)
			if err := replayOpenAICompatibleToolRound(context.Background(), &request, session, &call); err == nil {
				t.Fatal("mismatched predecessor was accepted")
			}
			if providerSchemaJSON(t, request) != before {
				t.Fatal("refused replay changed the request")
			}
		})
	}
}

func selectedCompatibleToolRoundFixture(t *testing.T) (context.Context, openAICompatibleRequest, *Session, managedProviderCall) {
	t.Helper()
	request, session, call := compatibleToolRoundFixture(t)
	prior, err := completionContinuationForPayload(call.toolRound.Payload, "openai_compatible")
	if err != nil {
		t.Fatal(err)
	}
	harness := effecttest.New()
	executionID := uuid.NewString()
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthoritySelectedContractFork, ID: executionID,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{ExecutionID: executionID, ForkRunID: session.MemoryIdentity.RunID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config"},
		Target: runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: session.MemoryIdentity.RunID,
			AgentID: session.AgentID, AgentIdentity: session.MemoryIdentity, SessionID: session.ID, Memory: session.Memory, FlowInstance: session.MemoryIdentity.FlowInstance()},
		CompletionRequest: prior.Usage.Evidence.Request, ExecutionOwner: "selected-round-test", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: runtimeeffects.ExecutionModeLive}
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, executionID, 1, session.MemoryIdentity.RunID, "actors", sourceartifactfixture.BundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: session.MemoryIdentity,
		RuntimeMode: call.provider.RuntimeMode, Provider: call.provider.Provider, Transport: call.provider.Transport, ProviderContract: "selected-round-test",
		Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: managedcapabilities.ExecutionSelectedContractFork, ExecutionAuthorityID: executionID, RunID: session.MemoryIdentity.RunID,
			SessionID: session.ID, TurnOrdinal: session.TurnCount}, CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := managedexecution.WithAdmission(runtimeeffects.WithAuthority(managedEffectHarnessContext(t, harness, "selected-round"), authority), admission)
	ctx = managedcapabilities.WithContext(ctx, surface)
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	ctx = withEffectTestRequest(t, ctx, profile, llmselection.ResolvedModel{ModelAlias: "regular", ConcreteModel: "model"})
	surface, _ = managedcapabilities.FromContext(ctx)
	probe := &acknowledgedForkChatCompletionProbe{&committedCleanupCompletionProbe{Harness: harness}}
	ctx = runtimeeffects.WithController(ctx, liveTestCompletionController(probe, probe, probe, probe))
	frame := managedProviderCallForEffectTest(t, ctx).frame
	handle, err := runtimeeffects.BeginManagedCompletion(ctx, "openai_compatible", call.toolRound.Request, frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	dispatch := newCompletionDispatch(handle, runtimeeffects.StateSettled)
	dispatch.request = append([]byte(nil), call.toolRound.Request...)
	dispatch.requestFingerprint = runtimeeffects.Fingerprint(dispatch.request)
	dispatch.markProviderInvocationStarted()
	projection := prior.Projection
	projection.FrameID = frame.FrameID
	projection.Messages = append([]Message(nil), projection.Messages[:len(projection.Messages)-1]...)
	dispatch.projection = &projection
	response := prior.Response
	attempt := handle.Attempt()
	response.completionAttempt, response.completionDispatch, response.CapabilitySurface = &attempt, dispatch, &surface
	response.ToolOutputAuthority = &ToolOutputAuthority{ProviderOperationID: attempt.OperationID, SettledAt: time.Now()}
	call.toolRound, call.selectedToolRound = nil, &response
	call.provider, call.frame.Turn.ParentFrameID = frame.Session.Provider, frame.FrameID
	return ctx, request, session, call
}

func compatibleToolRoundFixture(t *testing.T) (openAICompatibleRequest, *Session, managedProviderCall) {
	t.Helper()
	harness := effecttest.New()
	ctx := harness.CompletionContext("compatible-round")
	authority, _ := runtimeeffects.AuthorityFromContext(ctx)
	authority.CompletionRequest = runtimeeffects.CompletionRequest{ModelAlias: "regular", RequestedModel: "model", BackendProfile: "openai_compatible", Provider: "openai_compatible", Transport: "api"}
	claim, _ := runtimedelivery.ClaimFromContext(ctx)
	origin, err := runtimeeffects.DeliveryCompletionOrigin(claim)
	if err != nil {
		t.Fatal(err)
	}
	surface, _ := managedcapabilities.FromContext(ctx)
	identity := authority.Target.AgentIdentity.Normalize()
	const parent = "agent-frame:v1:00000000-0000-4000-8000-000000000091"
	toolResult := json.RawMessage(`[{"tool_call_id":"current-call","name":"inspect","ok":true,"result":"ok"}]`)
	successor, err := agentframe.NewToolContinuation(parent, toolResult)
	if err != nil {
		t.Fatal(err)
	}
	toolResult = successor.ToolResult()
	previousAssistant := openAICompatibleMessage{Role: "assistant", ReasoningContent: json.RawMessage(`"private reasoning"`),
		ToolCalls: []openAICompatibleToolCall{{ID: "current-call", Type: "function", Function: openAICompatibleToolCallFunction{Name: "inspect", Arguments: `{}`}}}}
	previousResponse := openAICompatibleResponse{}
	previousResponse.Choices = append(previousResponse.Choices, struct {
		Message openAICompatibleMessage `json:"message"`
	}{Message: previousAssistant})
	rawResponse, err := json.Marshal(previousResponse)
	if err != nil {
		t.Fatal(err)
	}
	response, err := convertOpenAICompatibleResponse(previousResponse)
	if err != nil {
		t.Fatal(err)
	}
	response.Raw = rawResponse
	response.ToolOutputAuthority = &ToolOutputAuthority{ProviderOperationID: uuid.NewString(), SettledAt: time.Unix(10, 0).UTC()}
	earlier := previousAssistant
	earlier.ToolCalls = append([]openAICompatibleToolCall(nil), previousAssistant.ToolCalls...)
	earlier.ToolCalls[0].ID = "earlier-call"
	previousRequest := openAICompatibleRequest{Model: "model", Messages: []openAICompatibleMessage{
		{Role: "user", Content: "contact a@example.test"}, earlier,
	}}
	messages := []Message{{Role: "user", Content: "contact a@example.test"}, {Role: "assistant"},
		{Role: "tool", Content: "second input"}, response.Message}
	// The last admitted input must be exactly the request's provider projection.
	messages[2].Content = `[{"tool_call_id":"earlier-call","ok":true,"result":"second input"}]`
	previousRequest.Messages = append(previousRequest.Messages, toOpenAICompatibleMessages(messages[2])...)
	rawRequest, err := json.Marshal(previousRequest)
	if err != nil {
		t.Fatal(err)
	}
	inputTokens, outputTokens := int64(1), int64(1)
	usage := runtimeeffects.CompletionUsage{ResolvedModel: "model", Exactness: runtimeeffects.CompletionUsageExact, InputTokens: &inputTokens, OutputTokens: &outputTokens,
		Evidence: runtimeeffects.CompletionEvidence{Request: authority.CompletionRequest, CostProvenance: runtimeeffects.CompletionCostEstimated,
			Models: []runtimeeffects.CompletionModelObservation{{Model: "model", Source: runtimeeffects.CompletionModelResponse}}}}
	usage.Evidence = usage.Evidence.Canonical()
	payload, err := json.Marshal(completionContinuationEnvelope{Version: completionContinuationVersion, Adapter: "openai_compatible", Response: response, Usage: usage,
		Projection: completionProjection{SessionID: authority.Target.SessionID, Identity: identity, Memory: authority.Target.Memory,
			ExpectedTurnCount: 1, TurnCount: 2, FrameID: parent, Messages: messages}})
	if err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{}
	if err := runtimeeffects.AttachCompletionContinuationEvidence(evidence, rawRequest, payload); err != nil {
		t.Fatal(err)
	}
	rawEvidence, _ := json.Marshal(evidence)
	rawSuccessor, _ := successor.Encode()
	attempt, err := runtimeeffects.AdmitCompletionContinuation(runtimeeffects.Attempt{OperationID: uuid.NewString(), AttemptID: uuid.NewString(),
		Kind: runtimeeffects.KindProviderTurn, Adapter: "openai_compatible", Authority: authority, Origin: origin},
		rawEvidence, runtimeeffects.Fingerprint(rawRequest), surface, runtimeeffects.CompletionProjectionResponseConsumed, rawSuccessor)
	if err != nil {
		t.Fatal(err)
	}
	probe := &settledContinuationProbe{Harness: harness, attempt: attempt}
	session := &Session{ID: authority.Target.SessionID, AgentID: authority.Target.AgentID, MemoryIdentity: identity, Memory: authority.Target.Memory, TurnCount: 2}
	recovered, found, err := recoverCompletionContinuation(ctx, runtimeeffects.NewCompletionController(probe, probe, probe, probe), nil, "", session, "openai_compatible")
	if err != nil || !found || recovered == nil {
		t.Fatalf("recover exact admitted round: found=%t err=%v", found, err)
	}
	if !reflect.DeepEqual(recovered.Message, response.Message) {
		t.Fatal("recovery changed its admitted response")
	}
	round, _ := recovered.completionHandle.CompletionContinuation()
	// Native hydration redacts content and drops tool calls. The request-only
	// continuation must still replay the admitted raw round, not this projection.
	session.Messages = []Message{{Role: "user", Content: "contact [EMAIL]"}, {Role: "assistant"}, {Role: "tool", Content: "redacted"}, {Role: "assistant"}}
	input := Message{Role: "tool", Content: string(toolResult)}
	call := managedProviderCall{message: input, toolRound: &round,
		provider: agentframe.Provider{RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport},
		frame:    agentframe.Frame{Turn: agentframe.TurnContext{Kind: agentframe.TurnToolContinuation, ParentFrameID: parent, ToolResult: toolResult}}}
	return openAICompatibleRequest{Model: "model"}, session, call
}
