package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type MockRuntime struct {
	cfg                  *config.Config
	sessions             sessions.Registry
	conversations        ConversationPersistence
	lockOwner            string
	events               EventPublisher
	completionController *runtimeeffects.Controller
}

func NewMockRuntime(cfg *config.Config, sessionRegistry sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher, controller *runtimeeffects.Controller) *MockRuntime {
	return &MockRuntime{cfg: cfg, sessions: sessionRegistry, conversations: conversations, lockOwner: lockOwner, events: publisher, completionController: controller}
}

func (r *MockRuntime) ProviderContract() ProviderContract { return MockProviderContract() }

func MockProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: llmselection.BackendMock,
		Provider:    llmselection.ProviderMock,
		Transport:   ProviderTransportInProcess,
		ToolSchema: ProviderToolSchemaContract{
			ValidatesInputSchemas: true,
			TranslatesTools:       true,
			ReturnsToolResults:    true,
		},
		SessionLifecycle: ProviderSessionLifecycleContract{
			StartsSessions: true, ContinuesSessions: true, SupportsMemoryPlans: true,
			ProviderSessionIDStrategy: "platform_managed", RotatesSessions: true, PreservesRetryLineage: true,
		},
		Response: ProviderResponseContract{
			NormalizesMessages: true, NormalizesToolCalls: true, PreservesRawResponse: true,
			StreamingParser: "mock_python_json",
		},
		NativeTools: ProviderNativeToolContract{FallbackToolsAllowed: false},
		Persistence: ProviderPersistenceContract{
			PersistsTurns: true, PersistsConversationSnapshots: true, PersistsStatelessAudit: true,
		},
		Budget: ProviderBudgetContract{UsageAccounting: BudgetUsageEstimated},
	}
}

func (r *MockRuntime) PersistConversationSnapshot(ctx context.Context, session *Session) error {
	if r.conversations == nil || session == nil {
		return nil
	}
	record, persist, err := memoryConversationRecord(session)
	if err != nil || !persist {
		return err
	}
	return r.conversations.UpsertConversation(ctx, record)
}

func (r *MockRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	if _, err := requireMockActor(ctx, agentID); err != nil {
		return nil, err
	}
	if err := ValidateProviderToolDefinitions(tools); err != nil {
		return nil, err
	}
	lease, hydrated, resolved, err := startMemory(ctx, r.sessions, r.conversations, agentID, r.lockOwner)
	if err != nil {
		return nil, err
	}
	if resolved.Enabled() {
		if err := r.sessions.Release(ctx, lease); err != nil {
			return nil, err
		}
	}
	session := &Session{
		ID: ensurePlatformSessionID(func() string {
			if lease != nil {
				return lease.SessionID
			}
			return ""
		}()),
		AgentID: agentID, Memory: resolved.Plan, MemoryIdentity: resolved.Identity,
		SystemPrompt: strings.TrimSpace(systemPrompt), Tools: append([]ToolDefinition(nil), tools...),
		Messages: append([]Message(nil), hydrated.Messages...), TurnCount: hydrated.TurnCount, Watchdog: hydrated.Watchdog,
	}
	if resolved.Enabled() {
		session.RetryReason = strings.TrimSpace(hydrated.RetryReason)
		session.RetriesFromSessionID = strings.TrimSpace(hydrated.RetriesFromSessionID)
	}
	publishAgentStarted(ctx, r.events, session, events.EventType("platform.agent_started"))
	return session, nil
}

func (r *MockRuntime) ContinueSession(ctx context.Context, session *Session, message Message) (*Response, error) {
	if session == nil {
		return nil, errors.New("nil session")
	}
	actor, err := requireMockActor(ctx, session.AgentID)
	if err != nil {
		return nil, err
	}
	entityID := actor.EffectiveEntityID()
	lease, resolved, err := acquireContinuedMemory(ctx, r.sessions, session, r.lockOwner)
	if err != nil {
		return nil, sessionAcquireFailure(err, session.AgentID)
	}
	if resolved.Enabled() {
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the mock session lease heartbeat failed", session.AgentID, session.ID, entityID, nil, heartbeatErr)
		})
		defer stopHeartbeat()
		if lease.SessionID != session.ID {
			LogSessionAdoptedForRun(ctx, r.events, resolved.Identity, session.ID, lease.SessionID)
			session.ID = lease.SessionID
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, session, "error", "Marking the reused mock agent delivery in progress failed", map[string]any{"memory_enabled": resolved.Enabled()}, entityID); err != nil {
		return nil, fmt.Errorf("mark inbound delivery active for reused mock session: %w", err)
	}

	request, err := buildMockRequest(ctx, session, message)
	if err != nil {
		return nil, err
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal mock completion input: %w", err)
	}
	ctx, _, err = withObservedMockRuntimeCapabilitySurface(ctx, session.Tools, actor.Mock.Digest)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "managed_capability_in_process_request_mismatch", "mock-python-adapter", "build_request", nil, err)
	}
	ctx, targetID, err := prepareCompletionContext(ctx, r.completionController, r.cfg, session, entityID)
	if err != nil {
		return nil, err
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendMock)
	start := time.Now()
	response, raw, usage, dispatch, executeErr := executeMockCompletion(ctx, actor, session.Tools, requestJSON)
	latency := time.Since(start)
	if response != nil {
		if surface, ok := managedcapabilities.FromContext(ctx); ok {
			response.CapabilitySurface = &surface
		}
	}
	turn := enrichTurnRecord(ctx, session, AgentTurnRecord{
		AgentID: session.AgentID, SessionID: session.ID, RequestPayload: requestJSON, ResponseRaw: raw,
		ParseOK: executeErr == nil, Latency: latency,
	}, response)
	if executeErr != nil {
		failure := runtimefailures.FromError(executeErr, "mock-python-adapter", "execute_completion")
		turn.Failure = &failure.Failure
		if dispatch == nil {
			return nil, executeErr
		}
		if settleErr := settleCompletionTurn(ctx, dispatch, targetID, turn, nil, profile, usage, runtimeeffects.StateTerminalFailure, turn.Failure, map[string]any{
			"execution_mode": runtimeeffects.ExecutionModeMock, "module_digest": actor.Mock.Digest,
		}); settleErr != nil {
			return nil, errors.Join(executeErr, settleErr)
		}
		return nil, executeErr
	}
	if err := settleCompletionTurn(ctx, dispatch, targetID, turn, response, profile, usage, runtimeeffects.StateSettled, nil, map[string]any{
		"execution_mode": runtimeeffects.ExecutionModeMock, "module_digest": actor.Mock.Digest,
	}); err != nil {
		return nil, err
	}
	if err := requireCurrentProviderProjection(ctx, session.AgentID); err != nil {
		return nil, err
	}
	session.Messages = append(session.Messages, message, response.Message)
	session.TurnCount++
	session.ParseFailures = 0
	if resolved.Enabled() {
		if err := r.sessions.IncrementTurn(ctx, resolved.Identity, session.ID); err != nil {
			return nil, err
		}
	}
	r.persistConversation(ctx, session)
	if resolved.Enabled() {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, session, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}
	return response, nil
}

func (r *MockRuntime) persistConversation(ctx context.Context, session *Session) {
	if r.conversations == nil || session == nil {
		return
	}
	record, persist, err := memoryConversationRecord(session)
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_mock_conversation_invalid_memory", "Persisting the mock conversation was skipped because the memory identity was invalid", session.AgentID, session.ID, "", nil, err)
		return
	}
	if persist {
		if err := r.conversations.UpsertConversation(ctx, record); err != nil {
			logPublisherRuntime(ctx, r.events, "error", "persist_mock_conversation_failed", "Persisting the mock conversation failed", session.AgentID, session.ID, "", nil, err)
		}
	}
}

type mockCompletionInput struct {
	Event        *mockCompletionEvent `json:"event,omitempty"`
	SystemPrompt string               `json:"system_prompt"`
	Messages     []Message            `json:"messages"`
	Tools        []ToolDefinition     `json:"tools"`
	ToolResults  []Message            `json:"tool_results"`
	Round        int                  `json:"round"`
}

type mockCompletionEvent struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	SourceAgent string          `json:"source_agent"`
	TaskID      string          `json:"task_id,omitempty"`
	RunID       string          `json:"run_id"`
	EntityID    string          `json:"entity_id,omitempty"`
	Flow        string          `json:"flow_instance,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

func buildMockRequest(ctx context.Context, session *Session, message Message) (mockCompletionInput, error) {
	messages := append([]Message(nil), session.Messages...)
	messages = append(messages, message)
	input := mockCompletionInput{
		SystemPrompt: session.SystemPrompt, Messages: messages, Tools: append([]ToolDefinition(nil), session.Tools...), Round: session.TurnCount + 1,
	}
	for _, item := range messages {
		if strings.EqualFold(strings.TrimSpace(item.Role), "tool") {
			input.ToolResults = append(input.ToolResults, item)
		}
	}
	if event, ok := runtimebus.InboundEventFromContext(ctx); ok {
		payload := event.Payload()
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		input.Event = &mockCompletionEvent{
			ID: event.ID(), Type: string(event.Type()), SourceAgent: event.SourceAgent(), TaskID: event.TaskID(), RunID: event.RunID(),
			EntityID: event.EntityID(), Flow: event.FlowInstance(), Payload: append(json.RawMessage(nil), payload...),
		}
	}
	return input, nil
}

type mockCompletionOutput struct {
	Text  *string        `json:"text,omitempty"`
	Calls []mockToolCall `json:"calls,omitempty"`
	Usage *mockUsage     `json:"usage,omitempty"`
}

type mockToolCall struct {
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type mockUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func executeMockCompletion(ctx context.Context, actor runtimeactors.AgentConfig, tools []ToolDefinition, request []byte) (*Response, []byte, runtimeeffects.CompletionUsage, *completionDispatch, error) {
	model := strings.TrimSpace(actor.ResolvedModel)
	if model == "" {
		model = "mock-regular"
	}
	attempt, err := runtimeeffects.BeginCompletion(ctx, "mock_python", request, nil)
	if err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), nil, err
	}
	dispatch := &completionDispatch{handle: attempt, state: runtimeeffects.StateTerminalFailure}
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, err
	}
	if err := attempt.MarkLaunched(heartbeatCtx); err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	result, err := pythonmodule.Execute(heartbeatCtx, pythonmodule.Request{
		ModuleID: "agent.mock." + actor.ID, RowID: actor.Mock.SourcePath, Digest: actor.Mock.Digest,
		Entry: mockperformance.EntryHandle, Source: actor.Mock.Source, Input: request,
		Fuel: mockperformance.ExecutionFuel, MemoryPages: mockperformance.ExecutionMemoryPages, OutputBytes: mockperformance.ExecutionOutputBytes,
	})
	if err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	raw := append([]byte(nil), result.Output...)
	dispatch.evidence = map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(raw), "fuel_consumed": result.FuelConsumed, "module_digest": actor.Mock.Digest}
	if err := attempt.MarkResponseObserved(heartbeatCtx, dispatch.evidence); err != nil {
		return nil, raw, estimatedMockUsage(request, raw, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	response, usage, err := parseMockCompletionOutput(raw, request, tools, model)
	if err != nil {
		return nil, raw, usage, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.state = runtimeeffects.StateSettled
	return response, raw, usage, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
}

func parseMockCompletionOutput(raw, request []byte, tools []ToolDefinition, model string) (*Response, runtimeeffects.CompletionUsage, error) {
	usage := estimatedMockUsage(request, raw, model)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var output mockCompletionOutput
	if err := decoder.Decode(&output); err != nil {
		return nil, usage, fmt.Errorf("mock completion output must be one JSON object with text, calls, or usage: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, usage, err
	}
	if output.Usage != nil {
		if output.Usage.InputTokens < 0 || output.Usage.OutputTokens < 0 {
			return nil, usage, fmt.Errorf("mock completion usage token counts must be non-negative")
		}
		usage = completionUsage(output.Usage.InputTokens, output.Usage.OutputTokens, model, runtimeeffects.CompletionUsageEstimated)
	}
	text := ""
	if output.Text != nil {
		text = strings.TrimSpace(*output.Text)
	}
	if text == "" && len(output.Calls) == 0 {
		return nil, usage, fmt.Errorf("mock completion produced no text or tool calls; return text or calls from handle(input)")
	}
	visible := make(map[string]ToolDefinition, len(tools))
	for _, tool := range tools {
		visible[strings.TrimSpace(tool.Name)] = tool
	}
	response := &Response{Message: Message{Role: "assistant", Content: text}, Raw: append([]byte(nil), raw...)}
	for index, call := range output.Calls {
		name := strings.TrimSpace(call.Name)
		tool, ok := visible[name]
		if !ok || name == "" {
			return nil, usage, fmt.Errorf("mock completion called tool %q, but it is not visible on this turn", name)
		}
		arguments := call.Arguments
		if arguments == nil {
			arguments = map[string]any{}
		}
		if schema, ok := tool.Schema.(map[string]any); ok {
			if err := eventschema.ValidateValueAgainstSchema(schema, arguments); err != nil {
				return nil, usage, fmt.Errorf("mock completion arguments for tool %q are invalid: %w", name, err)
			}
		}
		id := strings.TrimSpace(call.ID)
		if id == "" {
			id = fmt.Sprintf("mock-%d", index+1)
		}
		response.ToolCalls = append(response.ToolCalls, ToolCall{ID: id, Name: name, Arguments: arguments})
	}
	response.Message.ToolCalls = append([]ToolCall(nil), response.ToolCalls...)
	return response, usage, nil
}

func estimatedMockUsage(input, output []byte, model string) runtimeeffects.CompletionUsage {
	inputTokens := estimatedTokenCount(input)
	outputTokens := estimatedTokenCount(output)
	return completionUsage(inputTokens, outputTokens, model, runtimeeffects.CompletionUsageEstimated)
}

func estimatedTokenCount(raw []byte) int {
	if len(raw) == 0 {
		return 0
	}
	return (len(raw) + 3) / 4
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode trailing mock completion output: %w", err)
	}
	return fmt.Errorf("mock completion output must contain exactly one JSON object")
}

func requireMockActor(ctx context.Context, agentID string) (runtimeactors.AgentConfig, error) {
	actor, ok := runtimeactors.ActorFromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) != strings.TrimSpace(agentID) {
		return runtimeactors.AgentConfig{}, fmt.Errorf("mock runtime requires the exact executing agent descriptor")
	}
	if actor.ExecutionMode != runtimeeffects.ExecutionModeMock {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s is not authorized for mock execution", strings.TrimSpace(agentID))
	}
	if actor.Mock.Kind != mockperformance.KindPython || len(actor.Mock.Source) == 0 || strings.TrimSpace(actor.Mock.Digest) == "" {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s selects mock execution but has no compiled Python performance; add mock.kind: python and mock.module below the contracts root", strings.TrimSpace(agentID))
	}
	return actor, nil
}
