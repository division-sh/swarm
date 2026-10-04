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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

type MockRuntime struct {
	cfg                  *config.Config
	sessions             sessions.Registry
	liveSessions         LiveSessionAcquirer
	conversations        ConversationPersistence
	lockOwner            string
	events               EventPublisher
	completionController *runtimeeffects.Controller
	workspaces           workspace.Resolver
	mcpTurns             MCPTurnContextStore
	toolGateway          toolgateway.Binding
}

type MockRuntimeOptions struct {
	Workspaces  workspace.Resolver
	MCPTurns    MCPTurnContextStore
	ToolGateway toolgateway.Binding
}

func NewMockRuntime(cfg *config.Config, sessionRegistry sessions.Registry, lockOwner string, conversations ConversationPersistence, publisher EventPublisher, controller *runtimeeffects.Controller, opts MockRuntimeOptions) *MockRuntime {
	return &MockRuntime{cfg: cfg, sessions: sessionRegistry, liveSessions: newTransientLiveSessionAcquirer(sessionRegistry), conversations: conversations, lockOwner: lockOwner, events: publisher, completionController: controller, workspaces: opts.Workspaces, mcpTurns: opts.MCPTurns, toolGateway: opts.ToolGateway}
}

func (r *MockRuntime) ProviderContract() ProviderContract { return MockProviderContract() }

func MockProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: llmselection.BackendMock,
		Provider:    llmselection.ProviderMock,
		Transport:   ProviderTransportCLI,
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
		NativeTools: ProviderNativeToolContract{FallbackToolsAllowed: false, StartupVisibleSurfaceProbe: true},
		Persistence: ProviderPersistenceContract{
			PersistsTurns: true, PersistsConversationSnapshots: true, PersistsStatelessAudit: true,
		},
		Budget: ProviderBudgetContract{UsageAccounting: BudgetUsageEstimated},
	}
}

func (r *MockRuntime) PersistConversationSnapshot(ctx context.Context, lease *sessions.Lease, session *Session) error {
	if r.conversations == nil || session == nil {
		return nil
	}
	record, persist, err := memoryConversationRecord(session)
	if err != nil || !persist {
		return err
	}
	return r.conversations.UpsertConversation(ctx, lease, record)
}

func (r *MockRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	if _, err := requireMockActor(ctx, agentID); err != nil {
		return nil, err
	}
	if err := ValidateProviderToolDefinitions(tools); err != nil {
		return nil, err
	}
	lease, hydrated, resolved, err := startMemory(ctx, r.liveSessions, agentID, r.lockOwner)
	if err != nil {
		if lease != nil {
			err = releasePreProviderSessionLease(ctx, r.sessions, lease, agentID, r.events, err)
		}
		return nil, err
	}
	if resolved.Enabled() {
		if err := releasePreProviderSessionLease(ctx, r.sessions, lease, agentID, r.events, nil); err != nil {
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
		SystemPrompt: systemPrompt, Tools: append([]ToolDefinition(nil), tools...),
		Messages: append([]Message(nil), hydrated.Messages...), TurnCount: hydrated.TurnCount, Watchdog: hydrated.Watchdog,
	}
	if resolved.Enabled() {
		session.RetryReason = strings.TrimSpace(hydrated.RetryReason)
		session.RetriesFromSessionID = strings.TrimSpace(hydrated.RetriesFromSessionID)
	}
	publishAgentStarted(ctx, r.events, session, events.EventType("platform.agent_started"))
	return session, nil
}

func (r *MockRuntime) ContinueManagedSession(ctx context.Context, session *Session, call ManagedCall) (*Response, error) {
	managed, err := validateManagedProviderCall(ctx, session, call, r.ProviderContract())
	if err != nil {
		return nil, err
	}
	return r.continueSession(ctx, session, managed.message, &managed)
}

func (r *MockRuntime) recoverManagedCompletionContinuation(ctx context.Context, session *Session) (*Response, bool, error) {
	return recoverCompletionContinuation(ctx, r.completionController, r.sessions, r.lockOwner, session, "mock_python")
}

func (r *MockRuntime) PrepareManagedSession(ctx context.Context, session *Session) error {
	return prepareManagedSessionForTurn(ctx, session, r.sessions, r.liveSessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events)
}

func (r *MockRuntime) ContinueForkChatSession(ctx context.Context, session *Session, call ForkChatCall) (*Response, error) {
	message, err := validateForkChatCall(ctx, session, call)
	if err != nil {
		return nil, err
	}
	return r.continueSession(ctx, session, message, nil)
}

func (r *MockRuntime) continueSession(ctx context.Context, session *Session, message Message, managed *managedProviderCall) (result *Response, retErr error) {
	if session == nil {
		return nil, errors.New("nil session")
	}
	actor, err := requireMockActor(ctx, session.AgentID)
	if err != nil {
		return nil, err
	}
	entityID := actor.EffectiveEntityID()
	previousTurnCount := session.TurnCount
	lease, resolved, err := acquireContinuedMemory(ctx, r.liveSessions, session, r.lockOwner)
	if err != nil {
		if lease != nil {
			err = releasePreProviderSessionLease(ctx, r.sessions, lease, session.AgentID, r.events, err)
		}
		return nil, sessionAcquireFailure(err, session.AgentID)
	}
	if err := requireManagedAcquiredBase(ctx, session, previousTurnCount, managed); err != nil {
		return nil, releasePreProviderSessionLease(ctx, r.sessions, lease, session.AgentID, r.events, err)
	}
	if resolved.Enabled() {
		defer func() {
			retErr = releaseCompletedSessionLease(ctx, r.sessions, lease, session.AgentID, r.events, retErr)
		}()
		var cancelLease context.CancelFunc
		ctx, cancelLease = context.WithCancel(ctx)
		defer cancelLease()
		stopHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, func(heartbeatErr error) {
			cancelLease()
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the mock session lease heartbeat failed", session.AgentID, session.ID, entityID, nil, heartbeatErr)
		})
		defer stopHeartbeat()
		if session.adoptedFromID != "" {
			LogSessionAdoptedForRun(ctx, r.events, resolved.Identity, session.adoptedFromID, lease.SessionID)
			session.adoptedFromID = ""
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, session, "error", "Marking the reused mock agent delivery in progress failed", map[string]any{"memory_enabled": resolved.Enabled()}, entityID); err != nil {
		return nil, fmt.Errorf("mark inbound delivery active for reused mock session: %w", err)
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendMock)
	providerModel, err := resolveProviderModelForCall(ctx, r.cfg, profile, managed)
	if err != nil {
		return nil, err
	}
	completionModel := strings.TrimSpace(providerModel.ConcreteModel)
	if completionModel == "" {
		completionModel = "mock-regular"
	}

	request, err := buildMockRequest(ctx, session, message)
	if err != nil {
		return nil, err
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal mock completion input: %w", err)
	}
	ctx, targetID, err := prepareCompletionContext(ctx, r.completionController, r.cfg, session, lease, entityID)
	if err != nil {
		return nil, err
	}
	if r.workspaces == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "mock_workspace_required", "mock-python-adapter", "prepare_target", nil)
	}
	target, err := r.resolveWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, releaseMockTarget(ctx, target)) }()
	ctx, err = probeWorkspaceMCP(ctx, r.cfg, r.mcpTurns, session, r.toolGateway, target)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	response, raw, usage, dispatch, executeErr := executeMockCompletionWithExecutor(ctx, actor, session.Tools, requestJSON, providerModel, len(request.ToolResults) != 0, managed, func(ctx context.Context, request pythonmodule.Request) (pythonmodule.Result, error) {
		return r.executeWorkspaceMockModel(ctx, target, request)
	})
	latency := time.Since(start)
	ctx, executeErr = observeMockCompletionSurface(ctx, response, executeErr)
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
		if _, settleErr := settleCompletionTurn(ctx, dispatch, targetID, turn, nil, profile, usage, dispatch.state, turn.Failure, map[string]any{
			"execution_mode": runtimeeffects.ExecutionModeMock, "module_digest": actor.Mock.Digest,
		}); settleErr != nil {
			return nil, errors.Join(executeErr, settleErr)
		}
		return nil, executeErr
	}
	if err := bindCompletionProjection(dispatch, session, message, managed); err != nil {
		return nil, err
	}
	settled, settlementErr := settleCompletionTurn(ctx, dispatch, targetID, turn, response, profile, usage, runtimeeffects.StateSettled, nil, map[string]any{
		"execution_mode": runtimeeffects.ExecutionModeMock, "module_digest": actor.Mock.Digest,
	})
	if !settled.Committed {
		return nil, unacknowledgedCompletionError(settlementErr)
	}
	handoffCtx := context.WithoutCancel(ctx)
	if settled.NoCurrentProjection() {
		return nil, settlementErr
	}
	if err := requireCurrentProviderProjection(handoffCtx, session.AgentID); err != nil {
		return nil, errors.Join(settlementErr, err)
	}
	projected, err := projectCompletionContinuation(handoffCtx, dispatch, session, response)
	if err != nil && !committedCompletionCleanup(response, err) {
		return nil, errors.Join(settlementErr, err)
	}
	if !projected {
		if resolved.Enabled() {
			if err := incrementCompletedSessionTurn(handoffCtx, r.sessions, lease, session.AgentID, r.events); err != nil {
				return nil, errors.Join(settlementErr, err)
			}
		}
		session.Messages = append(session.Messages, message, response.Message)
		session.TurnCount++
		session.ParseFailures = 0
		r.persistConversation(handoffCtx, lease, session)
	}
	return response, errors.Join(settlementErr, err)
}

func (r *MockRuntime) executeWorkspaceMockModel(ctx context.Context, target *workspace.Target, request pythonmodule.Request) (pythonmodule.Result, error) {
	result, err := workspace.RunWorker(ctx, target, r.cfg.Workspace.DockerBin, worker.Request{Mode: "model", Module: &request})
	if err != nil {
		return pythonmodule.Result{}, err
	}
	if result.Module == nil {
		return pythonmodule.Result{}, &workspace.WorkerExecutionError{Started: true, Observed: true, ModelStarted: result.ModelStarted, Err: runtimefailures.New(runtimefailures.ClassSchemaInvalid, "mock_worker_result_missing", "mock-python-adapter", "execute_completion", nil)}
	}
	return *result.Module, nil
}

func observeMockCompletionSurface(ctx context.Context, response *Response, executeErr error) (context.Context, error) {
	if response == nil {
		return ctx, executeErr
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return ctx, executeErr
	}
	observed, err := observeAllBindings(surface, managedcapabilities.BindingMCPProvider, evidenceMCPVisible, managedcapabilities.EvidenceConfirmed, "")
	if err != nil {
		return ctx, errors.Join(executeErr, err)
	}
	response.CapabilitySurface = &observed
	return managedcapabilities.WithContext(ctx, observed), executeErr
}

func (r *MockRuntime) persistConversation(ctx context.Context, lease *sessions.Lease, session *Session) {
	if r.conversations == nil || session == nil {
		return
	}
	record, persist, err := memoryConversationRecord(session)
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_mock_conversation_invalid_memory", "Persisting the mock conversation was skipped because the memory identity was invalid", session.AgentID, session.ID, "", nil, err)
		return
	}
	if persist {
		if err := r.conversations.UpsertConversation(ctx, lease, record); err != nil {
			logPublisherRuntime(ctx, r.events, "error", "persist_mock_conversation_failed", "Persisting the mock conversation failed", session.AgentID, session.ID, "", nil, err)
		}
	}
}

type mockCompletionInput struct {
	SystemPrompt string           `json:"system_prompt"`
	Messages     []Message        `json:"messages"`
	Tools        []ToolDefinition `json:"tools"`
	ToolResults  []Message        `json:"tool_results"`
	Round        int              `json:"round"`
}

func buildMockRequest(_ context.Context, session *Session, message Message) (mockCompletionInput, error) {
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

type mockCompletionExecutor func(context.Context, pythonmodule.Request) (pythonmodule.Result, error)

func executeMockCompletionWithExecutor(ctx context.Context, actor runtimeactors.AgentConfig, tools []ToolDefinition, request []byte, providerModel llmselection.ResolvedModel, postToolRound bool, managed *managedProviderCall, execute mockCompletionExecutor) (*Response, []byte, runtimeeffects.CompletionUsage, *completionDispatch, error) {
	model := strings.TrimSpace(providerModel.ConcreteModel)
	attempt, err := beginProviderCompletion(ctx, "mock_python", request, managed)
	if err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), nil, err
	}
	dispatch := newCompletionDispatch(attempt, runtimeeffects.StateTerminalFailure)
	dispatch.providerModel = providerModel
	dispatch.request = append([]byte(nil), request...)
	heartbeatCtx, heartbeat, err := startCompletionAttemptHeartbeat(ctx, attempt)
	if err != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, err
	}
	if err := attempt.MarkLaunched(heartbeatCtx); !dispatch.retainCommittedMutation(err, runtimeeffects.MutationLaunch) {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if gateErr := completionInvocationGate(ctx, heartbeatCtx); gateErr != nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, dispatch.noDispatchError(gateErr))
	}
	if execute == nil {
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "mock_provider_executor_missing", "mock-python-adapter", "execute_completion", nil))
	}
	dispatch.markProviderInvocationStarted()
	providerCtx, stopProvider := completionProviderContext(ctx, heartbeatCtx)
	defer stopProvider()
	result, err := execute(providerCtx, pythonmodule.Request{
		ModuleID: "agent.mock." + actor.ID, RowID: actor.Mock.SourcePath, Digest: actor.Mock.Digest,
		Entry: mockperformance.EntryHandle, Source: actor.Mock.Source, Input: request,
		Fuel: mockperformance.ExecutionFuel, MemoryPages: mockperformance.ExecutionMemoryPages, OutputBytes: mockperformance.ExecutionOutputBytes,
	})
	if err != nil {
		var execution *workspace.WorkerExecutionError
		if errors.As(err, &execution) && !execution.RemoteCleanupUnproven && (!execution.Started || execution.Observed) {
			if !execution.Started || !execution.ModelStarted {
				dispatch.invocation = completionProviderInvocationNotStarted
				err = dispatch.noDispatchError(err)
			}
		} else {
			dispatch.state = runtimeeffects.StateOutcomeUncertain
			err = runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "mock_worker_outcome_uncertain", "mock-python-adapter", "execute_completion", nil, err)
		}
		return nil, nil, estimatedMockUsage(request, nil, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	raw := append([]byte(nil), result.Output...)
	dispatch.evidence = map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(raw), "fuel_consumed": result.FuelConsumed, "module_digest": actor.Mock.Digest}
	if err := waitMockPostToolTail(providerCtx, actor.Mock, postToolRound); err != nil {
		return nil, raw, estimatedMockUsage(request, raw, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	if err := attempt.MarkResponseObserved(heartbeatCtx, dispatch.evidence); !dispatch.retainCommittedMutation(err, runtimeeffects.MutationObservation) {
		return nil, raw, estimatedMockUsage(request, raw, model), dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	response, usage, err := parseMockCompletionOutput(raw, request, tools, model)
	if err != nil {
		return nil, raw, usage, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, err)
	}
	dispatch.state = runtimeeffects.StateSettled
	return response, raw, usage, dispatch, finishCompletionDispatchHeartbeat(dispatch, heartbeat, nil)
}

func waitMockPostToolTail(ctx context.Context, performance mockperformance.Performance, postToolRound bool) error {
	if !postToolRound || performance.PostToolTailLatencyMS == 0 {
		return nil
	}
	if performance.PostToolTailLatencyMS < 0 {
		return fmt.Errorf("mock post-tool tail latency must be non-negative")
	}
	timer := time.NewTimer(time.Duration(performance.PostToolTailLatencyMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
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
