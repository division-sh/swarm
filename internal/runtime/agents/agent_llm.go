package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sharedjson"
)

type LLMAgent struct {
	cfg           models.AgentConfig
	subscriptions []events.EventType
	conversation  *llm.Conversation
	scopeKey      string
	toolExecutor  actorScopedToolExecutor
	mu            sync.Mutex
}

type LLMAgentOptions struct{}

type actorScopedToolExecutor interface {
	llm.CapabilityAwareToolExecutor
	ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition
}

type contextAwareActorScopedToolExecutor interface {
	ToolDefinitionsForActorInContext(context.Context, models.AgentConfig) []llm.ToolDefinition
}

func newLLMAgent(cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition, _ LLMAgentOptions) (*LLMAgent, error) {
	subs := make([]events.EventType, 0, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		if strings.TrimSpace(s) == "" {
			continue
		}
		subs = append(subs, events.EventType(s))
	}

	systemPrompt, err := cfg.DerivedSystemPrompt()
	if err != nil {
		return nil, err
	}
	systemPrompt = appendPromptPostamble(systemPrompt)

	maxTurns := 100
	if cfg.MaxTurnsPerTask > 0 {
		maxTurns = cfg.MaxTurnsPerTask
	}
	if err := agentmemory.ValidateFlowOwnership(cfg.Memory, cfg.CanonicalFlowPath()); err != nil {
		agentLabel := strings.TrimSpace(cfg.ID)
		if agentLabel == "" {
			agentLabel = strings.TrimSpace(cfg.Role)
		}
		if agentLabel == "" {
			agentLabel = "unknown-agent"
		}
		return nil, fmt.Errorf("invalid memory plan for agent %s: %w", agentLabel, err)
	}
	c := llm.NewConversation(cfg.ID, "", systemPrompt, tools, cfg.Memory, maxTurns, modelRuntime)
	c.SetToolExecutor(toolExecutor)
	return &LLMAgent{
		cfg:           cfg,
		subscriptions: subs,
		conversation:  c,
		toolExecutor:  toolExecutor,
	}, nil
}

func NewLLMAgentFactory(runtimes llm.AgentRuntimeResolver, toolExecutor actorScopedToolExecutor, opts LLMAgentOptions) runtimemanager.AgentFactory {
	return func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		if err := cfg.ValidateIntentCarrier(); err != nil {
			agentID := strings.TrimSpace(cfg.ID)
			if agentID == "" {
				agentID = strings.TrimSpace(cfg.Role)
			}
			if agentID == "" {
				agentID = "unknown-agent"
			}
			return nil, fmt.Errorf("invalid resolved intent carrier for agent %s: %w", agentID, err)
		}
		if runtimes == nil {
			return nil, errors.New("agent llm runtime resolver is required")
		}
		resolved, err := runtimes.ResolveAgentRuntime(cfg)
		if err != nil {
			return nil, err
		}
		cfg = resolved.Actor
		agentTools := toolExecutor.ToolDefinitionsForActor(cfg)
		return newLLMAgent(cfg, resolved.Runtime, toolExecutor, agentTools, opts)
	}
}

func (a *LLMAgent) ID() string                        { return a.cfg.ID }
func (a *LLMAgent) Conversation() *llm.Conversation   { return a.conversation }
func (a *LLMAgent) Type() string                      { return a.cfg.Type }
func (a *LLMAgent) Subscriptions() []events.EventType { return a.subscriptions }

func (a *LLMAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	turnMode, err := admitAgentTurnExecutionMode(a.cfg, evt)
	if err != nil {
		return nil, err
	}
	a.prepareConversationForInvocation(evt)

	ctx = models.WithActor(ctx, a.cfg)
	ctx = runtimeeffects.WithExecutionMode(ctx, turnMode)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
	ctx = runtimebus.WithInboundEvent(ctx, evt)
	ctx = agentmemory.WithExecution(ctx, a.cfg.Memory, agentmemory.Identity{RunID: evt.RunID(), Agent: a.cfg.Identity})
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)
	a.applyTurnToolDefinitions(ctx)

	// Human-task outcomes are delivered directly to the requester and correlate
	// through the canonical decision-card identity.
	if isHumanTaskOutcomeEvent(evt.Type()) {
		if err := a.injectHumanTaskToolResult(ctx, evt); err != nil {
			return nil, err
		}
	}

	input := formatEventForAgent(a.cfg, evt, a.conversation.Tools)
	resp, err := a.conversation.Step(ctx, input)
	if err != nil && a.shouldRetryAfterTaskScopeReset(err) {
		a.conversation.Reset()
		scopeKey := strings.TrimSpace(taskScopeKeyForEvent(evt))
		if scopeKey != "" {
			a.conversation.TaskID = scopeKey
			a.scopeKey = scopeKey
		}
		resp, err = a.conversation.Step(ctx, input)
	}
	if err != nil {
		return nil, err
	}
	_ = resp
	return nil, nil
}

func admitAgentTurnExecutionMode(cfg models.AgentConfig, evt events.Event) (runtimeeffects.ExecutionMode, error) {
	agentID := strings.TrimSpace(cfg.ID)
	if !cfg.ExecutionMode.Valid() {
		return "", fmt.Errorf("agent %s has no resolved execution mode", agentID)
	}
	inboundMode := evt.ExecutionMode()
	if !inboundMode.Valid() {
		return "", fmt.Errorf("agent %s received event %s without a typed causal execution mode", agentID, strings.TrimSpace(evt.ID()))
	}
	if inboundMode == runtimeeffects.ExecutionModeMock && cfg.ExecutionMode == runtimeeffects.ExecutionModeLive {
		return "", runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "mock_causal_live_agent_forbidden", "llm-agent", "admit_event", map[string]any{
			"action":   "agent_event_delivery",
			"agent_id": agentID,
			"event_id": strings.TrimSpace(evt.ID()),
		})
	}
	return cfg.ExecutionMode, nil
}

func (a *LLMAgent) applyTurnToolDefinitions(ctx context.Context) {
	if a == nil || a.conversation == nil || a.toolExecutor == nil {
		return
	}
	contextAware, ok := a.toolExecutor.(contextAwareActorScopedToolExecutor)
	if !ok {
		return
	}
	tools := contextAware.ToolDefinitionsForActorInContext(ctx, a.cfg)
	a.conversation.Tools = tools
	if a.conversation.Session != nil {
		a.conversation.Session.Tools = tools
	}
}

const promptEnvironmentPostamble = "## Environment\n\nWorkspace: /workspace (read-write logical path)\nReference data: /data (read-only logical path)\nContracts: /opt/swarm/contracts (read-only logical path)\nDocker-backed command execution exposes these as OS paths. Trusted host bash is full host-user shell execution from the workspace backing directory; use relative paths for workspace files, and absolute path availability follows the host deployment namespace and OS permissions."

func appendPromptPostamble(prompt string) string {
	if strings.TrimSpace(prompt) == "" {
		return ""
	}
	if promptHasEnvironmentPostambleContract(prompt) {
		return prompt
	}
	return prompt + "\n\n" + promptEnvironmentPostamble
}

func promptHasEnvironmentPostambleContract(prompt string) bool {
	for _, required := range []string{
		"Workspace: /workspace (read-write logical path)",
		"Reference data: /data (read-only logical path)",
		"Contracts: /opt/swarm/contracts (read-only logical path)",
		"Docker-backed command execution exposes these as OS paths",
		"Trusted host bash is full host-user shell execution from the workspace backing directory",
		"absolute path availability follows the host deployment namespace and OS permissions",
	} {
		if !strings.Contains(prompt, required) {
			return false
		}
	}
	return true
}

func (a *LLMAgent) prepareConversationForInvocation(evt events.Event) {
	if a == nil || a.conversation == nil {
		return
	}
	if a.cfg.Memory.Enabled {
		return
	}
	a.conversation.Reset()
	scopeKey := strings.TrimSpace(taskScopeKeyForEvent(evt))
	a.conversation.TaskID = scopeKey
	a.scopeKey = scopeKey
}

func taskScopeKeyForEvent(evt events.Event) string {
	entityID, taskID := extractContextIDs(evt)
	if strings.TrimSpace(taskID) != "" {
		return strings.TrimSpace(taskID)
	}
	return strings.TrimSpace(entityID)
}

func (a *LLMAgent) shouldRetryAfterTaskScopeReset(err error) bool {
	if a == nil || a.conversation == nil || a.cfg.Memory.Enabled || err == nil {
		return false
	}
	failure, ok := runtimefailures.As(err)
	return ok && failure.Failure.Class == runtimefailures.ClassBudgetExhausted && failure.Failure.Detail.Code == "agent_turn_budget_exhausted"
}

func isHumanTaskOutcomeEvent(t events.EventType) bool {
	switch string(t) {
	case "human_task.approved",
		"human_task.rejected",
		"human_task.deferred",
		"human_task.expired":
		return true
	default:
		return false
	}
}

func (a *LLMAgent) injectHumanTaskToolResult(ctx context.Context, evt events.Event) error {
	if len(evt.Payload()) == 0 || a.conversation == nil {
		return nil
	}
	value, err := canonicaljson.Decode(evt.Payload())
	if err != nil {
		return fmt.Errorf("decode human-task outcome: %w", err)
	}
	payloadValue, ok := value.ObjectMap()
	if !ok {
		return fmt.Errorf("human-task outcome payload must be an object")
	}
	payload := value.Interface().(map[string]any)
	cardIDValue, found := payloadValue["card_id"]
	if !found {
		return fmt.Errorf("human-task outcome card_id is required")
	}
	cardID, ok := cardIDValue.String()
	if !ok || strings.TrimSpace(cardID) == "" {
		return fmt.Errorf("human-task outcome card_id must be a non-empty string")
	}
	cardID = strings.TrimSpace(cardID)

	result := map[string]any{
		"card_id": cardID,
		"event":   string(evt.Type()),
		"payload": payload,
	}

	outcomeOK := true
	errText := ""
	switch string(evt.Type()) {
	case "human_task.rejected":
		outcomeOK = false
		fields, _ := payload["fields"].(map[string]any)
		if v, _ := fields["reason"].(string); strings.TrimSpace(v) != "" {
			errText = strings.TrimSpace(v)
		} else {
			errText = "human task rejected"
		}
	case "human_task.expired":
		outcomeOK = false
		if v, _ := payload["cause"].(string); strings.TrimSpace(v) != "" {
			errText = strings.TrimSpace(v)
		} else {
			errText = "human task expired"
		}
	}

	return a.conversation.InjectAsyncToolResult(ctx, "human_task_request", outcomeOK, result, errText)
}

func (a *LLMAgent) BoardStep(ctx context.Context, directive runtimeagentcontrol.BoardDirective) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := runtimeagentcontrol.ValidateBoardDirective(directive); err != nil {
		return "", err
	}
	directiveText := strings.TrimSpace(directive.Directive)
	evt := directive.Event
	a.prepareConversationForInvocation(evt)

	ctx = models.WithActor(ctx, a.cfg)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID()))
	ctx = runtimebus.WithInboundEvent(ctx, evt)
	ctx = agentmemory.WithExecution(ctx, a.cfg.Memory, agentmemory.Identity{RunID: evt.RunID(), Agent: a.cfg.Identity})
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)
	a.applyTurnToolDefinitions(ctx)
	beforeMessages := len(a.conversation.Messages)
	resp, err := a.conversation.Step(ctx, formatEventForAgent(a.cfg, evt, a.conversation.Tools))
	if err != nil {
		return "", err
	}
	if boardDirectiveSatisfied(recorder, a.conversation.Messages[beforeMessages:]) {
		return strings.TrimSpace(resp.Message.Content), nil
	}

	beforeRemediation := len(a.conversation.Messages)
	resp, err = a.conversation.Step(ctx, boardDirectiveRemediationPrompt(directiveText, strings.TrimSpace(resp.Message.Content)))
	if err != nil {
		return "", err
	}
	if boardDirectiveSatisfied(recorder, a.conversation.Messages[beforeRemediation:]) {
		return strings.TrimSpace(resp.Message.Content), nil
	}
	return "", fmt.Errorf("directive completed without taking action; assistant response: %s", strings.TrimSpace(resp.Message.Content))
}

func boardDirectiveSatisfied(recorder *runtimebus.EmittedEventsRecorder, delta []llm.Message) bool {
	if recorder != nil && len(recorder.Snapshot()) > 0 {
		return true
	}
	for _, msg := range delta {
		if strings.EqualFold(strings.TrimSpace(msg.Role), "tool") && toolMessageHasSuccessfulResult(msg.Content) {
			return true
		}
	}
	return false
}

func boardDirectiveRemediationPrompt(directive, assistantText string) string {
	directive = strings.TrimSpace(directive)
	assistantText = strings.TrimSpace(assistantText)
	var b strings.Builder
	b.WriteString("The previous reply described an intended action but did not take one.\n")
	b.WriteString("You must act now using tools. If the directive should trigger workflow execution, call the appropriate emit_* tool in this turn.\n")
	b.WriteString("Do not explain what you plan to do. Do it.\n")
	if directive != "" {
		b.WriteString("\nOriginal directive:\n")
		b.WriteString(directive)
	}
	if assistantText != "" {
		b.WriteString("\n\nPrevious reply:\n")
		b.WriteString(assistantText)
	}
	return b.String()
}

func toolMessageHasSuccessfulResult(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return false
	}
	for _, item := range items {
		if ok, exists := item["ok"].(bool); exists && ok {
			return true
		}
	}
	return false
}

func formatEventForAgent(cfg models.AgentConfig, evt events.Event, _ []llm.ToolDefinition) string {
	payload := strings.TrimSpace(string(evt.Payload()))
	if payload == "" {
		payload = "{}"
	}
	return fmt.Sprintf(
		"Agent: %s\nRole: %s\nMode: %s\nEvent:\n- id: %s\n- type: %s\n- source: %s\n- task_id: %s\n- entity_id: %s\n- payload: %s\n\nExecution contract (required):\n- Act via tools when needed.\n- Emit events by calling emit_* tools only.\n- Do not return JSON envelopes for event emission.",
		cfg.ID,
		cfg.Role,
		cfg.FlowID,
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		evt.EntityID(),
		payload,
	)
}

func transitionContextKey(primary events.Event, fallback events.Event) string {
	entityID, taskID := extractContextIDs(primary)
	if strings.TrimSpace(entityID) == "" || strings.TrimSpace(taskID) == "" {
		fallbackEntity, fallbackTask := extractContextIDs(fallback)
		if strings.TrimSpace(entityID) == "" {
			entityID = fallbackEntity
		}
		if strings.TrimSpace(taskID) == "" {
			taskID = fallbackTask
		}
	}
	return entityID + "|" + taskID
}

func extractContextIDs(evt events.Event) (entityID, taskID string) {
	entityID = strings.TrimSpace(evt.EntityID())
	taskID = strings.TrimSpace(evt.TaskID())
	if len(evt.Payload()) == 0 {
		return entityID, taskID
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil || payload == nil {
		return entityID, taskID
	}
	if taskID == "" {
		for _, key := range []string{"task_id", "task_ref"} {
			v := strings.TrimSpace(sharedjson.AsString(payload[key]))
			if v != "" {
				taskID = v
				break
			}
		}
	}
	return entityID, taskID
}
