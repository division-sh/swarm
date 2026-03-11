package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"empireai/internal/commgraph"
	"empireai/internal/events"
	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/runtime/sessions"
	"empireai/internal/runtime/sharedjson"
	runtimetools "empireai/internal/runtime/tools"
)

type LLMAgent struct {
	cfg           models.AgentConfig
	subscriptions []events.EventType
	conversation  *llm.Conversation
	scopeKey      string
	promptCache   map[string]string
	mu            sync.Mutex
}

func NewLLMAgent(cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor llm.ToolExecutor, tools []llm.ToolDefinition) *LLMAgent {
	subs := make([]events.EventType, 0, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		if strings.TrimSpace(s) == "" {
			continue
		}
		subs = append(subs, events.EventType(s))
	}

	systemPrompt := strings.TrimSpace(extractSystemPrompt(cfg))
	allowedToolSet, constrained := extractAllowedToolSet(cfg)
	tools = mergeTools(filterTools(tools, allowedToolSet, constrained), runtimetools.GenerateEmitToolsForRole(cfg.Role, runtimeWarnOnce))

	maxTurns := 1000
	mode := llm.SessionScoped
	if cfg.Mode == "factory" {
		mode = llm.TaskScoped
		maxTurns = 100
	}
	if overrideMode, overrideMaxTurns := extractConversationConstraints(cfg.Config); overrideMode != nil {
		mode = *overrideMode
		if overrideMaxTurns > 0 {
			maxTurns = overrideMaxTurns
		}
	} else if overrideMaxTurns > 0 {
		maxTurns = overrideMaxTurns
	}
	c := llm.NewConversation(cfg.ID, "", systemPrompt, tools, mode, maxTurns, modelRuntime)
	c.SetToolExecutor(toolExecutor)
	promptCache := map[string]string{}
	if systemPrompt != "" {
		promptCache[""] = systemPrompt
	}
	return &LLMAgent{
		cfg:           cfg,
		subscriptions: subs,
		conversation:  c,
		promptCache:   promptCache,
	}
}

func extractConversationConstraints(raw json.RawMessage) (*llm.ConversationMode, int) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, 0
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, 0
	}
	var (
		modePtr  *llm.ConversationMode
		maxTurns int
	)
	if constraints, ok := obj["constraints"].(map[string]any); ok {
		if mode, ok := parseConversationMode(sharedjson.AsString(constraints["conversation_mode"])); ok {
			modeCopy := mode
			modePtr = &modeCopy
		}
		if v := asIntFromAny(constraints["max_turns_per_task"]); v > 0 {
			maxTurns = v
		}
	}
	// Backward-compatible top-level overrides.
	if modePtr == nil {
		if mode, ok := parseConversationMode(sharedjson.AsString(obj["conversation_mode"])); ok {
			modeCopy := mode
			modePtr = &modeCopy
		}
	}
	if maxTurns == 0 {
		if v := asIntFromAny(obj["max_turns_per_task"]); v > 0 {
			maxTurns = v
		}
	}
	return modePtr, maxTurns
}

func parseConversationMode(raw string) (llm.ConversationMode, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "task", "task_scoped", "task-scoped":
		return llm.TaskScoped, true
	case "session", "session_scoped", "session-scoped":
		return llm.SessionScoped, true
	case "session_per_vertical", "session-per-vertical", "session_per_scope":
		return llm.SessionPerVerticalScoped, true
	default:
		return llm.TaskScoped, false
	}
}

func asIntFromAny(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func NewLLMAgentFactory(modelRuntime llm.Runtime, toolExecutor llm.ToolExecutor, tools []llm.ToolDefinition) runtimemanager.AgentFactory {
	return func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		if strings.TrimSpace(extractSystemPrompt(cfg)) == "" {
			agentID := strings.TrimSpace(cfg.ID)
			if agentID == "" {
				agentID = strings.TrimSpace(cfg.Role)
			}
			if agentID == "" {
				agentID = "unknown-agent"
			}
			return nil, errors.New("missing required system_prompt for agent " + agentID)
		}
		return NewLLMAgent(cfg, modelRuntime, toolExecutor, tools), nil
	}
}

func (a *LLMAgent) ID() string                        { return a.cfg.ID }
func (a *LLMAgent) Conversation() *llm.Conversation   { return a.conversation }
func (a *LLMAgent) Type() string                      { return a.cfg.Type }
func (a *LLMAgent) Subscriptions() []events.EventType { return a.subscriptions }

func (a *LLMAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.applyPromptForEvent(evt)
	a.resetConversationScopeIfNeeded(evt)

	ctx = runtimeactor.WithActor(ctx, a.cfg)
	ctx = runtimebus.WithInboundEvent(ctx, evt)
	ctx = sessions.WithScope(ctx, llm.ConversationModeString(a.conversation.Mode), conversationScopeKeyForEvent(a.conversation.Mode, evt))
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)

	// Human task events must feed back into the requesting agent's reasoning context
	// as an async tool-result style message correlated by task_id.
	if isHumanTaskOutcomeEvent(evt.Type) {
		_ = a.injectHumanTaskToolResult(ctx, evt)
	}

	input := formatEventForAgent(a.cfg, evt)
	resp, err := a.conversation.Step(ctx, input)
	if err != nil && a.shouldRetryAfterTaskScopeReset(err) {
		a.conversation.Reset()
		scopeKey := strings.TrimSpace(conversationScopeKeyForEvent(a.conversation.Mode, evt))
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
	if err := a.enforcePostTurnExpectations(evt, recorder); err != nil {
		if remediateErr := a.attemptPostTurnContractRemediation(ctx, evt, recorder, err); remediateErr == nil {
			return nil, nil
		}
		return nil, err
	}
	return nil, nil
}

func (a *LLMAgent) applyPromptForEvent(evt events.Event) {
	if a == nil || a.conversation == nil {
		return
	}
	prompt := strings.TrimSpace(a.resolvePromptForMode(promptModeFromEvent(evt)))
	if prompt == "" {
		return
	}
	if strings.TrimSpace(a.conversation.SystemPrompt) == prompt {
		return
	}
	a.conversation.SystemPrompt = prompt
	a.conversation.Reset()
	a.scopeKey = ""
}

func (a *LLMAgent) resolvePromptForMode(mode string) string {
	if a == nil || a.conversation == nil {
		return ""
	}
	mode = runtimetools.NormalizeScanModeCompat(mode)
	cacheKey := mode
	if a.promptCache == nil {
		a.promptCache = map[string]string{}
	}
	if cached, ok := a.promptCache[cacheKey]; ok {
		return strings.TrimSpace(cached)
	}

	prompt, found, err := runtimecontracts.LoadPromptForAgent(a.cfg, mode)
	if err != nil {
		runtimeWarn(
			"agent-llm",
			"contract prompt load failed agent_id=%s mode=%s err=%v",
			strings.TrimSpace(a.cfg.ID),
			strings.TrimSpace(mode),
			err,
		)
	}
	if found && strings.TrimSpace(prompt) != "" {
		prompt = strings.TrimSpace(prompt)
		a.promptCache[cacheKey] = prompt
		if cacheKey == "" {
			a.promptCache[""] = prompt
		}
		return prompt
	}

	if cacheKey != "" {
		if fallback, ok := a.promptCache[""]; ok && strings.TrimSpace(fallback) != "" {
			fallback = strings.TrimSpace(fallback)
			a.promptCache[cacheKey] = fallback
			return fallback
		}
	}

	base := strings.TrimSpace(extractSystemPrompt(a.cfg))
	if base == "" {
		base = strings.TrimSpace(a.conversation.SystemPrompt)
	}
	if base != "" {
		a.promptCache[""] = base
		if cacheKey != "" {
			a.promptCache[cacheKey] = base
		}
	}
	return base
}

func promptModeFromEvent(evt events.Event) string {
	payload := sharedjson.ParsePayloadMap(evt.Payload)
	return strings.TrimSpace(sharedjson.AsString(payload["mode"]))
}

func (a *LLMAgent) resetConversationScopeIfNeeded(evt events.Event) {
	if a == nil || a.conversation == nil {
		return
	}
	scopeKey := strings.TrimSpace(conversationScopeKeyForEvent(a.conversation.Mode, evt))
	if scopeKey == "" {
		return
	}
	if a.conversation.Mode == llm.SessionScoped {
		return
	}
	if a.scopeKey == scopeKey {
		return
	}
	a.conversation.Reset()
	a.conversation.TaskID = scopeKey
	a.scopeKey = scopeKey
}

func taskScopeKeyForEvent(evt events.Event) string {
	verticalID, taskID := extractContextIDs(evt)
	if strings.TrimSpace(taskID) != "" {
		return strings.TrimSpace(taskID)
	}
	return strings.TrimSpace(verticalID)
}

func conversationScopeKeyForEvent(mode llm.ConversationMode, evt events.Event) string {
	switch mode {
	case llm.TaskScoped:
		return taskScopeKeyForEvent(evt)
	case llm.SessionPerVerticalScoped:
		verticalID, _ := extractContextIDs(evt)
		return strings.TrimSpace(verticalID)
	default:
		return ""
	}
}

func (a *LLMAgent) shouldRetryAfterTaskScopeReset(err error) bool {
	if a == nil || a.conversation == nil || a.conversation.Mode != llm.TaskScoped || err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "max turns reached")
}

func (a *LLMAgent) attemptPostTurnContractRemediation(ctx context.Context, inbound events.Event, recorder *runtimebus.EmittedEventsRecorder, contractErr error) error {
	prompt, ok := contractRemediationPrompt(a.cfg, inbound, contractErr)
	if !ok {
		return contractErr
	}
	if _, err := a.conversation.Step(ctx, prompt); err != nil {
		return err
	}
	return a.enforcePostTurnExpectations(inbound, recorder)
}

func (a *LLMAgent) enforcePostTurnExpectations(inbound events.Event, recorder *runtimebus.EmittedEventsRecorder) error {
	eventsOut := recorder.Snapshot()
	return runtimetools.EnforceRequiredEmitContract(a.cfg.Role, inbound, eventsOut)
}

func isHumanTaskOutcomeEvent(t events.EventType) bool {
	switch string(t) {
	case "human_task.approved",
		"human_task.rejected",
		"human_task.deferred",
		"human_task.completed",
		"human_task.expired":
		return true
	default:
		return false
	}
}

func (a *LLMAgent) injectHumanTaskToolResult(ctx context.Context, evt events.Event) error {
	if len(evt.Payload) == 0 || a.conversation == nil {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return nil
	}
	reqAgent, _ := payload["requesting_agent"].(string)
	reqAgent = strings.TrimSpace(reqAgent)
	if reqAgent == "" || reqAgent != a.cfg.ID {
		return nil
	}
	taskID, _ := payload["task_id"].(string)
	taskID = strings.TrimSpace(taskID)

	result := map[string]any{
		"task_id": taskID,
		"event":   string(evt.Type),
		"payload": payload,
	}

	ok := true
	errText := ""
	switch string(evt.Type) {
	case "human_task.rejected":
		ok = false
		if v, _ := payload["rejection_reason"].(string); strings.TrimSpace(v) != "" {
			errText = strings.TrimSpace(v)
		} else {
			errText = "human task rejected"
		}
	case "human_task.expired":
		ok = false
		if v, _ := payload["expiry_reason"].(string); strings.TrimSpace(v) != "" {
			errText = strings.TrimSpace(v)
		} else {
			errText = "human task expired"
		}
	}

	return a.conversation.InjectAsyncToolResult(ctx, "human_task_request", ok, result, errText)
}

func (a *LLMAgent) BoardStep(ctx context.Context, directive string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	ctx = runtimeactor.WithActor(ctx, a.cfg)
	resp, err := a.conversation.StepWithRole(ctx, "board_directive", directive)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp.Message.Content), nil
}

func extractSystemPrompt(cfg models.AgentConfig) string {
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return ""
	}
	if v, ok := obj["system_prompt"].(string); ok {
		return v
	}
	return ""
}

func extractAllowedToolSet(cfg models.AgentConfig) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{})
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return allowed, false
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return allowed, false
	}
	found := false
	for _, key := range []string{"tools", "allowed_tools"} {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		arr, ok := raw.([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			name, _ := item.(string)
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			found = true
			allowed[name] = struct{}{}
		}
	}
	return allowed, found
}

func filterTools(in []llm.ToolDefinition, allowed map[string]struct{}, constrained bool) []llm.ToolDefinition {
	if !constrained {
		return in
	}
	out := make([]llm.ToolDefinition, 0, len(in))
	for _, t := range in {
		if runtimetools.IsUniversal(t.Name) {
			out = append(out, t)
			continue
		}
		if _, ok := allowed[t.Name]; ok {
			out = append(out, t)
		}
	}
	return out
}

func mergeTools(in []llm.ToolDefinition, extra []llm.ToolDefinition) []llm.ToolDefinition {
	if len(extra) == 0 {
		return in
	}
	if len(in) == 0 {
		out := make([]llm.ToolDefinition, len(extra))
		copy(out, extra)
		return out
	}
	out := make([]llm.ToolDefinition, 0, len(in)+len(extra))
	seen := make(map[string]struct{}, len(in)+len(extra))
	for _, t := range in {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, t)
	}
	for _, t := range extra {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, t)
	}
	return out
}

func formatEventForAgent(cfg models.AgentConfig, evt events.Event) string {
	payload := strings.TrimSpace(string(evt.Payload))
	if payload == "" {
		payload = "{}"
	}
	allowed := commgraph.ProducerEventsForRole(cfg.Role)
	emitTools := make([]string, 0, len(allowed))
	for _, evtType := range allowed {
		emitTools = append(emitTools, runtimetools.EmitToolName(evtType))
	}
	toolsLine := "(none declared)"
	if len(emitTools) > 0 {
		toolsLine = strings.Join(emitTools, ", ")
	}
	strictRequirement := ""
	strictRequirement = runtimetools.RequiredEmitToolContractText(cfg.Role, evt)
	return fmt.Sprintf(
		"Agent: %s\nRole: %s\nMode: %s\nEvent:\n- id: %s\n- type: %s\n- source: %s\n- task_id: %s\n- vertical_id: %s\n- payload: %s\n\nExecution contract (required):\n- Act via tools when needed.\n- Emit events by calling emit_* tools only.\n- Do not return JSON envelopes for event emission.\n- Available emit tools for your role: %s%s",
		cfg.ID,
		cfg.Role,
		cfg.Mode,
		evt.ID,
		evt.Type,
		evt.SourceAgent,
		evt.TaskID,
		evt.VerticalID,
		payload,
		toolsLine,
		strictRequirement,
	)
}

func canonicalRuntimeRole(role string) string {
	return commgraph.CanonicalRole(role)
}

func contractRemediationPrompt(cfg models.AgentConfig, evt events.Event, contractErr error) (string, bool) {
	return runtimetools.EmitContractRemediationPrompt(cfg.Role, evt, contractErr)
}

func transitionContextKey(primary events.Event, fallback events.Event) string {
	verticalID, taskID := extractContextIDs(primary)
	if strings.TrimSpace(verticalID) == "" || strings.TrimSpace(taskID) == "" {
		fallbackVertical, fallbackTask := extractContextIDs(fallback)
		if strings.TrimSpace(verticalID) == "" {
			verticalID = fallbackVertical
		}
		if strings.TrimSpace(taskID) == "" {
			taskID = fallbackTask
		}
	}
	return verticalID + "|" + taskID
}

func extractContextIDs(evt events.Event) (verticalID, taskID string) {
	verticalID = strings.TrimSpace(evt.VerticalID)
	taskID = strings.TrimSpace(evt.TaskID)
	if len(evt.Payload) == 0 {
		return verticalID, taskID
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
		return verticalID, taskID
	}
	if verticalID == "" {
		for _, key := range []string{"vertical_id", "vertical_ref"} {
			v := strings.TrimSpace(sharedjson.AsString(payload[key]))
			if v != "" {
				verticalID = v
				break
			}
		}
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
	return verticalID, taskID
}

func normalizeScanMode(raw string) string {
	return runtimetools.NormalizeScanModeCompat(raw)
}

func normalizeScanPriority(raw string) string {
	return runtimetools.NormalizeScanPriorityCompat(raw)
}
