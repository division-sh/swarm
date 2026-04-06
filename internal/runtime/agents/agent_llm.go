package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	llm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/runtime/sessions"
	"swarm/internal/runtime/sharedjson"
	runtimetools "swarm/internal/runtime/tools"
)

type LLMAgent struct {
	cfg            models.AgentConfig
	subscriptions  []events.EventType
	conversation   *llm.Conversation
	scopeKey       string
	promptCache    map[string]string
	promptResolver runtimecontracts.PromptResolver
	authority      runtimeauthority.Provider
	emitRegistry   *runtimetools.EmitRegistry
	mu             sync.Mutex
}

type LLMAgentOptions struct {
	PromptResolver    runtimecontracts.PromptResolver
	AuthorityProvider runtimeauthority.Provider
	EmitRegistry      *runtimetools.EmitRegistry
}

type actorScopedToolExecutor interface {
	llm.CapabilityAwareToolExecutor
	ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition
}

func NewLLMAgentWithOptions(cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition, opts LLMAgentOptions) (*LLMAgent, error) {
	return newLLMAgent(cfg, modelRuntime, toolExecutor, tools, false, opts)
}

func NewLLMAgent(cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition) (*LLMAgent, error) {
	return newLLMAgent(cfg, modelRuntime, toolExecutor, tools, false, LLMAgentOptions{})
}

func newLLMAgent(cfg models.AgentConfig, modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition, precomposed bool, opts LLMAgentOptions) (*LLMAgent, error) {
	subs := make([]events.EventType, 0, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		if strings.TrimSpace(s) == "" {
			continue
		}
		subs = append(subs, events.EventType(s))
	}

	systemPrompt := strings.TrimSpace(extractSystemPrompt(cfg))
	systemPrompt = appendPromptPostamble(systemPrompt)
	authority := runtimeauthority.ProviderOrNoop(opts.AuthorityProvider)
	emitRegistry := opts.EmitRegistry
	if emitRegistry == nil {
		emitRegistry = runtimetools.NewEmitRegistry(nil, authority)
	}
	tools = composeConversationTools(cfg, modelRuntime, tools, precomposed, authority, emitRegistry)

	maxTurns := 100
	mode := llm.TaskScoped
	if strings.TrimSpace(cfg.ConversationMode) != "" {
		overrideMode, ok := parseConversationMode(cfg.ConversationMode)
		if !ok {
			agentLabel := strings.TrimSpace(cfg.ID)
			if agentLabel == "" {
				agentLabel = strings.TrimSpace(cfg.Role)
			}
			if agentLabel == "" {
				agentLabel = "unknown-agent"
			}
			return nil, fmt.Errorf("invalid conversation_mode %q for agent %s", strings.TrimSpace(cfg.ConversationMode), agentLabel)
		}
		mode = overrideMode
	}
	if cfg.MaxTurnsPerTask > 0 {
		maxTurns = cfg.MaxTurnsPerTask
	}
	if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
		agentLabel := strings.TrimSpace(cfg.ID)
		if agentLabel == "" {
			agentLabel = strings.TrimSpace(cfg.Role)
		}
		if agentLabel == "" {
			agentLabel = "unknown-agent"
		}
		return nil, fmt.Errorf("invalid session scope for agent %s: %w", agentLabel, err)
	}
	c := llm.NewConversation(cfg.ID, "", systemPrompt, tools, mode, maxTurns, modelRuntime)
	c.SetToolExecutor(toolExecutor)
	promptCache := map[string]string{}
	if systemPrompt != "" {
		promptCache[""] = systemPrompt
	}
	return &LLMAgent{
		cfg:            cfg,
		subscriptions:  subs,
		conversation:   c,
		promptCache:    promptCache,
		promptResolver: opts.PromptResolver,
		authority:      authority,
		emitRegistry:   emitRegistry,
	}, nil
}

func composeConversationTools(cfg models.AgentConfig, modelRuntime llm.Runtime, tools []llm.ToolDefinition, precomposed bool, authority runtimeauthority.Provider, emitRegistry *runtimetools.EmitRegistry) []llm.ToolDefinition {
	if precomposed {
		return tools
	}
	allowedToolSet, constrained := extractAllowedToolSet(cfg)
	tools = mergeTools(filterTools(tools, allowedToolSet, constrained), emitToolDefinitions(cfg, authority, emitRegistry))
	tools = mergeTools(tools, nativeFallbackToolDefinitions(cfg, modelRuntime))
	return tools
}

func parseConversationMode(raw string) (llm.ConversationMode, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "task", "stateless":
		return llm.TaskScoped, true
	case "session":
		return llm.SessionScoped, true
	case "session_per_entity":
		return llm.SessionPerEntityScoped, true
	default:
		return llm.TaskScoped, false
	}
}

func NewLLMAgentFactory(modelRuntime llm.Runtime, toolExecutor actorScopedToolExecutor, tools []llm.ToolDefinition, opts LLMAgentOptions) runtimemanager.AgentFactory {
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
		agentTools := tools
		agentTools = toolExecutor.ToolDefinitionsForActor(cfg)
		return newLLMAgent(cfg, modelRuntime, toolExecutor, agentTools, true, opts)
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

	ctx = models.WithActor(ctx, a.cfg)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID))
	ctx = runtimebus.WithInboundEvent(ctx, evt)
	ctx = sessions.WithScope(ctx, llm.ConversationModeString(a.conversation.Mode), a.cfg.SessionScope, conversationScopeKeyForEvent(a.conversation.Mode, evt))
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)

	// Human task events must feed back into the requesting agent's reasoning context
	// as an async tool-result style message correlated by task_id.
	if isHumanTaskOutcomeEvent(evt.Type) {
		if err := a.injectHumanTaskToolResult(ctx, evt); err != nil {
			return nil, err
		}
	}

	input := formatEventForAgent(a.cfg, evt, a.conversation.Tools)
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
	if err != nil && a.shouldRetryAfterTaskScopeFatalCLIError(err) {
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
	mode = strings.TrimSpace(mode)
	cacheKey := mode
	if a.promptCache == nil {
		a.promptCache = map[string]string{}
	}
	if cached, ok := a.promptCache[cacheKey]; ok {
		return strings.TrimSpace(cached)
	}

	if a.promptResolver == nil {
		return strings.TrimSpace(a.promptCache[""])
	}
	prompt, found, err := a.promptResolver.LoadPromptForAgent(a.cfg, mode)
	if err != nil {
		processWarn(
			"agent-llm",
			"contract prompt load failed agent_id=%s mode=%s err=%v",
			strings.TrimSpace(a.cfg.ID),
			strings.TrimSpace(mode),
			err,
		)
	}
	if found && strings.TrimSpace(prompt) != "" {
		prompt = strings.TrimSpace(prompt)
		prompt = appendPromptPostamble(prompt)
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
		base = appendPromptPostamble(base)
		a.promptCache[""] = base
		if cacheKey != "" {
			a.promptCache[cacheKey] = base
		}
	}
	return base
}

func expandConfigPromptTemplate(prompt string, raw json.RawMessage) string {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" || len(raw) == 0 {
		return prompt
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || len(obj) == 0 {
		return prompt
	}
	replacer := make([]string, 0, len(obj)*2)
	for key, value := range obj {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		rendered := stringifyPromptTemplateValue(value)
		replacer = append(replacer,
			"{{"+key+"}}", rendered,
			"{"+key+"}", rendered,
		)
	}
	if len(replacer) == 0 {
		return prompt
	}
	return strings.NewReplacer(replacer...).Replace(prompt)
}

func stringifyPromptTemplateValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case json.RawMessage:
		return strings.TrimSpace(string(typed))
	default:
		if raw, err := json.MarshalIndent(value, "", "  "); err == nil {
			return strings.TrimSpace(string(raw))
		}
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	}
}

func promptModeFromEvent(evt events.Event) string {
	payload := sharedjson.ParsePayloadMap(evt.Payload)
	return strings.TrimSpace(sharedjson.AsString(payload["mode"]))
}

const promptEnvironmentPostamble = "## Environment\n\nWorking directory: /workspace (read-write)\nReference data: /data (read-only)\nContracts: /opt/swarm/contracts (read-only)"

func appendPromptPostamble(prompt string) string {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return ""
	}
	if strings.Contains(prompt, "Working directory: /workspace") &&
		strings.Contains(prompt, "Reference data: /data") &&
		strings.Contains(prompt, "Contracts: /opt/swarm/contracts") {
		return prompt
	}
	return prompt + "\n\n" + promptEnvironmentPostamble
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
	entityID, taskID := extractContextIDs(evt)
	if strings.TrimSpace(taskID) != "" {
		return strings.TrimSpace(taskID)
	}
	return strings.TrimSpace(entityID)
}

func conversationScopeKeyForEvent(mode llm.ConversationMode, evt events.Event) string {
	switch mode {
	case llm.TaskScoped:
		return taskScopeKeyForEvent(evt)
	case llm.SessionPerEntityScoped:
		entityID, _ := extractContextIDs(evt)
		return strings.TrimSpace(entityID)
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

func (a *LLMAgent) shouldRetryAfterTaskScopeFatalCLIError(err error) bool {
	if a == nil || a.conversation == nil || a.conversation.Mode != llm.TaskScoped || err == nil {
		return false
	}
	return llm.ShouldResetTaskScopedConversationOnCLIError(err)
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
	_ = inbound
	_ = recorder
	return nil
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

	evt := boardDirectiveEvent(strings.TrimSpace(directive))
	a.applyPromptForEvent(evt)
	a.resetConversationScopeIfNeeded(evt)

	ctx = models.WithActor(ctx, a.cfg)
	ctx = runtimecorrelation.WithRunID(ctx, strings.TrimSpace(evt.RunID))
	ctx = runtimebus.WithInboundEvent(ctx, evt)
	ctx = sessions.WithScope(ctx, llm.ConversationModeString(a.conversation.Mode), a.cfg.SessionScope, conversationScopeKeyForEvent(a.conversation.Mode, evt))
	recorder := runtimebus.NewEmittedEventsRecorder()
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)
	beforeMessages := len(a.conversation.Messages)
	resp, err := a.conversation.Step(ctx, formatEventForAgent(a.cfg, evt, a.conversation.Tools))
	if err != nil {
		return "", err
	}
	if boardDirectiveSatisfied(recorder, a.conversation.Messages[beforeMessages:]) {
		return strings.TrimSpace(resp.Message.Content), nil
	}

	beforeRemediation := len(a.conversation.Messages)
	resp, err = a.conversation.Step(ctx, boardDirectiveRemediationPrompt(directive, strings.TrimSpace(resp.Message.Content)))
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

func boardDirectiveEvent(directive string) events.Event {
	payload := map[string]any{
		"directive_text": strings.TrimSpace(directive),
		"mode":           "directive",
	}
	raw, _ := json.Marshal(payload)
	return events.Event{
		Type:        events.EventType("board.directive"),
		SourceAgent: "dashboard",
		Payload:     raw,
	}
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
	if len(cfg.Tools) == 0 {
		return allowed, false
	}
	found := false
	for _, item := range cfg.Tools {
		name := strings.TrimSpace(item)
		if name == "" {
			continue
		}
		found = true
		allowed[name] = struct{}{}
	}
	return allowed, found
}

func extractNativeToolConfig(cfg models.AgentConfig) map[string]bool {
	if !cfg.NativeTools.Any() {
		return nil
	}
	return map[string]bool{
		"bash":       cfg.NativeTools.Bash,
		"web_search": cfg.NativeTools.WebSearch,
		"file_io":    cfg.NativeTools.FileIO,
	}
}

func supportedNativeToolCapabilities(runtime llm.Runtime) llm.NativeToolCapabilities {
	if provider, ok := runtime.(llm.NativeToolCapabilityProvider); ok && provider != nil {
		return provider.NativeToolCapabilities()
	}
	return llm.NativeToolCapabilities{}
}

func nativeFallbackToolDefinitions(cfg models.AgentConfig, modelRuntime llm.Runtime) []llm.ToolDefinition {
	native := extractNativeToolConfig(cfg)
	if len(native) == 0 {
		return nil
	}
	supported := supportedNativeToolCapabilities(modelRuntime)
	defs := make([]llm.ToolDefinition, 0, 4)
	if native["bash"] && !supported.Bash {
		defs = append(defs, llm.ToolDefinition{
			Name:        "bash",
			Description: "Execute a shell command locally in the agent workspace and return stdout, stderr, exit code, and duration.",
			Schema: runtimetools.ObjectSchema(map[string]any{
				"command":         map[string]any{"type": "string"},
				"timeout_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 300},
			}, "command"),
		})
	}
	if native["web_search"] && !supported.WebSearch {
		defs = append(defs, llm.ToolDefinition{
			Name:        "web_search",
			Description: "Search the web and return normalized results with title, url, and snippet.",
			Schema: runtimetools.ObjectSchema(map[string]any{
				"query":       map[string]any{"type": "string"},
				"max_results": map[string]any{"type": "integer", "minimum": 1, "maximum": 20},
			}, "query"),
		})
	}
	if native["file_io"] && !supported.FileIO {
		defs = append(defs,
			llm.ToolDefinition{
				Name:        "read_file",
				Description: "Read a file from the agent workspace or mounted read-only data/contracts paths.",
				Schema: runtimetools.ObjectSchema(map[string]any{
					"path": map[string]any{"type": "string"},
				}, "path"),
			},
			llm.ToolDefinition{
				Name:        "write_file",
				Description: "Write a file within the agent workspace.",
				Schema: runtimetools.ObjectSchema(map[string]any{
					"path":    map[string]any{"type": "string"},
					"content": map[string]any{"type": "string"},
				}, "path", "content"),
			},
		)
	}
	return defs
}

func extractEmitEvents(cfg models.AgentConfig) []string {
	return uniqueStrings(cfg.EmitEvents)
}

func emitToolDefinitions(cfg models.AgentConfig, authority runtimeauthority.Provider, emitRegistry *runtimetools.EmitRegistry) []llm.ToolDefinition {
	if emitRegistry == nil {
		emitRegistry = runtimetools.NewEmitRegistry(nil, authority)
	}
	if emitEvents := extractEmitEvents(cfg); len(emitEvents) > 0 {
		return emitRegistry.GenerateEmitToolsForEvents(emitEvents, processWarnOnce)
	}
	return emitRegistry.GenerateEmitToolsForRole(cfg.Role, processWarnOnce)
}

func filterTools(in []llm.ToolDefinition, allowed map[string]struct{}, constrained bool) []llm.ToolDefinition {
	out := make([]llm.ToolDefinition, 0, len(in))
	for _, t := range in {
		if runtimetools.IsUniversal(t.Name) {
			out = append(out, t)
			continue
		}
		if !constrained {
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

func formatEventForAgent(cfg models.AgentConfig, evt events.Event, tools []llm.ToolDefinition) string {
	payload := strings.TrimSpace(string(evt.Payload))
	if payload == "" {
		payload = "{}"
	}
	surface := llm.AgentVisibleToolSurfaceForActor(cfg, tools)
	toolsLine := "(none declared)"
	if len(surface.EmitToolNames) > 0 {
		toolsLine = strings.Join(surface.EmitToolNames, ", ")
	}
	toolSummaryLine := ""
	if len(surface.NonEmitToolNames) > 0 {
		toolSummaryLine = "\n- Available non-emit tools in this turn: " + strings.Join(surface.NonEmitToolNames, ", ")
	}
	if len(surface.NativeBuiltinTools) > 0 {
		toolSummaryLine += "\n- Available native CLI tools in this turn: " + strings.Join(surface.NativeBuiltinTools, ", ")
	}
	if len(surface.ControlToolNames) > 0 {
		toolSummaryLine += "\n- Available control tools in this turn: " + strings.Join(surface.ControlToolNames, ", ")
	}
	return fmt.Sprintf(
		"Agent: %s\nRole: %s\nMode: %s\nEvent:\n- id: %s\n- type: %s\n- source: %s\n- task_id: %s\n- entity_id: %s\n- payload: %s\n\nExecution contract (required):\n- Act via tools when needed.\n- Emit events by calling emit_* tools only.\n- Do not return JSON envelopes for event emission.\n- Available emit tools in this turn: %s%s%s",
		cfg.ID,
		cfg.Role,
		cfg.Mode,
		evt.ID,
		evt.Type,
		evt.SourceAgent,
		evt.TaskID,
		evt.EntityID(),
		payload,
		toolsLine,
		toolSummaryLine,
		"",
	)
}

func canonicalRuntimeRole(authority runtimeauthority.Provider, role string) string {
	return runtimeauthority.ProviderOrNoop(authority).CanonicalRole(role)
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func contractRemediationPrompt(cfg models.AgentConfig, evt events.Event, contractErr error) (string, bool) {
	_ = cfg
	_ = evt
	_ = contractErr
	return "", false
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
	taskID = strings.TrimSpace(evt.TaskID)
	if len(evt.Payload) == 0 {
		return entityID, taskID
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
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
