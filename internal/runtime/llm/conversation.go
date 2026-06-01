package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type cliUsableToolsContextKey struct{}

type cliUsableToolsContextValue struct {
	tools []string
}

type ConversationMode int

const (
	TaskScoped ConversationMode = iota
	SessionScoped
	SessionPerEntityScoped
)

const defaultMaxToolRounds = 8

const (
	maxToolResultBytes              = 16 * 1024
	maxToolMessageBytes             = 64 * 1024
	maxReadFileResultBytes          = 256 * 1024
	maxReadFileEnvelopeReserveBytes = 8 * 1024
	maxReadFileMessageBytes         = maxReadFileResultBytes + maxToolResultBytes + maxReadFileEnvelopeReserveBytes
	maxToolErrorTextRunes           = 600
	maxToolResultPreviewRunes       = 1200
)

type ToolResult struct {
	Name    string
	Payload string
}

type ToolExecutor interface {
	Execute(ctx context.Context, name string, input any) (any, error)
}

type CapabilityAwareToolExecutor interface {
	ToolExecutor
	ToolCapabilitiesForActor(models.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

type ContextAwareCapabilityToolExecutor interface {
	ToolCapabilitiesForActorInContext(context.Context, models.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

type executedToolCall struct {
	Name     string
	OK       bool
	Terminal bool
}

type Conversation struct {
	AgentID      string
	TaskID       string
	Session      *Session
	SystemPrompt string
	Messages     []Message
	Tools        []ToolDefinition
	MaxTurns     int
	TurnCount    int
	Mode         ConversationMode

	runtime       Runtime
	toolExecutor  CapabilityAwareToolExecutor
	maxToolRounds int
}

func ConversationModeString(mode ConversationMode) string {
	switch mode {
	case TaskScoped:
		return sessions.RuntimeModeTask.String()
	case SessionScoped:
		return sessions.RuntimeModeSession.String()
	case SessionPerEntityScoped:
		return sessions.RuntimeModeSessionPerEntity.String()
	default:
		return ""
	}
}

func NewConversation(agentID, taskID, systemPrompt string, tools []ToolDefinition, mode ConversationMode, maxTurns int, runtime Runtime) *Conversation {
	if maxTurns <= 0 {
		maxTurns = 25
	}
	return &Conversation{
		AgentID:       agentID,
		TaskID:        taskID,
		SystemPrompt:  systemPrompt,
		Tools:         tools,
		MaxTurns:      maxTurns,
		Mode:          mode,
		runtime:       runtime,
		maxToolRounds: defaultMaxToolRounds,
	}
}

func (c *Conversation) SetToolExecutor(executor CapabilityAwareToolExecutor) {
	c.toolExecutor = executor
}

func (c *Conversation) SetMaxToolRounds(n int) {
	if n <= 0 {
		n = defaultMaxToolRounds
	}
	c.maxToolRounds = n
}

func (c *Conversation) Step(ctx context.Context, input string) (*Response, error) {
	return c.StepWithRole(ctx, "user", input)
}

func (c *Conversation) StepWithRole(ctx context.Context, role, input string) (*Response, error) {
	msg := Message{Role: strings.TrimSpace(role), Content: input}
	if msg.Role == "" {
		msg.Role = "user"
	}
	return c.stepWithMessage(ctx, msg)
}

func (c *Conversation) stepWithMessage(ctx context.Context, msg Message) (*Response, error) {
	if c.runtime == nil {
		return nil, errors.New("runtime not set")
	}
	if err := c.ensureSession(ctx); err != nil {
		return nil, err
	}
	resp, err := c.continueOnce(ctx, msg)
	if err != nil {
		return nil, err
	}

	if c.toolExecutor == nil || len(resp.ToolCalls) == 0 {
		return resp, nil
	}

	return c.resolveToolCalls(ctx, resp)
}

func (c *Conversation) ensureSession(ctx context.Context) error {
	if c.Session != nil {
		return nil
	}
	scope := sessions.ScopeFromContext(ctx)
	if strings.TrimSpace(scope.ConversationMode) == "" {
		scope.ConversationMode = ConversationModeString(c.Mode)
	}
	if strings.TrimSpace(scope.SessionScope) == "" {
		if actor, ok := models.ActorFromContext(ctx); ok {
			scope.SessionScope = strings.TrimSpace(actor.SessionScope)
		}
	}
	if strings.TrimSpace(scope.ScopeKey) == "" {
		switch c.Mode {
		case TaskScoped, SessionPerEntityScoped:
			scope.ScopeKey = strings.TrimSpace(c.TaskID)
		}
	}
	ctx = sessions.WithScope(ctx, scope.ConversationMode, scope.SessionScope, scope.ScopeKey)
	s, err := c.runtime.StartSession(ctx, c.AgentID, c.SystemPrompt, c.Tools)
	if err != nil {
		return err
	}
	c.Session = s
	return nil
}

func (c *Conversation) continueOnce(ctx context.Context, msg Message) (*Response, error) {
	if c.TurnCount >= c.MaxTurns {
		return nil, fmt.Errorf("max turns reached (%d)", c.MaxTurns)
	}
	resp, err := c.runtime.ContinueSession(ctx, c.Session, msg)
	if err != nil {
		return nil, err
	}
	c.Messages = append(c.Messages, msg, resp.Message)
	c.TurnCount++
	return resp, nil
}

func (c *Conversation) resolveToolCalls(ctx context.Context, initial *Response) (*Response, error) {
	resp := initial
	rounds := c.maxToolRounds
	if rounds <= 0 {
		rounds = defaultMaxToolRounds
	}
	for round := 0; round < rounds; round++ {
		if len(resp.ToolCalls) == 0 {
			return resp, nil
		}
		ctx = withCLIUsableToolsForTurn(ctx, c.turnToolDefinitions(), resp)
		toolPayload, executed := c.executeToolCalls(ctx, resp.ToolCalls)
		if shouldTerminateAfterToolCalls(executed) {
			terminal := *resp
			terminal.ToolCalls = nil
			return &terminal, nil
		}
		toolMsg := Message{Role: "tool", Content: toolPayload}
		next, err := c.continueOnce(ctx, toolMsg)
		if err != nil {
			return nil, err
		}
		resp = next
	}
	return nil, fmt.Errorf("tool resolution exceeded max rounds (%d)", rounds)
}

func (c *Conversation) executeToolCalls(ctx context.Context, calls []ToolCall) (string, []executedToolCall) {
	ctx = c.withToolCapabilities(ctx)
	results := make([]map[string]any, 0, len(calls))
	executed := make([]executedToolCall, 0, len(calls))
	for _, tc := range calls {
		terminal := toolIsTerminalInContext(ctx, tc.Name)
		out, err := c.safeExecuteTool(ctx, tc.Name, tc.Arguments)
		entry := map[string]any{
			"name": tc.Name,
		}
		if id := strings.TrimSpace(tc.ID); id != "" {
			entry["tool_call_id"] = id
		}
		if err == nil {
			projected, projectErr := c.projectToolResult(ctx, tc.Name, tc.Arguments, out)
			if projectErr != nil {
				err = projectErr
			} else {
				entry["ok"] = true
				entry["result"] = projected
			}
		}
		if err != nil {
			entry["ok"] = false
			entry["error"] = clampRunes(err.Error(), maxToolErrorTextRunes)
		}
		results = append(results, entry)
		executed = append(executed, executedToolCall{
			Name:     strings.TrimSpace(tc.Name),
			OK:       err == nil,
			Terminal: terminal,
		})
		if err != nil && !terminal {
			break
		}
	}
	b, err := json.Marshal(results)
	if err != nil {
		return fmt.Sprintf(`[%q]`, err.Error()), executed
	}
	if len(b) > toolRelayMessageLimit(calls) {
		if toolCallBatchHasRoleScopedTypedRead(ctx, calls) {
			overflow := []map[string]any{{
				"name":  "__runtime_guardrail__",
				"ok":    false,
				"error": fmt.Sprintf("%s: role-scoped typed read results exceeded the complete delivery limit of %d bytes", toolresultpolicy.TypedReadResultTooLargeCode, toolresultpolicy.MaxCompleteTypedReadResultBytes),
			}}
			guarded, gerr := json.Marshal(overflow)
			if gerr != nil {
				return fmt.Sprintf(`[{"name":"__runtime_guardrail__","ok":false,"error":%q}]`, toolresultpolicy.TypedReadResultTooLargeCode), executed
			}
			return strings.TrimSpace(string(guarded)), executed
		}
		overflow := []map[string]any{{
			"name":  "__runtime_guardrail__",
			"ok":    false,
			"error": fmt.Sprintf("tool output exceeded %d bytes and was truncated", toolRelayMessageLimit(calls)),
		}}
		guarded, gerr := json.Marshal(overflow)
		if gerr != nil {
			return `[{"name":"__runtime_guardrail__","ok":false,"error":"tool output too large"}]`, executed
		}
		return strings.TrimSpace(string(guarded)), executed
	}
	return strings.TrimSpace(string(b)), executed
}

func (c *Conversation) projectToolResult(ctx context.Context, name string, input any, result any) (any, error) {
	if result == nil {
		return nil, nil
	}
	b, err := json.Marshal(result)
	if err != nil {
		if toolIsRoleScopedTypedReadInContext(ctx, name) {
			return nil, toolresultpolicy.NewTypedReadResultMarshalError("llm-conversation", "tool_result.project", name, err)
		}
		return map[string]any{
			"truncated": false,
			"error":     "marshal tool result",
		}, nil
	}
	if toolIsRoleScopedTypedReadInContext(ctx, name) {
		if len(b) > toolresultpolicy.MaxCompleteTypedReadResultBytes {
			return nil, toolresultpolicy.NewTypedReadResultTooLargeError("llm-conversation", "tool_result.project", name, len(b))
		}
		return result, nil
	}
	limit := toolRelayResultLimitForRuntime(ctx, c.runtime, c.turnToolDefinitions(), name, input)
	if len(b) <= limit {
		return result, nil
	}
	if !runtimeReadFileFollowUpAllowedForTurn(ctx, c.turnToolDefinitions()) {
		return map[string]any{
			"truncated": true,
			"bytes":     len(b),
			"preview":   clampRunes(string(b), maxToolResultPreviewRunes),
		}, nil
	}
	if relay, ok, err := relayOversizedToolResult(ctx, c.runtime, c.Session, name, b); ok {
		if err != nil {
			return nil, fmt.Errorf("persist oversized tool result relay for %s: %w", strings.TrimSpace(name), err)
		}
		followUp := map[string]any{
			"kind":        "runtime_read_file",
			"tool":        relay.ReadTool,
			"format":      relay.Format,
			"visibility":  relay.Visibility,
			"description": "full tool result stored in a runtime-accessible workspace file",
		}
		if len(relay.Chunks) > 0 {
			followUp["kind"] = "runtime_read_file_chunks"
			followUp["chunks"] = relay.Chunks
			followUp["description"] = "full tool result stored across runtime-accessible workspace chunk files; read chunks in order"
		} else {
			followUp["path"] = relay.Path
		}
		return map[string]any{
			"truncated": true,
			"bytes":     len(b),
			"preview":   clampRunes(string(b), maxToolResultPreviewRunes),
			"follow_up": followUp,
		}, nil
	}
	return map[string]any{
		"truncated": true,
		"bytes":     len(b),
		"preview":   clampRunes(string(b), maxToolResultPreviewRunes),
	}, nil
}

func (c *Conversation) turnToolDefinitions() []ToolDefinition {
	if c == nil {
		return nil
	}
	if c.Session != nil && len(c.Session.Tools) > 0 {
		return c.Session.Tools
	}
	return c.Tools
}

func shouldTerminateAfterToolCalls(calls []executedToolCall) bool {
	if len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		if !call.OK || !call.Terminal {
			return false
		}
	}
	return true
}

func (c *Conversation) safeExecuteTool(ctx context.Context, name string, input any) (out any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("tool panic: %v", r)
			out = nil
		}
	}()
	ctx = c.withToolCapabilities(ctx)
	return c.toolExecutor.Execute(ctx, name, input)
}

func toolIsTerminalInContext(ctx context.Context, name string) bool {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return false
	}
	cap, ok := set.Capability(name)
	if !ok {
		return false
	}
	return cap.Kind == toolcapabilities.KindEmit
}

func (c *Conversation) withToolCapabilities(ctx context.Context) context.Context {
	if c == nil || c.toolExecutor == nil || ctx == nil {
		return ctx
	}
	actor, ok := models.ActorFromContext(ctx)
	if !ok {
		return ctx
	}
	if c.toolExecutor == nil {
		return ctx
	}
	defs := c.turnToolDefinitions()
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		name := strings.TrimSpace(def.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	if contextAware, ok := c.toolExecutor.(ContextAwareCapabilityToolExecutor); ok {
		return toolcapabilities.WithContext(ctx, contextAware.ToolCapabilitiesForActorInContext(ctx, actor, names, nil))
	}
	return toolcapabilities.WithContext(ctx, c.toolExecutor.ToolCapabilitiesForActor(actor, names, nil))
}

func toolIsRoleScopedTypedReadInContext(ctx context.Context, name string) bool {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return false
	}
	return toolresultpolicy.IsRoleScopedTypedReadInContext(set, name)
}

func toolCallBatchHasRoleScopedTypedRead(ctx context.Context, calls []ToolCall) bool {
	for _, call := range calls {
		if toolIsRoleScopedTypedReadInContext(ctx, call.Name) {
			return true
		}
	}
	return false
}

func runtimeReadFileFollowUpAllowedForTurn(ctx context.Context, tools []ToolDefinition) bool {
	if usable, ok := cliUsableToolsForTurnFromContext(ctx); ok {
		for _, name := range usable {
			if name == "read_file" {
				return true
			}
		}
		return false
	}
	actor, _ := models.ActorFromContext(ctx)
	for _, name := range plannedCanonicalVisibleToolsForActor(actor, tools) {
		if name == "read_file" {
			return true
		}
	}
	return false
}

func clampToolResult(name string, result any) any {
	if result == nil {
		return nil
	}
	b, err := json.Marshal(result)
	if err != nil {
		return map[string]any{
			"truncated": false,
			"error":     "marshal tool result",
		}
	}
	limit := toolRelayResultLimit(name)
	if len(b) <= limit {
		return result
	}
	return map[string]any{
		"truncated": true,
		"bytes":     len(b),
		"preview":   clampRunes(string(b), maxToolResultPreviewRunes),
	}
}

func relayOversizedToolResult(ctx context.Context, runtime Runtime, session *Session, name string, rawJSON []byte) (toolResultRelayRef, bool, error) {
	writer, ok := runtime.(oversizedToolResultRelayWriter)
	if !ok {
		return toolResultRelayRef{}, false, nil
	}
	relay, err := writer.PersistOversizedToolResultRelay(ctx, session, name, rawJSON)
	return relay, true, err
}

func toolRelayResultLimitForRuntime(ctx context.Context, runtime Runtime, tools []ToolDefinition, name string, input any) int {
	limit := toolRelayResultLimit(name)
	if _, ok := runtime.(oversizedToolResultRelayWriter); ok && helperReadFileShouldUseGenericRelayLimit(ctx, tools, name, input) {
		return maxToolResultBytes
	}
	return limit
}

func helperReadFileShouldUseGenericRelayLimit(ctx context.Context, tools []ToolDefinition, name string, input any) bool {
	if !toolIsLargeRelayFileRead(name) {
		return false
	}
	if toolInputTargetsRuntimeRelayPath(input) {
		return false
	}
	return !runtimeReadFileFollowUpAllowedForTurn(ctx, tools)
}

func toolRelayResultLimit(name string) int {
	if toolIsLargeRelayFileRead(name) {
		return maxReadFileResultBytes
	}
	return maxToolResultBytes
}

func toolRelayMessageLimit(calls []ToolCall) int {
	for _, call := range calls {
		if toolIsLargeRelayFileRead(call.Name) {
			return maxReadFileMessageBytes
		}
	}
	return maxToolMessageBytes
}

func toolIsLargeRelayFileRead(name string) bool {
	return toolidentity.CanonicalName(name) == "read_file"
}

func toolInputTargetsRuntimeRelayPath(input any) bool {
	rawPath := strings.TrimSpace(toolInputPath(input))
	if rawPath == "" {
		return false
	}
	cleaned := strings.TrimSpace(rawPath)
	if strings.HasPrefix(cleaned, "/"+workspaceToolResultRelayDir+"/") {
		return true
	}
	return strings.Contains(cleaned, "/"+workspaceToolResultRelayDir+"/")
}

func toolInputPath(input any) string {
	switch v := input.(type) {
	case map[string]any:
		if path, ok := v["path"].(string); ok {
			return strings.TrimSpace(path)
		}
	case map[string]string:
		return strings.TrimSpace(v["path"])
	}
	var pathCarrier struct {
		Path string `json:"path"`
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return ""
	}
	if err := json.Unmarshal(raw, &pathCarrier); err != nil {
		return ""
	}
	return strings.TrimSpace(pathCarrier.Path)
}

func withCLIUsableToolsForTurn(ctx context.Context, tools []ToolDefinition, resp *Response) context.Context {
	actor, _ := models.ActorFromContext(ctx)
	usable := resolvedCLIUsableToolsForTurn(actor, tools, resp)
	return context.WithValue(ctx, cliUsableToolsContextKey{}, cliUsableToolsContextValue{tools: append([]string(nil), usable...)})
}

func cliUsableToolsForTurnFromContext(ctx context.Context) ([]string, bool) {
	if ctx == nil {
		return nil, false
	}
	v, ok := ctx.Value(cliUsableToolsContextKey{}).(cliUsableToolsContextValue)
	if !ok {
		return nil, false
	}
	return append([]string(nil), v.tools...), true
}

func clampRunes(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "...(truncated)"
}

func (c *Conversation) appendMessage(ctx context.Context, msg Message) error {
	if err := c.ensureSession(ctx); err != nil {
		return err
	}
	c.Messages = append(c.Messages, msg)
	if c.Session != nil {
		c.Session.Messages = append(c.Session.Messages, msg)
	}
	_ = PersistConversationSnapshotForRuntime(ctx, c.runtime, c.Session)
	return nil
}

// InjectAsyncToolResult appends a "tool-result style" message into the active session
// without taking an extra model turn. This is used for async tool completion flows
// like human tasks.
func (c *Conversation) InjectAsyncToolResult(ctx context.Context, toolName string, ok bool, result any, errText string) error {
	entry := map[string]any{"name": strings.TrimSpace(toolName)}
	if ok {
		entry["ok"] = true
		entry["result"] = result
	} else {
		entry["ok"] = false
		entry["error"] = strings.TrimSpace(errText)
		if result != nil {
			entry["result"] = result
		}
	}
	b, err := json.Marshal([]map[string]any{entry})
	if err != nil {
		return fmt.Errorf("marshal async tool result: %w", err)
	}
	return c.appendMessage(ctx, Message{Role: "tool", Content: strings.TrimSpace(string(b))})
}

func (c *Conversation) AppendResult(toolResult ToolResult) {
	content := fmt.Sprintf("Tool %s result:\n%s", toolResult.Name, toolResult.Payload)
	c.Messages = append(c.Messages, Message{Role: "tool", Content: content})
	if c.Session != nil {
		c.Session.Messages = append(c.Session.Messages, Message{Role: "tool", Content: content})
	}
}

func (c *Conversation) AppendFeedback(feedback string) {
	feedback = strings.TrimSpace(feedback)
	if feedback == "" {
		return
	}
	c.Messages = append(c.Messages, Message{Role: "system", Content: feedback})
	if c.Session != nil {
		c.Session.Messages = append(c.Session.Messages, Message{Role: "system", Content: feedback})
	}
}

func (c *Conversation) Reset() {
	c.Session = nil
	c.Messages = nil
	c.TurnCount = 0
}
