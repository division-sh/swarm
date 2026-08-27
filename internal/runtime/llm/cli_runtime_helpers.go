package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

func (r *ClaudeCLIRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	record, persist, err := memoryConversationRecord(s)
	if err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_cli_conversation_invalid_memory", "Persisting the CLI conversation was skipped because the memory identity was invalid", s.AgentID, s.ID, "", nil, err)
		return
	}
	if !persist {
		return
	}
	if err := r.conversations.UpsertConversation(ctx, record); err != nil {
		logPublisherRuntime(ctx, r.events, "error", "persist_cli_conversation_failed", "Persisting the CLI conversation failed", s.AgentID, s.ID, "", map[string]any{
			"run_id":        record.Identity.RunID,
			"flow_instance": record.Identity.FlowInstance(),
		}, err)
	}
}

func parseCLIResponse(raw []byte) *Response {
	resp := &Response{
		Message: Message{Role: "assistant"},
	}
	if len(raw) == 0 {
		return finalizeCLIResponse(resp)
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		if sid := strings.TrimSpace(asString(obj["session_id"])); sid != "" {
			resp.SessionID = sid
		}
		if resp.SessionID == "" {
			if sid := strings.TrimSpace(asString(obj["sessionId"])); sid != "" {
				resp.SessionID = sid
			}
		}
		texts := make([]string, 0, 4)
		if v, ok := obj["result"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["content"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["message"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["output"].(string); ok {
			texts = append(texts, v)
		}
		if content, ok := obj["content"].([]any); ok {
			for _, item := range content {
				m, _ := item.(map[string]any)
				typ := strings.TrimSpace(strings.ToLower(asString(m["type"])))
				switch typ {
				case "text":
					text := strings.TrimSpace(asString(m["text"]))
					if text != "" {
						texts = append(texts, text)
					}
				case "tool_use":
					name := strings.TrimSpace(asString(m["name"]))
					if name == "" {
						continue
					}
					args := m["input"]
					if args == nil {
						args = m["arguments"]
					}
					resp.ToolCalls = append(resp.ToolCalls, ToolCall{
						Name:      toolidentity.CanonicalName(name),
						Arguments: args,
					})
				}
			}
		}
		if calls, ok := obj["tool_calls"].([]any); ok {
			for _, c := range calls {
				m, _ := c.(map[string]any)
				name := strings.TrimSpace(asString(m["name"]))
				if name == "" {
					continue
				}
				args := m["arguments"]
				if args == nil {
					args = m["input"]
				}
				resp.ToolCalls = append(resp.ToolCalls, ToolCall{
					Name:      toolidentity.CanonicalName(name),
					Arguments: args,
				})
			}
		}
		if len(texts) > 0 {
			resp.Message.Content = strings.TrimSpace(strings.Join(texts, "\n"))
			return finalizeCLIResponse(resp)
		}
		if len(resp.ToolCalls) > 0 {
			return finalizeCLIResponse(resp)
		}
	}

	resp.Message.Content = strings.TrimSpace(string(raw))
	return finalizeCLIResponse(resp)
}

func finalizeCLIResponse(resp *Response) *Response {
	if resp == nil {
		return &Response{}
	}
	resp.ObservedToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
	return resp
}

func dedupeToolCalls(calls []ToolCall) []ToolCall {
	if len(calls) <= 1 {
		return calls
	}
	type key struct {
		name string
		args string
	}
	seen := map[key]struct{}{}
	out := make([]ToolCall, 0, len(calls))
	for _, c := range calls {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		argsRaw, _ := json.Marshal(c.Arguments)
		k := key{name: name, args: string(argsRaw)}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, c)
	}
	return out
}

type conversationForkSandboxTransportSurface struct {
	CanonicalVisibleTools []string
	RuntimeToolNames      []string
	PromptRuntimeTools    []string
	ProviderMCPTools      []string
	LocalFallbackTools    []string
}

func buildConversationForkSandboxTransportSurface(tools []ToolDefinition) conversationForkSandboxTransportSurface {
	runtimeNames := appendCanonicalToolNames(nil, toolNames(tools))

	canonicalVisible := make([]string, 0, len(runtimeNames))
	visibleSet := make(map[string]struct{}, len(runtimeNames))
	addCanonicalVisible := func(name string) {
		name = toolidentity.CanonicalName(name)
		if name == "" {
			return
		}
		if _, ok := visibleSet[name]; ok {
			return
		}
		visibleSet[name] = struct{}{}
		canonicalVisible = append(canonicalVisible, name)
	}
	for _, name := range runtimeNames {
		addCanonicalVisible(name)
	}

	promptRuntime := make([]string, 0, len(runtimeNames))
	providerMCPTools := make([]string, 0, len(runtimeNames))
	localFallbackTools := make([]string, 0, len(runtimeNames))
	for _, name := range runtimeNames {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" {
			continue
		}
		providerMCPTools = append(providerMCPTools, toolidentity.RuntimeToolsMCPPrefix+canonical)
		if strings.HasPrefix(canonical, "emit_") {
			localFallbackTools = append(localFallbackTools, canonical)
			promptRuntime = append(promptRuntime, canonical)
			continue
		}
		promptRuntime = append(promptRuntime, toolidentity.RuntimeToolsMCPPrefix+canonical)
	}

	slices.Sort(canonicalVisible)
	slices.Sort(promptRuntime)
	slices.Sort(providerMCPTools)
	slices.Sort(localFallbackTools)

	return conversationForkSandboxTransportSurface{
		CanonicalVisibleTools: canonicalVisible,
		RuntimeToolNames:      runtimeNames,
		PromptRuntimeTools:    promptRuntime,
		ProviderMCPTools:      providerMCPTools,
		LocalFallbackTools:    localFallbackTools,
	}
}

func claudeControlToolNames() []string {
	return []string{"ExitPlanMode"}
}

func isCLIControlToolName(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, control := range claudeControlToolNames() {
		if name == control {
			return true
		}
	}
	return false
}

func conversationForkSandboxObservedCanonicalTools(tools []ToolDefinition, observed []string) []string {
	if len(observed) == 0 {
		return nil
	}
	surface := buildConversationForkSandboxTransportSurface(tools)
	if len(surface.CanonicalVisibleTools) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(surface.CanonicalVisibleTools))
	for _, name := range surface.CanonicalVisibleTools {
		allowed[strings.TrimSpace(name)] = struct{}{}
	}
	filtered := make([]string, 0, len(observed))
	seen := make(map[string]struct{}, len(observed))
	for _, name := range observed {
		name = toolidentity.CanonicalName(name)
		if name == "" || isCLIControlToolName(name) {
			continue
		}
		if _, ok := allowed[name]; !ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		filtered = append(filtered, name)
	}
	slices.Sort(filtered)
	return filtered
}

func conversationForkSandboxObservedToolsForTurn(tools []ToolDefinition, resp *Response) []string {
	if resp == nil {
		return nil
	}
	observed := append([]string(nil), resp.VisibleTools...)
	observed = append(observed, resp.MCPVisibleTools...)
	return conversationForkSandboxObservedCanonicalTools(tools, observed)
}

func conversationForkSandboxPlannedTools(tools []ToolDefinition) []string {
	surface := buildConversationForkSandboxTransportSurface(tools)
	return append([]string(nil), surface.CanonicalVisibleTools...)
}

func conversationForkSandboxLocalFallbackTools(tools []ToolDefinition) []string {
	surface := buildConversationForkSandboxTransportSurface(tools)
	return append([]string(nil), surface.LocalFallbackTools...)
}

func conversationForkSandboxUsableToolsForTurn(tools []ToolDefinition, resp *Response) []string {
	usable := appendCanonicalToolNames(nil, conversationForkSandboxLocalFallbackTools(tools))
	if observed := conversationForkSandboxObservedToolsForTurn(tools, resp); len(observed) > 0 {
		return appendCanonicalToolNames(usable, observed)
	}
	if conversationForkSandboxHasObservedSurface(resp) {
		return usable
	}
	return appendCanonicalToolNames(usable, conversationForkSandboxPlannedTools(tools))
}

func conversationForkSandboxHasObservedSurface(resp *Response) bool {
	if resp == nil {
		return false
	}
	return len(resp.VisibleTools) > 0 || len(resp.MCPVisibleTools) > 0 || len(resp.MCPServers) > 0
}

func conversationForkSandboxToolCallAllowed(tools []ToolDefinition, resp *Response, name string) bool {
	name = toolidentity.CanonicalName(name)
	if name == "" {
		return false
	}
	for _, visible := range conversationForkSandboxUsableToolsForTurn(tools, resp) {
		if visible == name {
			return true
		}
	}
	return false
}

func appendCanonicalToolNames(dst []string, names []string) []string {
	if len(names) == 0 {
		return dst
	}
	seen := make(map[string]struct{}, len(dst)+len(names))
	for _, existing := range dst {
		existing = toolidentity.CanonicalName(existing)
		if existing == "" {
			continue
		}
		seen[existing] = struct{}{}
	}
	for _, name := range names {
		name = toolidentity.CanonicalName(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		dst = append(dst, name)
	}
	slices.Sort(dst)
	return dst
}

func toolNames(tools []ToolDefinition) []string {
	if len(tools) == 0 {
		return nil
	}
	names := make([]string, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func buildInitialPrompt(s *Session, firstMessage string) string {
	var b strings.Builder
	if strings.TrimSpace(s.SystemPrompt) != "" {
		b.WriteString("System: ")
		b.WriteString(s.SystemPrompt)
		b.WriteString("\n\n")
	}
	if len(s.Tools) > 0 {
		b.WriteString("Tools:\n")
		for _, t := range s.Tools {
			b.WriteString("- ")
			b.WriteString(t.Name)
			if delivered := strings.TrimSpace(DeliveredToolDescription(t)); delivered != "" {
				b.WriteString(": ")
				b.WriteString(delivered)
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString(firstMessage)
	return b.String()
}

func jsonBytes(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func configuredCLIOutputFormat(cfg *config.Config) string {
	if cfg == nil {
		return "json"
	}
	switch strings.TrimSpace(cfg.LLM.ClaudeCLI.OutputFormat) {
	case "stream-json":
		return "stream-json"
	default:
		return "json"
	}
}

func shouldIncludePartialMessages(cfg *config.Config) bool {
	return configuredCLIOutputFormat(cfg) == "stream-json"
}

func appendClaudePrintModeArgs(args []string, cfg *config.Config) []string {
	if shouldIncludePartialMessages(cfg) {
		args = append(args, "--include-partial-messages", "--verbose")
	}
	return args
}

func permissionModeArgs() []string {
	args := make([]string, 0, 3)
	if mode := strings.TrimSpace(os.Getenv("SWARM_CLAUDE_PERMISSION_MODE")); mode != "" {
		args = append(args, "--permission-mode", mode)
	}
	v := strings.TrimSpace(strings.ToLower(os.Getenv("SWARM_CLAUDE_BYPASS_PERMISSIONS")))
	if v == "1" || v == "true" || v == "yes" {
		args = append(args, "--dangerously-skip-permissions")
	}
	return args
}

func joinRawLines(lines [][]byte) []byte {
	if len(lines) == 0 {
		return nil
	}
	var b bytes.Buffer
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return bytes.TrimSpace(b.Bytes())
}
