package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"slices"
	"strings"

	"swarm/internal/config"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

func (r *ClaudeCLIRuntime) persistTurn(ctx context.Context, turn AgentTurnRecord) {
	if r.turns == nil {
		return
	}
	if err := r.turns.AppendAgentTurn(ctx, turn); err != nil {
		log.Printf("failed to persist cli agent turn: agent=%s session=%s err=%v", turn.AgentID, turn.SessionID, err)
	}
}

func (r *ClaudeCLIRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = sessions.RuntimeModeSession
	}
	if !shouldPersistConversationMode(mode) {
		return
	}
	if err := r.conversations.UpsertConversation(ctx, ConversationRecord{
		SessionID: s.ID,
		AgentID:   s.AgentID,
		ScopeKey:  strings.TrimSpace(s.ScopeKey),
		Mode:      mode,
		Messages:  s.Messages,
		Summary:   BuildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	}); err != nil {
		log.Printf("failed to persist cli conversation: agent=%s err=%v", s.AgentID, err)
	}
}

func parseCLIResponse(raw []byte) *Response {
	resp := &Response{
		Message: Message{Role: "assistant"},
	}
	if len(raw) == 0 {
		return resp
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
						Name:      name,
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
					Name:      name,
					Arguments: args,
				})
			}
		}
		if len(texts) > 0 {
			resp.Message.Content = strings.TrimSpace(strings.Join(texts, "\n"))
			return resp
		}
		if len(resp.ToolCalls) > 0 {
			return resp
		}
	}

	resp.Message.Content = strings.TrimSpace(string(raw))
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

type sessionIDAdopter interface {
	AdoptSessionID(ctx context.Context, agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error
}

func adoptRegistrySessionID(ctx context.Context, reg sessions.Registry, agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
	if reg == nil {
		return nil
	}
	adopter, ok := reg.(sessionIDAdopter)
	if !ok {
		return nil
	}
	return adopter.AdoptSessionID(ctx, agentID, runtimeMode, lockOwner, newSessionID, scopeKey)
}

func claudeToolsArg(tools []ToolDefinition) string {
	if len(tools) == 0 {
		return ""
	}
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	b, err := json.Marshal(names)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

var claudeProviderBuiltinToolNames = []string{
	"AskUserQuestion",
	"Bash",
	"Edit",
	"EnterPlanMode",
	"EnterWorktree",
	"Glob",
	"Grep",
	"MultiEdit",
	"NotebookEdit",
	"Read",
	"Skill",
	"Task",
	"TaskOutput",
	"TaskStop",
	"TodoWrite",
	"ToolSearch",
	"WebFetch",
	"WebSearch",
	"Write",
}

func claudeDisallowedBuiltinToolsArgForActor(actor models.AgentConfig) string {
	allowed := map[string]struct{}{}
	if len(actor.Config) != 0 && json.Valid(actor.Config) {
		var parsed map[string]any
		if err := json.Unmarshal(actor.Config, &parsed); err == nil {
			if raw, ok := parsed["native_tools"].(map[string]any); ok {
				if enabled, _ := raw["bash"].(bool); enabled {
					allowed["Bash"] = struct{}{}
				}
				if enabled, _ := raw["web_search"].(bool); enabled {
					allowed["WebSearch"] = struct{}{}
				}
				if enabled, _ := raw["file_io"].(bool); enabled {
					allowed["Read"] = struct{}{}
					allowed["Write"] = struct{}{}
					allowed["Edit"] = struct{}{}
				}
			}
		}
	}
	names := make([]string, 0, len(claudeProviderBuiltinToolNames))
	for _, name := range claudeProviderBuiltinToolNames {
		if _, ok := allowed[name]; ok {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

func estimateCLIUsageTokens(in Message, out *Response, actor models.AgentConfig) UsageTokens {
	// This is intentionally crude. Claude Code does not currently expose usage
	// metadata in a stable non-interactive way, so we approximate from payload sizes
	// and apply a role-based floor to avoid undercounting long-session context.
	inText := strings.TrimSpace(in.Content)
	outRaw := []byte{}
	if out != nil && len(out.Raw) > 0 {
		outRaw = out.Raw
	}

	inTokens := estimateTokensFromBytes([]byte(inText))
	outTokens := estimateTokensFromBytes(outRaw)

	minIn := 800
	if strings.TrimSpace(actor.EffectiveEntityID()) == "" {
		minIn = 1200
	}
	if inTokens < minIn {
		inTokens = minIn
	}
	if outTokens < 200 {
		outTokens = 200
	}

	// BudgetTracker only needs model string for tier detection. For CLI mode we use
	// the configured model tier (e.g. "haiku" or "sonnet") from actor.Type.
	model := strings.TrimSpace(actor.Type)

	return UsageTokens{
		InputTokens:  inTokens,
		OutputTokens: outTokens,
		Model:        model,
	}
}

func estimateTokensFromBytes(b []byte) int {
	// Rough: ~4 bytes per token for English/ASCII-heavy text.
	// Clamp to zero for empty payloads.
	if len(b) == 0 {
		return 0
	}
	return (len(b) + 3) / 4
}

func toolNamesCSV(tools []ToolDefinition) string {
	if len(tools) == 0 {
		return ""
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
	return strings.Join(names, ",")
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
			if t.Description != "" {
				b.WriteString(": ")
				b.WriteString(t.Description)
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
