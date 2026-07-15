package llm

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
)

type cliStreamAccumulator struct {
	raw                  bytes.Buffer
	message              Message
	toolCalls            []ToolCall
	observedToolCalls    []ToolCall
	streamedCalls        []cliRecordedToolCall
	sessionID            string
	resultText           string
	pending              map[int]*cliPendingToolCall
	completedToolIDs     map[string]struct{}
	mcpServers           map[string]string
	visibleTools         []string
	providerVisibleTools []string
	mcpVisibleTools      []string
}

type cliPendingToolCall struct {
	ID        string
	Name      string
	Input     any
	InputJSON strings.Builder
}

type cliRecordedToolCall struct {
	ID   string
	Call ToolCall
}

func newCLIStreamAccumulator() *cliStreamAccumulator {
	return &cliStreamAccumulator{
		message:          Message{Role: "assistant"},
		pending:          make(map[int]*cliPendingToolCall),
		completedToolIDs: make(map[string]struct{}),
	}
}

func (a *cliStreamAccumulator) AddLine(line []byte) {
	if a == nil {
		return
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	a.raw.Write(line)
	a.raw.WriteByte('\n')

	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		return
	}
	a.captureDiagnostics(obj)
	if sid := strings.TrimSpace(coalesce(asString(obj["session_id"]), asString(obj["sessionId"]))); sid != "" {
		a.sessionID = sid
	}

	switch strings.ToLower(strings.TrimSpace(asString(obj["type"]))) {
	case "assistant":
		a.mergeAssistantObject(obj)
	case "user":
		a.mergeUserObject(obj)
	case "result":
		if text := strings.TrimSpace(asString(obj["result"])); text != "" {
			a.resultText = text
		}
	case "stream_event":
		a.mergeStreamEvent(obj)
	default:
		// Ignore other stream events for final-response assembly.
	}
}

func (a *cliStreamAccumulator) mergeAssistantObject(obj map[string]any) {
	payload := obj["message"]
	if payload == nil {
		payload = obj
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	resp := parseCLIResponse(b)
	if resp == nil {
		return
	}
	if sid := strings.TrimSpace(resp.SessionID); sid != "" {
		a.sessionID = sid
	}
	if text := strings.TrimSpace(resp.Message.Content); text != "" {
		if strings.TrimSpace(a.message.Content) == "" {
			a.message.Content = text
		} else if !strings.Contains(a.message.Content, text) {
			a.message.Content = strings.TrimSpace(a.message.Content + "\n" + text)
		}
	}
	if len(resp.ToolCalls) > 0 && !a.hasConnectedRuntimeMCP() {
		a.toolCalls = dedupeToolCalls(append(a.toolCalls, resp.ToolCalls...))
	}
	if len(resp.ObservedToolCalls) > 0 {
		a.observedToolCalls = append(a.observedToolCalls, resp.ObservedToolCalls...)
	}
}

func (a *cliStreamAccumulator) mergeUserObject(obj map[string]any) {
	content, ok := cliMessageContent(obj)
	if !ok {
		return
	}
	for _, item := range content {
		entry, _ := item.(map[string]any)
		if strings.TrimSpace(strings.ToLower(asString(entry["type"]))) != "tool_result" {
			continue
		}
		toolUseID := strings.TrimSpace(asString(entry["tool_use_id"]))
		if toolUseID == "" {
			continue
		}
		a.completedToolIDs[toolUseID] = struct{}{}
	}
}

func cliMessageContent(obj map[string]any) ([]any, bool) {
	if content, ok := obj["content"].([]any); ok {
		return content, true
	}
	msg, _ := obj["message"].(map[string]any)
	if len(msg) == 0 {
		return nil, false
	}
	content, ok := msg["content"].([]any)
	return content, ok
}

func (a *cliStreamAccumulator) mergeStreamEvent(obj map[string]any) {
	event, _ := obj["event"].(map[string]any)
	if len(event) == 0 {
		return
	}
	switch strings.ToLower(strings.TrimSpace(asString(event["type"]))) {
	case "content_block_start":
		index := asInt(event["index"])
		block, _ := event["content_block"].(map[string]any)
		if strings.ToLower(strings.TrimSpace(asString(block["type"]))) != "tool_use" {
			return
		}
		call := &cliPendingToolCall{
			ID:   strings.TrimSpace(asString(block["id"])),
			Name: strings.TrimSpace(asString(block["name"])),
		}
		if input, ok := block["input"]; ok {
			call.Input = input
		}
		if call.Name != "" {
			a.pending[index] = call
		}
	case "content_block_delta":
		index := asInt(event["index"])
		call, ok := a.pending[index]
		if !ok || call == nil {
			return
		}
		delta, _ := event["delta"].(map[string]any)
		if strings.ToLower(strings.TrimSpace(asString(delta["type"]))) != "input_json_delta" {
			return
		}
		call.InputJSON.WriteString(asString(delta["partial_json"]))
	case "content_block_stop":
		index := asInt(event["index"])
		call, ok := a.pending[index]
		if !ok || call == nil {
			return
		}
		delete(a.pending, index)
		args := call.Input
		if raw := strings.TrimSpace(call.InputJSON.String()); raw != "" {
			var decoded any
			if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
				args = decoded
			}
		}
		if strings.TrimSpace(call.Name) != "" {
			a.streamedCalls = append(a.streamedCalls, cliRecordedToolCall{
				ID: strings.TrimSpace(call.ID),
				Call: ToolCall{
					Name:      toolidentity.CanonicalName(call.Name),
					Arguments: args,
				},
			})
		}
	}
}

func (a *cliStreamAccumulator) captureDiagnostics(obj map[string]any) {
	if a == nil || len(obj) == 0 {
		return
	}
	if value, ok := obj["mcp_servers"]; ok {
		for name, status := range parseMCPServers(value) {
			if a.mcpServers == nil {
				a.mcpServers = map[string]string{}
			}
			a.mcpServers[name] = status
		}
	}
	if value, ok := obj["tools"]; ok {
		for _, name := range parseVisibleToolNames(value) {
			a.appendVisibleTool(name)
		}
		for _, name := range parseMCPVisibleToolNames(value) {
			a.appendVisibleTool(name)
		}
	}
}

func parseMCPServers(value any) map[string]string {
	list, ok := value.([]any)
	if !ok || len(list) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, item := range list {
		entry, _ := item.(map[string]any)
		name := strings.TrimSpace(asString(entry["name"]))
		status := strings.TrimSpace(asString(entry["status"]))
		if name != "" && status != "" {
			out[name] = status
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseVisibleToolNames(value any) []string {
	list, ok := value.([]any)
	if !ok || len(list) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(list))
	for _, item := range list {
		switch typed := item.(type) {
		case string:
			name := strings.TrimSpace(typed)
			if name != "" {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					out = append(out, name)
				}
			}
		case map[string]any:
			name := strings.TrimSpace(asString(typed["name"]))
			if name != "" {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					out = append(out, name)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func parseMCPVisibleToolNames(value any) []string {
	list, ok := value.([]any)
	if !ok || len(list) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(list))
	for _, item := range list {
		switch typed := item.(type) {
		case string:
			name := strings.TrimSpace(typed)
			if strings.HasPrefix(name, runtimeToolsMCPPrefix) {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					out = append(out, name)
				}
			}
		case map[string]any:
			name := strings.TrimSpace(asString(typed["name"]))
			if strings.HasPrefix(name, runtimeToolsMCPPrefix) {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					out = append(out, name)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

func (a *cliStreamAccumulator) appendVisibleTool(name string) {
	if a == nil {
		return
	}
	name = strings.TrimSpace(name)
	if isCLIControlToolName(name) {
		return
	}
	if !strings.HasPrefix(name, runtimeToolsMCPPrefix) {
		found := false
		for _, existing := range a.providerVisibleTools {
			if existing == name {
				found = true
				break
			}
		}
		if !found {
			a.providerVisibleTools = append(a.providerVisibleTools, name)
			sort.Strings(a.providerVisibleTools)
		}
	}
	if canonical := toolidentity.CanonicalName(name); canonical != "" {
		if isCLIControlToolName(canonical) {
			return
		}
		found := false
		for _, existing := range a.visibleTools {
			if existing == canonical {
				found = true
				break
			}
		}
		if !found {
			a.visibleTools = append(a.visibleTools, canonical)
			sort.Strings(a.visibleTools)
		}
	}
	if !strings.HasPrefix(name, runtimeToolsMCPPrefix) {
		return
	}
	for _, existing := range a.mcpVisibleTools {
		if existing == name {
			return
		}
	}
	a.mcpVisibleTools = append(a.mcpVisibleTools, name)
	sort.Strings(a.mcpVisibleTools)
}

func (a *cliStreamAccumulator) hasConnectedRuntimeMCP() bool {
	if a == nil || len(a.mcpServers) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(a.mcpServers["runtime-tools"]), "connected")
}

func (a *cliStreamAccumulator) Response() *Response {
	if a == nil {
		return &Response{Message: Message{Role: "assistant"}}
	}
	message := a.message
	if strings.TrimSpace(message.Content) == "" && strings.TrimSpace(a.resultText) != "" {
		message.Content = strings.TrimSpace(a.resultText)
	}
	toolCalls := append([]ToolCall(nil), a.toolCalls...)
	observedToolCalls := append([]ToolCall(nil), a.observedToolCalls...)
	for _, call := range a.streamedCalls {
		observedToolCalls = append(observedToolCalls, call.Call)
		if call.ID != "" {
			if _, completed := a.completedToolIDs[call.ID]; completed {
				continue
			}
		}
		toolCalls = append(toolCalls, call.Call)
	}
	// If Claude already completed a runtime-tools MCP roundtrip inside the
	// provider session, local replay would duplicate side effects. Treat the
	// provider-managed tool loop as authoritative for that turn.
	if a.hasConnectedRuntimeMCP() && len(a.completedToolIDs) > 0 {
		toolCalls = nil
	}
	return &Response{
		Message:              message,
		ToolCalls:            dedupeToolCalls(toolCalls),
		ObservedToolCalls:    observedToolCalls,
		SessionID:            strings.TrimSpace(a.sessionID),
		Raw:                  bytes.TrimSpace(a.raw.Bytes()),
		VisibleTools:         append([]string(nil), a.visibleTools...),
		ProviderVisibleTools: append([]string(nil), a.providerVisibleTools...),
		MCPServers:           a.mcpServers,
		MCPVisibleTools:      append([]string(nil), a.mcpVisibleTools...),
	}
}

func asInt(v any) int {
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
	default:
		return 0
	}
}
