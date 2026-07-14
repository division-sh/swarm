package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
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
			"flow_instance": record.Identity.FlowInstance,
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
	slices.Sort(names)
	return strings.Join(names, ",")
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

type AgentVisibleToolSurface struct {
	RuntimeToolNames    []string
	EmitToolNames       []string
	NonEmitToolNames    []string
	NativeBuiltinTools  []string
	WritableEntityPaths []string
}

type CLIExecutionToolSurface struct {
	CanonicalVisibleTools []string
	RuntimeToolNames      []string
	PromptRuntimeTools    []string
	ProviderBuiltinTools  []string
	ProviderMCPTools      []string
	LocalFallbackTools    []string
}

func cliNativeCapabilityToolSet(actor models.AgentConfig) map[string]struct{} {
	out := map[string]struct{}{}
	if actor.NativeTools.Bash {
		out["bash"] = struct{}{}
	}
	if actor.NativeTools.WebSearch {
		out["web_search"] = struct{}{}
	}
	if actor.NativeTools.FileIO {
		out["read_file"] = struct{}{}
		out["write_file"] = struct{}{}
	}
	return out
}

func cliExecutionToolSurfaceForActor(actor models.AgentConfig, tools []ToolDefinition) CLIExecutionToolSurface {
	rawRuntimeNames := toolNames(tools)
	nativeCapabilityTools := cliNativeCapabilityToolSet(actor)
	runtimeNames := make([]string, 0, len(rawRuntimeNames))
	for _, name := range rawRuntimeNames {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" {
			continue
		}
		if _, ok := nativeCapabilityTools[canonical]; ok {
			continue
		}
		runtimeNames = append(runtimeNames, canonical)
	}
	slices.Sort(runtimeNames)

	canonicalVisible := make([]string, 0, len(runtimeNames)+4)
	visibleSet := make(map[string]struct{}, len(runtimeNames)+4)
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

	providerBuiltins := make([]string, 0, 5)
	addNativeCapabilityTool := func(name string) {
		name = toolidentity.CanonicalName(name)
		if name == "" {
			return
		}
		addCanonicalVisible(name)
	}

	if actor.NativeTools.Bash {
		providerBuiltins = append(providerBuiltins, "Bash")
		addNativeCapabilityTool("bash")
	}
	if actor.NativeTools.WebSearch {
		providerBuiltins = append(providerBuiltins, "WebFetch", "WebSearch")
		addNativeCapabilityTool("web_search")
	}
	if actor.NativeTools.FileIO {
		providerBuiltins = append(providerBuiltins, "Read", "Write", "Edit")
		addNativeCapabilityTool("read_file")
		addNativeCapabilityTool("write_file")
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

	slices.Sort(providerBuiltins)
	slices.Sort(canonicalVisible)
	slices.Sort(promptRuntime)
	slices.Sort(providerMCPTools)
	slices.Sort(localFallbackTools)

	return CLIExecutionToolSurface{
		CanonicalVisibleTools: canonicalVisible,
		RuntimeToolNames:      runtimeNames,
		PromptRuntimeTools:    promptRuntime,
		ProviderBuiltinTools:  providerBuiltins,
		ProviderMCPTools:      providerMCPTools,
		LocalFallbackTools:    localFallbackTools,
	}
}

func AgentVisibleToolSurfaceForActor(actor models.AgentConfig, tools []ToolDefinition) AgentVisibleToolSurface {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	runtimeNames := append([]string(nil), surface.CanonicalVisibleTools...)
	runtimeNames = runtimeNames[:0]
	runtimeNames = append(runtimeNames, surface.RuntimeToolNames...)

	runtimeSet := make(map[string]struct{}, len(runtimeNames))
	for _, name := range runtimeNames {
		runtimeSet[name] = struct{}{}
	}

	emitNames := make([]string, 0, len(runtimeNames))
	nonEmitNames := make([]string, 0, len(runtimeNames))
	for _, name := range runtimeNames {
		if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
			emitNames = append(emitNames, name)
			continue
		}
		nonEmitNames = append(nonEmitNames, name)
	}

	return AgentVisibleToolSurface{
		RuntimeToolNames:    runtimeNames,
		EmitToolNames:       emitNames,
		NonEmitToolNames:    nonEmitNames,
		NativeBuiltinTools:  append([]string(nil), surface.ProviderBuiltinTools...),
		WritableEntityPaths: saveEntityFieldWritablePaths(tools),
	}
}

func saveEntityFieldWritablePaths(tools []ToolDefinition) []string {
	for _, tool := range tools {
		if toolidentity.CanonicalName(tool.Name) != "save_entity_field" {
			continue
		}
		schema, ok := tool.Schema.(map[string]any)
		if !ok {
			return nil
		}
		properties, ok := schema["properties"].(map[string]any)
		if !ok {
			return nil
		}
		fieldSchema, ok := properties["field"].(map[string]any)
		if !ok {
			return nil
		}
		enumValues, ok := fieldSchema["enum"].([]any)
		if !ok {
			return nil
		}
		out := make([]string, 0, len(enumValues))
		seen := make(map[string]struct{}, len(enumValues))
		for _, value := range enumValues {
			path, ok := value.(string)
			if !ok {
				continue
			}
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			out = append(out, path)
		}
		slices.Sort(out)
		return out
	}
	return nil
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

func filterCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition, observed []string) []string {
	if len(observed) == 0 {
		return nil
	}
	surface := cliExecutionToolSurfaceForActor(actor, tools)
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

func providerNativeCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	if len(surface.ProviderBuiltinTools) == 0 {
		return nil
	}
	out := make([]string, 0, len(surface.ProviderBuiltinTools))
	seen := make(map[string]struct{}, len(surface.ProviderBuiltinTools))
	for _, name := range surface.ProviderBuiltinTools {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" {
			continue
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
	}
	slices.Sort(out)
	return out
}

func filterProviderNativeVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition, observed []string) []string {
	if len(observed) == 0 {
		return nil
	}
	providerNative := make(map[string]struct{}, len(claudeProviderBuiltinToolNames))
	for _, name := range claudeProviderBuiltinToolNames {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" {
			continue
		}
		providerNative[canonical] = struct{}{}
	}
	filtered := make([]string, 0, len(observed))
	seen := make(map[string]struct{}, len(observed))
	for _, name := range observed {
		canonical := toolidentity.CanonicalName(name)
		if canonical == "" || isCLIControlToolName(canonical) {
			continue
		}
		if _, ok := providerNative[canonical]; !ok {
			continue
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		filtered = append(filtered, canonical)
	}
	slices.Sort(filtered)
	return filtered
}

func claudeAllowedToolNamesForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	allowed := make([]string, 0, len(surface.ProviderMCPTools)+len(surface.LocalFallbackTools)+len(surface.ProviderBuiltinTools)+len(claudeControlToolNames()))
	seen := make(map[string]struct{}, cap(allowed))
	addAllowed := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		allowed = append(allowed, name)
	}
	for _, name := range surface.ProviderMCPTools {
		addAllowed(name)
	}
	for _, name := range surface.LocalFallbackTools {
		addAllowed(name)
	}
	for _, name := range surface.ProviderBuiltinTools {
		addAllowed(name)
	}
	for _, name := range claudeControlToolNames() {
		addAllowed(name)
	}
	slices.Sort(allowed)
	return allowed
}

func claudeDisallowedBuiltinToolsArgForActor(actor models.AgentConfig, tools []ToolDefinition) string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	allowed := make(map[string]struct{}, len(surface.ProviderBuiltinTools))
	for _, name := range surface.ProviderBuiltinTools {
		allowed[name] = struct{}{}
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

func claudeAllowedToolsArgForActor(actor models.AgentConfig, tools []ToolDefinition) string {
	allowed := claudeAllowedToolNamesForActor(actor, tools)
	if len(allowed) == 0 {
		return ""
	}
	return strings.Join(allowed, ",")
}

const cliToolInvocationMarker = "## Swarm Tool Invocation"
const deliveryToolSurfaceMarker = "## Swarm Tool Surface"

func AgentVisibleToolSummaryLinesForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := AgentVisibleToolSurfaceForActor(actor, tools)
	lines := make([]string, 0, 4)
	emitLine := "(none declared)"
	if len(surface.EmitToolNames) > 0 {
		emitLine = strings.Join(surface.EmitToolNames, ", ")
	}
	lines = append(lines, "Available emit tools in this turn: "+emitLine)
	if len(surface.NonEmitToolNames) > 0 {
		lines = append(lines, "Available non-emit tools in this turn: "+strings.Join(surface.NonEmitToolNames, ", "))
	}
	if len(surface.NativeBuiltinTools) > 0 {
		lines = append(lines, "Available native CLI tools in this turn: "+strings.Join(surface.NativeBuiltinTools, ", "))
	}
	if len(surface.WritableEntityPaths) > 0 {
		lines = append(lines, "Writable entity paths for save_entity_field in this turn: "+strings.Join(surface.WritableEntityPaths, ", "))
	}
	return lines
}

func augmentAgentSystemPrompt(systemPrompt string, actor models.AgentConfig, tools []ToolDefinition) string {
	systemPrompt = strings.TrimSpace(systemPrompt)
	if systemPrompt == "" {
		return systemPrompt
	}
	if strings.Contains(systemPrompt, deliveryToolSurfaceMarker) {
		return systemPrompt
	}
	lines := AgentVisibleToolSummaryLinesForActor(actor, tools)
	if len(lines) == 0 {
		return systemPrompt
	}
	var b strings.Builder
	b.WriteString(systemPrompt)
	b.WriteString("\n\n")
	b.WriteString(deliveryToolSurfaceMarker)
	b.WriteString("\n")
	b.WriteString("Use only the tool definitions delivered in this session.\n")
	b.WriteString("Publish events by calling the matching emit_* tool directly.\n")
	b.WriteString("Do not return JSON envelopes as a substitute for event emission.\n")
	for _, line := range lines {
		b.WriteString("- ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func augmentCLISystemPrompt(systemPrompt string, actor models.AgentConfig, tools []ToolDefinition) string {
	systemPrompt = strings.TrimSpace(systemPrompt)
	if systemPrompt == "" {
		return systemPrompt
	}
	if strings.Contains(systemPrompt, cliToolInvocationMarker) {
		return systemPrompt
	}
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	controlTools := claudeControlToolNames()
	if len(surface.PromptRuntimeTools) == 0 && len(controlTools) == 0 {
		return systemPrompt
	}
	summaryLines := AgentVisibleToolSummaryLinesForActor(actor, tools)
	var b strings.Builder
	b.WriteString(systemPrompt)
	b.WriteString("\n\n")
	b.WriteString(cliToolInvocationMarker)
	b.WriteString("\n")
	if len(surface.PromptRuntimeTools) > 0 {
		b.WriteString("Call Swarm runtime tools by these exact names when they are available in this turn:\n")
		for _, name := range surface.PromptRuntimeTools {
			b.WriteString("- ")
			b.WriteString(name)
			b.WriteString("\n")
		}
	}
	nonEmitProviderTools := make([]string, 0, len(surface.ProviderMCPTools))
	localFallbackSet := make(map[string]struct{}, len(surface.LocalFallbackTools))
	for _, name := range surface.LocalFallbackTools {
		localFallbackSet[strings.TrimSpace(name)] = struct{}{}
	}
	for _, name := range surface.ProviderMCPTools {
		canonical := toolidentity.CanonicalName(name)
		if _, ok := localFallbackSet[canonical]; ok {
			continue
		}
		nonEmitProviderTools = append(nonEmitProviderTools, name)
	}
	if len(nonEmitProviderTools) > 0 {
		b.WriteString("Provider-callable non-emit workflow tools are exposed through these exact MCP names:\n")
		for _, name := range nonEmitProviderTools {
			b.WriteString("- ")
			b.WriteString(name)
			b.WriteString("\n")
		}
	}
	if len(surface.PromptRuntimeTools) > 0 || len(nonEmitProviderTools) > 0 {
		b.WriteString("Non-emit workflow tools are exposed through MCP-prefixed names like `mcp__runtime-tools__...`. Raw `emit_*` calls remain local runtime fallbacks.\n")
	}
	if len(controlTools) > 0 {
		b.WriteString("Claude CLI control tools available in this turn: ")
		b.WriteString(strings.Join(controlTools, ", "))
		b.WriteString(".\n")
	}
	for _, line := range summaryLines {
		b.WriteString("- ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	if hasToolPrefix(surface.LocalFallbackTools, "emit_") {
		b.WriteString("When you need to publish an event, call the matching `emit_*` tool directly. Emit tools may not appear as MCP-prefixed variants in Claude CLI; Swarm will execute the exact `emit_*` call locally. Do not write JSON files under `/workspace/events` as a substitute for emission.\n")
	}
	return strings.TrimSpace(b.String())
}

func observedCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition, resp *Response) []string {
	if resp == nil {
		return nil
	}
	observed := append([]string(nil), resp.VisibleTools...)
	observed = append(observed, resp.MCPVisibleTools...)
	return filterCanonicalVisibleToolsForActor(actor, tools, observed)
}

func cliTurnContextAllowedToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	return append([]string(nil), surface.RuntimeToolNames...)
}

func plannedCanonicalVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	return append([]string(nil), surface.CanonicalVisibleTools...)
}

func cliLocalFallbackVisibleToolsForActor(actor models.AgentConfig, tools []ToolDefinition) []string {
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	return append([]string(nil), surface.LocalFallbackTools...)
}

func resolvedCLIUsableToolsForTurn(actor models.AgentConfig, tools []ToolDefinition, resp *Response) []string {
	usable := appendCanonicalToolNames(nil, cliLocalFallbackVisibleToolsForActor(actor, tools))
	if observed := observedCanonicalVisibleToolsForActor(actor, tools, resp); len(observed) > 0 {
		return appendCanonicalToolNames(usable, observed)
	}
	if hasObservedCLIExecutionSurface(resp) {
		return usable
	}
	return appendCanonicalToolNames(usable, plannedCanonicalVisibleToolsForActor(actor, tools))
}

func hasObservedCLIExecutionSurface(resp *Response) bool {
	if resp == nil {
		return false
	}
	return len(resp.VisibleTools) > 0 || len(resp.MCPVisibleTools) > 0 || len(resp.MCPServers) > 0
}

func cliToolCallAllowedForTurn(actor models.AgentConfig, tools []ToolDefinition, resp *Response, name string) bool {
	name = toolidentity.CanonicalName(name)
	if name == "" {
		return false
	}
	for _, visible := range resolvedCLIUsableToolsForTurn(actor, tools, resp) {
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

func hasToolPrefix(names []string, prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return false
	}
	for _, name := range names {
		if strings.HasPrefix(strings.TrimSpace(name), prefix) {
			return true
		}
	}
	return false
}

func toolNamesCSV(tools []ToolDefinition) string {
	return strings.Join(toolNames(tools), ",")
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
