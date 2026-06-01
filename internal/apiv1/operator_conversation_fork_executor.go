package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store"
)

type LLMForkChatExecutor struct {
	Runtime runtimellm.Runtime
}

func NewLLMForkChatExecutor(runtime runtimellm.Runtime) *LLMForkChatExecutor {
	return &LLMForkChatExecutor{Runtime: runtime}
}

func (e *LLMForkChatExecutor) ExecuteForkChat(ctx context.Context, prepared store.ConversationForkChatPrepared, message string) (store.ConversationForkChatExecution, error) {
	if e == nil || e.Runtime == nil {
		return store.ConversationForkChatExecution{}, fmt.Errorf("conversation fork chat llm runtime is required")
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return store.ConversationForkChatExecution{}, fmt.Errorf("conversation fork chat message is required")
	}
	actor := conversationForkChatActor(prepared)
	tools := conversationForkChatToolDefinitions(prepared)
	toolExec := newConversationForkChatToolExecutor(prepared)
	conv := runtimellm.NewConversation(actor.ID, prepared.Fork.ForkID, conversationForkChatSystemPrompt(prepared), tools, runtimellm.TaskScoped, 8, e.Runtime)
	conv.SetToolExecutor(toolExec)
	ctx = runtimeactors.WithActor(ctx, actor)
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeTask.String(), "", prepared.Fork.ForkID)
	resp, err := conv.Step(ctx, message)
	if err != nil {
		return store.ConversationForkChatExecution{}, fmt.Errorf("execute conversation fork chat turn: %w", err)
	}
	assistant := strings.TrimSpace(resp.Message.Content)
	if assistant == "" {
		assistant = "Forkchat sandbox turn completed."
	}
	return store.ConversationForkChatExecution{
		AssistantMessage: assistant,
		ToolCalls:        toolExec.toolCalls(),
		ToolResults:      toolExec.toolResults(),
		AvailableTools:   toolNamesFromDefinitions(tools),
	}, nil
}

func conversationForkChatActor(prepared store.ConversationForkChatPrepared) runtimeactors.AgentConfig {
	return runtimeactors.AgentConfig{
		ID:               strings.TrimSpace(prepared.Fork.SourceAgentID),
		Type:             "forkchat",
		Role:             "forkchat",
		ConversationMode: sessions.RuntimeModeTask.String(),
		SessionScope:     "",
		Tools:            append([]string(nil), prepared.AvailableTools...),
	}
}

func conversationForkChatSystemPrompt(prepared store.ConversationForkChatPrepared) string {
	var b strings.Builder
	b.WriteString("You are executing a forkchat turn inside an isolated forensic sandbox.\n")
	b.WriteString("Use only the provided forkchat tools when they are useful for answering the operator.\n")
	b.WriteString("Read source context only through fork_snapshot_read_entities.\n")
	b.WriteString("Side-effecting tools are sandbox stubs: they record fork-local facts and never mutate live state.\n")
	b.WriteString("Source agent: ")
	b.WriteString(strings.TrimSpace(prepared.Fork.SourceAgentID))
	b.WriteString("\nSource run: ")
	b.WriteString(strings.TrimSpace(prepared.Fork.SourceRunID))
	b.WriteString("\nFork id: ")
	b.WriteString(strings.TrimSpace(prepared.Fork.ForkID))
	b.WriteString("\nSnapshot owner: ")
	b.WriteString(strings.TrimSpace(prepared.Snapshot.SnapshotOwner))
	return b.String()
}

func conversationForkChatToolDefinitions(prepared store.ConversationForkChatPrepared) []runtimellm.ToolDefinition {
	names := append([]string(nil), prepared.AvailableTools...)
	if len(names) == 0 {
		names = []string{"fork_snapshot_read_entities"}
	}
	defs := make([]runtimellm.ToolDefinition, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		desc := "Forkchat sandbox tool. Results are fork-local and never mutate live runtime state."
		if name == "fork_snapshot_read_entities" {
			desc = "Read the source-at-fork entity snapshot captured for this forkchat session."
		}
		defs = append(defs, runtimellm.ToolDefinition{
			Name:        name,
			Description: desc,
			Schema: map[string]any{
				"type":                 "object",
				"additionalProperties": true,
				"properties":           map[string]any{},
			},
		})
	}
	return defs
}

func toolNamesFromDefinitions(defs []runtimellm.ToolDefinition) []string {
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		name := strings.TrimSpace(def.Name)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

type conversationForkChatToolExecutor struct {
	prepared store.ConversationForkChatPrepared
	mu       sync.Mutex
	calls    []store.OperatorConversationToolCall
	results  []store.OperatorConversationToolResult
}

func newConversationForkChatToolExecutor(prepared store.ConversationForkChatPrepared) *conversationForkChatToolExecutor {
	return &conversationForkChatToolExecutor{prepared: prepared}
}

func (e *conversationForkChatToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	canonical := conversationForkChatCanonicalToolName(name)
	switch canonical {
	case "fork_snapshot.read_entities":
		out := map[string]any{
			"status":          "read_from_snapshot",
			"read_policy":     e.prepared.SandboxPolicy.ReadPolicy,
			"snapshot_owner":  e.prepared.Snapshot.SnapshotOwner,
			"source_agent_id": e.prepared.Fork.SourceAgentID,
			"entity_count":    len(e.prepared.Snapshot.EntitySnapshot),
			"entities":        e.prepared.Snapshot.EntitySnapshot,
		}
		return e.record(name, input, out)
	default:
		if !conversationForkChatIsSideEffectTool(e.prepared.SandboxPolicy, canonical) {
			return nil, fmt.Errorf("forkchat tool %q is not available in sandbox policy", name)
		}
		out := map[string]any{
			"status":              "stubbed",
			"owner":               e.prepared.SandboxPolicy.Owner,
			"write_policy":        e.prepared.SandboxPolicy.WritePolicy,
			"requested_tool":      canonical,
			"requested_tool_name": name,
			"live_mutation":       false,
			"reason":              "forkchat sandbox records side-effecting tools as fork-local facts only",
		}
		return e.record(name, input, out)
	}
}

func (e *conversationForkChatToolExecutor) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		caps = append(caps, toolcapabilities.Capability{Name: name, Kind: toolcapabilities.KindStandard, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

func (e *conversationForkChatToolExecutor) record(name string, input any, out map[string]any) (map[string]any, error) {
	argsRaw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	resultRaw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	toolUseID := fmt.Sprintf("forkchat-tool-%02d", len(e.calls)+1)
	call := store.OperatorConversationToolCall{
		ToolUseID: toolUseID,
		Name:      strings.TrimSpace(name),
		Arguments: argsRaw,
		Result:    resultRaw,
	}
	result := store.OperatorConversationToolResult{
		ToolName:  strings.TrimSpace(name),
		ToolUseID: toolUseID,
		Output:    cloneRawJSON(resultRaw),
	}
	e.calls = append(e.calls, call)
	e.results = append(e.results, result)
	return out, nil
}

func (e *conversationForkChatToolExecutor) toolCalls() []store.OperatorConversationToolCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]store.OperatorConversationToolCall, len(e.calls))
	copy(out, e.calls)
	for i := range out {
		out[i].Arguments = cloneRawJSON(out[i].Arguments)
		out[i].Result = cloneRawJSON(out[i].Result)
	}
	return out
}

func (e *conversationForkChatToolExecutor) toolResults() []store.OperatorConversationToolResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]store.OperatorConversationToolResult, len(e.results))
	copy(out, e.results)
	for i := range out {
		out[i].Output = cloneRawJSON(out[i].Output)
	}
	return out
}

func conversationForkChatCanonicalToolName(name string) string {
	switch strings.TrimSpace(name) {
	case "fork_snapshot_read_entities", "fork_snapshot.read_entities":
		return "fork_snapshot.read_entities"
	case "mailbox_approve":
		return "mailbox.approve"
	case "mailbox_reject":
		return "mailbox.reject"
	case "mailbox_defer":
		return "mailbox.defer"
	case "run_start":
		return "run.start"
	case "run_continue":
		return "run.continue"
	case "run_pause":
		return "run.pause"
	case "run_stop":
		return "run.stop"
	default:
		return strings.TrimSpace(name)
	}
}

func conversationForkChatIsSideEffectTool(policy store.ConversationForkSandboxPolicy, canonical string) bool {
	for _, name := range policy.StubbedTools {
		if conversationForkChatCanonicalToolName(name) == canonical {
			return true
		}
	}
	return false
}

func cloneRawJSON(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}
