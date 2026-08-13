package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type ToolDefinition struct {
	Name            string `json:"name"`
	Description     string `json:"description"`
	Schema          any    `json:"schema,omitempty"`
	Usage           string `json:"-"`
	GeneratedSchema bool   `json:"-"`
}

func DeliveredToolDescription(def ToolDefinition) string {
	return DescriptionWithUsage(def.Description, def.Usage)
}

func DescriptionWithUsage(description, usage string) string {
	description = strings.TrimSpace(description)
	usage = strings.TrimSpace(usage)
	if usage == "" {
		return description
	}
	if description == "" {
		return "Usage:\n" + usage
	}
	if strings.Contains(description, "\n\nUsage:\n") {
		return description
	}
	return description + "\n\nUsage:\n" + usage
}

type Message struct {
	Role      string     `json:"role"`
	Content   string     `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

type providerToolResult struct {
	CallID  string
	Payload string
}

// providerToolResults preserves native tool-call linkage while delivering the
// canonical continuation-frame projection as the provider-visible result.
func providerToolResults(content string) []providerToolResult {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}
	if entries, err := agentframe.DecodeProviderToolResults(content); err == nil {
		results := make([]providerToolResult, 0, len(entries))
		for _, entry := range entries {
			results = append(results, providerToolResult{CallID: entry.CallID, Payload: content})
		}
		return results
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(content), &entries); err != nil {
		return nil
	}
	results := make([]providerToolResult, 0, len(entries))
	for _, entry := range entries {
		callID, _ := entry["tool_call_id"].(string)
		payload, err := json.Marshal(entry)
		if err != nil {
			continue
		}
		results = append(results, providerToolResult{CallID: strings.TrimSpace(callID), Payload: string(payload)})
	}
	return results
}

type ToolCall struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name"`
	Arguments any    `json:"arguments,omitempty"`
}

type Response struct {
	Message              Message                      `json:"message"`
	ToolCalls            []ToolCall                   `json:"tool_calls,omitempty"`
	ObservedToolCalls    []ToolCall                   `json:"-"`
	SessionID            string                       `json:"session_id,omitempty"`
	Raw                  []byte                       `json:"raw,omitempty"`
	VisibleTools         []string                     `json:"visible_tools,omitempty"`
	ProviderVisibleTools []string                     `json:"provider_visible_tools,omitempty"`
	MCPServers           map[string]string            `json:"mcp_servers,omitempty"`
	MCPVisibleTools      []string                     `json:"mcp_visible_tools,omitempty"`
	CapabilitySurface    *managedcapabilities.Surface `json:"capability_surface,omitempty"`
}

type Session struct {
	ID                   string
	ProviderSessionID    string
	AgentID              string
	Memory               agentmemory.Plan
	MemoryIdentity       agentmemory.Identity
	RetryReason          string
	RetriesFromSessionID string
	Watchdog             *ConversationWatchdog
	TurnCount            int
	ParseFailures        int
	SystemPrompt         string
	Tools                []ToolDefinition
	Messages             []Message
}

type UsageTokens struct {
	InputTokens  int
	OutputTokens int
	Model        string
}

type Runtime interface {
	StartSession(ctx context.Context, agentID string, systemPrompt string, tools []ToolDefinition) (*Session, error)
}

type ManagedSessionRuntime interface {
	Runtime
	ContinueManagedSession(context.Context, *Session, ManagedCall) (*Response, error)
}

type ForkChatSessionRuntime interface {
	Runtime
	ContinueForkChatSession(context.Context, *Session, ForkChatCall) (*Response, error)
}

// ManagedCall is the only provider-call carrier admitted for managed agents.
// Its payload is sealed so callers cannot substitute transport-local messages.
type ManagedCall struct {
	frame agentframe.Frame
}

func newManagedCall(frame agentframe.Frame) (ManagedCall, error) {
	if err := frame.Validate(); err != nil {
		return ManagedCall{}, fmt.Errorf("managed call execution frame: %w", err)
	}
	return ManagedCall{frame: frame}, nil
}

func (c ManagedCall) Frame() agentframe.Frame { return c.frame }

// ProviderMessage validates the call against the active managed authority
// before exposing its transport-neutral provider projection.
func (c ManagedCall) ProviderMessage(ctx context.Context, session *Session) (Message, error) {
	return validateManagedCall(ctx, session, c)
}

func (c ManagedCall) providerMessage() (Message, error) {
	role, content, err := c.frame.ProviderInput()
	if err != nil {
		return Message{}, err
	}
	return Message{Role: role, Content: content}, nil
}

// ForkChatCall is intentionally disjoint from ManagedCall. Operator fork chat
// has its own authority and cannot carry or synthesize a managed frame.
type ForkChatCall struct {
	message Message
}

func NewForkChatCall(message Message) (ForkChatCall, error) {
	message.Role = strings.TrimSpace(message.Role)
	message.Content = strings.TrimSpace(message.Content)
	if message.Role == "" || message.Content == "" {
		return ForkChatCall{}, fmt.Errorf("fork-chat role and content are required")
	}
	if len(message.ToolCalls) > 0 {
		return ForkChatCall{}, fmt.Errorf("fork-chat caller cannot inject tool calls")
	}
	return ForkChatCall{message: message}, nil
}

// ProviderMessage validates the call against fork-chat authority before
// exposing its transport-neutral provider projection.
func (c ForkChatCall) ProviderMessage(ctx context.Context, session *Session) (Message, error) {
	return validateForkChatCall(ctx, session, c)
}

func (c ForkChatCall) providerMessage() Message { return c.message }

func validateManagedCall(ctx context.Context, session *Session, call ManagedCall) (Message, error) {
	if session == nil {
		return Message{}, fmt.Errorf("managed call requires session")
	}
	if !managedAgentExecutionContext(ctx) {
		return Message{}, fmt.Errorf("managed call requires managed execution authority")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return Message{}, fmt.Errorf("managed call requires capability surface")
	}
	frame := call.Frame()
	if !frame.MatchesSurface(surface) {
		return Message{}, fmt.Errorf("managed call frame does not match capability authority")
	}
	if surface.Authority.SessionID != strings.TrimSpace(session.ID) ||
		frame.Session.AgentIdentity.AgentID() != strings.TrimSpace(session.AgentID) {
		return Message{}, fmt.Errorf("managed call frame does not match session")
	}
	if frame.Session.ProviderPrompt != session.SystemPrompt {
		return Message{}, fmt.Errorf("managed call provider prompt does not match session contract")
	}
	return call.providerMessage()
}

func validateForkChatCall(ctx context.Context, session *Session, call ForkChatCall) (Message, error) {
	if session == nil {
		return Message{}, fmt.Errorf("fork-chat call requires session")
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthorityConversationForkChat {
		return Message{}, fmt.Errorf("fork-chat call requires conversation-fork authority")
	}
	if _, managed := managedcapabilities.FromContext(ctx); managed || managedAgentExecutionContext(ctx) {
		return Message{}, fmt.Errorf("fork-chat call rejects managed execution context")
	}
	return call.providerMessage(), nil
}

type toolResultRelayRef struct {
	Path       string
	Chunks     []string
	ReadTool   string
	Format     string
	Visibility string
}

type oversizedToolResultRelayWriter interface {
	PersistOversizedToolResultRelay(ctx context.Context, session *Session, toolName string, rawJSON []byte) (toolResultRelayRef, error)
}

type NativeToolCapabilities struct {
	Bash      bool
	WebSearch bool
	FileIO    bool
}

type StartupVisibleToolSurfaceProber interface {
	ProbeStartupVisibleToolSurface(ctx context.Context, actor models.AgentConfig, systemPrompt string, tools []ToolDefinition) (*Response, error)
}
