package llm

import (
	"context"
	"strings"

	models "swarm/internal/runtime/core/actors"
)

type ToolDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Schema      any    `json:"schema,omitempty"`
	Usage       string `json:"-"`
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
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ToolCall struct {
	Name      string `json:"name"`
	Arguments any    `json:"arguments,omitempty"`
}

type Response struct {
	Message           Message           `json:"message"`
	ToolCalls         []ToolCall        `json:"tool_calls,omitempty"`
	ObservedToolCalls []ToolCall        `json:"-"`
	SessionID         string            `json:"session_id,omitempty"`
	Raw               []byte            `json:"raw,omitempty"`
	VisibleTools      []string          `json:"visible_tools,omitempty"`
	MCPServers        map[string]string `json:"mcp_servers,omitempty"`
	MCPVisibleTools   []string          `json:"mcp_visible_tools,omitempty"`
}

type Session struct {
	ID                   string
	ProviderSessionID    string
	AgentID              string
	RuntimeMode          string
	ConversationMode     string
	SessionScope         string
	ScopeKey             string
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

type BudgetGuard interface {
	LockExecutionScope(scope string) func()
	IsEntityEmergency(entityID string) bool
	IsEntityThrottle(entityID string) bool
	IsEmergency(entityID string) bool
	IsThrottle(entityID string) bool
	RecordEntityLLMUsage(ctx context.Context, entityID string, agentID string, runtimeMode string, usage UsageTokens, exact bool, meta any) error
	RecordLLMUsage(ctx context.Context, entityID string, agentID string, runtimeMode string, usage UsageTokens, exact bool, meta any) error
}

type Runtime interface {
	StartSession(ctx context.Context, agentID string, systemPrompt string, tools []ToolDefinition) (*Session, error)
	ContinueSession(ctx context.Context, session *Session, message Message) (*Response, error)
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

type NativeToolCapabilityProvider interface {
	NativeToolCapabilities() NativeToolCapabilities
}

type NativeToolStrictProvider interface {
	EnforceProviderNativeToolSupport() bool
}

type StartupVisibleToolSurfaceProber interface {
	ProbeStartupVisibleToolSurface(ctx context.Context, actor models.AgentConfig, systemPrompt string, tools []ToolDefinition) (*Response, error)
}
