package llm

import "context"

type ToolDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Schema      any    `json:"schema,omitempty"`
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
	Message   Message    `json:"message"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	SessionID string     `json:"session_id,omitempty"`
	Raw       []byte     `json:"raw,omitempty"`
}

type Session struct {
	ID                string
	ProviderSessionID string
	AgentID           string
	RuntimeMode       string
	ConversationMode  string
	ScopeKey          string
	TurnCount         int
	ParseFailures     int
	SystemPrompt      string
	Tools             []ToolDefinition
	Messages          []Message
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

type NativeToolCapabilities struct {
	Bash      bool
	WebSearch bool
	FileIO    bool
}

type NativeToolCapabilityProvider interface {
	NativeToolCapabilities() NativeToolCapabilities
}
