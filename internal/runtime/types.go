package runtime

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
	ID               string
	AgentID          string
	RuntimeMode      string
	ConversationMode string
	ScopeKey         string
	TurnCount        int
	ParseFailures    int
	SystemPrompt     string
	Tools            []ToolDefinition
	Messages         []Message
}

type LLMRuntime interface {
	StartSession(ctx context.Context, agentID string, systemPrompt string, tools []ToolDefinition) (*Session, error)
	ContinueSession(ctx context.Context, session *Session, message Message) (*Response, error)
}
