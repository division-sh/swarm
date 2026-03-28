package llm

import (
	"context"
	"time"
)

type AgentTurnRecord struct {
	AgentID        string
	RuntimeMode    string
	SessionID      string
	TaskID         string
	RequestPayload []byte
	ResponseRaw    []byte
	ParseOK        bool
	Latency        time.Duration
	RetryCount     int
	Error          string
}

type TurnPersistence interface {
	AppendAgentTurn(ctx context.Context, rec AgentTurnRecord) error
}

type ConversationRecord struct {
	SessionID string
	AgentID   string
	ScopeKey  string
	TaskID    string
	Mode      string
	Messages  []Message
	Summary   string
	TurnCount int
	Status    string
}

type ConversationPersistence interface {
	UpsertConversation(ctx context.Context, rec ConversationRecord) error
	LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (ConversationRecord, bool, error)
}
