package runtime

import (
	"context"
	"time"

	llm "empireai/internal/runtime/llm"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
)

type SchedulePersistence = runtimepipeline.SchedulePersistence

type MailboxItem = runtimetools.MailboxItem

type MailboxPersistence = runtimetools.MailboxPersistence

type InboundPersistence interface {
	RecordInboundEvent(ctx context.Context, providerEventID, verticalID, provider string) (bool, error)
	ResolveInboundTarget(ctx context.Context, verticalKey, provider string) (InboundTarget, error)
	PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error)
}

type InboundTarget struct {
	VerticalID    string
	VerticalSlug  string
	WebhookSecret string
}

type VerticalDigestRow struct {
	VerticalID     string
	Name           string
	Stage          string
	UsersTotal     int
	MRRCents       int
	SpendCents30d  int
	LastMetricDate time.Time
}

type DigestPersistence interface {
	CountActiveVerticals(ctx context.Context) (int, error)
	ListVerticalDigestRows(ctx context.Context, limit int) ([]VerticalDigestRow, error)
}

type ScanCampaign = runtimepipeline.ScanCampaign

type CreateScanCampaignInput = runtimepipeline.CreateScanCampaignInput

type ScanCampaignFilter = runtimepipeline.ScanCampaignFilter

type ScanCampaignPersistence = runtimepipeline.ScanCampaignPersistence

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
	AgentID   string
	ScopeKey  string
	TaskID    string
	Mode      string
	Messages  []llm.Message
	Summary   string
	TurnCount int
	Status    string
}

type ConversationPersistence interface {
	UpsertConversation(ctx context.Context, rec ConversationRecord) error
	LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (ConversationRecord, bool, error)
}
