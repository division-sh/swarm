package runtime

import (
	"context"
	"encoding/json"
	"time"

	llm "empireai/internal/runtime/llm"

	"empireai/internal/events"
	"empireai/internal/models"
)

type PersistedAgent struct {
	Config          models.AgentConfig
	ParentAgentID   string
	CoordinatorID   string
	Status          string
	HiredBy         string
	TemplateVersion string
	StartedAt       time.Time
}

type PersistedRoutingRule struct {
	VerticalID       string
	EventPattern     string
	SubscriberID     string
	InstalledBy      string
	Reason           string
	Status           string
	Source           string
	BootstrapVersion int
}

type ManagerPersistence interface {
	UpsertAgent(ctx context.Context, rec PersistedAgent) error
	LoadAgents(ctx context.Context) ([]PersistedAgent, error)
	MarkAgentTerminated(ctx context.Context, agentID string) error
	EnsureVerticalSchema(ctx context.Context, verticalID string) error
	LoadLatestOrgTemplate(ctx context.Context) (OrgTemplateRecord, error)
	LoadOrgTemplate(ctx context.Context, version string) (OrgTemplateRecord, error)
	SetVerticalTemplateVersion(ctx context.Context, verticalID, version string) error
	UpsertRoutingRule(ctx context.Context, rule PersistedRoutingRule) error
	LoadRoutingRules(ctx context.Context) ([]PersistedRoutingRule, error)
	DeactivateRoutingRulesByVertical(ctx context.Context, verticalID string) error
	UpsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error
	ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error)
	ListPendingSubscribedEvents(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error)
}

// BootstrapVersionResolver is an optional capability for stores that persist
// bootstrap route baselines. When available, SpawnOpCo can tag installed routes
// with the effective bootstrap version instead of a hardcoded value.
type BootstrapVersionResolver interface {
	ResolveBootstrapVersion(ctx context.Context, templateVersion string) (int, error)
}

// VerticalInfoReader is an optional capability for ManagerPersistence implementations
// to provide vertical metadata used for template placeholder expansion.
type VerticalInfoReader interface {
	GetVerticalInfo(ctx context.Context, verticalID string) (VerticalInfo, bool, error)
}

type VerticalInfo struct {
	ID        string
	Name      string
	Slug      string
	Geography string
	Stage     string
}

// EventReceiptReader is an optional capability for stores to support
// spec-driven dead-letter escalation behavior.
type EventReceiptReader interface {
	GetEventReceipt(ctx context.Context, eventID, agentID string) (EventReceipt, bool, error)
}

type EventReceipt struct {
	EventID    string
	AgentID    string
	Status     string
	RetryCount int
	Error      string
}

type PromptOverrideRecord struct {
	AgentID        string
	Prompt         string
	PreviousPrompt string
	Source         string
	Notes          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// PromptOverridePersistence is optional; when present the manager can apply
// hot-reload prompt overrides without mutating template-backed agent config.
type PromptOverridePersistence interface {
	GetPromptOverride(ctx context.Context, agentID string) (PromptOverrideRecord, bool, error)
	UpsertPromptOverride(ctx context.Context, rec PromptOverrideRecord) error
	DeletePromptOverride(ctx context.Context, agentID string) error
}

type OrgTemplateRecord struct {
	Version         string
	Agents          []byte
	BootstrapRoutes []byte
	SeededRoutes    []byte
	CreatedBy       string
	Description     string
	CreatedAt       time.Time
}

type SchedulePersistence interface {
	UpsertSchedule(ctx context.Context, sc Schedule) error
	CancelSchedule(ctx context.Context, agentID, eventType string) error
	LoadActiveSchedules(ctx context.Context) ([]Schedule, error)
	MarkScheduleFired(ctx context.Context, sc Schedule) error
}

type MailboxItem struct {
	ID            string
	EventID       string
	VerticalID    string
	FromAgent     string
	Type          string
	Priority      string
	Status        string
	Notified      bool
	Context       []byte
	Summary       string
	TimeoutAt     time.Time
	Decision      string
	DecisionNotes string
}

type MailboxPersistence interface {
	InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error)
	ListMailboxItems(ctx context.Context, status string, limit int) ([]MailboxItem, error)
	CountMailboxItems(ctx context.Context, status string) (int, error)
	GetMailboxItem(ctx context.Context, id string) (MailboxItem, error)
	DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error
	ExpireMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	MarkMailboxItemNotified(ctx context.Context, id string) error
}

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

type ScanCampaign struct {
	ID               string
	GeographyID      string
	DirectiveID      string
	Mode             string
	Categories       []string
	Priority         string
	Status           string
	Discoveries      int
	RescanInterval   string
	StrategicContext json.RawMessage
	CreatedAt        time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	DeadlineAt       *time.Time
	NextRescanAt     *time.Time
}

type CreateScanCampaignInput struct {
	GeographyID      string
	DirectiveID      string
	Mode             string
	Categories       []string
	Priority         string
	Status           string
	RescanInterval   string
	StrategicContext json.RawMessage
	DeadlineAt       *time.Time
	NextRescanAt     *time.Time
}

type ScanCampaignFilter struct {
	Status string
	Limit  int
}

type ScanCampaignPersistence interface {
	CreateScanCampaign(ctx context.Context, in CreateScanCampaignInput) (ScanCampaign, error)
	ListScanCampaigns(ctx context.Context, filter ScanCampaignFilter) ([]ScanCampaign, error)

	ClaimNextDueScanCampaign(ctx context.Context) (ScanCampaign, bool, error)
	LookupGeographyLabel(ctx context.Context, geographyID string) (string, error)
	MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error
	RequeueDueRescans(ctx context.Context, now time.Time) (int, error)
	PauseQueuedScanCampaigns(ctx context.Context) (int, error)
	ResumePausedScanCampaigns(ctx context.Context) (int, error)
}

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
