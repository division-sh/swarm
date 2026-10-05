package tools

import (
	"context"

	corestate "github.com/division-sh/swarm/internal/runtime/core/state"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type MailboxItem = corestate.MailboxItem

type MailboxPersistence interface {
	InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error)
	ListMailboxItems(ctx context.Context, status string, limit int) ([]MailboxItem, error)
	CountMailboxItems(ctx context.Context, status string) (int, error)
	CountUnreadInformationalNotices(ctx context.Context) (int, error)
	GetMailboxItem(ctx context.Context, id string) (MailboxItem, error)
	ExpireMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
}

// EntityPersistence exposes read products only. Live writes use the pipeline's
// entity lock and canonical workflow mutation owner.
type EntityPersistence interface {
	LoadEntityState(ctx context.Context, identity EntityIdentity) (map[string]any, bool, error)
	QueryEntityStates(ctx context.Context, query EntityStateQuery) ([]map[string]any, error)
}

type EntityIdentity struct {
	RunID    string
	EntityID string
}

type EntityFlowScope struct {
	Root               string
	IncludeDescendants bool
}

type EntityFieldEquals struct {
	Path  string
	Value any
}

type EntityStateQuery struct {
	RunID              string
	FlowScope          EntityFlowScope
	RequestedFlowScope EntityFlowScope
	RequestedFlowExact string
	CurrentState       string
	FieldEquals        []EntityFieldEquals
	OrderByCreatedDesc bool
}

type HumanTaskCardStore = decisioncard.HumanTaskAcknowledgedCreationStore
