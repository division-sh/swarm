package tools

import (
	"context"
	"encoding/json"
	"time"

	corestate "github.com/division-sh/swarm/internal/runtime/core/state"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type MailboxItem = corestate.MailboxItem

type MailboxPersistence interface {
	InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error)
	ListMailboxItems(ctx context.Context, status string, limit int) ([]MailboxItem, error)
	CountMailboxItems(ctx context.Context, status string) (int, error)
	CountUnreadInformationalNotices(ctx context.Context) (int, error)
	GetMailboxItem(ctx context.Context, id string) (MailboxItem, error)
	ExpireMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]MailboxItem, error)
	MarkMailboxItemNotified(ctx context.Context, id string) error
}

// EntityPersistence is the backend-neutral store owner for entity tool reads
// and writes. Executors own tool admission; entityruntime owns typed mutations,
// which store implementations evaluate against the locked current snapshot.
type EntityPersistence interface {
	LoadEntityState(ctx context.Context, identity EntityIdentity) (map[string]any, bool, error)
	QueryEntityStates(ctx context.Context, query EntityStateQuery) ([]map[string]any, error)
	SaveEntityField(ctx context.Context, update EntityFieldUpdate) (int, error)
	CreateEntity(ctx context.Context, rec EntityCreateRecord) error
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

type EntityMutationWriter struct {
	Type        string
	ID          string
	HandlerStep string
}

type EntityFieldUpdate struct {
	RunID     string
	EntityID  string
	FieldPath string
	Value     any
	Source    semanticview.Source
	Writer    EntityMutationWriter
}

type EntityCreateRecord struct {
	Source       semanticview.Source
	RunID        string
	EntityID     string
	FlowInstance string
	EntityType   string
	Name         string
	CurrentState string
	FieldsJSON   json.RawMessage
	CreatedAt    time.Time
	Writer       EntityMutationWriter
}

type HumanTaskCardStore = decisioncard.HumanTaskCreationStore
