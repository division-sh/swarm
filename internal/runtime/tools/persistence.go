package tools

import (
	"context"
	"encoding/json"
	"time"

	corestate "github.com/division-sh/swarm/internal/runtime/core/state"
)

type MailboxItem = corestate.MailboxItem

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

// EntityPersistence is the backend-neutral store owner for entity tool reads
// and writes. Executor code owns tool semantics; store implementations own SQL.
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
	RunID        string
	EntityID     string
	FieldPath    string
	PathSegments []string
	ValueJSON    json.RawMessage
	Writer       EntityMutationWriter
}

type EntityCreateRecord struct {
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

// HumanTaskPersistence owns human_task_request / human_task_decide mailbox-row
// semantics used by the runtime tool executor.
type HumanTaskPersistence interface {
	CreateHumanTask(ctx context.Context, rec HumanTaskCreateRecord) (string, error)
	HumanTaskRequeueCount(ctx context.Context, taskID string) (int, error)
	CountApprovedHumanTasksSince(ctx context.Context, since time.Time) (int, error)
	DecideHumanTask(ctx context.Context, rec HumanTaskDecisionRecord) error
}

type HumanTaskCreateRecord struct {
	ActorID       string
	EntityID      string
	Category      string
	Description   string
	TalkingPoints json.RawMessage
	ExpectedValue string
	Priority      string
	Deadline      time.Time
}

type HumanTaskDecisionRecord struct {
	TaskID       string
	Status       string
	ActorID      string
	Reason       string
	PriorityRank int
	RequeueDate  string
	DecidedAt    time.Time
}
