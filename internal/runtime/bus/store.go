package bus

import (
	"context"
	"database/sql"
	"time"

	"swarm/internal/events"
)

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

type FlowInstanceRouteRecord struct {
	TemplateID     string
	InstanceID     string
	InstancePath   string
	EventPattern   string
	SubscriberType string
	SubscriberID   string
	SourceFlow     string
}

type FlowInstanceRoutePersistence interface {
	UpsertFlowInstanceRoute(ctx context.Context, route FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(ctx context.Context, templateID, instanceID string) error
	ListFlowInstanceRoutes(ctx context.Context) ([]FlowInstanceRouteRecord, error)
}

// ActiveAgentLister is an optional capability for broadcast-style events.
// PostgresStore implements this; InMemoryEventStore does not.
type ActiveAgentLister interface {
	ListActiveAgentIDs(ctx context.Context) ([]string, error)
}

// PipelineReceiptPersistence is an optional capability for marking whether
// persisted events were fully routed/delivered by the runtime publish path.
type PipelineReceiptPersistence interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error
}

// AtomicEventPersistence is an optional capability for transactionally
// persisting an event row and its delivery manifest together.
type AtomicEventPersistence interface {
	PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error
}

// TransactionalEventStore is an optional capability for full publish-time
// transactional semantics: interceptor state writes + event persistence +
// deferred event persistence in one DB transaction.
type TransactionalEventStore interface {
	BeginEventTx(ctx context.Context) (*sql.Tx, error)
	AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error
	InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error
	UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error
}

type PipelineReceiptSweeperStore interface {
	ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.Event, error)
}

type EventDeliveryReader interface {
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
}

type InMemoryEventStore struct{}

func (InMemoryEventStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (InMemoryEventStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}
