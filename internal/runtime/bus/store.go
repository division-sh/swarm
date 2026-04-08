package bus

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"swarm/internal/events"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

type FlowInstanceRouteRecord struct {
	Identity       runtimeflowidentity.Route
	EventPattern   string
	SubscriberType string
	SubscriberID   string
	SourceFlow     string
}

type FlowInstanceRoutePersistence interface {
	UpsertFlowInstanceRoute(ctx context.Context, route FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
	ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error)
}

type FlowInstanceRouteRollbackPersistence interface {
	RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
}

type ActiveAgentDescriptor struct {
	AgentID      string
	EntityID     string
	FlowInstance string
}

func (d ActiveAgentDescriptor) Normalized() ActiveAgentDescriptor {
	return ActiveAgentDescriptor{
		AgentID:      strings.TrimSpace(d.AgentID),
		EntityID:     strings.TrimSpace(d.EntityID),
		FlowInstance: strings.TrimSpace(d.FlowInstance),
	}
}

// ActiveAgentDescriptorLister is an optional capability for runtime delivery
// planning. PostgresStore implements this; InMemoryEventStore does not.
type ActiveAgentDescriptorLister interface {
	ListActiveAgentDescriptors(ctx context.Context) ([]ActiveAgentDescriptor, error)
}

// PipelineReceiptPersistence is an optional capability for marking whether
// persisted events were fully routed/delivered by the runtime publish path.
type PipelineReceiptPersistence interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error
}

// RunLifecyclePersistence is an optional capability for persisting canonical
// terminal run lifecycle state once execution proves the run is done.
type RunLifecyclePersistence interface {
	MarkRunTerminal(ctx context.Context, runID, status, errorSummary string, endedAt time.Time) error
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

type PipelineReceiptSweeperStore = runtimereplayclaim.Lister

type PipelineReplayClaimStore = runtimereplayclaim.Owner

type EventDeliveryReader = runtimereplayclaim.RecipientReader

type InMemoryEventStore struct{}

func (InMemoryEventStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (InMemoryEventStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}
