package bus

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
	runtimereplayclaim.RecipientReader
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

type StandaloneRuntimePlatformRunConvergencePersistence interface {
	ConvergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error
}

type NormalRunCompletionConvergencePersistence interface {
	ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error
}

type RunLifecycleSnapshot struct {
	RunID        string
	Status       string
	EventCount   int
	EntityCount  int
	ErrorSummary string
	StartedAt    time.Time
	EndedAt      *time.Time
}

type RunLifecycleReadPersistence interface {
	LoadRunLifecycleSnapshot(ctx context.Context, runID string) (RunLifecycleSnapshot, error)
}

// AtomicEventPersistence is an optional capability for transactionally
// persisting an event row and its delivery manifest together.
type AtomicEventPersistence interface {
	PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error
}

type AtomicEventReplayScopePersistence interface {
	PersistEventWithDeliveriesAndScope(ctx context.Context, evt events.Event, agentIDs []string, scope runtimereplayclaim.CommittedReplayScope) error
}

type AtomicEventRoutePersistence interface {
	PersistEventWithDeliveryRoutesAndScope(ctx context.Context, evt events.Event, agentIDs []string, deliveryTargets map[string]events.RouteIdentity, scope runtimereplayclaim.CommittedReplayScope) error
}

type AtomicEventDeliveryRouteSetPersistence interface {
	PersistEventWithDeliveryRouteSetAndScope(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error
}

type EventDeliveryRoutePersistence interface {
	InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error
}

type EventDeliveryRouteSetPersistence interface {
	InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error
}

type EventDeliveryTargetReader interface {
	ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error)
}

type EventDeliveryRouteSetReader interface {
	ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error)
}

type EventReplayScopePersistence interface {
	UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error
}

type TransactionalEventReplayScopePersistence interface {
	UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error
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

type TransactionalEventDeliveryRoutePersistence interface {
	InsertEventDeliveriesWithTargetsTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error
}

type TransactionalEventDeliveryRouteSetPersistence interface {
	InsertEventDeliveryRoutesTx(ctx context.Context, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error
}

type PipelineReceiptSweeperStore = runtimereplayclaim.Lister

type PipelineReplayClaimStore = runtimereplayclaim.Owner

type InMemoryEventStore struct{}

func (InMemoryEventStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (InMemoryEventStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}
func (InMemoryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
}
func (InMemoryEventStore) SupportsPersistedReplay() bool { return false }
