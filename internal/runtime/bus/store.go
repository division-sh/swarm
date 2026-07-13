package bus

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

func (d ActiveAgentDescriptor) TargetDescriptor() ActiveTargetDescriptor {
	d = d.Normalized()
	return ActiveTargetDescriptor{
		ID:           d.AgentID,
		EntityID:     d.EntityID,
		FlowInstance: d.FlowInstance,
	}.Normalized()
}

// ActiveAgentDescriptorLister is an optional capability for runtime delivery
// planning. PostgresStore implements this; InMemoryEventStore does not.
type ActiveAgentDescriptorLister interface {
	ListActiveAgentDescriptors(ctx context.Context) ([]ActiveAgentDescriptor, error)
}

type ActiveFlowInstanceDescriptor struct {
	InstanceID    string
	EntityID      string
	FlowInstance  string
	FlowTemplate  string
	AddressFields map[string]string
}

func (d ActiveFlowInstanceDescriptor) Normalized() ActiveFlowInstanceDescriptor {
	flowInstance := strings.Trim(strings.TrimSpace(d.FlowInstance), "/")
	instanceID := strings.TrimSpace(d.InstanceID)
	if flowInstance == "" {
		flowInstance = strings.Trim(strings.TrimSpace(instanceID), "/")
	}
	if instanceID == "" && flowInstance != "" {
		instanceID = runtimeflowidentity.LogicalInstanceID(flowInstance)
	}
	entityID := strings.TrimSpace(d.EntityID)
	if entityID == "" && flowInstance != "" {
		entityID = runtimeflowidentity.EntityID(flowInstance)
	}
	return ActiveFlowInstanceDescriptor{
		InstanceID:    instanceID,
		EntityID:      entityID,
		FlowInstance:  flowInstance,
		FlowTemplate:  strings.TrimSpace(d.FlowTemplate),
		AddressFields: normalizeDescriptorAddressFields(d.AddressFields),
	}
}

func (d ActiveFlowInstanceDescriptor) TargetDescriptor() ActiveTargetDescriptor {
	d = d.Normalized()
	return ActiveTargetDescriptor{
		ID:            d.InstanceID,
		EntityID:      d.EntityID,
		FlowInstance:  d.FlowInstance,
		AddressFields: normalizeDescriptorAddressFields(d.AddressFields),
	}.Normalized()
}

// ActiveFlowInstanceDescriptorLister exposes active dynamic flow instances as
// routable target descriptors. Stores implement this from persisted flow
// instance state, not from live subscriptions or readback.
type ActiveFlowInstanceDescriptorLister interface {
	ListActiveFlowInstanceDescriptors(ctx context.Context) ([]ActiveFlowInstanceDescriptor, error)
}

type ActiveTargetDescriptor struct {
	ID            string
	EntityID      string
	FlowInstance  string
	AddressFields map[string]string
}

func (d ActiveTargetDescriptor) Normalized() ActiveTargetDescriptor {
	flowInstance := strings.Trim(strings.TrimSpace(d.FlowInstance), "/")
	entityID := strings.TrimSpace(d.EntityID)
	if entityID == "" && flowInstance != "" {
		entityID = runtimeflowidentity.EntityID(flowInstance)
	}
	return ActiveTargetDescriptor{
		ID:            strings.TrimSpace(d.ID),
		EntityID:      entityID,
		FlowInstance:  flowInstance,
		AddressFields: normalizeDescriptorAddressFields(d.AddressFields),
	}
}

func normalizeDescriptorAddressFields(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// PipelineReceiptPersistence is an optional capability for marking whether
// persisted events were fully routed/delivered by the runtime publish path.
type PipelineReceiptPersistence interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error
}

// RunLifecyclePersistence is an optional capability for persisting canonical
// terminal run lifecycle state once execution proves the run is done.
type RunLifecyclePersistence interface {
	MarkRunTerminal(ctx context.Context, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (RunLifecycleSnapshot, error)
}

type StandaloneRuntimePlatformRunConvergencePersistence interface {
	ConvergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error
}

type NormalRunCompletionConvergencePersistence interface {
	ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error
}

type RunLifecycleSnapshot struct {
	RunID       string
	Status      string
	EventCount  int
	EntityCount int
	Failure     *runtimefailures.Envelope
	StartedAt   time.Time
	EndedAt     *time.Time
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

type eventMutationContextKey struct{}

// EventMutation is the typed event-publish unit of work consumed by runtime
// producers. Backend SQL transaction details stay below this semantic boundary.
type EventMutation interface {
	Context() context.Context
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
	InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error
	InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error
	UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error
	UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error
	RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error
}

// InboundDeliveryMutation extends the selected-store event mutation with the
// external delivery identity claim. The marker and every event/lifecycle write
// performed through the mutation commit or roll back together.
type InboundDeliveryMutation interface {
	EventMutation
	ClaimInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error)
}

type EventMutationRunner interface {
	RunEventMutation(ctx context.Context, fn func(EventMutation) error) error
}

type EventMutationContextProvider interface {
	EventMutationFromContext(ctx context.Context) (EventMutation, bool)
}

func WithEventMutationContext(ctx context.Context, mutation EventMutation) context.Context {
	if mutation == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, eventMutationContextKey{}, mutation)
}

func EventMutationFromContext(ctx context.Context) (EventMutation, bool) {
	if ctx == nil {
		return nil, false
	}
	mutation, ok := ctx.Value(eventMutationContextKey{}).(EventMutation)
	return mutation, ok && mutation != nil
}

// WithoutEventMutationContext crosses the commit boundary without retaining a
// mutation whose transaction is no longer usable. Post-commit consumers must
// acquire a fresh mutation from their own active transaction.
func WithoutEventMutationContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, eventMutationContextKey{}, nil)
}

type TransactionalEventReplayScopePersistence interface {
	UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error
}

type TransactionalInboundEventPersistence interface {
	ClaimInboundEventTx(ctx context.Context, tx *sql.Tx, providerEventID, entityID, provider string) (bool, error)
}

// TransactionalEventStore is the backend-local raw-SQL helper used below
// EventMutation implementations. Selected runtime producers consume
// EventMutationRunner/EventMutation instead of this raw transaction shape.
type TransactionalEventStore interface {
	BeginEventTx(ctx context.Context) (*sql.Tx, error)
	AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error
	InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error
	UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error
}

type EventTransactionRunner interface {
	RunEventTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error
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
