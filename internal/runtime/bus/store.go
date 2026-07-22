package bus

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type EventStore interface {
	CommitPublishOwner
	runtimereplayclaim.RecipientReader
}

type CommitPublishOwner interface {
	CommitPublish(ctx context.Context, plan CommitPublishPlan) (PreparedPublish, error)
}

// CommitPublishPlan is a sealed EventBus-owned publication plan. Selected
// stores execute only this exact semantic plan inside their transaction; no
// caller-supplied function or transaction capability crosses the boundary.
type CommitPublishPlan interface {
	PrepareCommitPublish(context.Context) (PreparedPublish, error)
	commitPublishPlan()
}

type EventAppendOutcome uint8

const (
	EventAppendOutcomeUnknown EventAppendOutcome = iota
	EventAppendInserted
	EventAppendExactDuplicate
)

type InitialPipelineReceipt struct {
	Status  string
	Failure *runtimefailures.Envelope
}

// CommitPublishRequest is the closed journal operation for event classes whose
// mandatory initial side effects are the delivery manifest, replay scope, and
// optional failure evidence declared here.
type CommitPublishRequest struct {
	Event           events.AdmittedEvent
	DeliveryRoutes  []events.DeliveryRoute
	ReplayScope     runtimereplayclaim.CommittedReplayScope
	PipelineReceipt *InitialPipelineReceipt
	DeadLetter      *runtimedeadletters.Record
}

// CommitPublishTransaction is the transaction-local half of the sealed
// CommitPublish operation. The opaque values below can only be constructed by
// EventBus, so this capability cannot be used as an alternate event writer.
// Beginning the event before route materialization permits lifecycle writes to
// reference it while finalization still commits every declared initial fact in
// the same selected-store transaction.
type CommitPublishTransaction interface {
	BeginPreparedPublish(ctx context.Context, event PreparedPublishEvent) (EventAppendOutcome, error)
	FinalizePreparedPublish(ctx context.Context, finalization PreparedPublishFinalization) error
}

type PreparedPublishEvent struct {
	event events.AdmittedEvent
}

func (e PreparedPublishEvent) AdmittedEvent() events.AdmittedEvent {
	return e.event
}

type PreparedPublishFinalization struct {
	request CommitPublishRequest
}

func (f PreparedPublishFinalization) Request() CommitPublishRequest {
	return f.request
}

type commitPublishTransactionContextKey struct{}

func WithCommitPublishTransaction(ctx context.Context, transaction CommitPublishTransaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, commitPublishTransactionContextKey{}, transaction)
}

func CommitPublishTransactionFromContext(ctx context.Context) (CommitPublishTransaction, bool) {
	if ctx == nil {
		return nil, false
	}
	transaction, ok := ctx.Value(commitPublishTransactionContextKey{}).(CommitPublishTransaction)
	return transaction, ok && transaction != nil
}

func WithoutCommitPublishTransaction(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, commitPublishTransactionContextKey{}, nil)
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

type ProcessedPipelineReceiptReader interface {
	HasProcessedPipelineReceipt(ctx context.Context, eventID string) (bool, error)
}

// RunLifecyclePersistence owns explicit failed, cancelled, and forked
// terminalization. Successful completion is owned by normal-run convergence.
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

type EventDeliveryTargetReader interface {
	ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error)
}

type EventDeliveryRouteSetReader interface {
	ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error)
}

type PipelineReceiptSweeperStore = runtimereplayclaim.Lister

type PipelineReplayClaimStore = runtimereplayclaim.Owner

type InMemoryEventStore struct{}

func (s InMemoryEventStore) CommitPublish(ctx context.Context, plan CommitPublishPlan) (PreparedPublish, error) {
	if plan == nil {
		return PreparedPublish{}, errors.New("event publish plan is required")
	}
	transaction := &inMemoryCommitPublishTransaction{}
	return commitPublishInMemory(ctx, plan, transaction)
}

func commitPublishInMemory(ctx context.Context, plan CommitPublishPlan, transaction CommitPublishTransaction) (PreparedPublish, error) {
	postCommit := make([]runtimepipeline.OwnerAction, 0, 4)
	rollback := make([]runtimepipeline.OwnerAction, 0, 4)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
	prepared, err := plan.PrepareCommitPublish(WithCommitPublishTransaction(ctx, transaction))
	if err != nil {
		runtimepipeline.FlushPipelineRollbackActions(rollback)
		return PreparedPublish{}, err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return prepared, nil
}

type inMemoryCommitPublishTransaction struct {
	activeEventIDs []string
}

func (t *inMemoryCommitPublishTransaction) BeginPreparedPublish(_ context.Context, event PreparedPublishEvent) (EventAppendOutcome, error) {
	admitted := event.AdmittedEvent()
	if err := events.ValidateGenericPublishEvent(admitted.Event()); err != nil {
		return EventAppendOutcomeUnknown, err
	}
	if err := events.ValidatePersistentEvent(admitted.Event()); err != nil {
		return EventAppendOutcomeUnknown, err
	}
	eventID := strings.TrimSpace(admitted.ID())
	if eventID == "" {
		return EventAppendOutcomeUnknown, errors.New("admitted event is required")
	}
	t.activeEventIDs = append(t.activeEventIDs, eventID)
	return EventAppendInserted, nil
}

func (t *inMemoryCommitPublishTransaction) FinalizePreparedPublish(_ context.Context, finalization PreparedPublishFinalization) error {
	request := finalization.Request()
	if len(t.activeEventIDs) == 0 || t.activeEventIDs[len(t.activeEventIDs)-1] != request.Event.ID() {
		return errors.New("prepared event finalization does not match the active event")
	}
	t.activeEventIDs = t.activeEventIDs[:len(t.activeEventIDs)-1]
	return nil
}

func (InMemoryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
}
func (InMemoryEventStore) SupportsPersistedReplay() bool { return false }
