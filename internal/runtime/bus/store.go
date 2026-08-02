package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

var (
	ErrAuthoritativeRecipientManifestUnavailable = errors.New("authoritative delivery recipient manifest is unavailable for non-persistent event stores")
	ErrExactDirectRecipientUnavailable           = errors.New("exact direct delivery recipient is unavailable")
)

type EventStore interface {
	CommitPublishOwner
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
}

type CommitPublishOwner interface {
	CommitPublish(ctx context.Context, plan CommitPublishPlan) (PreparedPublish, error)
}

// CommitPublicationOwner owns the closed selected-store publication
// operation. Runtime supplies immutable semantic facts and receives immutable
// commit evidence; no callback or transaction capability crosses this port.
type CommitPublicationOwner interface {
	CommitPublication(context.Context, PublicationCommand) (CommittedPublication, error)
}

// PublicationCommand is the complete durable publication command after event
// admission and route planning. Activation plans are semantic facts derived by
// the runtime; selected-store adapters persist them atomically with the event.
type PublicationCommand struct {
	Commit              CommitPublishRequest
	Activations         []runtimepipeline.FlowInstanceActivationPlan
	DynamicFlowCreation *runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest
	AuthorScope         runtimeauthoractivity.Scope
	HasAuthorScope      bool
	AuthorDescriptor    runtimeauthoractivity.EventDescriptor
	HasAuthorDescriptor bool
}

func (c PublicationCommand) Validate() error {
	if err := events.ValidatePersistentEvent(c.Commit.Event.Event()); err != nil {
		return err
	}
	if c.HasAuthorScope {
		if c.AuthorScope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(c.AuthorScope.RuntimeInstanceID) == "" || strings.TrimSpace(c.AuthorScope.BundleHash) == "" {
			return fmt.Errorf("publication author scope requires exact runtime and bundle identity")
		}
	} else if c.AuthorScope.Kind != "" || strings.TrimSpace(c.AuthorScope.RuntimeInstanceID) != "" || strings.TrimSpace(c.AuthorScope.BundleHash) != "" {
		return fmt.Errorf("publication author scope facts require explicit presence")
	}
	if c.HasAuthorDescriptor {
		if !c.HasAuthorScope {
			return fmt.Errorf("publication author descriptor requires exact author scope")
		}
		if strings.TrimSpace(c.AuthorDescriptor.EventType) != strings.TrimSpace(string(c.Commit.Event.Event().Type())) {
			return fmt.Errorf("publication author descriptor does not match event type")
		}
		if strings.TrimSpace(string(c.AuthorDescriptor.Disposition)) == "" {
			return fmt.Errorf("publication author descriptor requires disposition")
		}
	}
	for index, activation := range c.Activations {
		if err := activation.Validate(); err != nil {
			return fmt.Errorf("publication activation %d: %w", index, err)
		}
	}
	if c.DynamicFlowCreation != nil {
		if err := c.DynamicFlowCreation.Validate(); err != nil {
			return err
		}
		if c.DynamicFlowCreation.Event.ID() != c.Commit.Event.ID() {
			return fmt.Errorf("dynamic flow creation event does not match publication event")
		}
		if len(c.Activations) != 0 {
			return fmt.Errorf("dynamic flow creation publication cannot activate another flow instance")
		}
	}
	return nil
}

// CommittedPublication is exact post-commit evidence. Delivery handoffs are
// transferred to the process-local generation owner only after this value is
// returned.
type CommittedPublication struct {
	AppendOutcome    EventAppendOutcome
	DeliveryHandoffs []runtimedelivery.DurableHandoffProof
	Activations      []CommittedFlowInstanceActivation
}

func (r CommittedPublication) Validate() error {
	if err := validateEventAppendOutcome(r.AppendOutcome); err != nil {
		return err
	}
	for index, handoff := range r.DeliveryHandoffs {
		if err := handoff.Validate(); err != nil {
			return fmt.Errorf("committed delivery handoff %d: %w", index, err)
		}
	}
	for index, activation := range r.Activations {
		if err := activation.Validate(); err != nil {
			return fmt.Errorf("committed flow activation %d: %w", index, err)
		}
	}
	return nil
}

// CommittedFlowInstanceActivation is immutable post-commit evidence that the
// exact activation plan is durable. Runtime may install process-local topology
// only after consuming this evidence.
type CommittedFlowInstanceActivation struct {
	Plan    runtimepipeline.FlowInstanceActivationPlan
	Created bool
}

func (a CommittedFlowInstanceActivation) Validate() error {
	if err := a.Plan.Validate(); err != nil {
		return err
	}
	return nil
}

// PreparedPublishEventReader exposes canonical persisted event facts without
// exposing a query handle. It exists so duplicate planning can reuse stamped
// route identity instead of consulting changed topology.
type PreparedPublishEventReader interface {
	LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error)
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

// CommitPublishRequest is the closed journal operation for event classes whose
// mandatory initial side effects are the delivery manifest, replay scope, and
// optional failure evidence declared here.
type CommitPublishRequest struct {
	Event             events.AdmittedEvent
	DeliveryRoutes    []events.DeliveryRoute
	DeliveryAuthority runtimedelivery.ExecutionAuthority
	DeliveryReceipt   *DeliveryCommitReceipt
	ReplayScope       runtimepipelineobligation.CommittedScope
	PipelineClaim     runtimepipelineobligation.Claim
	Disposition       *runtimepipelineobligation.Disposition
	DeadLetter        *runtimedeadletters.Record
	ReplyCreations    []runtimereplycontext.Record
	ReplyClaims       []runtimereplycontext.ClaimCommand
}

type CommitSelectedForkEventRequest struct {
	Commit  CommitPublishRequest
	Lineage runfork.RunForkSelectedContractExecutionLineage
}

// DeliveryCommitReceipt is the transaction result channel for exact committed
// delivery handoffs. Only the selected-store commit owner may populate it;
// dispatch consumers receive an immutable copy after commit.
type DeliveryCommitReceipt struct {
	mu       sync.Mutex
	recorded bool
	proofs   []runtimedelivery.DurableHandoffProof
}

func newDeliveryCommitReceipt() *DeliveryCommitReceipt {
	return &DeliveryCommitReceipt{}
}

func (r *DeliveryCommitReceipt) Record(proofs []runtimedelivery.DurableHandoffProof) error {
	if r == nil {
		return errors.New("delivery commit receipt is required")
	}
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recorded {
		return errors.New("delivery commit receipt was already recorded")
	}
	r.recorded = true
	r.proofs = append([]runtimedelivery.DurableHandoffProof(nil), proofs...)
	return nil
}

func (r *DeliveryCommitReceipt) Handoffs() ([]runtimedelivery.DurableHandoffProof, error) {
	if r == nil {
		return nil, errors.New("delivery commit receipt is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.recorded {
		return nil, errors.New("delivery commit receipt has not been recorded")
	}
	return append([]runtimedelivery.DurableHandoffProof(nil), r.proofs...), nil
}

// CommitPublishTransaction is the transaction-local half of the sealed
// CommitPublish operation. The opaque values below can only be constructed by
// EventBus, so this capability cannot be used as an alternate event writer.
// Beginning the event before route materialization permits lifecycle writes to
// reference it while finalization still commits every declared initial fact in
// the same selected-store transaction.
type CommitPublishTransaction interface {
	LoadPreparedPublishEvent(ctx context.Context, eventID string) (events.AdmittedEvent, bool, error)
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

// FlowInstanceRouteSetPersistence replaces one materialized route owner's
// complete active record set inside the selected mutation.
type FlowInstanceRouteSetPersistence interface {
	ReplaceFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.Route, routes []FlowInstanceRouteRecord) error
}

// FlowInstanceRouteRecordSet is one exact route owner's complete materialized
// record set within a topology replacement.
type FlowInstanceRouteRecordSet struct {
	Identity runtimeflowidentity.Route
	Routes   []FlowInstanceRouteRecord
}

// FlowInstanceRouteTopologyPersistence atomically replaces every affected
// route owner in one closed selected-store operation.
type FlowInstanceRouteTopologyPersistence interface {
	ReplaceFlowInstanceRouteTopology(ctx context.Context, sets []FlowInstanceRouteRecordSet) error
}

type FlowInstanceRouteRecordReader interface {
	ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.Route) ([]FlowInstanceRouteRecord, error)
}

type FlowInstanceRouteRollbackPersistence interface {
	RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
}

type ActiveAgentDescriptor struct {
	Identity agentidentity.Identity
	EntityID string
}

func (d ActiveAgentDescriptor) Normalized() ActiveAgentDescriptor {
	return ActiveAgentDescriptor{
		Identity: d.Identity.Normalize(),
		EntityID: strings.TrimSpace(d.EntityID),
	}
}

func (d ActiveAgentDescriptor) TargetDescriptor() ActiveTargetDescriptor {
	d = d.Normalized()
	return ActiveTargetDescriptor{
		ID:           d.Identity.AgentID(),
		EntityID:     d.EntityID,
		FlowInstance: d.Identity.FlowInstance(),
	}.Normalized()
}

// ActiveAgentDescriptorLister is an optional capability for runtime delivery
// planning. PostgresStore implements this; InMemoryEventStore does not.
type ActiveAgentDescriptorLister interface {
	ListActiveAgentDescriptors(ctx context.Context) ([]ActiveAgentDescriptor, error)
}

type ActiveFlowInstanceDescriptor struct {
	InstanceID      string
	EntityID        string
	FlowInstance    string
	FlowTemplate    string
	BundleHash      string
	BundleSource    string
	WorkflowVersion string
	AddressFields   map[string]string
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
		InstanceID:      instanceID,
		EntityID:        entityID,
		FlowInstance:    flowInstance,
		FlowTemplate:    strings.TrimSpace(d.FlowTemplate),
		BundleHash:      strings.TrimSpace(d.BundleHash),
		BundleSource:    strings.TrimSpace(d.BundleSource),
		WorkflowVersion: strings.TrimSpace(d.WorkflowVersion),
		AddressFields:   normalizeDescriptorAddressFields(d.AddressFields),
	}
}

func (d ActiveFlowInstanceDescriptor) HasSemanticSource() bool {
	d = d.Normalized()
	return d.BundleHash != "" &&
		(d.BundleSource == "persisted" || d.BundleSource == "ephemeral") &&
		d.WorkflowVersion != ""
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

func (*inMemoryCommitPublishTransaction) LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error) {
	return events.AdmittedEvent{}, false, nil
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
	if request.DeliveryReceipt == nil {
		return errors.New("in-memory event commit requires a delivery receipt")
	}
	if err := request.DeliveryReceipt.Record(nil); err != nil {
		return err
	}
	t.activeEventIDs = t.activeEventIDs[:len(t.activeEventIDs)-1]
	return nil
}

func (InMemoryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, ErrAuthoritativeRecipientManifestUnavailable
}
