package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
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
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
}

// CommitPublicationOwner owns the closed selected-store publication
// operation. Runtime supplies immutable semantic facts and receives immutable
// commit evidence; no callback or transaction capability crosses this port.
type CommitPublicationOwner interface {
	CommitPublication(context.Context, PublicationCommand) (CommittedPublication, error)
}

// APIEventPublicationCommitOwner owns the event.publish-specific atomic
// operation. The selected store commits publication facts and the deterministic
// API completion together without exposing its transaction to the API layer.
type APIEventPublicationCommitOwner interface {
	LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
	CommitAPIEventPublication(context.Context, APIEventPublicationCommand) (CommittedAPIEventPublication, error)
}

type APIEventPublicationCommand struct {
	Publication PublicationCommand
	Idempotency apiidempotency.Request
	Completion  apiidempotency.Completion
}

func (c APIEventPublicationCommand) Validate() error {
	if err := c.Publication.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Idempotency.Method) != "event.publish" {
		return fmt.Errorf("API event publication requires event.publish idempotency authority")
	}
	if strings.TrimSpace(c.Completion.ResourceID) != strings.TrimSpace(c.Publication.Commit.Event.ID()) {
		return fmt.Errorf("API event publication completion resource does not match the publication event")
	}
	if len(c.Completion.Response) == 0 {
		return fmt.Errorf("API event publication completion response is required")
	}
	if strings.TrimSpace(c.Idempotency.IdempotencyKey) != "" &&
		(strings.TrimSpace(c.Idempotency.ActorTokenID) == "" || strings.TrimSpace(c.Idempotency.RequestHash) == "") {
		return fmt.Errorf("API event publication actor token and request hash are required for idempotency")
	}
	return nil
}

type CommittedAPIEventPublication struct {
	Publication CommittedPublication
	Completion  apiidempotency.Completion
	Replay      bool
}

func (r CommittedAPIEventPublication) Validate() error {
	if strings.TrimSpace(r.Completion.ResourceID) == "" || len(r.Completion.Response) == 0 {
		return fmt.Errorf("committed API event publication completion is incomplete")
	}
	if r.Replay {
		return nil
	}
	return r.Publication.Validate()
}

type FlowInstanceActivationCommand struct {
	Plan          runtimepipeline.FlowInstanceActivationPlan
	RouteTopology []FlowInstanceRouteRecordSet
}

func (c FlowInstanceActivationCommand) Validate() error {
	if err := c.Plan.Validate(); err != nil {
		return err
	}
	if err := validateFlowInstanceRouteTopology(c.RouteTopology); err != nil {
		return err
	}
	if len(c.RouteTopology) == 0 {
		return errors.New("flow instance activation requires exact route topology")
	}
	return nil
}

type FlowInstanceActivationCommitOwner interface {
	CommitFlowInstanceActivation(context.Context, FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error)
}

// PublicationCommand is the complete durable publication command after event
// admission and route planning. Activation plans are semantic facts derived by
// the runtime; selected-store adapters persist them atomically with the event.
type PublicationCommand struct {
	Commit              CommitPublishRequest
	Activations         []runtimepipeline.FlowInstanceActivationPlan
	RouteTopology       []FlowInstanceRouteRecordSet
	DynamicFlowCreation *runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest
	AuthorScope         runtimeauthoractivity.Scope
	HasAuthorScope      bool
	AuthorDescriptor    runtimeauthoractivity.EventDescriptor
	HasAuthorDescriptor bool
}

func (c PublicationCommand) Validate() error {
	if err := events.ValidateGenericPublishEvent(c.Commit.Event.Event()); err != nil {
		return err
	}
	if err := events.ValidatePersistentEvent(c.Commit.Event.Event()); err != nil {
		return err
	}
	if err := c.Commit.ValidateRouteSettlement(); err != nil {
		return err
	}
	if c.Commit.RouteSettlement.WriteClass() != events.EventWriteNormalPublication {
		return fmt.Errorf("publication command requires normal publication settlement")
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
	if err := validateFlowInstanceRouteTopology(c.RouteTopology); err != nil {
		return fmt.Errorf("publication route topology: %w", err)
	}
	if len(c.RouteTopology) > 0 && len(c.Activations) == 0 {
		return fmt.Errorf("publication route topology requires a flow activation")
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
	RouteTopology    []FlowInstanceRouteRecordSet
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
	if err := validateFlowInstanceRouteTopology(r.RouteTopology); err != nil {
		return fmt.Errorf("committed route topology: %w", err)
	}
	return nil
}

type CommittedFlowInstanceActivation = runtimepipeline.CommittedFlowInstanceActivation

// PreparedPublishEventReader exposes canonical persisted event facts without
// exposing a query handle. It exists so duplicate planning can reuse stamped
// route identity instead of consulting changed topology.
type PreparedPublishEventReader interface {
	LoadPreparedPublishEvent(context.Context, string) (PreparedPublishEvent, bool, error)
}

type PreparedPublishEvent struct {
	Event          events.AdmittedEvent
	Settlement     events.RouteSettlement
	DeliveryRoutes []events.DeliveryRoute
}

func (p PreparedPublishEvent) Validate() error {
	if strings.TrimSpace(p.Event.ID()) == "" {
		return errors.New("prepared publication event identity is required")
	}
	if err := events.ValidateDeliveryRoutes(p.DeliveryRoutes); err != nil {
		return fmt.Errorf("prepared publication delivery routes: %w", err)
	}
	if err := p.Settlement.Validate(p.DeliveryRoutes); err != nil {
		return fmt.Errorf("prepared publication route settlement: %w", err)
	}
	return nil
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
	RouteSettlement   events.RouteSettlement
	DeliveryRoutes    []events.DeliveryRoute
	DeliveryAuthority runtimedelivery.ExecutionAuthority
	ReplayScope       runtimepipelineobligation.CommittedScope
	PipelineClaim     runtimepipelineobligation.Claim
	Disposition       *runtimepipelineobligation.Disposition
	DeadLetter        *runtimedeadletters.Record
	ReplyCreations    []runtimereplycontext.Record
	ReplyClaims       []runtimereplycontext.ClaimCommand
}

func (r CommitPublishRequest) ValidateRouteSettlement() error {
	return r.RouteSettlement.Validate(r.DeliveryRoutes)
}

type CommitSelectedForkEventRequest struct {
	Commit  CommitPublishRequest
	Lineage runfork.RunForkSelectedContractExecutionLineage
}

type CommittedSelectedForkEvent struct {
	AppendOutcome    EventAppendOutcome
	DeliveryHandoffs []runtimedelivery.DurableHandoffProof
}

func (r CommittedSelectedForkEvent) Validate() error {
	if err := validateEventAppendOutcome(r.AppendOutcome); err != nil {
		return err
	}
	for index, handoff := range r.DeliveryHandoffs {
		if err := handoff.Validate(); err != nil {
			return fmt.Errorf("committed selected-fork delivery handoff %d: %w", index, err)
		}
	}
	return nil
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

func validateFlowInstanceRouteTopology(sets []FlowInstanceRouteRecordSet) error {
	seen := make(map[runtimeflowidentity.Route]struct{}, len(sets))
	for setIndex, set := range sets {
		identity := runtimeflowidentity.StoredRoute(set.Identity.ScopeKey, set.Identity.InstanceID, set.Identity.InstancePath)
		if !identity.Valid() || identity != set.Identity {
			return fmt.Errorf("route set %d requires canonical exact identity", setIndex)
		}
		if _, exists := seen[identity]; exists {
			return fmt.Errorf("route set %d repeats owner %s", setIndex, identity.InstancePath)
		}
		seen[identity] = struct{}{}
		for routeIndex, route := range set.Routes {
			if route.Identity != identity || strings.TrimSpace(route.EventPattern) == "" ||
				strings.TrimSpace(route.SubscriberType) == "" || strings.TrimSpace(route.SubscriberID) == "" {
				return fmt.Errorf("route set %d record %d requires exact owner, event pattern, and subscriber", setIndex, routeIndex)
			}
		}
	}
	return nil
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
	Materializing bool
}

func (d ActiveTargetDescriptor) Normalized() ActiveTargetDescriptor {
	flowInstance := strings.Trim(strings.TrimSpace(d.FlowInstance), "/")
	return ActiveTargetDescriptor{
		ID:            strings.TrimSpace(d.ID),
		EntityID:      strings.TrimSpace(d.EntityID),
		FlowInstance:  flowInstance,
		AddressFields: normalizeDescriptorAddressFields(d.AddressFields),
		Materializing: d.Materializing,
	}
}

// SelectedRunTargetOwnerLister exposes exact receiver ownership rows from the
// selected run. It is deliberately separate from template route descriptors:
// static and root owners come from entity_state, while template descriptors
// additionally carry readiness and address evidence.
type SelectedRunTargetOwnerLister interface {
	ListSelectedRunTargetOwners(ctx context.Context) ([]ActiveTargetDescriptor, error)
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

type InMemoryEventStore struct{}

func (InMemoryEventStore) CommitPublication(_ context.Context, command PublicationCommand) (CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return CommittedPublication{}, err
	}
	if command.DynamicFlowCreation != nil {
		return CommittedPublication{}, errors.New("dynamic flow creation requires a durable selected store")
	}
	result := CommittedPublication{
		AppendOutcome: EventAppendInserted,
		RouteTopology: cloneFlowInstanceRouteTopology(command.RouteTopology),
	}
	for _, plan := range command.Activations {
		result.Activations = append(result.Activations, CommittedFlowInstanceActivation{Plan: plan, Created: true})
	}
	return result, result.Validate()
}

func cloneFlowInstanceRouteTopology(sets []FlowInstanceRouteRecordSet) []FlowInstanceRouteRecordSet {
	if len(sets) == 0 {
		return nil
	}
	out := make([]FlowInstanceRouteRecordSet, len(sets))
	for index, set := range sets {
		out[index] = FlowInstanceRouteRecordSet{
			Identity: set.Identity,
			Routes:   append([]FlowInstanceRouteRecord(nil), set.Routes...),
		}
	}
	return out
}

func (InMemoryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, ErrAuthoritativeRecipientManifestUnavailable
}
