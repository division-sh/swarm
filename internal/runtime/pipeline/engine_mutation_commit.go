package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// EnginePublicationPlanner converts engine emission intents into immutable
// persistence plans and consumes only exact post-commit evidence. It does not
// expose a transaction or executable callback.
type EnginePublicationPlanner interface {
	PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error)
	ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error
	FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error
}

// WorkflowEngineStateTransition is the closed atomic relation between the
// selected entity_state row and its exact flow_instances companion.
type WorkflowEngineStateTransition uint8

const (
	WorkflowEngineStateTransitionUnknown WorkflowEngineStateTransition = iota
	WorkflowEngineStateTransitionCreateStateAndCompanion
	WorkflowEngineStateTransitionUpdateStateAndCompanion
	WorkflowEngineStateTransitionUpdateStateCreateCompanion
)

func (t WorkflowEngineStateTransition) Valid() bool {
	switch t {
	case WorkflowEngineStateTransitionCreateStateAndCompanion,
		WorkflowEngineStateTransitionUpdateStateAndCompanion,
		WorkflowEngineStateTransitionUpdateStateCreateCompanion:
		return true
	default:
		return false
	}
}

func (t WorkflowEngineStateTransition) CreatesState() bool {
	return t == WorkflowEngineStateTransitionCreateStateAndCompanion
}

func (t WorkflowEngineStateTransition) UpdatesState() bool {
	return t == WorkflowEngineStateTransitionUpdateStateAndCompanion ||
		t == WorkflowEngineStateTransitionUpdateStateCreateCompanion
}

func (t WorkflowEngineStateTransition) CreatesLifecycleCompanion() bool {
	return t == WorkflowEngineStateTransitionCreateStateAndCompanion ||
		t == WorkflowEngineStateTransitionUpdateStateCreateCompanion
}

func (t WorkflowEngineStateTransition) UpdatesLifecycleCompanion() bool {
	return t == WorkflowEngineStateTransitionUpdateStateAndCompanion
}

func WorkflowEngineStateTransitionForPresence(presence WorkflowTargetPersistencePresence) (WorkflowEngineStateTransition, error) {
	switch presence {
	case WorkflowTargetPersistenceAbsent:
		return WorkflowEngineStateTransitionCreateStateAndCompanion, nil
	case WorkflowTargetPersistenceStateOnly:
		return WorkflowEngineStateTransitionUpdateStateCreateCompanion, nil
	case WorkflowTargetPersistenceComplete:
		return WorkflowEngineStateTransitionUpdateStateAndCompanion, nil
	case WorkflowTargetPersistenceLifecycleOnly:
		return WorkflowEngineStateTransitionUnknown, fmt.Errorf("workflow engine mutation rejects lifecycle companion without state")
	default:
		return WorkflowEngineStateTransitionUnknown, fmt.Errorf("workflow engine mutation requires closed target persistence presence")
	}
}

// WorkflowEngineStateRecord is the complete selected-store projection for one
// workflow state mutation. Runtime owns semantic projection; private adapters
// own SQL representation and compare-and-write mechanics.
type WorkflowEngineStateRecord struct {
	RunID            string
	Route            runtimeflowidentity.Route
	EntityID         string
	WorkflowName     string
	WorkflowVersion  string
	Mode             string
	Status           string
	CurrentState     string
	EntityType       string
	Slug             string
	Name             string
	Fields           json.RawMessage
	Bookkeeping      json.RawMessage
	Gates            json.RawMessage
	Accumulator      json.RawMessage
	Config           json.RawMessage
	InitialFields    json.RawMessage
	EnteredStageAt   time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	TerminatedAt     time.Time
	ExpectedState    string
	ExpectedRevision int64
	Transition       WorkflowEngineStateTransition
}

func (r WorkflowEngineStateRecord) Validate() error {
	r.Route = runtimeflowidentity.StoredRoute(r.Route.ScopeKey, r.Route.InstanceID, r.Route.InstancePath)
	if strings.TrimSpace(r.RunID) == "" || !r.Route.Valid() || strings.TrimSpace(r.EntityID) == "" {
		return fmt.Errorf("workflow engine state record requires exact run, route, and entity identity")
	}
	if strings.TrimSpace(r.WorkflowName) == "" || strings.TrimSpace(r.CurrentState) == "" {
		return fmt.Errorf("workflow engine state record requires workflow and current state")
	}
	if strings.TrimSpace(r.EntityType) == "" {
		return fmt.Errorf("workflow engine state record requires exact entity contract")
	}
	if strings.TrimSpace(r.Mode) == "" || strings.TrimSpace(r.Status) == "" {
		return fmt.Errorf("workflow engine state record requires mode and status")
	}
	for name, raw := range map[string]json.RawMessage{
		"fields": r.Fields, "bookkeeping": r.Bookkeeping, "gates": r.Gates, "accumulator": r.Accumulator, "config": r.Config, "initial_fields": r.InitialFields,
	} {
		if len(raw) == 0 || !json.Valid(raw) {
			return fmt.Errorf("workflow engine state record %s must be valid JSON", name)
		}
	}
	if r.EnteredStageAt.IsZero() || r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() {
		return fmt.Errorf("workflow engine state record requires exact persisted times")
	}
	if r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("workflow engine state record update time %s cannot precede creation %s", r.UpdatedAt.Format(time.RFC3339Nano), r.CreatedAt.Format(time.RFC3339Nano))
	}
	if strings.TrimSpace(r.Status) == "terminated" {
		if r.TerminatedAt.IsZero() {
			return fmt.Errorf("terminated workflow engine state requires exact termination time")
		}
		if r.TerminatedAt.Before(r.CreatedAt) || r.UpdatedAt.Before(r.TerminatedAt) {
			return fmt.Errorf("workflow termination time must be between creation and update time")
		}
	} else if !r.TerminatedAt.IsZero() {
		return fmt.Errorf("non-terminal workflow engine state cannot carry termination time")
	}
	if !r.Transition.Valid() {
		return fmt.Errorf("workflow engine state record requires a closed persistence transition")
	}
	if r.Transition.CreatesState() {
		if r.ExpectedRevision != 0 || strings.TrimSpace(r.ExpectedState) != "" {
			return fmt.Errorf("workflow engine state creation cannot carry an expected existing revision")
		}
	} else if r.ExpectedRevision <= 0 || strings.TrimSpace(r.ExpectedState) == "" {
		return fmt.Errorf("workflow engine state mutation requires exact expected revision and state")
	}
	return nil
}

type WorkflowEngineMutationCommand struct {
	State                   WorkflowEngineStateRecord
	EntitylessTarget        events.DeliveryTargetOwnership
	EntitylessRunID         string
	GateRouteAdmissionRunID string
	Lifecycle               WorkflowLifecycleMutationPlan
	ProposedEffects         []WorkflowEngineProposedEffect
	Publications            []runtimeengine.DurablePublicationPlan
	RouteRetirement         *WorkflowEngineRouteRetirement
	DeliverySuccess         *WorkflowEngineDeliverySuccess
	PostCommit              WorkflowEnginePostCommitPlan
	FanOutIntent            *fanoutobligation.IntentRequest
	FanOutBarrier           *fanoutbarrier.Registration
	FanOutBarrierCompletion *fanoutbarrier.Completion
}

// WorkflowEngineDeliverySuccess declares the exact inbound node claim that
// must become terminal in the same transaction as its successful mutation.
type WorkflowEngineDeliverySuccess struct {
	Claim         runtimedelivery.Claim
	SideEffects   []string
	Duration      time.Duration
	RuleSelection runtimedelivery.HandlerRuleSelectionFact
}

func (s WorkflowEngineDeliverySuccess) Validate(runID string) error {
	if err := s.Claim.Validate(); err != nil {
		return fmt.Errorf("workflow engine delivery success: %w", err)
	}
	if s.Claim.SubscriberClass() != runtimedelivery.SubscriberNode {
		return fmt.Errorf("workflow engine delivery success requires a node claim")
	}
	if s.Claim.RunID() != strings.TrimSpace(runID) {
		return fmt.Errorf("workflow engine delivery run %s disagrees with mutation run %s", s.Claim.RunID(), strings.TrimSpace(runID))
	}
	if s.Duration < 0 {
		return fmt.Errorf("workflow engine delivery success duration cannot be negative")
	}
	if err := s.RuleSelection.Validate(); err != nil {
		return fmt.Errorf("workflow engine delivery success rule selection: %w", err)
	}
	if len(s.SideEffects) != 1 || strings.TrimSpace(s.SideEffects[0]) != "handler_completed" {
		return fmt.Errorf("workflow engine delivery success requires the exact handler_completed effect")
	}
	return nil
}

// WorkflowEngineRouteRetirement declares that the exact persisted route must
// be retired in the same selected-store transaction as terminal workflow state.
type WorkflowEngineRouteRetirement struct {
	Route runtimeflowidentity.Route
}

// WorkflowEnginePostCommitPlan carries semantic work that is legal only after
// the selected-store mutation commits. It contains data, never callbacks.
type WorkflowEnginePostCommitPlan struct {
	FlowDeactivation *WorkflowEngineFlowDeactivation
}

type WorkflowEngineFlowDeactivation struct {
	Route     runtimeflowidentity.Route
	EntityID  string
	NextState string
}

type WorkflowEngineProposedEffect struct {
	Card         decisioncard.Card
	Continuation decisioncard.ProposedEffectContinuation
}

func (p WorkflowEngineProposedEffect) Validate() error {
	if err := p.Card.Validate(); err != nil {
		return err
	}
	return p.Continuation.Canonical().Validate(p.Card)
}

func (c WorkflowEngineMutationCommand) Validate() error {
	entityless := !c.EntitylessTarget.Empty()
	runID := strings.TrimSpace(c.State.RunID)
	if entityless {
		if err := c.EntitylessTarget.Validate(); err != nil {
			return fmt.Errorf("workflow engine entityless target: %w", err)
		}
		if !c.EntitylessTarget.EntitylessReceiver() {
			return fmt.Errorf("workflow engine entityless mutation requires entityless_receiver target ownership")
		}
		runID = strings.TrimSpace(c.EntitylessRunID)
		if runID == "" {
			return fmt.Errorf("workflow engine entityless mutation requires exact run identity")
		}
		if !workflowEngineStateRecordEmpty(c.State) {
			return fmt.Errorf("workflow engine entityless mutation cannot carry workflow state")
		}
		if len(c.Lifecycle.Timers)+len(c.Lifecycle.Schedules)+len(c.Lifecycle.GateCards) > 0 || c.Lifecycle.RequestCompletionCandidate {
			return fmt.Errorf("workflow engine entityless mutation cannot carry lifecycle effects")
		}
		if len(c.ProposedEffects) > 0 {
			return fmt.Errorf("workflow engine entityless mutation cannot carry proposed effects")
		}
		if c.RouteRetirement != nil || c.PostCommit.FlowDeactivation != nil {
			return fmt.Errorf("workflow engine entityless mutation cannot carry lifecycle post-commit work")
		}
	} else {
		if strings.TrimSpace(c.EntitylessRunID) != "" {
			return fmt.Errorf("workflow engine state mutation cannot carry entityless run identity")
		}
		if err := c.State.Validate(); err != nil {
			return err
		}
		if err := c.Lifecycle.Validate(c.State.RunID, c.State.Route, c.State.EntityID); err != nil {
			return fmt.Errorf("workflow engine lifecycle plan: %w", err)
		}
	}
	if gateRunID := strings.TrimSpace(c.GateRouteAdmissionRunID); gateRunID != "" && gateRunID != runID {
		return fmt.Errorf("workflow gate route admission run %s disagrees with engine mutation run %s", gateRunID, runID)
	}
	for index, effect := range c.ProposedEffects {
		if err := effect.Validate(); err != nil {
			return fmt.Errorf("workflow engine proposed effect %d: %w", index, err)
		}
	}
	if c.DeliverySuccess != nil {
		if err := c.DeliverySuccess.Validate(runID); err != nil {
			return err
		}
	}
	if c.FanOutIntent != nil {
		if err := c.FanOutIntent.Validate(); err != nil {
			return fmt.Errorf("workflow engine fan-out intent: %w", err)
		}
		if c.DeliverySuccess == nil {
			return fmt.Errorf("workflow engine fan-out intent requires exact delivery settlement")
		}
		if c.FanOutIntent.Key.RunID != runID || c.FanOutIntent.Key.TriggeringDeliveryID != c.DeliverySuccess.Claim.DeliveryID() {
			return fmt.Errorf("workflow engine fan-out intent disagrees with the settled delivery")
		}
	}
	if c.FanOutBarrier != nil && c.FanOutIntent == nil {
		return fmt.Errorf("workflow engine fan-out delivery barrier must be committed with its exact intent")
	}
	if c.FanOutBarrier != nil {
		if err := c.FanOutBarrier.Validate(); err != nil {
			return fmt.Errorf("workflow engine fan-out barrier: %w", err)
		}
		if c.FanOutBarrier.IntentKey != c.FanOutIntent.Key {
			return fmt.Errorf("workflow engine fan-out barrier disagrees with its exact intent")
		}
	}
	if c.FanOutBarrierCompletion != nil {
		if err := c.FanOutBarrierCompletion.Validate(); err != nil {
			return fmt.Errorf("workflow engine fan-out barrier completion: %w", err)
		}
		key, err := c.FanOutBarrierCompletion.IntentKey(runID)
		if err != nil {
			return err
		}
		if key.RunID != runID || c.DeliverySuccess == nil {
			return fmt.Errorf("workflow engine fan-out barrier completion requires exact delivery settlement")
		}
	}
	seen := make(map[string]struct{}, len(c.Publications))
	for index, publication := range c.Publications {
		if publication == nil {
			return fmt.Errorf("workflow engine publication %d is required", index)
		}
		if err := publication.ValidateDurablePublicationPlan(); err != nil {
			return fmt.Errorf("workflow engine publication %d: %w", index, err)
		}
		eventID := strings.TrimSpace(publication.DurablePublicationEventID())
		if eventID == "" {
			return fmt.Errorf("workflow engine publication %d requires event identity", index)
		}
		if _, exists := seen[eventID]; exists {
			return fmt.Errorf("workflow engine publication repeats event %s", eventID)
		}
		seen[eventID] = struct{}{}
	}
	if retirement := c.RouteRetirement; retirement != nil {
		route := runtimeflowidentity.StoredRoute(retirement.Route.ScopeKey, retirement.Route.InstanceID, retirement.Route.InstancePath)
		if !route.Valid() || route != c.State.Route || c.State.Transition.CreatesState() || strings.TrimSpace(c.State.Status) != "terminated" {
			return fmt.Errorf("workflow engine route retirement requires the exact terminal state route")
		}
	}
	if deactivation := c.PostCommit.FlowDeactivation; deactivation != nil {
		route := runtimeflowidentity.StoredRoute(deactivation.Route.ScopeKey, deactivation.Route.InstanceID, deactivation.Route.InstancePath)
		if !route.Valid() || route != c.State.Route || strings.TrimSpace(deactivation.EntityID) != c.State.EntityID || strings.TrimSpace(deactivation.NextState) == "" {
			return fmt.Errorf("workflow engine post-commit flow deactivation requires exact state identity")
		}
	}
	return nil
}

func workflowEngineStateRecordEmpty(record WorkflowEngineStateRecord) bool {
	return strings.TrimSpace(record.RunID) == "" && !record.Route.Valid() && strings.TrimSpace(record.EntityID) == "" &&
		strings.TrimSpace(record.WorkflowName) == "" && strings.TrimSpace(record.WorkflowVersion) == "" &&
		strings.TrimSpace(record.Mode) == "" && strings.TrimSpace(record.Status) == "" && strings.TrimSpace(record.CurrentState) == "" &&
		strings.TrimSpace(record.EntityType) == "" && strings.TrimSpace(record.Slug) == "" && strings.TrimSpace(record.Name) == "" &&
		len(record.Fields) == 0 && len(record.Bookkeeping) == 0 && len(record.Gates) == 0 && len(record.Accumulator) == 0 && len(record.Config) == 0 && len(record.InitialFields) == 0 &&
		record.EnteredStageAt.IsZero() && record.CreatedAt.IsZero() && record.UpdatedAt.IsZero() && record.TerminatedAt.IsZero() &&
		strings.TrimSpace(record.ExpectedState) == "" && record.ExpectedRevision == 0 && record.Transition == WorkflowEngineStateTransitionUnknown
}

func workflowEngineStateRecord(
	runID string,
	route runtimeflowidentity.Route,
	instance WorkflowInstance,
	expectedState string,
	expectedRevision int64,
	transition WorkflowEngineStateTransition,
	updatedAt time.Time,
) (WorkflowEngineStateRecord, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	if !ok || !route.Valid() || identity.StorageRef != route.InstancePath || identity.InstanceID != route.InstanceID {
		return WorkflowEngineStateRecord{}, fmt.Errorf("workflow engine state identity disagrees with its canonical route")
	}
	projection, err := workflowInstancePersistedProjectionFromInstance(instance, identity.StorageRef)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	fields, err := canonicaljson.Bytes(projection.Fields)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	bookkeeping, err := canonicaljson.Bytes(projection.Bookkeeping)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	gates, err := canonicaljson.Bytes(projection.GatesAny())
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	accumulator, err := canonicaljson.Bytes(projection.Accumulator)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	config, err := canonicaljson.Bytes(projection.ConfigPayload(instance.WorkflowVersion))
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	initialFields, err := canonicaljson.Bytes(instance.InitialFieldValues)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	status := strings.TrimSpace(instance.Status)
	if status == "" {
		status = "active"
	}
	record := WorkflowEngineStateRecord{
		RunID: strings.TrimSpace(runID), Route: route, EntityID: identity.RowID(),
		WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion,
		Mode: workflowInstanceMode(instance), Status: status, CurrentState: instance.CurrentState,
		EntityType: projection.Control.EntityType, Slug: projection.Control.Slug, Name: projection.Control.Name,
		Fields: fields, Bookkeeping: bookkeeping, Gates: gates, Accumulator: accumulator, Config: config, InitialFields: initialFields,
		EnteredStageAt: canonicalWorkflowInstancePersistedTime(instance.EnteredStageAt),
		CreatedAt:      canonicalWorkflowInstancePersistedTime(instance.CreatedAt),
		UpdatedAt:      canonicalWorkflowInstancePersistedTime(updatedAt),
		TerminatedAt:   canonicalWorkflowInstanceOptionalPersistedTime(instance.TerminatedAt),
		ExpectedState:  strings.TrimSpace(expectedState), ExpectedRevision: expectedRevision, Transition: transition,
	}
	if err := record.Validate(); err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	return record, nil
}

func canonicalWorkflowInstanceOptionalPersistedTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return canonicalWorkflowInstancePersistedTime(value)
}

type CommittedWorkflowEngineMutation struct {
	Publications    []runtimeengine.CommittedDurablePublication
	Lifecycle       CommittedWorkflowLifecycleMutation
	RouteRetirement *WorkflowEngineRouteRetirement
	DeliverySuccess *runtimedelivery.Claim
	PostCommit      WorkflowEnginePostCommitPlan
}

func (r CommittedWorkflowEngineMutation) Validate() error {
	for index, publication := range r.Publications {
		if publication == nil {
			return fmt.Errorf("committed workflow engine publication %d is required", index)
		}
		if err := publication.ValidateCommittedDurablePublication(); err != nil {
			return fmt.Errorf("committed workflow engine publication %d: %w", index, err)
		}
	}
	if err := r.Lifecycle.Validate(); err != nil {
		return fmt.Errorf("committed workflow engine lifecycle: %w", err)
	}
	if retirement := r.RouteRetirement; retirement != nil {
		route := runtimeflowidentity.StoredRoute(retirement.Route.ScopeKey, retirement.Route.InstanceID, retirement.Route.InstancePath)
		if !route.Valid() {
			return fmt.Errorf("committed workflow engine route retirement requires exact identity")
		}
	}
	if r.DeliverySuccess != nil {
		if err := r.DeliverySuccess.Validate(); err != nil {
			return fmt.Errorf("committed workflow engine delivery success: %w", err)
		}
		if r.DeliverySuccess.SubscriberClass() != runtimedelivery.SubscriberNode {
			return fmt.Errorf("committed workflow engine delivery success requires a node claim")
		}
	}
	if deactivation := r.PostCommit.FlowDeactivation; deactivation != nil {
		route := runtimeflowidentity.StoredRoute(deactivation.Route.ScopeKey, deactivation.Route.InstanceID, deactivation.Route.InstancePath)
		if !route.Valid() || strings.TrimSpace(deactivation.EntityID) == "" || strings.TrimSpace(deactivation.NextState) == "" {
			return fmt.Errorf("committed workflow engine flow deactivation requires exact identity and state")
		}
	}
	return nil
}

// WorkflowEngineMutationOwner owns the complete state/publication transaction.
// It is implemented by the selected store, never by runtime orchestration.
type WorkflowEngineMutationOwner interface {
	CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error)
}
