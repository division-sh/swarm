package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

// EnginePublicationPlanner converts engine emission intents into immutable
// persistence plans and consumes only exact post-commit evidence. It does not
// expose a transaction or executable callback.
type EnginePublicationPlanner interface {
	PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error)
	ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error
	FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error
}

type EngineMutationPublicationPlanner interface {
	EnginePublicationPlanner
	PrepareEngineMutationPublications(context.Context, []runtimeengine.EmitIntent, PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error)
}

// WorkflowEngineStateTransition is the closed atomic relation between the
// selected entity_state row and its exact flow_instances companion.
type WorkflowEngineStateTransition uint8

const (
	WorkflowEngineStateTransitionUnknown WorkflowEngineStateTransition = iota
	WorkflowEngineStateTransitionCreateStateAndCompanion
	WorkflowEngineStateTransitionUpdateStateAndCompanion
	WorkflowEngineStateTransitionPreserveStateAndCompanion
)

func (t WorkflowEngineStateTransition) Valid() bool {
	switch t {
	case WorkflowEngineStateTransitionCreateStateAndCompanion,
		WorkflowEngineStateTransitionUpdateStateAndCompanion,
		WorkflowEngineStateTransitionPreserveStateAndCompanion:
		return true
	default:
		return false
	}
}

func (t WorkflowEngineStateTransition) CreatesState() bool {
	return t == WorkflowEngineStateTransitionCreateStateAndCompanion
}

func (t WorkflowEngineStateTransition) UpdatesState() bool {
	return t == WorkflowEngineStateTransitionUpdateStateAndCompanion
}

func (t WorkflowEngineStateTransition) PreservesState() bool {
	return t == WorkflowEngineStateTransitionPreserveStateAndCompanion
}

func WorkflowEngineStateTransitionForPresence(presence WorkflowTargetPersistencePresence) (WorkflowEngineStateTransition, error) {
	switch presence {
	case WorkflowTargetPersistenceAbsent:
		return WorkflowEngineStateTransitionUnknown, fmt.Errorf("ordinary workflow mutation requires a constructed header")
	case WorkflowTargetPersistenceStateOnly:
		return WorkflowEngineStateTransitionUnknown, fmt.Errorf("ordinary workflow mutation cannot repair imported state without construction")
	case WorkflowTargetPersistenceComplete, WorkflowTargetPersistenceCompleteFieldless:
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
	Identity         runtimeflowidentity.RunScopedFlowInstance
	EntityID         string
	ParentInstance   string
	InstanceKey      string
	WorkflowName     string
	WorkflowVersion  string
	Mode             string
	Status           string
	CurrentState     string
	StageDefined     bool
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
	if err := r.validateConstructionIdentity(); err != nil {
		return err
	}
	if strings.TrimSpace(r.WorkflowName) == "" || strings.TrimSpace(r.CurrentState) == "" {
		return fmt.Errorf("workflow engine state record requires workflow and current state")
	}
	if strings.TrimSpace(r.EntityType) == "" {
		var fields map[string]any
		if err := json.Unmarshal(r.Fields, &fields); err != nil || fields == nil || len(fields) != 0 {
			return fmt.Errorf("fieldless workflow header requires an empty field projection")
		}
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

func (r WorkflowEngineStateRecord) validateConstructionIdentity() error {
	r.Identity = r.Identity.Normalize()
	if err := r.Identity.Validate(); err != nil || strings.TrimSpace(r.EntityID) == "" {
		return fmt.Errorf("workflow engine state record requires exact run, route, and entity identity")
	}
	construction, err := DecodeWorkflowInstanceRecordedHeader(r.Identity.Route, r.Config)
	if err != nil {
		return err
	}
	if construction.ParentRoute().FlowInstance != r.ParentInstance {
		return fmt.Errorf("workflow engine state parent disagrees with exact construction identity")
	}
	if (r.Mode == "template") != (r.InstanceKey != "") {
		return fmt.Errorf("workflow engine state key presence disagrees with its declaration mode")
	}
	return nil
}

type WorkflowEngineMutationCommand struct {
	State                   WorkflowEngineStateRecord
	Writer                  *mutationlog.Writer
	WriterSource            correlation.SourceArtifactFact
	AcceptedEvent           *workflowlifecycle.Effect
	AcceptedEventSource     correlation.SourceArtifactFact
	GateRouteAdmissionRunID string
	Lifecycle               WorkflowLifecycleMutationPlan
	ProposedEffects         []WorkflowEngineProposedEffect
	Publications            []runtimeengine.DurablePublicationPlan
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

// WorkflowEnginePostCommitPlan carries semantic work that is legal only after
// the selected-store mutation commits. It contains data, never callbacks.
type WorkflowEnginePostCommitPlan struct {
	FlowDeactivation *WorkflowEngineFlowDeactivation
}

type WorkflowEngineFlowDeactivation struct {
	Identity  runtimeflowidentity.RunScopedFlowInstance
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
	if !c.State.Transition.UpdatesState() && !c.State.Transition.PreservesState() {
		return fmt.Errorf("ordinary workflow mutation requires a constructed target; construction and companion repair are not execution permissions")
	}
	runID := strings.TrimSpace(c.State.Identity.RunID)
	if err := c.State.Validate(); err != nil {
		return err
	}
	if c.Writer != nil {
		if c.Writer.Type != "agent" || strings.TrimSpace(c.Writer.ID) == "" || c.Writer.HandlerStep != "save_entity_field" || !c.State.Transition.UpdatesState() || c.State.Status != "active" {
			return fmt.Errorf("entity tool mutation requires exact agent writer attribution")
		}
		if err := c.WriterSource.Validate(); err != nil {
			return fmt.Errorf("entity tool mutation source: %w", err)
		}
		if c.AcceptedEvent != nil || c.GateRouteAdmissionRunID != "" || len(c.ProposedEffects) != 0 || len(c.Publications) != 0 ||
			c.DeliverySuccess != nil || c.PostCommit.FlowDeactivation != nil ||
			c.FanOutIntent != nil || c.FanOutBarrier != nil || c.FanOutBarrierCompletion != nil ||
			c.Lifecycle.StageEntry != nil || len(c.Lifecycle.Timers) != 0 || len(c.Lifecycle.Schedules) != 0 || len(c.Lifecycle.GateCards) != 0 {
			return fmt.Errorf("entity tool attribution permits only the exact field mutation")
		}
	} else if c.WriterSource != (correlation.SourceArtifactFact{}) {
		return fmt.Errorf("entity tool source requires its attributed writer")
	}
	if err := c.Lifecycle.ValidateState(c.State); err != nil {
		return fmt.Errorf("workflow engine lifecycle plan: %w", err)
	}
	if c.State.Transition.PreservesState() {
		if err := c.AcceptedEventSource.Validate(); err != nil {
			return fmt.Errorf("accepted-event preservation source: %w", err)
		}
		cause := c.AcceptedEvent
		if cause == nil {
			return fmt.Errorf("preserved constructed state requires exact accepted-event evidence")
		}
		_, transition := cause.Transition()
		if cause.Kind() != workflowlifecycle.KindAcceptedEvent || transition || cause.Route() != c.State.Identity.Route ||
			cause.EntityID().String() != c.State.EntityID || cause.EventID() == "" || cause.EventType() == "" ||
			cause.OccurredAt().IsZero() || !cause.ExecutionMode().Valid() {
			return fmt.Errorf("preserved constructed state has inconsistent accepted-event evidence")
		}
		if c.State.ExpectedState != c.State.CurrentState || c.Lifecycle.StageEntry != nil ||
			len(c.Lifecycle.Timers) == 0 || len(c.Lifecycle.Schedules) != 0 || len(c.Lifecycle.GateCards) != 0 ||
			len(c.ProposedEffects) != 0 || len(c.Publications) != 0 ||
			c.PostCommit.FlowDeactivation != nil || c.FanOutIntent != nil || c.FanOutBarrier != nil || c.FanOutBarrierCompletion != nil {
			return fmt.Errorf("preserved constructed state permits only accepted-event timer reactions and exact settlement")
		}
	} else if c.AcceptedEvent != nil || c.AcceptedEventSource != (correlation.SourceArtifactFact{}) {
		return fmt.Errorf("accepted-event preservation evidence requires the preservation projection")
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
		if c.FanOutBarrier.IntentKey != c.FanOutIntent.Key || c.FanOutBarrier.PlanRef != c.FanOutIntent.PlanRef {
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
	if deactivation := c.PostCommit.FlowDeactivation; deactivation != nil {
		if c.State.Status != "terminated" {
			return fmt.Errorf("stage mutation cannot declare operational flow retirement")
		}
		identity := deactivation.Identity.Normalize()
		if identity.Validate() != nil || identity != c.State.Identity || strings.TrimSpace(deactivation.EntityID) != c.State.EntityID || strings.TrimSpace(deactivation.NextState) == "" {
			return fmt.Errorf("workflow engine post-commit flow deactivation requires exact state identity")
		}
	}
	return nil
}

func workflowEngineStateRecord(
	owner runtimeflowidentity.RunScopedFlowInstance,
	instance WorkflowInstance,
	expectedState string,
	expectedRevision int64,
	transition WorkflowEngineStateTransition,
	updatedAt time.Time,
) (WorkflowEngineStateRecord, error) {
	owner = owner.Normalize()
	if err := owner.Validate(); err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	route := owner.Route
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
	fields, err := canonicaljson.MarshalPreservingNumberKinds(projection.Fields)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	bookkeeping, err := canonicaljson.MarshalPreservingNumberKinds(projection.Bookkeeping)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	gates, err := canonicaljson.MarshalPreservingNumberKinds(projection.GatesAny())
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	accumulator, err := canonicaljson.MarshalPreservingNumberKinds(projection.Accumulator)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	config, err := canonicaljson.MarshalPreservingNumberKinds(projection.ConfigPayload(instance.WorkflowVersion))
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	initialFields, err := canonicaljson.MarshalPreservingNumberKinds(instance.InitialFieldValues)
	if err != nil {
		return WorkflowEngineStateRecord{}, err
	}
	status := strings.TrimSpace(instance.Status)
	if status == "" {
		status = "active"
	}
	record := WorkflowEngineStateRecord{
		Identity: owner, EntityID: identity.RowID(),
		ParentInstance: instance.ParentFlowInstance, InstanceKey: instance.InstanceKey,
		WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion,
		Mode: workflowInstanceMode(instance), Status: status, CurrentState: instance.CurrentState, StageDefined: instance.StageDefined,
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
	// Committed is set only after the selected store acknowledges COMMIT.
	Committed       bool
	Stage           runtimeengine.CommittedStage
	Publications    []runtimeengine.CommittedDurablePublication
	Lifecycle       CommittedWorkflowLifecycleMutation
	DeliverySuccess *runtimedelivery.Claim
	PostCommit      WorkflowEnginePostCommitPlan
}

func (r CommittedWorkflowEngineMutation) Validate() error {
	if !r.Committed {
		return fmt.Errorf("workflow engine mutation requires commit acknowledgment")
	}
	if err := r.Stage.Validate(); err != nil {
		return err
	}
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
	if r.DeliverySuccess != nil {
		if err := r.DeliverySuccess.Validate(); err != nil {
			return fmt.Errorf("committed workflow engine delivery success: %w", err)
		}
		if r.DeliverySuccess.SubscriberClass() != runtimedelivery.SubscriberNode {
			return fmt.Errorf("committed workflow engine delivery success requires a node claim")
		}
	}
	if deactivation := r.PostCommit.FlowDeactivation; deactivation != nil {
		if deactivation.Identity.Validate() != nil || strings.TrimSpace(deactivation.EntityID) == "" || strings.TrimSpace(deactivation.NextState) == "" {
			return fmt.Errorf("committed workflow engine flow deactivation requires exact identity and state")
		}
	}
	return nil
}

// CommittedWorkflowStage projects the exact CAS result. Only an acknowledged
// mutation owner may attach this value to a committed result.
func CommittedWorkflowStage(record WorkflowEngineStateRecord) (runtimeengine.CommittedStage, error) {
	if err := record.Validate(); err != nil {
		return runtimeengine.CommittedStage{}, err
	}
	revision := record.ExpectedRevision
	switch record.Transition {
	case WorkflowEngineStateTransitionCreateStateAndCompanion:
		revision = 1
	case WorkflowEngineStateTransitionUpdateStateAndCompanion:
		if revision == math.MaxInt64 {
			return runtimeengine.CommittedStage{}, fmt.Errorf("workflow stage revision exhausted")
		}
		revision++
	}
	return runtimeengine.CommittedStage{
		Instance: record.Identity, EntityID: record.EntityID,
		Stage: record.CurrentState, StageDefined: record.StageDefined, Revision: revision, UpdatedAt: record.UpdatedAt.UTC(),
	}, nil
}

// WorkflowEngineMutationOwner owns the complete state/publication transaction.
// It is implemented by the selected store, never by runtime orchestration.
type WorkflowEngineMutationOwner interface {
	CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error)
}
