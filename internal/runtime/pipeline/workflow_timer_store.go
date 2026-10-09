package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
)

const (
	workflowTimerStatusActive    = "active"
	workflowTimerStatusFired     = "fired"
	workflowTimerStatusCancelled = "cancelled"
)

type WorkflowTimerCancelCause = timerobligation.WorkflowTimerCancelCause

const WorkflowTimerCancelCauseRuleRemoved = timerobligation.WorkflowTimerCancelCauseRuleRemoved

// WorkflowTimerActivation is the only workflow interpretation of a
// task_type=workflow_timer row.
type WorkflowTimerActivation struct {
	Ref                     timeridentity.WorkflowTimerActivationRef
	RunID                   string
	EntityID                string
	Route                   runtimeflowidentity.Route
	RoutingSource           events.RoutingSource
	OwnerAgent              string
	EventType               string
	ExecutionMode           executionmode.Mode
	Payload                 []byte
	FireAt                  time.Time
	Recurring               bool
	RecurrenceInterval      time.Duration
	Status                  string
	FiredAt                 time.Time
	CancelCause             WorkflowTimerCancelCause
	CancelledAt             time.Time
	CreatedAt               time.Time
	SourceTimerID           string
	ForkedFromRunID         string
	ForkedFromPointKind     forkpoint.Kind
	ForkedFromPointRevision int64
	ForkedFromEventID       string
	// SourceArmedAt retains the original arm; CreatedAt remains the child row's birth.
	SourceArmedAt       time.Time
	ReconstructionOwner string
}

// WorkflowTimerActivationPersistenceRecord is the exact primitive record read
// by a selected-store adapter. Semantic task identity is decoded only here,
// not by SQL adapters.
type WorkflowTimerActivationPersistenceRecord = timerobligation.WorkflowTimerActivationRecord

func (a WorkflowTimerActivation) PersistenceRecord() WorkflowTimerActivationPersistenceRecord {
	a = a.Canonical()
	interval := ""
	if a.Recurring {
		interval = a.RecurrenceInterval.String()
	}
	return WorkflowTimerActivationPersistenceRecord{
		ActivationID: a.Ref.ActivationID, TaskID: a.Ref.TaskID(), RunID: a.RunID, EntityID: a.EntityID,
		Route: a.Route, RoutingSource: a.RoutingSource, EventType: a.EventType, ExecutionMode: a.ExecutionMode, Payload: a.Payload,
		FireAt: a.FireAt, Recurring: a.Recurring, RecurrenceInterval: interval, OwnerAgent: a.OwnerAgent, TaskType: workflowTimerTaskFamily,
		Status: a.Status, FiredAt: a.FiredAt, CancelCause: a.CancelCause, CancelledAt: a.CancelledAt, CreatedAt: a.CreatedAt,
		SourceTimerID: a.SourceTimerID, ForkedFromRunID: a.ForkedFromRunID, ForkedFromPointKind: a.ForkedFromPointKind,
		ForkedFromPointRevision: a.ForkedFromPointRevision, ForkedFromEventID: a.ForkedFromEventID,
		SourceArmedAt: a.SourceArmedAt, ReconstructionOwner: a.ReconstructionOwner,
	}
}

func DecodeWorkflowTimerActivationPersistenceRecord(record WorkflowTimerActivationPersistenceRecord) (WorkflowTimerActivation, error) {
	if strings.TrimSpace(record.TaskType) != workflowTimerTaskFamily {
		return WorkflowTimerActivation{}, fmt.Errorf("timer row %s is not a workflow timer family", record.ActivationID)
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(record.TaskID)
	if !ok || ref.ActivationID != strings.TrimSpace(record.ActivationID) {
		return WorkflowTimerActivation{}, fmt.Errorf("timer row %s has invalid workflow activation discriminator", record.ActivationID)
	}
	if strings.TrimSpace(record.OwnerNode) != "" || strings.TrimSpace(record.OwnerAgent) == "" {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s has invalid owner columns", record.ActivationID)
	}
	activation := WorkflowTimerActivation{
		Ref: ref, RunID: record.RunID, EntityID: record.EntityID, Route: record.Route,
		RoutingSource: record.RoutingSource, OwnerAgent: record.OwnerAgent, EventType: record.EventType, ExecutionMode: record.ExecutionMode, Payload: record.Payload,
		FireAt: record.FireAt, Recurring: record.Recurring, Status: record.Status,
		FiredAt: record.FiredAt, CreatedAt: record.CreatedAt, SourceTimerID: record.SourceTimerID,
		CancelCause: record.CancelCause, CancelledAt: record.CancelledAt,
		ForkedFromRunID: record.ForkedFromRunID, ForkedFromEventID: record.ForkedFromEventID,
		ForkedFromPointKind: record.ForkedFromPointKind, ForkedFromPointRevision: record.ForkedFromPointRevision,
		SourceArmedAt:       record.SourceArmedAt,
		ReconstructionOwner: record.ReconstructionOwner,
	}
	if interval := strings.TrimSpace(record.RecurrenceInterval); interval != "" {
		value, ok := timeridentity.ParseDelayDuration(interval)
		if !ok {
			return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s has invalid recurrence interval %q", record.ActivationID, interval)
		}
		activation.RecurrenceInterval = value
	}
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, err
	}
	return activation, nil
}

// WorkflowTimerActivationPersistence is the selected-store owner for durable
// timer activation readback and declaration reconciliation. It exposes no
// transaction, query executor, or executable post-commit callback.
type WorkflowTimerActivationPersistence interface {
	LoadWorkflowTimerActivation(context.Context, string) (WorkflowTimerActivation, bool, error)
	ListWorkflowTimerActivations(context.Context, string, string, bool) ([]WorkflowTimerActivation, error)
	ListActiveWorkflowTimerActivationsForRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) ([]WorkflowTimerActivation, error)
	CommitWorkflowTimerReconciliation(context.Context, WorkflowTimerReconciliationCommand) (CommittedWorkflowLifecycleMutation, error)
}

type WorkflowTimerReconciliationCommand struct {
	RunID             string
	Route             runtimeflowidentity.Route
	EntityID          string
	Plan              WorkflowLifecycleMutationPlan
	ActivationAttempt *DynamicFlowRuntimeActivationAttempt
}

func (c WorkflowTimerReconciliationCommand) Validate() error {
	c.RunID = strings.TrimSpace(c.RunID)
	c.EntityID = strings.TrimSpace(c.EntityID)
	c.Route = runtimeflowidentity.StoredRoute(c.Route.ScopeKey, c.Route.InstanceID, c.Route.InstancePath)
	if len(c.Plan.Schedules) != 0 || len(c.Plan.GateCards) != 0 {
		return fmt.Errorf("workflow timer reconciliation may contain only timer mutations")
	}
	if c.ActivationAttempt != nil {
		if err := c.ActivationAttempt.Validate(); err != nil {
			return err
		}
		if c.ActivationAttempt.RunID() != c.RunID || c.ActivationAttempt.InstancePath() != c.Route.InstancePath {
			return fmt.Errorf("workflow timer reconciliation attempt differs from instance")
		}
	}
	return c.Plan.Validate(c.RunID, c.Route, c.EntityID)
}

func (a WorkflowTimerActivation) Canonical() WorkflowTimerActivation { return a.normalized() }

func (a WorkflowTimerActivation) Validate() error { return a.validate() }

func (a WorkflowTimerActivation) Occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	return a.occurrence()
}

func (a WorkflowTimerActivation) normalized() WorkflowTimerActivation {
	a.Ref = a.Ref.Normalize()
	a.RunID = strings.TrimSpace(a.RunID)
	a.EntityID = strings.TrimSpace(a.EntityID)
	a.Route = runtimeflowidentity.StoredRoute(a.Route.ScopeKey, a.Route.InstanceID, a.Route.InstancePath)
	a.OwnerAgent = strings.TrimSpace(a.OwnerAgent)
	a.EventType = strings.TrimSpace(a.EventType)
	a.ExecutionMode = executionmode.Mode(strings.TrimSpace(string(a.ExecutionMode)))
	a.Status = strings.ToLower(strings.TrimSpace(a.Status))
	a.CancelCause = WorkflowTimerCancelCause(strings.TrimSpace(string(a.CancelCause)))
	a.SourceTimerID = strings.TrimSpace(a.SourceTimerID)
	a.ForkedFromRunID = strings.TrimSpace(a.ForkedFromRunID)
	a.ForkedFromPointKind = forkpoint.Kind(strings.TrimSpace(string(a.ForkedFromPointKind)))
	a.ForkedFromEventID = strings.TrimSpace(a.ForkedFromEventID)
	a.ReconstructionOwner = strings.TrimSpace(a.ReconstructionOwner)
	if len(a.Payload) == 0 {
		a.Payload = []byte("{}")
	} else {
		a.Payload = append([]byte(nil), a.Payload...)
	}
	a.FireAt = canonicalWorkflowTimerTime(a.FireAt)
	a.FiredAt = canonicalWorkflowTimerTime(a.FiredAt)
	a.CancelledAt = canonicalWorkflowTimerTime(a.CancelledAt)
	a.CreatedAt = canonicalWorkflowTimerTime(a.CreatedAt)
	a.SourceArmedAt = canonicalWorkflowTimerTime(a.SourceArmedAt)
	return a
}

func (a WorkflowTimerActivation) validate() error {
	a = a.normalized()
	if !a.Ref.Valid() || a.Ref.ActivationID == "" {
		return fmt.Errorf("workflow timer activation identity is required")
	}
	if a.RunID == "" || a.EntityID == "" || !a.Route.Valid() {
		return fmt.Errorf("workflow timer activation requires run, entity, and exact route scope")
	}
	switch a.RoutingSource.Kind() {
	case events.RoutingSourceRoot:
		if a.RoutingSource.Route() != (events.RouteIdentity{EntityID: a.EntityID}) {
			return fmt.Errorf("root workflow timer activation requires its exact persisted entity source")
		}
	case events.RoutingSourceFlowOwnedControl:
		if a.RoutingSource.Route().FlowInstance != a.Route.InstancePath || a.RoutingSource.Route().EntityID != a.EntityID {
			return fmt.Errorf("flow workflow timer activation requires its exact persisted flow source")
		}
	default:
		return fmt.Errorf("workflow timer activation requires exact root or flow-owned routing provenance")
	}
	if a.OwnerAgent == "" || a.EventType == "" {
		return fmt.Errorf("workflow timer activation requires owner agent and fire event")
	}
	if !a.ExecutionMode.Valid() {
		return fmt.Errorf("workflow timer activation execution_mode %q is invalid", a.ExecutionMode)
	}
	if _, err := events.AdmitRuntimeControlEventType(events.EventType(a.EventType), a.RoutingSource); err != nil {
		return fmt.Errorf("workflow timer activation event/source admission: %w", err)
	}
	if a.FireAt.IsZero() || a.CreatedAt.IsZero() {
		return fmt.Errorf("workflow timer activation requires created_at and fire_at")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(a.Payload, &payload); err != nil || payload == nil {
		return fmt.Errorf("workflow timer business payload must be a JSON object")
	}
	hasLineage := a.SourceTimerID != "" || a.ForkedFromRunID != "" || a.ForkedFromPointKind != "" ||
		a.ForkedFromPointRevision != 0 || a.ForkedFromEventID != "" || a.ReconstructionOwner != "" || !a.SourceArmedAt.IsZero()
	if hasLineage {
		if a.SourceTimerID == "" || a.ForkedFromRunID == "" || a.ReconstructionOwner == "" || a.SourceArmedAt.IsZero() {
			return fmt.Errorf("workflow timer fork lineage requires exact source timer, run, point, owner, and armed_at")
		}
		if err := forkpoint.ValidateIdentity(a.ForkedFromPointKind, a.ForkedFromPointRevision, a.ForkedFromEventID); err != nil {
			return fmt.Errorf("workflow timer fork source point: %w", err)
		}
		if a.SourceTimerID == a.Ref.ActivationID || a.ForkedFromRunID == a.RunID {
			return fmt.Errorf("workflow timer fork lineage must identify a different source timer and run")
		}
		if a.SourceArmedAt.After(a.CreatedAt) {
			return fmt.Errorf("workflow timer source armed_at cannot follow child created_at")
		}
	}
	if a.FireAt.Before(a.armedAt()) {
		return fmt.Errorf("workflow timer fire_at cannot precede its original arming coordinate")
	}
	if a.Recurring && a.RecurrenceInterval <= 0 {
		return fmt.Errorf("recurring workflow timer requires a positive interval")
	}
	if a.Recurring && !workflowTimerRecurringCoordinateValid(a) {
		return fmt.Errorf("recurring workflow timer fire_at is outside its persisted occurrence lattice")
	}
	if !a.Recurring && a.RecurrenceInterval != 0 {
		return fmt.Errorf("one-shot workflow timer cannot carry recurrence")
	}
	if a.CancelCause == "" {
		if !a.CancelledAt.IsZero() {
			return fmt.Errorf("workflow timer cancellation cause and time must be stamped together")
		}
	} else {
		if !a.CancelCause.Valid() || a.CancelledAt.IsZero() || a.Status != workflowTimerStatusCancelled {
			return fmt.Errorf("workflow timer requires a known cause and exact time only on cancelled status")
		}
		if !hasLineage || a.CancelledAt.Before(a.CreatedAt) || (!a.FiredAt.IsZero() && a.CancelledAt.Before(a.FiredAt)) {
			return fmt.Errorf("rule_removed cancellation requires inherited lineage and cannot precede child birth or accepted fire")
		}
	}
	if a.Recurring {
		if a.Status != workflowTimerStatusActive && a.Status != workflowTimerStatusCancelled {
			return fmt.Errorf("recurring workflow timer has unreachable status %q", a.Status)
		}
		if !a.FiredAt.IsZero() {
			previousDue := canonicalWorkflowTimerTime(a.FireAt.Add(-a.RecurrenceInterval))
			if a.FiredAt.Before(previousDue) {
				return fmt.Errorf("recurring workflow timer fired_at precedes its previous occurrence")
			}
		}
		return nil
	}
	switch a.Status {
	case workflowTimerStatusActive, workflowTimerStatusCancelled:
		if !a.FiredAt.IsZero() {
			return fmt.Errorf("unfired one-shot workflow timer cannot carry fired_at")
		}
	case workflowTimerStatusFired:
		if a.FiredAt.IsZero() || a.FiredAt.Before(a.FireAt) {
			return fmt.Errorf("fired one-shot workflow timer requires fired_at at or after fire_at")
		}
	default:
		return fmt.Errorf("workflow timer activation has unsupported status %q", a.Status)
	}
	return nil
}

// Empty cause/time retains ordinary lifecycle cancellation; rule removal carries
// the exact child-history disposition, not a new arming or firing coordinate.
func (a WorkflowTimerActivation) ValidateCancellation(cause WorkflowTimerCancelCause, at time.Time) error {
	if err := a.Validate(); err != nil {
		return err
	}
	a.Status, a.CancelCause, a.CancelledAt = workflowTimerStatusCancelled, cause, at
	return a.Validate()
}

func (a WorkflowTimerActivation) occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	a = a.normalized()
	return timeridentity.WorkflowTimerOccurrenceRef{Activation: a.Ref, DueAt: a.FireAt}.Normalize()
}

func canonicalWorkflowTimerTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func (s *workflowInstanceStore) loadPersistedWorkflowTimerActivation(ctx context.Context, activationID string) (WorkflowTimerActivation, bool, error) {
	if s == nil || s.timerActivations == nil {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation reader is required")
	}
	return s.timerActivations.LoadWorkflowTimerActivation(ctx, activationID)
}

func (s *workflowInstanceStore) listPersistedWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]WorkflowTimerActivation, error) {
	if s == nil || s.timerActivations == nil {
		return nil, fmt.Errorf("workflow timer activation reader is required")
	}
	return s.timerActivations.ListWorkflowTimerActivations(ctx, runID, entityID, activeOnly)
}

func (s *workflowInstanceStore) listActiveWorkflowTimerActivationsForRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]WorkflowTimerActivation, error) {
	if s == nil || s.timerActivations == nil {
		return nil, fmt.Errorf("workflow timer activation reader is required")
	}
	return s.timerActivations.ListActiveWorkflowTimerActivationsForRoute(ctx, identity)
}

func workflowTimerIntervalString(activation WorkflowTimerActivation) string {
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return ""
	}
	return activation.RecurrenceInterval.String()
}

// ValidateCauseReplay compares current durable facts with the original cause,
// not the authority to execute a current occurrence or cancel an activation.
func (actual WorkflowTimerActivation) ValidateCauseReplay(expected WorkflowTimerActivation) error {
	actual, expected = actual.normalized(), expected.normalized()
	if err := actual.validate(); err != nil {
		return fmt.Errorf("persisted workflow timer cause replay: %w", err)
	}
	if err := expected.validate(); err != nil {
		return fmt.Errorf("requested workflow timer cause replay: %w", err)
	}
	if actual.Ref != expected.Ref || actual.RunID != expected.RunID || actual.EntityID != expected.EntityID ||
		actual.Route != expected.Route || actual.RoutingSource.Kind() != expected.RoutingSource.Kind() || actual.RoutingSource.Route() != expected.RoutingSource.Route() || actual.OwnerAgent != expected.OwnerAgent ||
		actual.EventType != expected.EventType || actual.ExecutionMode != expected.ExecutionMode || actual.Recurring != expected.Recurring ||
		actual.RecurrenceInterval != expected.RecurrenceInterval || !actual.CreatedAt.Equal(expected.CreatedAt) ||
		actual.SourceTimerID != expected.SourceTimerID || actual.ForkedFromRunID != expected.ForkedFromRunID ||
		actual.ForkedFromPointKind != expected.ForkedFromPointKind || actual.ForkedFromPointRevision != expected.ForkedFromPointRevision ||
		!actual.SourceArmedAt.Equal(expected.SourceArmedAt) ||
		actual.ForkedFromEventID != expected.ForkedFromEventID || actual.ReconstructionOwner != expected.ReconstructionOwner ||
		!workflowTimerJSONEqual(actual.Payload, expected.Payload) || !workflowTimerReplayCoordinateMatches(actual, expected) {
		return fmt.Errorf("workflow timer activation %s conflicts with persisted facts", expected.Ref.ActivationID)
	}
	return nil
}

func workflowTimerReplayCoordinateMatches(actual, expected WorkflowTimerActivation) bool {
	if !expected.Recurring {
		return actual.FireAt.Equal(expected.FireAt)
	}
	if expected.RecurrenceInterval <= 0 || actual.FireAt.Before(expected.FireAt) {
		return false
	}
	return actual.FireAt.Sub(expected.FireAt)%expected.RecurrenceInterval == 0
}

func workflowTimerRecurringCoordinateValid(activation WorkflowTimerActivation) bool {
	activation = activation.normalized()
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return false
	}
	firstDue := canonicalWorkflowTimerTime(activation.armedAt().Add(activation.RecurrenceInterval))
	if activation.FireAt.Before(firstDue) {
		return false
	}
	return activation.FireAt.Sub(firstDue)%activation.RecurrenceInterval == 0
}

func (a WorkflowTimerActivation) armedAt() time.Time {
	if a.SourceTimerID != "" {
		return a.SourceArmedAt
	}
	return a.CreatedAt
}

func workflowTimerJSONEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return bytes.Equal(left, right)
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return bytes.Equal(leftJSON, rightJSON)
}
