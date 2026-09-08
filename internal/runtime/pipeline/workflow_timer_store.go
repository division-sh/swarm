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
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

const (
	workflowTimerStatusActive    = "active"
	workflowTimerStatusFired     = "fired"
	workflowTimerStatusCancelled = "cancelled"
)

// WorkflowTimerActivation is the only workflow interpretation of a
// task_type=workflow_timer row.
type WorkflowTimerActivation struct {
	Ref                 timeridentity.WorkflowTimerActivationRef
	RunID               string
	EntityID            string
	Route               runtimeflowidentity.Route
	RoutingSource       events.RoutingSource
	OwnerAgent          string
	EventType           string
	ExecutionMode       executionmode.Mode
	Payload             []byte
	FireAt              time.Time
	Recurring           bool
	RecurrenceInterval  time.Duration
	Status              string
	FiredAt             time.Time
	CreatedAt           time.Time
	SourceTimerID       string
	ForkedFromRunID     string
	ForkedFromEventID   string
	ReconstructionOwner string
}

// WorkflowTimerActivationPersistenceRecord is the exact primitive record read
// by a selected-store adapter. Semantic task identity is decoded only here,
// not by SQL adapters.
type WorkflowTimerActivationPersistenceRecord struct {
	ActivationID        string
	TaskID              string
	RunID               string
	EntityID            string
	Route               runtimeflowidentity.Route
	RoutingSource       events.RoutingSource
	EventType           string
	ExecutionMode       executionmode.Mode
	Payload             []byte
	FireAt              time.Time
	Recurring           bool
	RecurrenceInterval  string
	OwnerNode           string
	OwnerAgent          string
	TaskType            string
	Status              string
	FiredAt             time.Time
	CreatedAt           time.Time
	SourceTimerID       string
	ForkedFromRunID     string
	ForkedFromEventID   string
	ReconstructionOwner string
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
		ForkedFromRunID: record.ForkedFromRunID, ForkedFromEventID: record.ForkedFromEventID,
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
	CommitWorkflowTimerReconciliation(context.Context, WorkflowTimerReconciliationCommand) (CommittedWorkflowLifecycleMutation, error)
}

type WorkflowTimerReconciliationCommand struct {
	RunID    string
	Route    runtimeflowidentity.Route
	EntityID string
	Plan     WorkflowLifecycleMutationPlan
}

func (c WorkflowTimerReconciliationCommand) Validate() error {
	c.RunID = strings.TrimSpace(c.RunID)
	c.EntityID = strings.TrimSpace(c.EntityID)
	c.Route = runtimeflowidentity.StoredRoute(c.Route.ScopeKey, c.Route.InstanceID, c.Route.InstancePath)
	if len(c.Plan.Schedules) != 0 || len(c.Plan.GateCards) != 0 {
		return fmt.Errorf("workflow timer reconciliation may contain only timer mutations")
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
	a.SourceTimerID = strings.TrimSpace(a.SourceTimerID)
	a.ForkedFromRunID = strings.TrimSpace(a.ForkedFromRunID)
	a.ForkedFromEventID = strings.TrimSpace(a.ForkedFromEventID)
	a.ReconstructionOwner = strings.TrimSpace(a.ReconstructionOwner)
	if len(a.Payload) == 0 {
		a.Payload = []byte("{}")
	} else {
		a.Payload = append([]byte(nil), a.Payload...)
	}
	a.FireAt = canonicalWorkflowTimerTime(a.FireAt)
	a.FiredAt = canonicalWorkflowTimerTime(a.FiredAt)
	a.CreatedAt = canonicalWorkflowTimerTime(a.CreatedAt)
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
	if a.FireAt.Before(a.CreatedAt) {
		return fmt.Errorf("workflow timer fire_at cannot precede created_at")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(a.Payload, &payload); err != nil || payload == nil {
		return fmt.Errorf("workflow timer business payload must be a JSON object")
	}
	lineageFacts := 0
	for _, fact := range []string{a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromEventID, a.ReconstructionOwner} {
		if fact != "" {
			lineageFacts++
		}
	}
	if lineageFacts != 0 && lineageFacts != 4 {
		return fmt.Errorf("workflow timer fork lineage must be complete or absent")
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

func workflowTimerIntervalString(activation WorkflowTimerActivation) string {
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return ""
	}
	return activation.RecurrenceInterval.String()
}

func requireSameWorkflowTimerActivationFacts(actual, expected WorkflowTimerActivation) error {
	actual, expected = actual.normalized(), expected.normalized()
	if actual.Ref != expected.Ref || actual.RunID != expected.RunID || actual.EntityID != expected.EntityID ||
		actual.Route != expected.Route || actual.RoutingSource.Kind() != expected.RoutingSource.Kind() || actual.RoutingSource.Route() != expected.RoutingSource.Route() || actual.OwnerAgent != expected.OwnerAgent ||
		actual.EventType != expected.EventType || actual.ExecutionMode != expected.ExecutionMode || actual.Recurring != expected.Recurring ||
		actual.RecurrenceInterval != expected.RecurrenceInterval || !actual.CreatedAt.Equal(expected.CreatedAt) ||
		actual.SourceTimerID != expected.SourceTimerID || actual.ForkedFromRunID != expected.ForkedFromRunID ||
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
	firstDue := canonicalWorkflowTimerTime(activation.CreatedAt.Add(activation.RecurrenceInterval))
	if activation.FireAt.Before(firstDue) {
		return false
	}
	return activation.FireAt.Sub(firstDue)%activation.RecurrenceInterval == 0
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
