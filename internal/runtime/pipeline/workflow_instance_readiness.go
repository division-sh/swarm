package pipeline

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

const dynamicFlowRuntimeReadinessVersion = 4

type DynamicFlowRuntimeAgentExpectation struct {
	Identity       runtimeagentidentity.Identity `json:"identity"`
	ConfigRevision string                        `json:"config_revision"`
}

type DynamicFlowRuntimeCreationEventPlan struct {
	EventID         string                 `json:"event_id"`
	EventType       string                 `json:"event_type"`
	RunID           string                 `json:"run_id"`
	ParentEventID   string                 `json:"parent_event_id"`
	ExecutionMode   executionmode.Mode     `json:"execution_mode"`
	Payload         json.RawMessage        `json:"payload"`
	CreatedAt       time.Time              `json:"created_at"`
	DeliveryContext events.DeliveryContext `json:"delivery_context,omitempty"`
}

type DynamicFlowRuntimeCreationOccurrenceRequest struct {
	RunID        string
	InstancePath string
	Plan         DynamicFlowRuntimeReadinessPlan
	Event        events.Event
	OccurredAt   time.Time
}

func (r DynamicFlowRuntimeCreationOccurrenceRequest) Validate() error {
	r.RunID = strings.TrimSpace(r.RunID)
	r.InstancePath = strings.Trim(strings.TrimSpace(r.InstancePath), "/")
	if _, err := uuid.Parse(r.RunID); err != nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires valid run_id: %w", err)
	}
	if r.InstancePath == "" || r.OccurredAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires exact instance and time")
	}
	expected, err := r.Plan.Normalized()
	if err != nil {
		return fmt.Errorf("normalize dynamic flow runtime creation occurrence plan: %w", err)
	}
	if expected.RunID != r.RunID || expected.Identity.InstancePath != r.InstancePath {
		return fmt.Errorf("dynamic flow runtime creation occurrence identity does not match readiness plan")
	}
	if expected.CreationEvent == nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence plan is missing creation event")
	}
	if strings.TrimSpace(r.Event.ID()) != expected.CreationEvent.EventID ||
		strings.TrimSpace(string(r.Event.Type())) != expected.CreationEvent.EventType ||
		strings.TrimSpace(r.Event.RunID()) != r.RunID {
		return fmt.Errorf("dynamic flow runtime creation occurrence event does not match readiness plan")
	}
	return nil
}

type DynamicFlowRuntimeCreationOccurrencePublisher interface {
	CommitDynamicFlowRuntimeCreationOccurrence(context.Context, DynamicFlowRuntimeCreationOccurrenceRequest) error
}

// DynamicFlowRuntimeReadinessPlan is the durable desired topology and
// creation occurrence for one dynamic flow instance.
type DynamicFlowRuntimeReadinessPlan struct {
	Version         int                                  `json:"version"`
	Identity        runtimeflowidentity.Instance         `json:"identity"`
	RunID           string                               `json:"run_id"`
	BundleHash      string                               `json:"bundle_hash"`
	BundleSource    string                               `json:"bundle_source"`
	WorkflowVersion string                               `json:"workflow_version"`
	ExecutionMode   executionmode.Mode                   `json:"execution_mode"`
	Agents          []DynamicFlowRuntimeAgentExpectation `json:"agents"`
	CreationEvent   *DynamicFlowRuntimeCreationEventPlan `json:"creation_event,omitempty"`
}

type DynamicFlowRuntimeReadiness struct {
	InstancePath           string
	Plan                   DynamicFlowRuntimeReadinessPlan
	OwningRunSource        runtimecorrelation.BundleSourceFact
	RunStatus              string
	InstanceStatus         string
	InstanceTerminatedAt   time.Time
	TopologyReadyAt        time.Time
	CreationEventEmittedAt time.Time
}

type DynamicFlowRuntimeReadinessKey struct {
	RunID        string
	InstancePath string
}

type DynamicFlowRuntimeReadinessProjection struct {
	CurrentCompleted         []DynamicFlowRuntimeReadiness
	CurrentPending           []DynamicFlowRuntimeReadiness
	SourceTransitionRequired []DynamicFlowRuntimeReadiness
}

// DynamicFlowRuntimeReadinessPlanReconciliation carries one exact observed
// row and its desired replacement into the selected-store batch owner.
type DynamicFlowRuntimeReadinessPlanReconciliation struct {
	Observed DynamicFlowRuntimeReadiness
	Expected DynamicFlowRuntimeReadinessPlan
}

type DynamicFlowRuntimeReadinessPlanReconciliationResult struct {
	RunID        string
	InstancePath string
	Changed      bool
}

var ErrDynamicFlowRuntimeReadinessObservationStale = errors.New("dynamic flow runtime readiness observation is stale")

type DynamicFlowRuntimeReadinessObservationConflict struct {
	RunID        string
	InstancePath string
	Coordinate   string
}

func (e *DynamicFlowRuntimeReadinessObservationConflict) Error() string {
	if e == nil {
		return ErrDynamicFlowRuntimeReadinessObservationStale.Error()
	}
	return fmt.Sprintf(
		"%s for %s/%s: %s changed",
		ErrDynamicFlowRuntimeReadinessObservationStale,
		strings.TrimSpace(e.RunID),
		strings.Trim(strings.TrimSpace(e.InstancePath), "/"),
		strings.TrimSpace(e.Coordinate),
	)
}

func (e *DynamicFlowRuntimeReadinessObservationConflict) Unwrap() error {
	return ErrDynamicFlowRuntimeReadinessObservationStale
}

func IsDynamicFlowRuntimeReadinessObservationConflict(err error) bool {
	return errors.Is(err, ErrDynamicFlowRuntimeReadinessObservationStale)
}

// DynamicFlowRuntimeReadinessPersistence owns the complete selected-store
// readiness projection. Runtime consumers receive only typed records and
// named mutations; transaction and query authority remain private.
type DynamicFlowRuntimeReadinessPersistence interface {
	ReconcileDynamicFlowRuntimeReadinessPlans(context.Context, []DynamicFlowRuntimeReadinessPlanReconciliation, time.Time) ([]DynamicFlowRuntimeReadinessPlanReconciliationResult, error)
	LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (DynamicFlowRuntimeReadiness, bool, error)
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.BundleSourceFact) (DynamicFlowRuntimeReadinessProjection, error)
	InspectDynamicFlowRuntimeReadinessForRun(context.Context, string, runtimecorrelation.BundleSourceFact) ([]DynamicFlowRuntimeReadiness, error)
	MarkDynamicFlowRuntimeTopologyReady(context.Context, DynamicFlowRuntimeReadinessPlan, time.Time) error
}

type DynamicFlowRuntimeReadinessPersistenceRecord struct {
	RunID                     string
	InstancePath              string
	Plan                      []byte
	OwningRunBundleHash       string
	OwningRunBundleSource     string
	RunStatus                 string
	InstanceStatus            string
	InstanceTerminatedAt      time.Time
	HasInstanceTerminatedAt   bool
	TopologyReadyAt           time.Time
	HasTopologyReadyAt        bool
	CreationEventEmittedAt    time.Time
	HasCreationEventEmittedAt bool
}

func DecodeDynamicFlowRuntimeReadinessPersistenceRecord(record DynamicFlowRuntimeReadinessPersistenceRecord) (DynamicFlowRuntimeReadiness, error) {
	item, err := decodeDynamicFlowRuntimeReadiness(
		record.RunID,
		record.InstancePath,
		record.Plan,
		record.RunStatus,
		record.InstanceStatus,
		dynamicFlowRuntimeReadinessTime{Time: record.InstanceTerminatedAt, Valid: record.HasInstanceTerminatedAt},
		dynamicFlowRuntimeReadinessTime{Time: record.TopologyReadyAt, Valid: record.HasTopologyReadyAt},
		dynamicFlowRuntimeReadinessTime{Time: record.CreationEventEmittedAt, Valid: record.HasCreationEventEmittedAt},
	)
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, err
	}
	item.OwningRunSource, err = runtimecorrelation.DecodeBundleSourceFact(
		record.OwningRunBundleHash,
		record.OwningRunBundleSource,
	)
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness %s owning run source: %w", record.InstancePath, err)
	}
	return item, nil
}

func (r DynamicFlowRuntimeReadiness) Eligible() bool {
	runState, err := runtimerunlifecycle.ParseState(r.RunStatus)
	return err == nil && runState.Active() &&
		strings.EqualFold(strings.TrimSpace(r.InstanceStatus), "active") &&
		r.InstanceTerminatedAt.IsZero()
}

func (r DynamicFlowRuntimeReadiness) Pending() bool {
	if !r.Eligible() || r.TopologyReadyAt.IsZero() {
		return r.Eligible()
	}
	return r.Plan.CreationEvent != nil && r.CreationEventEmittedAt.IsZero()
}

type dynamicFlowRuntimeReadinessTime struct {
	Time  time.Time
	Valid bool
}

func (t *dynamicFlowRuntimeReadinessTime) Scan(value any) error {
	if t == nil {
		return fmt.Errorf("dynamic flow runtime readiness time scanner is required")
	}
	switch value := value.(type) {
	case nil:
		t.Time = time.Time{}
		t.Valid = false
		return nil
	case time.Time:
		if value.IsZero() {
			t.Time = time.Time{}
			t.Valid = false
			return nil
		}
		t.Time = value.UTC()
		t.Valid = true
		return nil
	case string:
		return t.scanString(value)
	case []byte:
		return t.scanString(string(value))
	default:
		return fmt.Errorf("unsupported dynamic flow runtime readiness time value %T", value)
	}
}

func (t *dynamicFlowRuntimeReadinessTime) scanString(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		t.Time = time.Time{}
		t.Valid = false
		return nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			t.Time = parsed.UTC()
			t.Valid = true
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("parse dynamic flow runtime readiness time %q: %w", value, lastErr)
}

func (p DynamicFlowRuntimeReadinessPlan) Normalized() (DynamicFlowRuntimeReadinessPlan, error) {
	if p.Version != 0 && p.Version != dynamicFlowRuntimeReadinessVersion {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("unsupported dynamic flow runtime readiness version %d", p.Version)
	}
	p.Version = dynamicFlowRuntimeReadinessVersion
	p.RunID = strings.TrimSpace(p.RunID)
	p.WorkflowVersion = strings.TrimSpace(p.WorkflowVersion)
	sourceFact, err := runtimecorrelation.DecodeBundleSourceFact(p.BundleHash, p.BundleSource)
	if err != nil {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness bundle source: %w", err)
	}
	p.BundleHash, p.BundleSource = sourceFact.StorageValues()
	p.Identity.TemplateID = strings.TrimSpace(p.Identity.TemplateID)
	p.Identity.ScopeKey = strings.Trim(strings.TrimSpace(p.Identity.ScopeKey), "/")
	p.Identity.InstanceID = strings.Trim(strings.TrimSpace(p.Identity.InstanceID), "/")
	p.Identity.InstancePath = strings.Trim(strings.TrimSpace(p.Identity.InstancePath), "/")
	p.Identity.EntityID = strings.TrimSpace(p.Identity.EntityID)
	p.Identity.ParentEntityID = strings.TrimSpace(p.Identity.ParentEntityID)
	p.Identity.ParentRoute = p.Identity.ParentRoute.Normalized()
	if !p.Identity.HasStoredPath || !p.Identity.Route().Valid() || p.Identity.TemplateID == "" || p.Identity.EntityID == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires exact flow identity")
	}
	if p.RunID == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires run_id")
	}
	if _, err := uuid.Parse(p.RunID); err != nil {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness run_id: %w", err)
	}
	if p.WorkflowVersion == "" {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires workflow_version")
	}
	if !p.ExecutionMode.Valid() {
		return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness requires typed execution mode authority")
	}
	agents := append([]DynamicFlowRuntimeAgentExpectation(nil), p.Agents...)
	for idx := range agents {
		agents[idx].Identity = agents[idx].Identity.Normalize()
		agents[idx].ConfigRevision = strings.TrimSpace(agents[idx].ConfigRevision)
		if err := agents[idx].Identity.Validate(); err != nil {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf(
				"dynamic flow runtime readiness agent identity: %w",
				err,
			)
		}
		if agents[idx].ConfigRevision == "" {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow runtime readiness agent identity and config revision are required")
		}
		decodedRevision, err := hex.DecodeString(agents[idx].ConfigRevision)
		if err != nil || len(decodedRevision) != 32 {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf(
				"dynamic flow runtime readiness agent %s has invalid config revision",
				agents[idx].Identity.Description(),
			)
		}
	}
	sort.Slice(agents, func(i, j int) bool {
		return runtimeagentidentity.Less(agents[i].Identity, agents[j].Identity)
	})
	for idx := 1; idx < len(agents); idx++ {
		if agents[idx-1].Identity == agents[idx].Identity {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf(
				"dynamic flow runtime readiness has duplicate agent %s",
				agents[idx].Identity.Description(),
			)
		}
	}
	p.Agents = agents
	if p.CreationEvent != nil {
		event := *p.CreationEvent
		event.EventID = strings.TrimSpace(event.EventID)
		event.EventType = strings.TrimSpace(event.EventType)
		event.RunID = strings.TrimSpace(event.RunID)
		event.ParentEventID = strings.TrimSpace(event.ParentEventID)
		event.CreatedAt = canonicalWorkflowInstancePersistedTime(event.CreatedAt)
		event.DeliveryContext = event.DeliveryContext.Normalized()
		if _, err := uuid.Parse(event.EventID); err != nil {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event_id: %w", err)
		}
		if event.EventType == "" || event.RunID != p.RunID || event.ParentEventID == "" || !event.ExecutionMode.Valid() || event.CreatedAt.IsZero() {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event requires exact type, lineage, execution mode, and occurrence time")
		}
		if event.ExecutionMode != p.ExecutionMode {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event execution mode disagrees with readiness authority")
		}
		if len(event.Payload) == 0 || !json.Valid(event.Payload) {
			return DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("dynamic flow creation event requires valid payload")
		}
		p.CreationEvent = &event
	}
	return p, nil
}

func decodeDynamicFlowRuntimeReadiness(
	runID string,
	instancePath string,
	raw []byte,
	runStatus string,
	instanceStatus string,
	instanceTerminatedAt dynamicFlowRuntimeReadinessTime,
	topologyReadyAt dynamicFlowRuntimeReadinessTime,
	creationEventEmittedAt dynamicFlowRuntimeReadinessTime,
) (DynamicFlowRuntimeReadiness, error) {
	var plan DynamicFlowRuntimeReadinessPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("decode dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	if plan.Version != dynamicFlowRuntimeReadinessVersion {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness %s has unsupported version %d", instancePath, plan.Version)
	}
	normalized, err := plan.Normalized()
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("validate dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	if normalized.RunID != strings.TrimSpace(runID) {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness run identity mismatch for %s", instancePath)
	}
	if normalized.Identity.InstancePath != strings.Trim(strings.TrimSpace(instancePath), "/") {
		return DynamicFlowRuntimeReadiness{}, fmt.Errorf("dynamic flow runtime readiness identity mismatch for %s", instancePath)
	}
	item := DynamicFlowRuntimeReadiness{
		InstancePath: instancePath, Plan: normalized,
		RunStatus: strings.TrimSpace(runStatus), InstanceStatus: strings.TrimSpace(instanceStatus),
	}
	if instanceTerminatedAt.Valid {
		item.InstanceTerminatedAt = instanceTerminatedAt.Time.UTC()
	}
	if topologyReadyAt.Valid {
		item.TopologyReadyAt = topologyReadyAt.Time.UTC()
	}
	if creationEventEmittedAt.Valid {
		item.CreationEventEmittedAt = creationEventEmittedAt.Time.UTC()
	}
	return item, nil
}
