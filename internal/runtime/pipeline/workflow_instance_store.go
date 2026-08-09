package pipeline

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityquery"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	"github.com/lib/pq"
)

func (s *workflowInstanceStore) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if s == nil || s.timerObligations == nil {
		return runtimetimerobligation.Snapshot{}, fmt.Errorf("timer obligation reader requires workflow store")
	}
	return s.timerObligations.ReadTimerObligations(ctx, scope, observedAt)
}

type WorkflowInstance struct {
	InstanceID         string
	StorageRef         string
	WorkflowName       string
	WorkflowVersion    string
	RuntimeReadiness   *DynamicFlowRuntimeReadinessPlan
	Status             string
	TerminatedAt       time.Time
	CurrentState       string
	Revision           int64
	Config             map[string]any
	EnteredStageAt     time.Time
	TransitionHistory  []WorkflowTransitionRecord
	StateBuckets       map[string]any
	Metadata           map[string]any
	CreatedAt          time.Time
	UpdatedAt          time.Time
	InitialFieldValues map[string]any
}

// WorkflowInstancePersistenceReader owns exact selected-store workflow reads.
// Runtime consumers receive final semantic values and never select a backend,
// query shape, transaction, or SQL executor.
type WorkflowInstancePersistenceReader interface {
	LoadWorkflowInstance(context.Context, runtimeflowidentity.Route) (WorkflowInstance, bool, error)
	ListWorkflowInstances(context.Context) ([]WorkflowInstance, error)
	SelectActiveWorkflowInstances(context.Context, string, []WorkflowInstanceFieldSelector, []string) ([]WorkflowInstance, error)
}

// WorkflowInstancePersistenceRecord is the exact backend-neutral row shape
// consumed by the semantic workflow decoder. Store adapters scan backend
// values into this value; runtime owns interpretation of workflow metadata.
type WorkflowInstancePersistenceRecord struct {
	EntityID        string
	WorkflowName    string
	WorkflowVersion string
	Status          string
	TerminatedAt    time.Time
	CurrentState    string
	Revision        int64
	EnteredStageAt  time.Time
	Gates           json.RawMessage
	Fields          json.RawMessage
	Accumulator     json.RawMessage
	Config          json.RawMessage
	FlowInstance    string
	EntityType      string
	Slug            string
	Name            string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// DecodeWorkflowInstancePersistenceRecord converts one exact selected-store
// record into the canonical runtime workflow value.
func DecodeWorkflowInstancePersistenceRecord(record WorkflowInstancePersistenceRecord) (WorkflowInstance, error) {
	projection, err := decodeWorkflowInstancePersistedProjection(
		record.Fields,
		record.Gates,
		record.Accumulator,
		record.Config,
		workflowInstancePersistedControl{
			StorageRef: strings.TrimSpace(record.FlowInstance),
			EntityID:   strings.TrimSpace(record.EntityID),
			Slug:       strings.TrimSpace(record.Slug),
			Name:       strings.TrimSpace(record.Name),
			EntityType: strings.TrimSpace(record.EntityType),
		},
	)
	if err != nil {
		return WorkflowInstance{}, err
	}
	item := WorkflowInstance{
		WorkflowName:      strings.TrimSpace(record.WorkflowName),
		WorkflowVersion:   strings.TrimSpace(record.WorkflowVersion),
		Status:            strings.TrimSpace(record.Status),
		TerminatedAt:      record.TerminatedAt.UTC(),
		CurrentState:      strings.TrimSpace(record.CurrentState),
		Revision:          record.Revision,
		EnteredStageAt:    record.EnteredStageAt.UTC(),
		StateBuckets:      projection.Accumulator,
		Config:            projection.Config,
		Metadata:          projection.Metadata(),
		TransitionHistory: append([]WorkflowTransitionRecord(nil), projection.Control.TransitionHistory...),
		CreatedAt:         record.CreatedAt.UTC(),
		UpdatedAt:         record.UpdatedAt.UTC(),
	}
	persistedIdentity, err := workflowInstancePersistedIdentity(nil, WorkflowInstance{
		StorageRef:   projection.Control.StorageRef,
		WorkflowName: item.WorkflowName,
		Metadata:     item.Metadata,
	})
	if err != nil {
		return WorkflowInstance{}, err
	}
	item.StorageRef = persistedIdentity.StorageRef
	item.InstanceID = persistedIdentity.InstanceID
	if item.StateBuckets == nil {
		item.StateBuckets = map[string]any{}
	}
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}
	return item, nil
}

// WorkflowInstanceLookupMiss reports the exact key that failed to resolve.
// It is shared by every backend and is returned before any mutation callback runs.
type WorkflowInstanceLookupMiss struct {
	RequestedKey string
}

func (e *WorkflowInstanceLookupMiss) Error() string {
	return fmt.Sprintf("workflow instance lookup missed requested key %q", e.RequestedKey)
}

type WorkflowInitialMaterializationResult uint8

const (
	WorkflowInitialMaterializationUnknown WorkflowInitialMaterializationResult = iota
	WorkflowInitialMaterializationCreated
	WorkflowInitialMaterializationAlreadyExists
)

type WorkflowTransitionRecord struct {
	TransitionID    string    `json:"transition_id"`
	From            string    `json:"from"`
	To              string    `json:"to"`
	TriggerEventID  string    `json:"trigger_event_id"`
	FiredAt         time.Time `json:"fired_at"`
	GuardsEvaluated []string  `json:"guards_evaluated"`
}

type workflowInstancePersistedProjection struct {
	Fields      map[string]any                   `json:"fields"`
	Gates       map[string]bool                  `json:"gates"`
	Accumulator map[string]any                   `json:"accumulator"`
	Config      map[string]any                   `json:"config"`
	Control     workflowInstancePersistedControl `json:"control"`
}

type workflowInstancePersistedControl struct {
	StorageRef         string                     `json:"storage_ref"`
	EntityID           string                     `json:"entity_id"`
	Slug               string                     `json:"slug"`
	Name               string                     `json:"name"`
	EntityType         string                     `json:"entity_type"`
	InstanceID         string                     `json:"instance_id"`
	FlowPath           string                     `json:"flow_path"`
	InstanceKind       string                     `json:"instance_kind"`
	TemplateVersion    string                     `json:"template_version"`
	LastSourceEvent    string                     `json:"last_source_event"`
	Status             string                     `json:"status"`
	ParentFlowID       string                     `json:"parent_flow_id"`
	ParentFlowInstance string                     `json:"parent_flow_instance"`
	ParentEntityID     string                     `json:"parent_entity_id"`
	TransitionHistory  []WorkflowTransitionRecord `json:"transition_history"`
}

type workflowInstanceStore struct {
	entityQuery      entityquery.Reader
	routeRecovery    runtimeworkflowroute.RecoveryReader
	activityResults  runtimeactivityresult.Reader
	activityJournal  ActivityAttemptJournal
	gateRoutes       GateRouteAdmissionReader
	timerObligations runtimetimerobligation.Reader
	deliveryStore    runtimedelivery.Store
	pipelineStore    runtimepipelineobligation.Store
	decisionCards    decisioncard.Store
	lifecycleOwner   workflowInstanceLifecycleOwner
	runLifecycle     runtimerunlifecycle.OperationOwner
	engineMutations  WorkflowEngineMutationOwner
	cardMutations    DecisionCardMutationOwner
	timerOccurrences WorkflowTimerOccurrenceOwner
	timerActivations WorkflowTimerActivationPersistence
	readiness        DynamicFlowRuntimeReadinessPersistence
	standingServices StandingServicePersistence
	decisionRoutes   WorkflowDecisionRouteOwner
	instanceReader   WorkflowInstancePersistenceReader
	initialCommits   WorkflowInitialMaterializationCommitOwner
	deliverySignalMu sync.RWMutex
	deliverySignals  map[runtimedelivery.ExecutionAuthority]func()
}

type DeliveryContinuationSignalRegistration struct {
	owner     *workflowInstanceStore
	authority runtimedelivery.ExecutionAuthority
	released  bool
}

func (r *DeliveryContinuationSignalRegistration) Release() {
	if r == nil || r.owner == nil {
		return
	}
	r.owner.deliverySignalMu.Lock()
	defer r.owner.deliverySignalMu.Unlock()
	if r.released {
		return
	}
	r.released = true
	delete(r.owner.deliverySignals, r.authority)
}

type workflowInstanceLifecycleOwner interface {
	PrepareWorkflowLifecycleMutation(context.Context, *WorkflowInstance, []runtimeworkflowlifecycle.Effect, bool) (PreparedWorkflowLifecycleMutation, error)
	FinalizeWorkflowLifecycleMutation(context.Context, CommittedWorkflowLifecycleMutation) error
	ArmInitialEntryTimers(context.Context, runtimeflowidentity.Route) error
	ReconcileInitialEntryTimers(context.Context, runtimeflowidentity.Route) error
	RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.Route) error
}

type WorkflowInstanceFieldSelector struct {
	Field string
	Value any
}

type workflowInstanceFieldSelector = WorkflowInstanceFieldSelector

// WorkflowPersistence is an opaque selected-backend construction value. It
// carries storage mechanics into pipeline construction without exposing the
// concrete workflow store to runtime consumers.
type WorkflowPersistence struct {
	store *workflowInstanceStore
}

// WorkflowPersistenceOwner is the complete selected-store workflow operation
// surface. It exposes semantic operations only; transaction, backend, and SQL
// capabilities remain private to the selected store.
type WorkflowPersistenceOwner interface {
	entityquery.Reader
	runtimeworkflowroute.RecoveryReader
	runtimeactivityresult.Reader
	ActivityAttemptJournal
	GateRouteAdmissionReader
	runtimetimerobligation.Reader
	WorkflowEngineMutationOwner
	DecisionCardMutationOwner
	WorkflowTimerOccurrenceOwner
	WorkflowTimerActivationPersistence
	DynamicFlowRuntimeReadinessPersistence
	StandingServicePersistence
	WorkflowDecisionRouteOwner
	WorkflowInstancePersistenceReader
	WorkflowInitialMaterializationCommitOwner
}

func NewWorkflowPersistence(owner WorkflowPersistenceOwner) WorkflowPersistence {
	if owner == nil {
		return WorkflowPersistence{}
	}
	return WorkflowPersistence{store: &workflowInstanceStore{
		entityQuery: owner, routeRecovery: owner, activityResults: owner,
		activityJournal: owner, gateRoutes: owner, timerObligations: owner,
		engineMutations: owner, cardMutations: owner, timerOccurrences: owner,
		timerActivations: owner, readiness: owner, standingServices: owner,
		decisionRoutes: owner, instanceReader: owner, initialCommits: owner,
	}}
}

func (p WorkflowPersistence) empty() bool {
	return p.store == nil
}

// Configured reports whether a selected backend attempted to provide durable
// workflow persistence. It lets construction reject an incomplete owner rather
// than silently treating it as preview mode.
func (p WorkflowPersistence) Configured() bool {
	return p.store != nil
}

// Valid reports whether the selected backend supplied every named workflow
// persistence operation.
func (p WorkflowPersistence) Valid() bool {
	return !p.empty() && p.store.entityQuery != nil && p.store.routeRecovery != nil &&
		p.store.activityResults != nil && p.store.activityJournal != nil && p.store.gateRoutes != nil &&
		p.store.timerObligations != nil && p.store.engineMutations != nil && p.store.cardMutations != nil &&
		p.store.timerOccurrences != nil && p.store.timerActivations != nil && p.store.readiness != nil &&
		p.store.standingServices != nil && p.store.decisionRoutes != nil && p.store.instanceReader != nil &&
		p.store.initialCommits != nil
}

// RegisterDeliveryContinuationSignal installs the exact runtime-generation
// notification used after standing-run terminalization commits.
func (s *workflowInstanceStore) RegisterDeliveryContinuationSignal(authority runtimedelivery.ExecutionAuthority, signal func()) (*DeliveryContinuationSignalRegistration, error) {
	if s == nil || signal == nil {
		return nil, errors.New("delivery continuation signal owner is required")
	}
	if err := authority.Validate(); err != nil || authority.Kind() != runtimedelivery.ExecutionAuthorityNormalRuntime {
		return nil, errors.New("normal delivery continuation signal authority is required")
	}
	s.deliverySignalMu.Lock()
	defer s.deliverySignalMu.Unlock()
	if s.deliverySignals == nil {
		s.deliverySignals = make(map[runtimedelivery.ExecutionAuthority]func())
	}
	if _, exists := s.deliverySignals[authority]; exists {
		return nil, errors.New("delivery continuation signal authority is already registered")
	}
	s.deliverySignals[authority] = signal
	return &DeliveryContinuationSignalRegistration{owner: s, authority: authority}, nil
}

func (s *workflowInstanceStore) deliveryContinuationSignalOwners() []runtimedelivery.ExecutionAuthority {
	if s == nil {
		return nil
	}
	s.deliverySignalMu.RLock()
	defer s.deliverySignalMu.RUnlock()
	owners := make([]runtimedelivery.ExecutionAuthority, 0, len(s.deliverySignals))
	for authority := range s.deliverySignals {
		owners = append(owners, authority)
	}
	return owners
}

func (s *workflowInstanceStore) signalDeliveryContinuations() {
	if s == nil {
		return
	}
	authorities := s.deliveryContinuationSignalOwners()
	for _, authority := range authorities {
		s.deliverySignalMu.RLock()
		signal := s.deliverySignals[authority]
		s.deliverySignalMu.RUnlock()
		if signal != nil {
			signal()
		}
	}
}

func (s *workflowInstanceStore) enabled() bool {
	return s != nil && s.instanceReader != nil
}

func (s *workflowInstanceStore) Load(ctx context.Context, route runtimeflowidentity.Route) (WorkflowInstance, bool, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return WorkflowInstance{}, false, &WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(route.InstancePath)}
	}
	if s == nil || s.instanceReader == nil {
		return WorkflowInstance{}, false, nil
	}
	return s.instanceReader.LoadWorkflowInstance(ctx, route)
}

func (s *workflowInstanceStore) list(ctx context.Context) ([]WorkflowInstance, error) {
	if s == nil || s.instanceReader == nil {
		return nil, nil
	}
	return s.instanceReader.ListWorkflowInstances(ctx)
}

func (s *workflowInstanceStore) selectActiveByFields(ctx context.Context, scopeKey string, selectors []workflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	return s.selectActiveByFieldsExported(ctx, scopeKey, selectors, excludedStates)
}

func (s *workflowInstanceStore) selectActiveByFieldsExported(ctx context.Context, scopeKey string, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowInstance, error) {
	if s == nil || s.instanceReader == nil {
		return nil, nil
	}
	return s.instanceReader.SelectActiveWorkflowInstances(ctx, scopeKey, selectors, excludedStates)
}

func (s *workflowInstanceStore) MaterializeInitialEntry(ctx context.Context, instance WorkflowInstance, occurredAt time.Time) (WorkflowInitialMaterializationResult, error) {
	if s == nil || s.initialCommits == nil {
		return WorkflowInitialMaterializationUnknown, fmt.Errorf("workflow instance lifecycle store is required")
	}
	normalized, identity, lifecycle, err := s.prepareInitialEntryLifecycle(ctx, instance, occurredAt)
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	occurredAt = canonicalWorkflowInstancePersistedTime(occurredAt)
	initialProjection, err := newWorkflowInitialMaterializationProjection(ctx, identity, normalized, occurredAt)
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	state, err := workflowEngineStateRecord(runID, identity.Instance.Route(), normalized, "", 0, true, occurredAt)
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	record, err := workflowInitialMaterializationRecord(runID, state, initialProjection, normalized.RuntimeReadiness)
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	committed, err := s.initialCommits.CommitWorkflowInitialMaterialization(ctx, WorkflowInitialMaterializationCommand{Record: record, Lifecycle: lifecycle})
	if err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	if err := committed.Validate(); err != nil {
		return WorkflowInitialMaterializationUnknown, err
	}
	if committed.Result == WorkflowInitialMaterializationCreated {
		if err := s.finalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			return WorkflowInitialMaterializationUnknown, err
		}
	}
	return committed.Result, nil
}

func (s *workflowInstanceStore) prepareInitialEntryLifecycle(
	ctx context.Context,
	instance WorkflowInstance,
	occurredAt time.Time,
) (WorkflowInstance, runtimeflowidentity.Persisted, WorkflowLifecycleMutationPlan, error) {
	occurredAt = canonicalWorkflowInstancePersistedTime(occurredAt)
	if occurredAt.IsZero() {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow initial materialization requires an exact occurrence time")
	}
	if s == nil || s.lifecycleOwner == nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow instance lifecycle owner is required")
	}
	instance.EnteredStageAt = occurredAt
	instance.CreatedAt = occurredAt
	normalized, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, err
	}
	if !ok {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow initial materialization requires canonical instance identity")
	}
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok || !mode.Valid() {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow initial materialization requires typed execution mode authority")
	}
	effect, err := runtimeworkflowlifecycle.NewInitialEntry(identity.Instance.Route(), identity.RowID(), normalized.CurrentState, mode, occurredAt)
	if err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, err
	}
	prepared, err := s.lifecycleOwner.PrepareWorkflowLifecycleMutation(ctx, &normalized, []runtimeworkflowlifecycle.Effect{effect}, false)
	if err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, err
	}
	if len(prepared.Emissions) != 0 {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, fmt.Errorf("workflow initial materialization cannot leave %d lifecycle emissions outside its atomic commit", len(prepared.Emissions))
	}
	if err := prepared.Commit.Validate(runtimecorrelation.RunIDFromContext(ctx), identity.Instance.Route(), identity.RowID()); err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, WorkflowLifecycleMutationPlan{}, err
	}
	return normalized, identity, prepared.Commit, nil
}

func (s *workflowInstanceStore) finalizeInitialEntryLifecycle(ctx context.Context, committed CommittedWorkflowLifecycleMutation) error {
	if s == nil || s.lifecycleOwner == nil {
		return fmt.Errorf("workflow instance lifecycle owner is required")
	}
	postCommit := committed
	postCommit.Wakeups = nil
	postCommit.Cancellations = nil
	return s.lifecycleOwner.FinalizeWorkflowLifecycleMutation(ctx, postCommit)
}

// ArmInitialEntryTimers projects the durable initial-entry timer facts only
// after the caller has installed the runtime route that can receive them.
func (s *workflowInstanceStore) ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	if s == nil || !s.enabled() {
		return fmt.Errorf("workflow instance lifecycle store is required")
	}
	if s.lifecycleOwner == nil {
		return fmt.Errorf("workflow instance lifecycle owner is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return fmt.Errorf("workflow initial timer activation requires instance identity")
	}
	return s.lifecycleOwner.ArmInitialEntryTimers(ctx, route)
}

// ReconcileInitialEntryTimers mutates the durable initial-entry declaration
// set and projects exactly the committed successor set.
func (s *workflowInstanceStore) ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	if s == nil || !s.enabled() {
		return fmt.Errorf("workflow instance lifecycle store is required")
	}
	if s.lifecycleOwner == nil {
		return fmt.Errorf("workflow instance lifecycle owner is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return fmt.Errorf("workflow initial timer reconciliation requires instance identity")
	}
	return s.lifecycleOwner.ReconcileInitialEntryTimers(ctx, route)
}

// RetireInitialEntryTimerWakeups withdraws and joins the exact process-local
// projections for the durable active set. It does not mutate timer status.
func (s *workflowInstanceStore) RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error {
	if s == nil || !s.enabled() {
		return fmt.Errorf("workflow instance lifecycle store is required")
	}
	if s.lifecycleOwner == nil {
		return fmt.Errorf("workflow instance lifecycle owner is required")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return fmt.Errorf("workflow initial timer retirement requires instance identity")
	}
	return s.lifecycleOwner.RetireInitialEntryTimerWakeups(ctx, route)
}

func canonicalWorkflowInstancePersistedTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func normalizeWorkflowInstanceForPersistence(instance WorkflowInstance) (WorkflowInstance, runtimeflowidentity.Persisted, bool, error) {
	instance.InstanceID = strings.TrimSpace(instance.InstanceID)
	instance.WorkflowName = strings.TrimSpace(instance.WorkflowName)
	instance.WorkflowVersion = strings.TrimSpace(instance.WorkflowVersion)
	instance.CurrentState = strings.TrimSpace(instance.CurrentState)
	if instance.InstanceID == "" || instance.WorkflowName == "" || instance.WorkflowVersion == "" || instance.CurrentState == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, fmt.Errorf(
			"workflow instance requires instance_id, workflow_name, workflow_version, and current_state (id=%q workflow=%q version=%q state=%q)",
			instance.InstanceID,
			instance.WorkflowName,
			instance.WorkflowVersion,
			instance.CurrentState,
		)
	}
	if instance.EnteredStageAt.IsZero() {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, fmt.Errorf("workflow instance requires exact entered_stage_at")
	}
	if instance.CreatedAt.IsZero() {
		instance.CreatedAt = instance.EnteredStageAt.UTC()
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	instance.StorageRef = strings.TrimSpace(instance.StorageRef)
	identity, err := workflowInstancePersistedIdentity(nil, instance)
	if err != nil {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, err
	}
	if strings.TrimSpace(identity.StorageRef) == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, &WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(instance.StorageRef)}
	}
	if strings.TrimSpace(identity.RowID()) == "" {
		return WorkflowInstance{}, runtimeflowidentity.Persisted{}, false, &WorkflowInstanceLookupMiss{RequestedKey: strings.TrimSpace(identity.StorageRef)}
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	instance.StorageRef = identity.StorageRef
	instance.InstanceID = identity.InstanceID
	instance.Metadata["storage_ref"] = identity.StorageRef
	instance.Metadata["instance_id"] = identity.InstanceID
	instance.Metadata["entity_id"] = identity.EntityID
	if identity.HasStoredPath && identity.InstancePath != "" {
		instance.Metadata["flow_path"] = identity.InstancePath
	} else {
		delete(instance.Metadata, "flow_path")
	}
	if identity.ParentRoute.FlowID != "" {
		instance.Metadata["parent_flow_id"] = identity.ParentRoute.FlowID
	}
	if identity.ParentRoute.FlowInstance != "" {
		instance.Metadata["parent_flow_instance"] = identity.ParentRoute.FlowInstance
	}
	return instance, identity, true, nil
}

func (s *workflowInstanceStore) MarkTerminated(ctx context.Context, route runtimeflowidentity.Route, terminatedAt time.Time) error {
	if s == nil || !s.enabled() {
		return nil
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() || terminatedAt.IsZero() {
		return fmt.Errorf("workflow instance termination requires exact route and occurrence time")
	}
	if s.engineMutations == nil {
		return fmt.Errorf("workflow instance termination requires the selected workflow engine mutation owner")
	}
	if s.decisionCards != nil {
		return fmt.Errorf("gated workflow termination requires the pipeline lifecycle coordinator")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	instance, found, err := s.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	if strings.TrimSpace(instance.Status) == "terminated" {
		if instance.TerminatedAt.IsZero() {
			return fmt.Errorf("terminal workflow instance %s has no termination time", route.InstancePath)
		}
		return nil
	}
	expectedState := strings.TrimSpace(instance.CurrentState)
	expectedRevision := instance.Revision
	instance.Status = "terminated"
	instance.TerminatedAt = terminatedAt.UTC()
	record, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, false, terminatedAt.UTC())
	if err != nil {
		return err
	}
	_, err = s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record})
	return err
}

func (s *workflowInstanceStore) QueryEntityCount(ctx context.Context, runID string, source semanticview.Source, contract entityruntime.Contract, predicate workflowEntityQueryPredicate) (int, error) {
	if s == nil || s.entityQuery == nil {
		return 0, fmt.Errorf("workflow entity query reader is required")
	}
	return s.entityQuery.CountWorkflowEntities(ctx, entityquery.Request{
		RunID:    runID,
		Source:   source,
		Contract: contract,
		Predicate: entityquery.Predicate{
			Field: predicate.Field,
			Op:    predicate.Op,
			Value: predicate.Value,
		},
	})
}

func normalizeWorkflowInstanceFieldSelectors(selectors []workflowInstanceFieldSelector) []workflowInstanceFieldSelector {
	out := make([]workflowInstanceFieldSelector, 0, len(selectors))
	for _, selector := range selectors {
		field := strings.TrimSpace(selector.Field)
		if field == "" {
			continue
		}
		out = append(out, workflowInstanceFieldSelector{Field: field, Value: selector.Value})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Field < out[j].Field
	})
	return out
}

func NormalizeWorkflowInstanceFieldSelectors(selectors []WorkflowInstanceFieldSelector) []WorkflowInstanceFieldSelector {
	return normalizeWorkflowInstanceFieldSelectors(selectors)
}

func normalizeWorkflowInstanceExcludedStates(states []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(states))
	for _, state := range states {
		state = strings.ToLower(strings.TrimSpace(state))
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	sort.Strings(out)
	return out
}

func NormalizeWorkflowInstanceExcludedStates(states []string) []string {
	return normalizeWorkflowInstanceExcludedStates(states)
}

func workflowInstanceFieldSelectorPath(field string) []string {
	parts := strings.Split(strings.TrimSpace(field), ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func WorkflowInstanceFieldSelectorPath(field string) []string {
	return workflowInstanceFieldSelectorPath(field)
}

func decodeWorkflowInstancePersistedProjection(
	fieldsRaw, gatesRaw, accRaw, configRaw []byte,
	control workflowInstancePersistedControl,
) (workflowInstancePersistedProjection, error) {
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("entity_state.gates", gatesRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	accumulator, err := decodeWorkflowInstanceJSONMap("entity_state.accumulator", accRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	config, control, err := decodeWorkflowInstanceConfigPayload(configRaw, control)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	if strings.TrimSpace(control.EntityType) == "" {
		control.EntityType = "default"
	}
	return workflowInstancePersistedProjection{
		Fields:      fields,
		Gates:       gates,
		Accumulator: accumulator,
		Config:      config,
		Control:     control,
	}, nil
}

func workflowInstancePersistedProjectionFromInstance(instance WorkflowInstance, storageRef string) (workflowInstancePersistedProjection, error) {
	metadata := cloneStringAnyMap(instance.Metadata)
	gates, err := workflowInstanceMetadataGates(metadata)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	delete(metadata, "gates")
	persistedIdentity, err := workflowInstancePersistedIdentity(nil, instance)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	if strings.TrimSpace(storageRef) != "" && strings.TrimSpace(persistedIdentity.StorageRef) != "" && strings.TrimSpace(storageRef) != strings.TrimSpace(persistedIdentity.StorageRef) {
		return workflowInstancePersistedProjection{}, fmt.Errorf("workflow instance storage_ref %q disagrees with canonical storage_ref %q", storageRef, persistedIdentity.StorageRef)
	}
	control := workflowInstancePersistedControl{
		StorageRef:         strings.TrimSpace(persistedIdentity.StorageRef),
		EntityID:           strings.TrimSpace(persistedIdentity.EntityID),
		Slug:               strings.TrimSpace(asString(instance.Metadata["slug"])),
		Name:               strings.TrimSpace(asString(instance.Metadata["name"])),
		EntityType:         strings.TrimSpace(asString(instance.Metadata["entity_type"])),
		InstanceID:         strings.TrimSpace(persistedIdentity.InstanceID),
		InstanceKind:       strings.TrimSpace(asString(instance.Metadata["instance_kind"])),
		TemplateVersion:    strings.TrimSpace(asString(instance.Metadata["template_version"])),
		LastSourceEvent:    strings.TrimSpace(asString(instance.Metadata["last_source_event"])),
		Status:             strings.TrimSpace(asString(instance.Config["status"])),
		ParentFlowID:       strings.TrimSpace(persistedIdentity.ParentRoute.FlowID),
		ParentFlowInstance: strings.Trim(strings.TrimSpace(persistedIdentity.ParentRoute.FlowInstance), "/"),
		ParentEntityID:     strings.TrimSpace(asString(instance.Metadata["parent_entity_id"])),
		TransitionHistory:  append([]WorkflowTransitionRecord{}, instance.TransitionHistory...),
	}
	if persistedIdentity.HasStoredPath {
		control.FlowPath = strings.TrimSpace(persistedIdentity.InstancePath)
	}
	for _, key := range []string{
		"slug", "name", "entity_type", "entity_id", "parent_flow_id", "parent_flow_instance", "parent_entity_id",
		"instance_id", "storage_ref", "flow_path", "instance_kind",
		"template_version", "workflow_version", "transition_history",
	} {
		delete(metadata, key)
	}
	if control.EntityType == "" {
		control.EntityType = "default"
	}
	return workflowInstancePersistedProjection{
		Fields:      metadata,
		Gates:       gates,
		Accumulator: cloneStringAnyMap(instance.StateBuckets),
		Config:      cloneStringAnyMap(instance.Config),
		Control:     control,
	}, nil
}

func (p workflowInstancePersistedProjection) Metadata() map[string]any {
	metadata := cloneStringAnyMap(p.Fields)
	if len(p.Gates) > 0 {
		metadata["gates"] = p.GatesAny()
	}
	if strings.TrimSpace(p.Control.Slug) != "" {
		metadata["slug"] = strings.TrimSpace(p.Control.Slug)
	}
	if strings.TrimSpace(p.Control.Name) != "" {
		metadata["name"] = strings.TrimSpace(p.Control.Name)
	}
	if strings.TrimSpace(p.Control.EntityType) != "" {
		metadata["entity_type"] = strings.TrimSpace(p.Control.EntityType)
	}
	if strings.TrimSpace(p.Control.EntityID) != "" {
		metadata["entity_id"] = strings.TrimSpace(p.Control.EntityID)
	}
	if strings.TrimSpace(p.Control.StorageRef) != "" {
		metadata["storage_ref"] = strings.TrimSpace(p.Control.StorageRef)
	}
	if strings.TrimSpace(p.Control.InstanceID) != "" {
		metadata["instance_id"] = strings.TrimSpace(p.Control.InstanceID)
	}
	if strings.TrimSpace(p.Control.FlowPath) != "" {
		metadata["flow_path"] = strings.TrimSpace(p.Control.FlowPath)
	}
	if strings.TrimSpace(p.Control.InstanceKind) != "" {
		metadata["instance_kind"] = strings.TrimSpace(p.Control.InstanceKind)
	}
	if strings.TrimSpace(p.Control.TemplateVersion) != "" {
		metadata["template_version"] = strings.TrimSpace(p.Control.TemplateVersion)
	}
	if strings.TrimSpace(p.Control.LastSourceEvent) != "" {
		metadata["last_source_event"] = strings.TrimSpace(p.Control.LastSourceEvent)
	}
	if strings.TrimSpace(p.Control.ParentFlowID) != "" {
		metadata["parent_flow_id"] = strings.TrimSpace(p.Control.ParentFlowID)
	}
	if strings.TrimSpace(p.Control.ParentFlowInstance) != "" {
		metadata["parent_flow_instance"] = strings.Trim(strings.TrimSpace(p.Control.ParentFlowInstance), "/")
	}
	if strings.TrimSpace(p.Control.ParentEntityID) != "" {
		metadata["parent_entity_id"] = strings.TrimSpace(p.Control.ParentEntityID)
	}
	if len(p.Control.TransitionHistory) > 0 {
		metadata["transition_history"] = append([]WorkflowTransitionRecord{}, p.Control.TransitionHistory...)
	}
	return metadata
}

func (p workflowInstancePersistedProjection) ConfigPayload(workflowVersion string) map[string]any {
	config := cloneStringAnyMap(p.Config)
	if config == nil {
		config = map[string]any{}
	}
	config["workflow_version"] = strings.TrimSpace(workflowVersion)
	config["instance_id"] = strings.TrimSpace(p.Control.InstanceID)
	config["storage_ref"] = strings.TrimSpace(p.Control.StorageRef)
	if strings.TrimSpace(p.Control.FlowPath) != "" {
		config["flow_path"] = strings.TrimSpace(p.Control.FlowPath)
	}
	if strings.TrimSpace(p.Control.InstanceKind) != "" {
		config["instance_kind"] = strings.TrimSpace(p.Control.InstanceKind)
	}
	if strings.TrimSpace(p.Control.TemplateVersion) != "" {
		config["template_version"] = strings.TrimSpace(p.Control.TemplateVersion)
	}
	if strings.TrimSpace(p.Control.LastSourceEvent) != "" {
		config["last_source_event"] = strings.TrimSpace(p.Control.LastSourceEvent)
	}
	if strings.TrimSpace(p.Control.Status) != "" {
		config["status"] = strings.TrimSpace(p.Control.Status)
	}
	if strings.TrimSpace(p.Control.ParentFlowID) != "" {
		config["parent_flow_id"] = strings.TrimSpace(p.Control.ParentFlowID)
	}
	if strings.TrimSpace(p.Control.ParentFlowInstance) != "" {
		config["parent_flow_instance"] = strings.Trim(strings.TrimSpace(p.Control.ParentFlowInstance), "/")
	}
	if strings.TrimSpace(p.Control.ParentEntityID) != "" {
		config["parent_entity_id"] = strings.TrimSpace(p.Control.ParentEntityID)
	}
	if len(p.Control.TransitionHistory) > 0 {
		config["transition_history"] = append([]WorkflowTransitionRecord{}, p.Control.TransitionHistory...)
	}
	return config
}

func (p workflowInstancePersistedProjection) GatesAny() map[string]any {
	return workflowBoolGatesAsMap(p.Gates)
}

func decodeWorkflowInstanceJSONMap(label string, raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s must be a JSON object: %w", label, err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func decodeWorkflowInstanceJSONBoolMap(label string, raw []byte) (map[string]bool, error) {
	if len(raw) == 0 {
		return map[string]bool{}, nil
	}
	var out map[string]bool
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s must be an object of booleans: %w", label, err)
	}
	if out == nil {
		return map[string]bool{}, nil
	}
	return out, nil
}

func workflowInstanceMetadataGates(metadata map[string]any) (map[string]bool, error) {
	if metadata == nil {
		return map[string]bool{}, nil
	}
	raw, ok := metadata["gates"]
	if !ok || raw == nil {
		return map[string]bool{}, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("workflow instance metadata.gates must be JSON-serializable: %w", err)
	}
	return decodeWorkflowInstanceJSONBoolMap("workflow instance metadata.gates", encoded)
}

func decodeWorkflowInstanceConfigPayload(raw []byte, control workflowInstancePersistedControl) (map[string]any, workflowInstancePersistedControl, error) {
	config, err := decodeWorkflowInstanceJSONMap("flow_instances.config", raw)
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	instanceID, err := workflowInstanceOptionalString(config, "instance_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	flowPath, err := workflowInstanceOptionalString(config, "flow_path")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	if _, declared := config["flow_path"]; declared && strings.Trim(strings.TrimSpace(flowPath), "/") == "" {
		return nil, workflowInstancePersistedControl{}, fmt.Errorf("flow_instances.config flow_path must name an exact instance route when declared")
	}
	configStorageRef, err := workflowInstanceOptionalString(config, "storage_ref")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	instanceKind, err := workflowInstanceOptionalString(config, "instance_kind")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	templateVersion, err := workflowInstanceOptionalString(config, "template_version")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	lastSourceEvent, err := workflowInstanceOptionalString(config, "last_source_event")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	status, err := workflowInstanceOptionalString(config, "status")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentFlowID, err := workflowInstanceOptionalString(config, "parent_flow_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentFlowInstance, err := workflowInstanceOptionalString(config, "parent_flow_instance")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	parentEntityID, err := workflowInstanceOptionalString(config, "parent_entity_id")
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	transitionHistory, err := workflowInstanceTransitionHistoryFromConfig(config)
	if err != nil {
		return nil, workflowInstancePersistedControl{}, err
	}
	delete(config, "workflow_version")
	delete(config, "instance_id")
	delete(config, "storage_ref")
	delete(config, "flow_path")
	delete(config, "instance_kind")
	delete(config, "template_version")
	delete(config, "last_source_event")
	delete(config, "parent_flow_id")
	delete(config, "parent_flow_instance")
	delete(config, "parent_entity_id")
	delete(config, "transition_history")
	control.InstanceID = strings.TrimSpace(instanceID)
	control.FlowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if strings.TrimSpace(control.StorageRef) == "" {
		control.StorageRef = strings.TrimSpace(configStorageRef)
	}
	if strings.TrimSpace(control.StorageRef) != "" && strings.TrimSpace(configStorageRef) != "" && strings.TrimSpace(control.StorageRef) != strings.TrimSpace(configStorageRef) {
		return nil, workflowInstancePersistedControl{}, fmt.Errorf("flow_instances.config storage_ref %q disagrees with canonical storage_ref %q", configStorageRef, control.StorageRef)
	}
	control.InstanceKind = strings.TrimSpace(instanceKind)
	control.TemplateVersion = strings.TrimSpace(templateVersion)
	control.LastSourceEvent = strings.TrimSpace(lastSourceEvent)
	control.Status = strings.TrimSpace(status)
	control.ParentFlowID = strings.TrimSpace(parentFlowID)
	control.ParentFlowInstance = strings.Trim(strings.TrimSpace(parentFlowInstance), "/")
	control.ParentEntityID = strings.TrimSpace(parentEntityID)
	control.TransitionHistory = transitionHistory
	return config, control, nil
}

func workflowInstanceOptionalString(config map[string]any, key string) (string, error) {
	value, ok := config[key]
	if !ok || value == nil {
		return "", nil
	}
	typed, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("flow_instances.config %s must be a string", key)
	}
	return strings.TrimSpace(typed), nil
}

func workflowInstanceTransitionHistoryFromConfig(config map[string]any) ([]WorkflowTransitionRecord, error) {
	raw, ok := config["transition_history"]
	if !ok || raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("flow_instances.config transition_history must be JSON-serializable: %w", err)
	}
	var out []WorkflowTransitionRecord
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, fmt.Errorf("flow_instances.config transition_history must be an array of workflow transition records: %w", err)
	}
	return out, nil
}

func workflowInstanceMode(instance WorkflowInstance) string {
	if instance.RuntimeReadiness != nil {
		return "template"
	}
	return "static"
}

func workflowInstanceRowID(ref string) string {
	return runtimeflowidentity.EntityID(ref)
}

func FlowInstanceEntityID(ref string) string {
	return runtimeflowidentity.EntityID(ref)
}

func containsString(items []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}

type pqStringArray []string

func (a pqStringArray) Value() (driver.Value, error) {
	return pq.Array([]string(a)).Value()
}

func jsonOrDefault(raw []byte, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
