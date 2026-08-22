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

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
	EntityID           string
	EntityType         string
	Slug               string
	Name               string
	InstanceKind       string
	TemplateVersion    string
	ParentFlowID       string
	ParentFlowInstance string
	ParentEntityID     string
	WorkflowName       string
	WorkflowVersion    string
	Mode               string
	RuntimeReadiness   *DynamicFlowRuntimeReadinessPlan
	Status             string
	TerminatedAt       time.Time
	CurrentState       string
	Revision           int64
	Config             map[string]any
	EnteredStageAt     time.Time
	TransitionHistory  []WorkflowTransitionRecord
	StateBuckets       map[string]any
	Fields             map[string]any
	Bookkeeping        map[string]any
	Gates              map[string]bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
	InitialFieldValues map[string]any
}

// WorkflowEntityStatePersistenceReader owns exact state-row reads for an
// admitted route and entity. Scenario setup can establish this state before a
// flow_instances lifecycle row exists; declared-key acquisition therefore
// selects this authority directly rather than inferring state from lifecycle.
type WorkflowEntityStatePersistenceReader interface {
	LoadWorkflowEntityState(context.Context, runtimeflowidentity.Route, runtimeidentity.EntityID) (WorkflowEntityStatePersistenceRecord, bool, error)
	SelectActiveWorkflowEntityStates(context.Context, WorkflowEntityStateSelectionOwner, []WorkflowInstanceFieldSelector, []string) ([]WorkflowEntityStatePersistenceRecord, error)
}

type workflowEntityStateSelectionCardinality uint8

const (
	workflowEntityStateSelectionCardinalityUnknown workflowEntityStateSelectionCardinality = iota
	workflowEntityStateSelectionExact
	workflowEntityStateSelectionTemplate
)

// WorkflowEntityStateSelectionOwner is the admitted source-backed owner of
// state-only rows eligible for declared-key acquisition. Authored descendant
// scopes are reserved so their rows cannot be interpreted as parent instances.
type WorkflowEntityStateSelectionOwner struct {
	scopeKey         string
	cardinality      workflowEntityStateSelectionCardinality
	descendantScopes []string
}

func AdmitWorkflowEntityStateSelectionOwner(source semanticview.Source, flowID string) (WorkflowEntityStateSelectionOwner, error) {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return WorkflowEntityStateSelectionOwner{}, fmt.Errorf("workflow entity state selection owner requires source and flow identity")
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return WorkflowEntityStateSelectionOwner{}, fmt.Errorf("workflow entity state selection owner flow %s is not declared", flowID)
	}
	scopeKey := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, flowID)), "/")
	if scopeKey == "" {
		return WorkflowEntityStateSelectionOwner{}, fmt.Errorf("workflow entity state selection owner flow %s has no canonical scope", flowID)
	}
	owner := WorkflowEntityStateSelectionOwner{scopeKey: scopeKey}
	switch strings.ToLower(strings.TrimSpace(scope.Mode)) {
	case runtimecontracts.FlowModeStatic, runtimecontracts.FlowModeSingleton:
		owner.cardinality = workflowEntityStateSelectionExact
	case runtimecontracts.FlowModeTemplate:
		owner.cardinality = workflowEntityStateSelectionTemplate
	default:
		return WorkflowEntityStateSelectionOwner{}, fmt.Errorf("workflow entity state selection owner flow %s has unsupported mode %q", flowID, strings.TrimSpace(scope.Mode))
	}
	for _, candidate := range source.FlowScopes() {
		candidateID := strings.TrimSpace(candidate.ID)
		if candidateID == "" || candidateID == flowID {
			continue
		}
		candidateScope := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, candidateID)), "/")
		if strings.HasPrefix(candidateScope, scopeKey+"/") {
			owner.descendantScopes = append(owner.descendantScopes, candidateScope)
		}
	}
	sort.Strings(owner.descendantScopes)
	return owner, nil
}

func (o WorkflowEntityStateSelectionOwner) Valid() bool {
	return strings.TrimSpace(o.scopeKey) != "" &&
		(o.cardinality == workflowEntityStateSelectionExact || o.cardinality == workflowEntityStateSelectionTemplate)
}

func (o WorkflowEntityStateSelectionOwner) ScopeKey() string {
	if !o.Valid() {
		return ""
	}
	return o.scopeKey
}

func (o WorkflowEntityStateSelectionOwner) Owns(instancePath string) bool {
	if !o.Valid() {
		return false
	}
	instancePath = strings.TrimSpace(instancePath)
	if instancePath == "" || instancePath != strings.Trim(instancePath, "/") {
		return false
	}
	if o.cardinality == workflowEntityStateSelectionExact {
		return instancePath == o.scopeKey
	}
	if !strings.HasPrefix(instancePath, o.scopeKey+"/") {
		return false
	}
	instanceID := strings.TrimPrefix(instancePath, o.scopeKey+"/")
	if instanceID == "" || strings.Contains(instanceID, "/") {
		return false
	}
	for _, descendantScope := range o.descendantScopes {
		if instancePath == descendantScope || strings.HasPrefix(instancePath, descendantScope+"/") {
			return false
		}
	}
	return true
}

// WorkflowTargetPersistencePresence is the closed selected-store truth for an
// exact receiver. State may legitimately precede its lifecycle companion; the
// reverse ordering is an invariant violation rather than a recoverable form.
type WorkflowTargetPersistencePresence uint8

const (
	WorkflowTargetPersistencePresenceUnknown WorkflowTargetPersistencePresence = iota
	WorkflowTargetPersistenceAbsent
	WorkflowTargetPersistenceStateOnly
	WorkflowTargetPersistenceComplete
	WorkflowTargetPersistenceLifecycleOnly
)

func (p WorkflowTargetPersistencePresence) Valid() bool {
	switch p {
	case WorkflowTargetPersistenceAbsent,
		WorkflowTargetPersistenceStateOnly,
		WorkflowTargetPersistenceComplete,
		WorkflowTargetPersistenceLifecycleOnly:
		return true
	default:
		return false
	}
}

func (p WorkflowTargetPersistencePresence) HasState() bool {
	return p == WorkflowTargetPersistenceStateOnly || p == WorkflowTargetPersistenceComplete
}

func (p WorkflowTargetPersistencePresence) HasLifecycleCompanion() bool {
	return p == WorkflowTargetPersistenceComplete || p == WorkflowTargetPersistenceLifecycleOnly
}

// WorkflowLifecycleCompanionPersistenceRecord is the exact relational
// descriptor paired with entity_state. It is data only; runtime owns semantic
// validation and private store adapters own its representation.
type WorkflowLifecycleCompanionPersistenceRecord struct {
	FlowInstance    string
	WorkflowName    string
	WorkflowVersion string
	Mode            string
	Status          string
	Config          json.RawMessage
	TerminatedAt    time.Time
	CreatedAt       time.Time
}

// WorkflowTargetPersistenceRecord carries the two independently persisted
// halves of one exact receiver together with their closed presence state.
type WorkflowTargetPersistenceRecord struct {
	Presence  WorkflowTargetPersistencePresence
	State     WorkflowEntityStatePersistenceRecord
	Lifecycle WorkflowLifecycleCompanionPersistenceRecord
}

func (r WorkflowTargetPersistenceRecord) Validate(route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) error {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() || !r.Presence.Valid() {
		return fmt.Errorf("workflow target persistence requires exact identity and closed presence")
	}
	if r.Presence.HasState() {
		if strings.TrimSpace(r.State.FlowInstance) != route.InstancePath || runtimeidentity.NormalizeEntityID(r.State.EntityID) != entityID {
			return fmt.Errorf("workflow target state disagrees with exact receiver identity")
		}
		if strings.TrimSpace(r.State.CurrentState) == "" || r.State.Revision <= 0 || r.State.CreatedAt.IsZero() || r.State.UpdatedAt.IsZero() {
			return fmt.Errorf("workflow target state requires persisted state, revision, and times")
		}
	}
	if r.Presence.HasLifecycleCompanion() {
		companion := r.Lifecycle
		if strings.TrimSpace(companion.FlowInstance) != route.InstancePath || strings.TrimSpace(companion.WorkflowName) == "" ||
			strings.TrimSpace(companion.Mode) == "" || strings.TrimSpace(companion.Status) == "" ||
			len(companion.Config) == 0 || !json.Valid(companion.Config) || companion.CreatedAt.IsZero() {
			return fmt.Errorf("workflow target lifecycle companion is incomplete or disagrees with exact receiver route")
		}
		if strings.TrimSpace(companion.Status) == "terminated" {
			if companion.TerminatedAt.IsZero() {
				return fmt.Errorf("terminated workflow target lifecycle companion requires termination time")
			}
		} else if !companion.TerminatedAt.IsZero() {
			return fmt.Errorf("non-terminal workflow target lifecycle companion cannot carry termination time")
		}
	}
	return nil
}

func (r WorkflowTargetPersistenceRecord) DecodeComplete(route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (WorkflowInstance, error) {
	if err := r.Validate(route, entityID); err != nil {
		return WorkflowInstance{}, err
	}
	if r.Presence != WorkflowTargetPersistenceComplete {
		return WorkflowInstance{}, fmt.Errorf("complete workflow target persistence is required")
	}
	item, err := DecodeWorkflowInstancePersistenceRecord(WorkflowInstancePersistenceRecord{
		EntityID: r.State.EntityID, WorkflowName: r.Lifecycle.WorkflowName, WorkflowVersion: r.Lifecycle.WorkflowVersion,
		Mode: r.Lifecycle.Mode, Status: r.Lifecycle.Status, TerminatedAt: r.Lifecycle.TerminatedAt,
		CurrentState: r.State.CurrentState, Revision: r.State.Revision, EnteredStageAt: r.State.EnteredStageAt,
		Gates: r.State.Gates, Fields: r.State.Fields, Bookkeeping: r.State.Bookkeeping, Accumulator: r.State.Accumulator,
		Config: r.Lifecycle.Config, FlowInstance: r.State.FlowInstance, EntityType: r.State.EntityType,
		Slug: r.State.Slug, Name: r.State.Name, CreatedAt: r.State.CreatedAt, UpdatedAt: r.State.UpdatedAt,
	})
	if err != nil {
		return WorkflowInstance{}, err
	}
	if workflowInstanceMode(item) != strings.TrimSpace(r.Lifecycle.Mode) {
		return WorkflowInstance{}, fmt.Errorf("workflow target lifecycle mode disagrees with persisted descriptor")
	}
	return item, nil
}

type WorkflowTargetPersistenceReader interface {
	LoadWorkflowTargetPersistence(context.Context, runtimeflowidentity.Route, runtimeidentity.EntityID) (WorkflowTargetPersistenceRecord, error)
}

// WorkflowInstancePersistenceReader owns exact selected-store workflow reads.
// Runtime consumers receive final semantic values and never select a backend,
// query shape, transaction, or SQL executor.
type WorkflowInstancePersistenceReader interface {
	WorkflowEntityStatePersistenceReader
	LoadWorkflowInstance(context.Context, runtimeflowidentity.Route) (WorkflowInstance, bool, error)
	ListWorkflowInstances(context.Context) ([]WorkflowInstance, error)
	SelectActiveWorkflowInstances(context.Context, string, []WorkflowInstanceFieldSelector, []string) ([]WorkflowInstance, error)
}

type WorkflowEntityStatePersistenceRecord struct {
	EntityID       string
	FlowInstance   string
	EntityType     string
	Slug           string
	Name           string
	CurrentState   string
	Revision       int64
	EnteredStageAt time.Time
	Gates          json.RawMessage
	Fields         json.RawMessage
	Bookkeeping    json.RawMessage
	Accumulator    json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// FilterWorkflowEntityStatePersistenceRecords applies the backend-neutral
// terminal-state and declared-key semantics after a selected store has bounded
// rows to the active run, flow scope, and lifecycle companion state.
func FilterWorkflowEntityStatePersistenceRecords(records []WorkflowEntityStatePersistenceRecord, owner WorkflowEntityStateSelectionOwner, selectors []WorkflowInstanceFieldSelector, excludedStates []string) ([]WorkflowEntityStatePersistenceRecord, error) {
	if !owner.Valid() {
		return nil, fmt.Errorf("workflow entity state selection requires an admitted flow owner")
	}
	selectors = NormalizeWorkflowInstanceFieldSelectors(selectors)
	excluded := make(map[string]struct{})
	for _, state := range NormalizeWorkflowInstanceExcludedStates(excludedStates) {
		excluded[state] = struct{}{}
	}
	out := make([]WorkflowEntityStatePersistenceRecord, 0, len(records))
	for _, record := range records {
		if !owner.Owns(record.FlowInstance) {
			continue
		}
		if _, skip := excluded[strings.ToLower(strings.TrimSpace(record.CurrentState))]; skip {
			continue
		}
		var fields map[string]any
		if len(record.Fields) > 0 {
			if err := json.Unmarshal(record.Fields, &fields); err != nil {
				return nil, fmt.Errorf("decode workflow entity state fields for %s: %w", record.FlowInstance, err)
			}
		}
		matched := true
		for _, selector := range selectors {
			value, ok := WorkflowMetadataValue(fields, selector.Field)
			if !ok || !WorkflowJSONValuesEqual(value, selector.Value) {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, record)
		}
	}
	return out, nil
}

func DecodeWorkflowEntityStatePersistenceRecord(record WorkflowEntityStatePersistenceRecord, route runtimeflowidentity.Route, workflowName, workflowVersion, mode string) (WorkflowInstance, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() || strings.TrimSpace(record.FlowInstance) != route.InstancePath {
		return WorkflowInstance{}, fmt.Errorf("workflow entity state row disagrees with admitted route")
	}
	config, err := json.Marshal(map[string]any{
		"workflow_version": strings.TrimSpace(workflowVersion),
		"instance_id":      route.InstanceID,
		"storage_ref":      route.InstancePath,
		"flow_path":        route.InstancePath,
	})
	if err != nil {
		return WorkflowInstance{}, fmt.Errorf("encode workflow entity state control: %w", err)
	}
	return DecodeWorkflowInstancePersistenceRecord(WorkflowInstancePersistenceRecord{
		EntityID: record.EntityID, WorkflowName: strings.TrimSpace(workflowName), WorkflowVersion: strings.TrimSpace(workflowVersion),
		Mode:   strings.TrimSpace(mode),
		Status: "active", CurrentState: record.CurrentState, Revision: record.Revision, EnteredStageAt: record.EnteredStageAt,
		Gates: record.Gates, Fields: record.Fields, Bookkeeping: record.Bookkeeping, Accumulator: record.Accumulator,
		Config: config, FlowInstance: record.FlowInstance, EntityType: record.EntityType,
		Slug: record.Slug, Name: record.Name, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	})
}

// WorkflowInstancePersistenceRecord is the exact backend-neutral row shape
// consumed by the semantic workflow decoder. Store adapters scan backend
// values into this value; runtime owns interpretation of workflow metadata.
type WorkflowInstancePersistenceRecord struct {
	EntityID        string
	WorkflowName    string
	WorkflowVersion string
	Mode            string
	Status          string
	TerminatedAt    time.Time
	CurrentState    string
	Revision        int64
	EnteredStageAt  time.Time
	Gates           json.RawMessage
	Fields          json.RawMessage
	Bookkeeping     json.RawMessage
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
	route, err := workflowInstanceRouteForPath(record.FlowInstance)
	if err != nil {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance row route: %w", err)
	}
	entityID := runtimeidentity.NormalizeEntityID(record.EntityID)
	if entityID.IsZero() {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance %s: entity_id is required", route.InstancePath)
	}
	projection, err := decodeWorkflowInstancePersistedProjection(
		record.Fields,
		record.Bookkeeping,
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
	if got := strings.TrimSpace(projection.Control.InstanceID); got != route.InstanceID {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance %s identity: persisted instance_id %q disagrees with exact route instance_id %q", route.InstancePath, got, route.InstanceID)
	}
	if got := strings.Trim(strings.TrimSpace(projection.Control.StorageRef), "/"); got != route.InstancePath {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance %s identity: persisted storage_ref %q disagrees with exact route", route.InstancePath, got)
	}
	if got := strings.Trim(strings.TrimSpace(projection.Control.FlowPath), "/"); got != route.InstancePath {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance %s identity: persisted flow_path %q disagrees with exact route", route.InstancePath, got)
	}
	item := WorkflowInstance{
		StorageRef:         route.InstancePath,
		InstanceID:         route.InstanceID,
		EntityID:           strings.TrimSpace(record.EntityID),
		EntityType:         strings.TrimSpace(projection.Control.EntityType),
		Slug:               strings.TrimSpace(projection.Control.Slug),
		Name:               strings.TrimSpace(projection.Control.Name),
		InstanceKind:       strings.TrimSpace(projection.Control.InstanceKind),
		TemplateVersion:    strings.TrimSpace(projection.Control.TemplateVersion),
		ParentFlowID:       strings.TrimSpace(projection.Control.ParentFlowID),
		ParentFlowInstance: strings.TrimSpace(projection.Control.ParentFlowInstance),
		ParentEntityID:     strings.TrimSpace(projection.Control.ParentEntityID),
		WorkflowName:       strings.TrimSpace(record.WorkflowName),
		WorkflowVersion:    strings.TrimSpace(record.WorkflowVersion),
		Mode:               strings.TrimSpace(record.Mode),
		Status:             strings.TrimSpace(record.Status),
		TerminatedAt:       record.TerminatedAt.UTC(),
		CurrentState:       strings.TrimSpace(record.CurrentState),
		Revision:           record.Revision,
		EnteredStageAt:     record.EnteredStageAt.UTC(),
		StateBuckets:       projection.Accumulator,
		Config:             projection.Config,
		Fields:             projection.Fields,
		Bookkeeping:        projection.Bookkeeping,
		Gates:              projection.Gates,
		TransitionHistory:  append([]WorkflowTransitionRecord(nil), projection.Control.TransitionHistory...),
		CreatedAt:          record.CreatedAt.UTC(),
		UpdatedAt:          record.UpdatedAt.UTC(),
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, item); err != nil {
		return WorkflowInstance{}, fmt.Errorf("decode workflow instance %s identity: %w", route.InstancePath, err)
	}
	if item.StateBuckets == nil {
		item.StateBuckets = map[string]any{}
	}
	if item.Fields == nil {
		item.Fields = map[string]any{}
	}
	if item.Bookkeeping == nil {
		item.Bookkeeping = map[string]any{}
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
	Bookkeeping map[string]any                   `json:"bookkeeping"`
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
	Status             string                     `json:"status"`
	ParentFlowID       string                     `json:"parent_flow_id"`
	ParentFlowInstance string                     `json:"parent_flow_instance"`
	ParentEntityID     string                     `json:"parent_entity_id"`
	TransitionHistory  []WorkflowTransitionRecord `json:"transition_history"`
}

type workflowInstanceStore struct {
	entityQuery       entityquery.Reader
	routeRecovery     runtimeworkflowroute.RecoveryReader
	activityResults   runtimeactivityresult.Reader
	activityJournal   ActivityAttemptJournal
	gateRoutes        GateRouteAdmissionReader
	timerObligations  runtimetimerobligation.Reader
	deliveryStore     runtimedelivery.Store
	pipelineStore     runtimepipelineobligation.Store
	decisionCards     decisioncard.Store
	lifecycleOwner    workflowInstanceLifecycleOwner
	runLifecycle      runtimerunlifecycle.OperationOwner
	engineMutations   WorkflowEngineMutationOwner
	cardMutations     DecisionCardMutationOwner
	timerOccurrences  WorkflowTimerOccurrenceOwner
	timerActivations  WorkflowTimerActivationPersistence
	readiness         DynamicFlowRuntimeReadinessPersistence
	standingServices  StandingServicePersistence
	decisionRoutes    WorkflowDecisionRouteOwner
	instanceReader    WorkflowInstancePersistenceReader
	entityStateReader WorkflowEntityStatePersistenceReader
	targetReader      WorkflowTargetPersistenceReader
	initialCommits    WorkflowInitialMaterializationCommitOwner
	deliverySignalMu  sync.RWMutex
	deliverySignals   map[runtimedelivery.ExecutionAuthority]func()
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
	WorkflowEntityStatePersistenceReader
	WorkflowTargetPersistenceReader
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
		entityStateReader: owner, targetReader: owner,
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
		p.store.entityStateReader != nil && p.store.targetReader != nil && p.store.initialCommits != nil
}

func (s *workflowInstanceStore) LoadEntityState(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (WorkflowEntityStatePersistenceRecord, bool, error) {
	if s == nil || s.entityStateReader == nil {
		return WorkflowEntityStatePersistenceRecord{}, false, fmt.Errorf("workflow entity state reader is required")
	}
	return s.entityStateReader.LoadWorkflowEntityState(ctx, route, entityID)
}

func (s *workflowInstanceStore) LoadTargetPersistence(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID) (WorkflowTargetPersistenceRecord, error) {
	if s == nil || s.targetReader == nil {
		return WorkflowTargetPersistenceRecord{}, fmt.Errorf("workflow target persistence reader is required")
	}
	record, err := s.targetReader.LoadWorkflowTargetPersistence(ctx, route, entityID)
	if err != nil {
		return WorkflowTargetPersistenceRecord{}, err
	}
	if err := record.Validate(route, entityID); err != nil {
		return WorkflowTargetPersistenceRecord{}, err
	}
	return record, nil
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
	state, err := workflowEngineStateRecord(runID, identity.Instance.Route(), normalized, "", 0, WorkflowEngineStateTransitionCreateStateAndCompanion, occurredAt)
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
	effect, err := runtimeworkflowlifecycle.NewInitialEntry(identity.Instance.Route(), runtimeidentity.NormalizeEntityID(identity.RowID()), normalized.CurrentState, mode, occurredAt)
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
	if instance.Fields == nil {
		instance.Fields = map[string]any{}
	}
	if instance.Bookkeeping == nil {
		instance.Bookkeeping = map[string]any{}
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
	instance.StorageRef = identity.StorageRef
	instance.InstanceID = identity.InstanceID
	instance.EntityID = identity.EntityID
	instance.ParentEntityID = identity.ParentEntityID
	instance.ParentFlowID = identity.ParentRoute.FlowID
	instance.ParentFlowInstance = identity.ParentRoute.FlowInstance
	return instance, identity, true, nil
}

func (s *workflowInstanceStore) MarkTerminated(ctx context.Context, route runtimeflowidentity.Route, entityID runtimeidentity.EntityID, terminatedAt time.Time) error {
	if s == nil || !s.enabled() {
		return nil
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = runtimeidentity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() || terminatedAt.IsZero() {
		return fmt.Errorf("workflow instance termination requires exact route, entity, and occurrence time")
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
	if _, err := requireWorkflowInstanceIdentity(route, entityID, instance); err != nil {
		return fmt.Errorf("validate workflow instance termination owner: %w", err)
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
	record, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, WorkflowEngineStateTransitionUpdateStateAndCompanion, terminatedAt.UTC())
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
	fieldsRaw, bookkeepingRaw, gatesRaw, accRaw, configRaw []byte,
	control workflowInstancePersistedControl,
) (workflowInstancePersistedProjection, error) {
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		return workflowInstancePersistedProjection{}, err
	}
	bookkeeping, err := decodeWorkflowInstanceJSONMap("entity_state.bookkeeping", bookkeepingRaw)
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
		Bookkeeping: bookkeeping,
		Gates:       gates,
		Accumulator: accumulator,
		Config:      config,
		Control:     control,
	}, nil
}

func workflowInstancePersistedProjectionFromInstance(instance WorkflowInstance, storageRef string) (workflowInstancePersistedProjection, error) {
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
		Slug:               strings.TrimSpace(instance.Slug),
		Name:               strings.TrimSpace(instance.Name),
		EntityType:         strings.TrimSpace(instance.EntityType),
		InstanceID:         strings.TrimSpace(persistedIdentity.InstanceID),
		InstanceKind:       strings.TrimSpace(instance.InstanceKind),
		TemplateVersion:    strings.TrimSpace(instance.TemplateVersion),
		Status:             strings.TrimSpace(asString(instance.Config["status"])),
		ParentFlowID:       strings.TrimSpace(persistedIdentity.ParentRoute.FlowID),
		ParentFlowInstance: strings.Trim(strings.TrimSpace(persistedIdentity.ParentRoute.FlowInstance), "/"),
		ParentEntityID:     strings.TrimSpace(instance.ParentEntityID),
		TransitionHistory:  append([]WorkflowTransitionRecord{}, instance.TransitionHistory...),
	}
	if persistedIdentity.HasStoredPath {
		control.FlowPath = strings.TrimSpace(persistedIdentity.InstancePath)
	}
	if control.EntityType == "" {
		control.EntityType = "default"
	}
	return workflowInstancePersistedProjection{
		Fields:      cloneStringAnyMap(instance.Fields),
		Bookkeeping: cloneStringAnyMap(instance.Bookkeeping),
		Gates:       cloneWorkflowGates(instance.Gates),
		Accumulator: cloneStringAnyMap(instance.StateBuckets),
		Config:      cloneStringAnyMap(instance.Config),
		Control:     control,
	}, nil
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

func cloneWorkflowGates(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
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
	if mode := strings.TrimSpace(instance.Mode); mode != "" {
		return mode
	}
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
