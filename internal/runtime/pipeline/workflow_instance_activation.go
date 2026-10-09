package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// FlowConstructionInput is immutable construction provenance, not attachment
// progress or permission for a handler to create missing state.
type FlowConstructionInput struct {
	EventID string `json:"event_id"`
	Input   string `json:"input"`
}

func (i FlowConstructionInput) Validate() error {
	if i.EventID == "" && i.Input == "" {
		return nil // No-argument startup construction has no creating delivery.
	}
	id, err := uuid.Parse(i.EventID)
	if err != nil || id.String() != i.EventID || i.Input != strings.TrimSpace(i.Input) {
		return fmt.Errorf("flow construction requires exact creating event and local input")
	}
	return nil
}

type FlowInstanceActivationRequest struct {
	Context                       events.DeliveryContext
	ContractBundle                semanticview.Source
	Instance                      runtimeflowidentity.Instance
	InitialState                  string
	ConstructorInput              string
	ResolvedKey                   any
	PayloadProjection             events.DeliveryPayloadProjection
	Bookkeeping                   map[string]any
	TriggerEvent                  events.Event
	OccurredAt                    time.Time
	StandingGenerationReplacement bool
	ScenarioSeed                  *ScenarioSetupEntityRequest
}

func (r FlowInstanceActivationRequest) ConstructorPayload() (map[string]any, error) {
	if r.ConstructorInput == "" {
		if !r.PayloadProjection.Empty() {
			return nil, fmt.Errorf("keyless construction cannot accept a payload projection")
		}
		return nil, nil
	}
	_, err := events.NewDeliveryEvent(r.TriggerEvent, events.DeliveryRoute{PayloadProjection: r.PayloadProjection})
	if err != nil {
		return nil, err
	}
	if !r.PayloadProjection.Empty() {
		if r.ContractBundle == nil {
			return nil, fmt.Errorf("constructor projection requires its admitted field contract")
		}
		schema, found := r.ContractBundle.FlowSchemaByID(r.Instance.TemplateID)
		contract, declared := entityruntime.ResolveForFlow(r.ContractBundle, r.Instance.TemplateID)
		fields := r.PayloadProjection.Fields()
		if !found || !declared || schema.Instance.Empty() || len(fields) != 1 {
			return nil, fmt.Errorf("constructor projection must contain only its resolved instance key")
		}
		key := schema.Instance.Path()
		stamped, present := fields[key]
		value, valueErr := entityruntime.NormalizeFieldValue(contract, key, stamped)
		resolved, resolvedErr := entityruntime.NormalizeFieldValue(contract, key, r.ResolvedKey)
		if !present || r.ResolvedKey == nil || valueErr != nil || resolvedErr != nil || !reflect.DeepEqual(value, resolved) {
			return nil, fmt.Errorf("constructor projection key %s contradicts its resolved typed key", key)
		}
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(r.TriggerEvent.Payload(), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// FlowInstanceActivationPlan is the exact durable command derived from one
// admitted activation request. It contains semantic facts only: selected-store
// adapters own persistence and return post-commit evidence separately.
type FlowInstanceActivationPlan struct {
	Instance                      WorkflowInstance
	Identity                      runtimeflowidentity.Instance
	Readiness                     DynamicFlowRuntimeReadinessPlan
	Lifecycle                     WorkflowLifecycleMutationPlan
	CreatingInput                 FlowConstructionInput
	Children                      []FlowInstanceActivationPlan
	ActivationVariables           map[string]string
	OccurredAt                    time.Time
	StandingGenerationReplacement bool
}

// CommittedFlowInstanceActivation is exact selected-store evidence that the
// planned activation is durable. Process-local topology and readiness may be
// published only after this value is returned.
type CommittedFlowInstanceActivation struct {
	Plan                    FlowInstanceActivationPlan
	Created                 bool
	Lifecycle               CommittedWorkflowLifecycleMutation
	ReadinessAttemptOrdinal uint64
	Children                []CommittedFlowInstanceActivation
	// Acknowledged is set only after the selected-store commit is acknowledged.
	Acknowledged bool
}

// WithCommitAcknowledgment promotes the complete construction tree only after
// its enclosing selected-store transaction has acknowledged the commit.
func (a CommittedFlowInstanceActivation) WithCommitAcknowledgment() CommittedFlowInstanceActivation {
	a.Acknowledged = true
	children := make([]CommittedFlowInstanceActivation, len(a.Children))
	for index, child := range a.Children {
		children[index] = child.WithCommitAcknowledgment()
	}
	a.Children = children
	return a
}

func (a CommittedFlowInstanceActivation) Validate() error {
	if err := a.Plan.Validate(); err != nil {
		return err
	}
	if a.ReadinessAttemptOrdinal == 0 {
		return fmt.Errorf("committed flow instance activation requires an exact attachment attempt")
	}
	if err := a.Lifecycle.Validate(); err != nil {
		return fmt.Errorf("committed flow instance activation lifecycle: %w", err)
	}
	if !a.Created && !emptyCommittedWorkflowLifecycleMutation(a.Lifecycle) {
		return fmt.Errorf("replayed flow instance activation cannot carry new lifecycle evidence")
	}
	if len(a.Children) != len(a.Plan.Children) {
		return fmt.Errorf("committed construction omitted descendant evidence")
	}
	for index, child := range a.Children {
		if child.Plan.Identity != a.Plan.Children[index].Identity || (!a.Created && child.Created) {
			return fmt.Errorf("committed construction changed descendant identity or repeated construction")
		}
		if err := child.Validate(); err != nil {
			return fmt.Errorf("committed construction descendant %d: %w", index, err)
		}
	}
	return nil
}

// ConstructionPlans returns the exact parent-first tree for route projection.
// Persistence commits this same tree; callers cannot infer missing descendants.
func (p FlowInstanceActivationPlan) ConstructionPlans() []FlowInstanceActivationPlan {
	plans := []FlowInstanceActivationPlan{p}
	for _, child := range p.Children {
		plans = append(plans, child.ConstructionPlans()...)
	}
	return plans
}

// FlowInstanceActivationRecord is the exact immutable persistence projection
// for one planned activation. Runtime derives semantic facts; selected-store
// adapters decide only how those facts are represented by their backend.
type FlowInstanceActivationRecord struct {
	State                    WorkflowEngineStateRecord
	Identity                 runtimeflowidentity.RunScopedFlowInstance
	EntityID                 string
	WorkflowName             string
	WorkflowVersion          string
	Mode                     string
	CurrentState             string
	EntityType               string
	Slug                     string
	Name                     string
	Fields                   json.RawMessage
	Bookkeeping              json.RawMessage
	Gates                    json.RawMessage
	Accumulator              json.RawMessage
	Config                   json.RawMessage
	InitialProjectionVersion int
	InitialMaterialization   json.RawMessage
	Readiness                json.RawMessage
	ReadinessPlanHash        string
	EnteredStageAt           time.Time
	CreatedAt                time.Time
}

func (r FlowInstanceActivationRecord) Validate() error {
	if err := r.State.Validate(); err != nil {
		return fmt.Errorf("flow instance activation state: %w", err)
	}
	if !r.State.Transition.CreatesState() {
		return fmt.Errorf("flow instance activation requires a creating state record")
	}
	r.Identity = r.Identity.Normalize()
	if err := r.Identity.Validate(); err != nil || strings.TrimSpace(r.EntityID) == "" {
		return fmt.Errorf("flow instance activation record requires exact run, route, and entity identity")
	}
	if r.State.Identity != r.Identity || r.State.EntityID != r.EntityID {
		return fmt.Errorf("flow instance activation state identity disagrees with activation record")
	}
	if strings.TrimSpace(r.WorkflowName) == "" || strings.TrimSpace(r.WorkflowVersion) == "" || strings.TrimSpace(r.CurrentState) == "" {
		return fmt.Errorf("flow instance activation record requires exact workflow and initial state")
	}
	if strings.TrimSpace(r.State.EntityType) != strings.TrimSpace(r.EntityType) {
		return fmt.Errorf("flow instance activation record requires one exact entity contract")
	}
	if r.InitialProjectionVersion != workflowInitialMaterializationProjectionVersion {
		return fmt.Errorf("flow instance activation record requires initial projection version %d", workflowInitialMaterializationProjectionVersion)
	}
	if r.Mode != "template" && r.Mode != "static" {
		return fmt.Errorf("flow instance activation record mode %q is unsupported", r.Mode)
	}
	for label, raw := range map[string]json.RawMessage{
		"fields": r.Fields, "bookkeeping": r.Bookkeeping, "gates": r.Gates, "accumulator": r.Accumulator,
		"config": r.Config, "initial materialization": r.InitialMaterialization,
		"readiness": r.Readiness,
	} {
		if len(raw) == 0 || !json.Valid(raw) {
			return fmt.Errorf("flow instance activation record %s must be valid JSON", label)
		}
	}
	if r.EnteredStageAt.IsZero() || r.CreatedAt.IsZero() {
		return fmt.Errorf("flow instance activation record requires exact persisted times")
	}
	if _, err := DecodeFlowReadinessPlan(r.Readiness, r.ReadinessPlanHash); err != nil {
		return fmt.Errorf("flow instance activation record readiness hash is invalid")
	}
	return nil
}

// PersistenceRecord closes the planner/store boundary without exposing a
// database handle, transaction, dialect, or callback to runtime code.
func (p FlowInstanceActivationPlan) PersistenceRecord() (FlowInstanceActivationRecord, error) {
	normalized, err := p.Normalized()
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	if err := normalized.Validate(); err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(normalized.Instance)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	if !ok || identity.StorageRef != normalized.Identity.InstancePath || identity.RowID() != normalized.Identity.EntityID {
		return FlowInstanceActivationRecord{}, fmt.Errorf("flow instance activation persistence identity disagrees with planned route")
	}
	projection, err := workflowInstancePersistedProjectionFromInstance(instance, identity.StorageRef)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	fields, err := canonicaljson.MarshalPreservingNumberKinds(projection.Fields)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	bookkeeping, err := canonicaljson.MarshalPreservingNumberKinds(projection.Bookkeeping)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	gates, err := canonicaljson.MarshalPreservingNumberKinds(projection.GatesAny())
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	accumulator, err := canonicaljson.MarshalPreservingNumberKinds(projection.Accumulator)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	config, err := canonicaljson.MarshalPreservingNumberKinds(projection.ConfigPayload(instance.WorkflowVersion))
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	initial := workflowInitialMaterializationProjection{
		Version:         workflowInitialMaterializationProjectionVersion,
		RunID:           normalized.Readiness.RunID,
		EntityID:        identity.RowID(),
		FlowInstance:    identity.StorageRef,
		WorkflowName:    instance.WorkflowName,
		WorkflowVersion: instance.WorkflowVersion,
		InitialState:    instance.CurrentState,
		OccurredAt:      canonicalWorkflowInstancePersistedTime(normalized.OccurredAt),
		Persisted:       projection,
		Identity:        normalized.Identity,
		BundleHash:      normalized.Readiness.BundleHash,
		ExecutionMode:   normalized.Readiness.ExecutionMode,
		CreationEvent:   normalized.Readiness.CreationEvent,
		CreatingInput:   normalized.CreatingInput,
	}
	initialJSON, err := EncodeFlowConstructionReceipt(initial)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	readinessJSON, err := canonicaljson.MarshalPreservingNumberKinds(normalized.Readiness)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	readinessHash, err := normalized.Readiness.Hash()
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	flowOwner, err := runtimeflowidentity.NewRunScopedFlowInstance(normalized.Readiness.RunID, normalized.Identity.Route())
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	state, err := workflowEngineStateRecord(flowOwner, instance, "", 0, WorkflowEngineStateTransitionCreateStateAndCompanion, normalized.OccurredAt)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	record := FlowInstanceActivationRecord{
		State: state, Identity: state.Identity, EntityID: identity.RowID(),
		WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion, Mode: workflowInstanceMode(instance),
		CurrentState: instance.CurrentState, EntityType: projection.Control.EntityType, Slug: projection.Control.Slug, Name: projection.Control.Name,
		Fields: fields, Bookkeeping: bookkeeping, Gates: gates, Accumulator: accumulator, Config: config,
		InitialProjectionVersion: workflowInitialMaterializationProjectionVersion,
		InitialMaterialization:   initialJSON, Readiness: readinessJSON, ReadinessPlanHash: readinessHash,
		EnteredStageAt: canonicalWorkflowInstancePersistedTime(instance.EnteredStageAt),
		CreatedAt:      canonicalWorkflowInstancePersistedTime(instance.CreatedAt),
	}
	if err := record.Validate(); err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	return record, nil
}

func (p FlowInstanceActivationPlan) Normalized() (FlowInstanceActivationPlan, error) {
	p.Instance.Fields = cloneMap(p.Instance.Fields)
	p.Instance.Bookkeeping = cloneMap(p.Instance.Bookkeeping)
	readiness, err := p.Readiness.Normalized()
	if err != nil {
		return FlowInstanceActivationPlan{}, fmt.Errorf("flow instance activation readiness: %w", err)
	}
	p.Readiness = readiness
	p.Identity = readiness.Identity
	p.Instance.RuntimeReadiness = &p.Readiness
	p.Instance.EntityType = strings.TrimSpace(p.Instance.EntityType)
	p.ActivationVariables = cloneStringMap(p.ActivationVariables)
	p.OccurredAt = p.OccurredAt.UTC()
	children := make([]FlowInstanceActivationPlan, len(p.Children))
	for index, child := range p.Children {
		children[index], err = child.Normalized()
		if err != nil {
			return FlowInstanceActivationPlan{}, fmt.Errorf("construction descendant %d: %w", index, err)
		}
	}
	p.Children = children
	return p, nil
}

func (p FlowInstanceActivationPlan) Validate() error {
	if err := p.CreatingInput.Validate(); err != nil {
		return err
	}
	var err error
	p, err = p.Normalized()
	if err != nil {
		return err
	}
	if !p.Identity.Route().Valid() || p.Instance.StorageRef == "" || p.Instance.InstanceID == "" {
		return fmt.Errorf("flow instance activation plan requires exact instance identity")
	}
	if p.Instance.EntityType == "" && len(p.Instance.Fields) != 0 {
		return fmt.Errorf("fieldless flow instance activation cannot carry entity fields")
	}
	if p.Instance.StorageRef != p.Identity.InstancePath || p.Instance.InstanceID != p.Identity.InstanceID {
		return fmt.Errorf("flow instance activation plan identity does not match workflow instance")
	}
	if p.OccurredAt.IsZero() {
		return fmt.Errorf("flow instance activation plan requires exact occurrence time")
	}
	if err := p.Lifecycle.Validate(p.Readiness.RunID, p.Identity.Route(), p.Identity.EntityID); err != nil {
		return fmt.Errorf("flow instance activation lifecycle: %w", err)
	}
	if p.Lifecycle.RequestCompletionCandidate {
		return fmt.Errorf("flow instance activation cannot request run completion")
	}
	seen := map[string]bool{p.Identity.InstancePath: true}
	for index, child := range p.Children {
		withinParent := strings.HasPrefix(child.Identity.InstancePath, p.Identity.InstancePath+"/") ||
			(p.Identity.TemplateID == "." && p.Identity.InstancePath == p.Readiness.RunID && child.Identity.InstancePath == child.Identity.ScopeKey && !strings.Contains(child.Identity.ScopeKey, "/"))
		if child.Identity.ParentRoute != (runtimeflowidentity.ParentRoute{FlowID: p.Identity.TemplateID, FlowInstance: p.Identity.InstancePath, EntityID: p.Identity.EntityID}) ||
			child.Instance.ParentEntityID != p.Identity.EntityID || child.Readiness.RunID != p.Readiness.RunID ||
			child.Readiness.BundleHash != p.Readiness.BundleHash || child.Readiness.WorkflowVersion != p.Readiness.WorkflowVersion || child.Readiness.ExecutionMode != p.Readiness.ExecutionMode ||
			!child.OccurredAt.Equal(p.OccurredAt) || child.CreatingInput.EventID != p.CreatingInput.EventID || !withinParent {
			return fmt.Errorf("construction descendant %d disagrees with its exact parent/source/occurrence", index)
		}
		if err := child.Validate(); err != nil {
			return fmt.Errorf("construction descendant %d: %w", index, err)
		}
		for _, descendant := range child.ConstructionPlans() {
			if seen[descendant.Identity.InstancePath] {
				return fmt.Errorf("construction repeats descendant %s", descendant.Identity.InstancePath)
			}
			seen[descendant.Identity.InstancePath] = true
		}
	}
	return nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type FlowInstanceActivator func(context.Context, FlowInstanceActivationRequest) error

type FlowInstanceActivationPlanner interface {
	PrepareFlowInstanceActivation(context.Context, FlowInstanceActivationRequest) (FlowInstanceActivationPlan, error)
}

// FlowInstanceActivationConflict preserves an occupied immutable constructor
// as a hard error. Only a rolled-back inbound select-or-create plan may
// reconcile this coordinate through the durable receipt owner.
type FlowInstanceActivationConflict struct {
	Owner runtimeflowidentity.RunScopedFlowInstance
	Cause error
}

func (e *FlowInstanceActivationConflict) Error() string { return e.Cause.Error() }
func (e *FlowInstanceActivationConflict) Unwrap() error { return e.Cause }

// IsolatedFlowInstanceActivationConflict refuses independent errors joined
// either by the transaction owner or by a later operation-boundary wrapper.
func IsolatedFlowInstanceActivationConflict(err error) (runtimeflowidentity.RunScopedFlowInstance, bool) {
	for err != nil {
		if conflict, ok := err.(*FlowInstanceActivationConflict); ok {
			return conflict.Owner, conflict.Owner.Validate() == nil
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) != 1 {
				return runtimeflowidentity.RunScopedFlowInstance{}, false
			}
			err = children[0]
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		default:
			return runtimeflowidentity.RunScopedFlowInstance{}, false
		}
	}
	return runtimeflowidentity.RunScopedFlowInstance{}, false
}

// CommittedFlowInstanceActivationFinalizer consumes only durable activation
// evidence. It owns process-local topology installation and readiness retry;
// persistence remains entirely inside the selected-store operation.
type CommittedFlowInstanceActivationFinalizer interface {
	FinalizeCommittedFlowInstanceActivation(context.Context, CommittedFlowInstanceActivation) error
}

type CommittedFlowInstanceActivationFinalizerFunc func(context.Context, CommittedFlowInstanceActivation) error

func (fn CommittedFlowInstanceActivationFinalizerFunc) FinalizeCommittedFlowInstanceActivation(ctx context.Context, committed CommittedFlowInstanceActivation) error {
	if fn == nil {
		return fmt.Errorf("committed flow instance activation finalizer is required")
	}
	return fn(ctx, committed)
}

type FlowInstanceActivationPlannerFunc func(context.Context, FlowInstanceActivationRequest) (FlowInstanceActivationPlan, error)

func (fn FlowInstanceActivationPlannerFunc) PrepareFlowInstanceActivation(ctx context.Context, req FlowInstanceActivationRequest) (FlowInstanceActivationPlan, error) {
	if fn == nil {
		return FlowInstanceActivationPlan{}, fmt.Errorf("flow instance activation planner is required")
	}
	return fn(ctx, req)
}

type FlowInstanceDeactivationRequest struct {
	ContractBundle semanticview.Source
	Instance       runtimeflowidentity.Instance
	FinalState     string
}

// PreparedFlowInstanceDeactivation owns completion before a terminal mutation.
// Commit consumes selected-store commit evidence at the caller; Abort releases
// the unused reservation on rollback. Neither operation admits another task.
type PreparedFlowInstanceDeactivation interface {
	Commit() error
	Abort() error
}

func DeriveFlowInstancePath(source semanticview.Source, templateID, instanceID string) string {
	return runtimeflowidentity.InstancePath(source, templateID, instanceID)
}

func (pc *PipelineCoordinator) handlerEmitEnvelope(ctx context.Context, triggerCtx workflowTriggerContext, eventType string) map[string]any {
	payload := parsePayloadMap(triggerCtx.Event.Payload())
	out := map[string]any{}
	entityID := resolveEmittedEntityID(
		pc.SemanticSource(),
		pipelineFlowScope(ctx),
		eventType,
		triggerCtx.State,
		triggerCtx.Event,
		triggerCtx.State.EntityID,
		workflowEventEntityIDWithPayload(triggerCtx.Event, payload),
	)
	if entityID != "" {
		out["entity_id"] = entityID
	}
	if strings.TrimSpace(eventType) != "" {
		out["trigger_event_type"] = strings.TrimSpace(string(triggerCtx.Event.Type()))
	}
	if state := strings.TrimSpace(string(triggerCtx.State.Stage)); state != "" {
		out["current_state"] = state
	}
	return out
}
