package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type FlowInstanceActivationRequest struct {
	Context                       events.DeliveryContext
	ContractBundle                semanticview.Source
	Instance                      runtimeflowidentity.Instance
	InitialState                  string
	Config                        map[string]any
	Fields                        map[string]any
	Bookkeeping                   map[string]any
	TriggerEvent                  events.Event
	OccurredAt                    time.Time
	StandingGenerationReplacement bool
}

// FlowInstanceActivationPlan is the exact durable command derived from one
// admitted activation request. It contains semantic facts only: selected-store
// adapters own persistence and return post-commit evidence separately.
type FlowInstanceActivationPlan struct {
	Instance                      WorkflowInstance
	Identity                      runtimeflowidentity.Instance
	Readiness                     DynamicFlowRuntimeReadinessPlan
	Lifecycle                     WorkflowLifecycleMutationPlan
	ActivationVariables           map[string]string
	OccurredAt                    time.Time
	StandingGenerationReplacement bool
}

// CommittedFlowInstanceActivation is exact selected-store evidence that the
// planned activation is durable. Process-local topology and readiness may be
// published only after this value is returned.
type CommittedFlowInstanceActivation struct {
	Plan      FlowInstanceActivationPlan
	Created   bool
	Lifecycle CommittedWorkflowLifecycleMutation
}

func (a CommittedFlowInstanceActivation) Validate() error {
	if err := a.Plan.Validate(); err != nil {
		return err
	}
	if err := a.Lifecycle.Validate(); err != nil {
		return fmt.Errorf("committed flow instance activation lifecycle: %w", err)
	}
	if !a.Created && !emptyCommittedWorkflowLifecycleMutation(a.Lifecycle) {
		return fmt.Errorf("replayed flow instance activation cannot carry new lifecycle evidence")
	}
	return nil
}

// FlowInstanceActivationRecord is the exact immutable persistence projection
// for one planned activation. Runtime derives semantic facts; selected-store
// adapters decide only how those facts are represented by their backend.
type FlowInstanceActivationRecord struct {
	State                    WorkflowEngineStateRecord
	RunID                    string
	Route                    runtimeflowidentity.Route
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
	r.Route = runtimeflowidentity.StoredRoute(r.Route.ScopeKey, r.Route.InstanceID, r.Route.InstancePath)
	if strings.TrimSpace(r.RunID) == "" || !r.Route.Valid() || strings.TrimSpace(r.EntityID) == "" {
		return fmt.Errorf("flow instance activation record requires exact run, route, and entity identity")
	}
	if r.State.RunID != r.RunID || r.State.Route != r.Route || r.State.EntityID != r.EntityID {
		return fmt.Errorf("flow instance activation state identity disagrees with activation record")
	}
	if strings.TrimSpace(r.WorkflowName) == "" || strings.TrimSpace(r.WorkflowVersion) == "" || strings.TrimSpace(r.CurrentState) == "" {
		return fmt.Errorf("flow instance activation record requires exact workflow and initial state")
	}
	if r.InitialProjectionVersion != workflowInitialMaterializationProjectionVersion {
		return fmt.Errorf("flow instance activation record requires initial projection version %d", workflowInitialMaterializationProjectionVersion)
	}
	if r.Mode != "template" {
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
	fields, err := canonicaljson.Bytes(projection.Fields)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	bookkeeping, err := canonicaljson.Bytes(projection.Bookkeeping)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	gates, err := canonicaljson.Bytes(projection.GatesAny())
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	accumulator, err := canonicaljson.Bytes(projection.Accumulator)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	config, err := canonicaljson.Bytes(projection.ConfigPayload(instance.WorkflowVersion))
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
	}
	initialJSON, err := canonicaljson.Bytes(initial)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	readinessJSON, err := canonicaljson.Bytes(normalized.Readiness)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	state, err := workflowEngineStateRecord(normalized.Readiness.RunID, normalized.Identity.Route(), instance, "", 0, WorkflowEngineStateTransitionCreateStateAndCompanion, normalized.OccurredAt)
	if err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	record := FlowInstanceActivationRecord{
		State: state,
		RunID: normalized.Readiness.RunID, Route: normalized.Identity.Route(), EntityID: identity.RowID(),
		WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion, Mode: workflowInstanceMode(instance),
		CurrentState: instance.CurrentState, EntityType: projection.Control.EntityType, Slug: projection.Control.Slug, Name: projection.Control.Name,
		Fields: fields, Bookkeeping: bookkeeping, Gates: gates, Accumulator: accumulator, Config: config,
		InitialProjectionVersion: workflowInitialMaterializationProjectionVersion,
		InitialMaterialization:   initialJSON, Readiness: readinessJSON,
		EnteredStageAt: canonicalWorkflowInstancePersistedTime(instance.EnteredStageAt),
		CreatedAt:      canonicalWorkflowInstancePersistedTime(instance.CreatedAt),
	}
	if err := record.Validate(); err != nil {
		return FlowInstanceActivationRecord{}, err
	}
	return record, nil
}

func (p FlowInstanceActivationPlan) Normalized() (FlowInstanceActivationPlan, error) {
	p.Instance.Config = cloneMap(p.Instance.Config)
	p.Instance.Fields = cloneMap(p.Instance.Fields)
	p.Instance.Bookkeeping = cloneMap(p.Instance.Bookkeeping)
	readiness, err := p.Readiness.Normalized()
	if err != nil {
		return FlowInstanceActivationPlan{}, fmt.Errorf("flow instance activation readiness: %w", err)
	}
	p.Readiness = readiness
	p.Identity = readiness.Identity
	p.Instance.RuntimeReadiness = &p.Readiness
	p.ActivationVariables = cloneStringMap(p.ActivationVariables)
	p.OccurredAt = p.OccurredAt.UTC()
	return p, nil
}

func (p FlowInstanceActivationPlan) Validate() error {
	var err error
	p, err = p.Normalized()
	if err != nil {
		return err
	}
	if !p.Identity.Route().Valid() || p.Instance.StorageRef == "" || p.Instance.InstanceID == "" {
		return fmt.Errorf("flow instance activation plan requires exact instance identity")
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

type FlowInstanceDeactivator func(context.Context, FlowInstanceDeactivationRequest) error

type flowInstanceConfigRefError struct {
	Key    string
	Ref    string
	Reason string
}

func (e flowInstanceConfigRefError) Error() string {
	return fmt.Sprintf("create_flow_instance config_from %q ref %q %s", e.Key, e.Ref, e.Reason)
}

func (pc *PipelineCoordinator) createFlowInstance(ctx context.Context, triggerCtx workflowTriggerContext, plan handlerExecutionPlan, handlerContext values.Context) error {
	if pc == nil || pc.instanceActivator == nil {
		return fmt.Errorf("flow instance activator is not configured")
	}
	templateID := strings.TrimSpace(plan.Template)
	if templateID == "" {
		return fmt.Errorf("flow instance template is required")
	}
	if source := pc.SemanticSource(); source != nil {
		schema, ok := source.FlowSchemaByID(templateID)
		if !ok || !strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			return fmt.Errorf("flow template %s is not a template flow", templateID)
		}
	}
	entityID := workflowEventEntityID(triggerCtx.Event)
	payload := parsePayloadMap(triggerCtx.Event.Payload())
	entity := map[string]any{
		"entity_id": entityID,
	}
	if !hasRequiredCreateFlowInstanceSiblings(plan) {
		return fmt.Errorf("create_flow_instance requires non-empty instance_id_from and config_from")
	}
	instanceID := strings.TrimSpace(resolveFlowInstanceID(plan.InstanceIDPath, plan.InstanceIDFrom, payload, entity))
	if instanceID == "" {
		return fmt.Errorf("create_flow_instance instance_id_from resolved empty")
	}
	sourceEntityID := strings.TrimSpace(entityID)
	instance := runtimeflowidentity.Derive(pc.SemanticSource(), templateID, instanceID)
	instance.ParentEntityID = sourceEntityID
	instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID:       strings.TrimSpace(pipelineFlowScope(ctx)),
		FlowInstance: strings.Trim(strings.TrimSpace(triggerCtx.Event.FlowInstance()), "/"),
		EntityID:     sourceEntityID,
	}
	req := FlowInstanceActivationRequest{
		Context:        events.DeliveryContextFromContext(ctx),
		ContractBundle: pc.SemanticSource(),
		Instance:       instance,
		InitialState:   strings.TrimSpace(plan.AdvancesTo),
		Config:         map[string]any{},
		TriggerEvent:   triggerCtx.Event,
		OccurredAt:     triggerCtx.Event.CreatedAt(),
	}
	config, err := resolveFlowInstanceConfig(plan.ConfigFrom, handlerContext)
	if err != nil {
		return err
	}
	req.Config = config
	if len(req.Config) == 0 {
		return fmt.Errorf("create_flow_instance config_from resolved empty")
	}
	if err := pc.instanceActivator(ctx, req); err != nil {
		return err
	}
	return nil
}

func hasRequiredCreateFlowInstanceSiblings(plan handlerExecutionPlan) bool {
	if strings.TrimSpace(plan.InstanceIDFrom) == "" && !plan.InstanceIDPath.HasExplicitRoot() {
		return false
	}
	if plan.ConfigFrom == nil {
		return false
	}
	return len(plan.ConfigFrom.ConfigEntries()) > 0
}

func createFlowInstanceHandlerContext(triggerCtx workflowTriggerContext, payload, entity map[string]any) values.Context {
	handlerContext := values.NewContext()
	handlerContext.Event = values.Wrap(triggerCtx.Event.ContextMap(string(triggerCtx.State.Stage)))
	handlerContext.Payload = values.Wrap(payload)
	handlerContext.Entity = values.Wrap(entity)
	return handlerContext
}

func resolveFlowInstanceConfig(spec *runtimecontracts.ConfigFromSpec, handlerContext values.Context) (map[string]any, error) {
	if spec == nil {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	for _, entry := range spec.ConfigEntries() {
		key := strings.TrimSpace(entry.Key)
		if key == "" {
			continue
		}
		value, err := resolveFlowInstanceConfigValue(entry, handlerContext)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func resolveFlowInstanceConfigValue(entry runtimecontracts.ConfigBinding, handlerContext values.Context) (any, error) {
	key := strings.TrimSpace(entry.Key)
	ref := strings.TrimSpace(entry.Ref)
	if ref == "" {
		return nil, flowInstanceConfigRefError{Key: key, Ref: entry.Ref, Reason: "is empty"}
	}
	if entry.RefPath.HasExplicitRoot() {
		switch entry.RefPath.Root {
		case paths.RootPayload, paths.RootEntity, paths.RootPlatformEntity, paths.RootEvent:
			if entry.RefPath.Root == paths.RootEvent {
				if err := events.ValidateEventContextReference(strings.Join(entry.RefPath.Segments, ".")); err != nil {
					return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: err.Error()}
				}
			}
			value, ok := lookupFlowInstanceConfigPath(handlerContext, entry.RefPath)
			if !ok {
				return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: "resolved empty"}
			}
			return value, nil
		default:
			root := entry.RefPath.Root.String()
			if root == "" {
				root = strings.Split(ref, ".")[0]
			}
			return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: fmt.Sprintf("uses unsupported root %q", root)}
		}
	}
	segments := strings.Split(ref, ".")
	if len(segments) == 1 {
		if value, ok := lookupFlowInstanceConfigPath(handlerContext, paths.Path{Root: paths.RootPayload, Segments: segments, Raw: ref}); ok {
			return value, nil
		}
		if value, ok := lookupFlowInstanceConfigPath(handlerContext, paths.Path{Root: paths.RootEntity, Segments: segments, Raw: ref}); ok {
			return value, nil
		}
		return ref, nil
	}
	return nil, flowInstanceConfigRefError{Key: key, Ref: ref, Reason: "requires supported root payload, entity, _entity, or event"}
}

func lookupFlowInstanceConfigPath(handlerContext values.Context, path paths.Path) (any, bool) {
	if path.IsZero() || !path.HasExplicitRoot() {
		return nil, false
	}
	current := any(handlerContext.Bucket(path.Root).Raw())
	for _, segment := range path.Segments {
		object, ok := flowInstanceConfigObject(current)
		if !ok {
			return nil, false
		}
		current, ok = object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func flowInstanceConfigObject(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case values.Bucket:
		return typed.Raw(), true
	default:
		return nil, false
	}
}

func resolveFlowInstanceID(pathSpec paths.Path, expr string, payload, entity map[string]any) string {
	if value, ok := resolveFlowInstanceValue(pathSpec, expr, payload, entity); ok {
		return strings.TrimSpace(asString(value))
	}
	return ""
}

func resolveFlowInstanceValue(pathSpec paths.Path, expr string, payload, entity map[string]any) (any, bool) {
	if pathSpec.HasExplicitRoot() {
		if value, ok := resolveFlowInstancePath(pathSpec, payload, entity); ok {
			return value, true
		}
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	segments := strings.Split(expr, ".")
	if len(segments) == 1 {
		if value, ok := payload[segments[0]]; ok {
			return value, true
		}
		if value, ok := entity[segments[0]]; ok {
			return value, true
		}
		return expr, true
	}
	switch strings.TrimSpace(segments[0]) {
	case "payload":
		return resolveFlowInstanceSegments(payload, segments[1:])
	case "entity":
		return resolveFlowInstanceSegments(entity, segments[1:])
	default:
		return nil, false
	}
}

func resolveFlowInstancePath(pathSpec paths.Path, payload, entity map[string]any) (any, bool) {
	switch pathSpec.Root {
	case paths.RootPayload:
		return resolveFlowInstanceSegments(payload, pathSpec.Segments)
	case paths.RootEntity:
		return resolveFlowInstanceSegments(entity, pathSpec.Segments)
	default:
		return nil, false
	}
}

func resolveFlowInstanceSegments(root map[string]any, segments []string) (any, bool) {
	current := any(root)
	for _, segment := range segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
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

func workflowEntityMetadataPayload(source semanticview.Source, flowID string, metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	allowed := workflowEntitySchemaFields(source, flowID)
	if len(allowed) == 0 {
		return nil
	}
	materialized := workflowMaterializeEntityFields(source, flowID, metadata)
	out := make(map[string]any, len(allowed))
	for key := range allowed {
		if value, ok := materialized[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
