package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerequiredagents "github.com/division-sh/swarm/internal/runtime/requiredagents"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type flowInstancePersistence interface {
	MaterializeInitialEntry(ctx context.Context, instance runtimepipeline.WorkflowInstance, occurredAt time.Time) (runtimepipeline.WorkflowInitialMaterializationResult, error)
	PrepareInitialEntryLifecycle(ctx context.Context, instance runtimepipeline.WorkflowInstance, occurredAt time.Time) (runtimepipeline.WorkflowInstance, runtimepipeline.WorkflowLifecycleMutationPlan, error)
	FinalizeInitialEntryLifecycle(ctx context.Context, committed runtimepipeline.CommittedWorkflowLifecycleMutation) error
	ArmInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error
	ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error
	RetireInitialEntryTimerWakeups(ctx context.Context, route runtimeflowidentity.Route) error
	ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error)
	LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error)
	ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error)
	MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error
	MarkTerminated(ctx context.Context, route runtimeflowidentity.Route, entityID identity.EntityID, terminatedAt time.Time) error
	Load(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error)
	LoadRouteRecoveryProjection(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstanceRouteRecoveryProjection, error)
}

type FlowInstanceRouteContextInstaller interface {
	StageFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type FlowInstanceActivationCommitter interface {
	CommitFlowInstanceActivation(context.Context, runtimepipeline.FlowInstanceActivationPlan) (runtimepipeline.CommittedFlowInstanceActivation, error)
}

type FlowInstanceRouteContextVerifier interface {
	HasFlowInstanceRoute(runtimeflowidentity.Route) bool
	VerifyFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error
}

type PersistedFlowInstanceRouteRestorer interface {
	PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type PublishedFlowInstanceRouteRetirer interface {
	RetirePublishedFlowInstanceRoute(runtimeflowidentity.Route) error
}

type FlowInstanceRouteContextRemover interface {
	RemoveFlowInstanceRouteContext(context.Context, runtimeflowidentity.Route) error
}

type FlowInstanceTerminalMutationOwner interface {
	CommitFlowInstanceTermination(context.Context, runtimepipeline.FlowInstanceTerminationRequest) (runtimepipeline.FlowInstanceTermination, error)
}

type terminalFlowInstanceSideEffectPlan struct {
	EntityID          string
	FlowPath          string
	AgentIdentities   []runtimeagentidentity.Identity
	SelfRetiringAgent runtimeagentidentity.Identity
	Route             runtimeflowidentity.Route
	RunID             string
	FinalState        string
}

func (am *AgentManager) ActivateFlowInstance(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	lease, err := am.beginWork(ctx, "flow instance activation")
	if err != nil {
		return err
	}
	defer func() { _ = lease.Done() }()
	ctx = lease.Context()
	if am.workflowInstances == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	preparedRequest, plan, err := am.prepareFlowInstanceActivation(ctx, req)
	if err != nil {
		return err
	}
	req = preparedRequest
	ctx = events.WithDeliveryContext(ctx, req.Context)
	flowPath := plan.Identity.InstancePath
	committer := am.roles.FlowActivation
	if committer == nil {
		return fmt.Errorf("flow instance activation requires the selected commit owner")
	}
	committed, err := committer.CommitFlowInstanceActivation(ctx, plan)
	if err != nil {
		return fmt.Errorf("persist flow instance %s: %w", flowPath, err)
	}
	if committed.Plan.Identity.Route() != plan.Identity.Route() {
		return fmt.Errorf("committed flow instance activation identity does not match plan")
	}
	if err := am.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		am.signalDynamicFlowRuntimeReadiness()
		return err
	}
	return nil
}

// PrepareFlowInstanceActivation derives the complete durable activation
// command without opening or joining a persistence transaction. Publication
// planning uses this operation before handing the command to the selected
// store's named commit owner.
func (am *AgentManager) PrepareFlowInstanceActivation(
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
) (runtimepipeline.FlowInstanceActivationPlan, error) {
	_, plan, err := am.prepareFlowInstanceActivation(ctx, req)
	return plan, err
}

func (am *AgentManager) FinalizeCommittedFlowInstanceActivation(
	ctx context.Context,
	committed runtimepipeline.CommittedFlowInstanceActivation,
) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	if err := committed.Validate(); err != nil {
		return err
	}
	plan := committed.Plan
	if committed.Created {
		if err := am.workflowInstances.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			return fmt.Errorf("finalize flow instance initial lifecycle: %w", err)
		}
	}
	err := am.reconcileCommittedDynamicFlowRuntimeReadinessPlan(ctx, plan.Readiness, am.semanticReadinessSource.source)
	if err != nil {
		am.signalDynamicFlowRuntimeReadiness()
	}
	return err
}

func (am *AgentManager) prepareFlowInstanceActivation(
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
) (runtimepipeline.FlowInstanceActivationRequest, runtimepipeline.FlowInstanceActivationPlan, error) {
	if am == nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("agent manager is required")
	}
	if am.workflowInstances == nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("workflow instance store is required")
	}
	if req.Context.Empty() {
		req.Context = events.DeliveryContextFromContext(ctx)
	}
	ctx = events.WithDeliveryContext(ctx, req.Context)
	if req.ContractBundle == nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("contract bundle is required")
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, req.ContractBundle)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	req.ContractBundle = admittedSource.source
	instance := req.Instance
	templateID := strings.TrimSpace(instance.TemplateID)
	instanceID := strings.TrimSpace(instance.InstanceID)
	flowEntityID := strings.TrimSpace(instance.EntityID)
	flowPath := strings.TrimSpace(instance.InstancePath)
	if templateID == "" || instanceID == "" || flowEntityID == "" || flowPath == "" {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("template_id, instance_id, and entity_id are required")
	}
	scope, ok := semanticview.FlowScopeByID(req.ContractBundle, templateID)
	if !ok {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("flow contract view not found: %s", templateID)
	}
	schema, ok := req.ContractBundle.FlowSchemaByID(templateID)
	if !ok {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("flow schema not found: %s", templateID)
	}
	initialState := strings.TrimSpace(schema.LoweredInitialState())
	if initialState == "" {
		initialState = strings.TrimSpace(req.InitialState)
	}
	if initialState == "" {
		initialState = "pending"
	}
	triggerEventID := strings.TrimSpace(req.TriggerEvent.ID())
	contextRunID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	triggerRunID := strings.TrimSpace(req.TriggerEvent.RunID())
	autoEmitRunID, err := exactFlowActivationRunID(ctx, req)
	if err != nil {
		if strings.TrimSpace(schema.AutoEmitOnCreate.Event) != "" &&
			((contextRunID == "" && triggerRunID == "") || triggerEventID == "") {
			return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf(
				"auto-emit %s requires exact trigger run_id and parent_event_id",
				strings.TrimSpace(schema.AutoEmitOnCreate.Event),
			)
		}
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	mode, err := flowInstanceActivationExecutionMode(ctx, req)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	autoEmitLineage := events.EventLineage{
		RunID:         autoEmitRunID,
		ParentEventID: triggerEventID,
		ExecutionMode: mode,
	}
	if strings.TrimSpace(schema.AutoEmitOnCreate.Event) != "" &&
		(strings.TrimSpace(autoEmitLineage.RunID) == "" || strings.TrimSpace(autoEmitLineage.ParentEventID) == "") {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf(
			"auto-emit %s requires exact trigger run_id and parent_event_id",
			strings.TrimSpace(schema.AutoEmitOnCreate.Event),
		)
	}
	occurredAt := req.OccurredAt.UTC()
	if !req.TriggerEvent.CreatedAt().IsZero() {
		occurredAt = req.TriggerEvent.CreatedAt().UTC()
	}
	if occurredAt.IsZero() {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("flow activation requires an exact occurrence time")
	}
	agentRecords, err := am.flowInstanceAgentRecords(req, schema, scope)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	readinessPlan, err := am.buildDynamicFlowRuntimeReadinessPlan(ctx, req, agentRecords, schema, autoEmitLineage, occurredAt)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Instance: runtimepipeline.WorkflowInstance{
			InstanceID:         instanceID,
			StorageRef:         flowPath,
			EntityID:           flowEntityID,
			EntityType:         strings.TrimSpace(schema.Entity),
			InstanceKind:       strings.TrimSpace(schema.Mode),
			ParentFlowID:       strings.TrimSpace(instance.ParentRoute.FlowID),
			ParentFlowInstance: strings.Trim(instance.ParentRoute.FlowInstance, "/"),
			ParentEntityID:     strings.TrimSpace(instance.ParentEntityID),
			WorkflowName:       templateID,
			WorkflowVersion:    strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
			RuntimeReadiness:   &readinessPlan,
			CurrentState:       initialState,
			Config:             cloneFlowConfig(req.Config),
			Fields:             cloneFlowConfig(req.Fields),
			Bookkeeping:        cloneFlowConfig(req.Bookkeeping),
			EnteredStageAt:     occurredAt,
			CreatedAt:          occurredAt,
		},
		Identity:                      instance,
		Readiness:                     readinessPlan,
		ActivationVariables:           flowActivationVars(req),
		OccurredAt:                    occurredAt,
		StandingGenerationReplacement: req.StandingGenerationReplacement,
	}
	ctx = runtimeeffects.WithExecutionMode(ctx, mode)
	plan.Instance, plan.Lifecycle, err = am.workflowInstances.PrepareInitialEntryLifecycle(ctx, plan.Instance, occurredAt)
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	plan, err = plan.Normalized()
	if err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	if err := plan.Validate(); err != nil {
		return runtimepipeline.FlowInstanceActivationRequest{}, runtimepipeline.FlowInstanceActivationPlan{}, err
	}
	return req, plan, nil
}

func flowInstanceActivationExecutionMode(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (executionmode.Mode, error) {
	triggerMode := req.TriggerEvent.ExecutionMode()
	contextMode, hasContextMode := runtimeeffects.ExecutionModeFromContext(ctx)
	if triggerMode.Valid() {
		if hasContextMode && contextMode != triggerMode {
			return "", fmt.Errorf("flow activation execution mode conflicts with trigger event")
		}
		return triggerMode, nil
	}
	if hasContextMode && contextMode.Valid() {
		return contextMode, nil
	}
	return "", fmt.Errorf("flow activation requires typed execution mode authority")
}

func (am *AgentManager) EnsureFlowInstance(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (bool, error) {
	if am == nil || am.workflowInstances == nil {
		return false, fmt.Errorf("workflow instance store is required")
	}
	if req.ContractBundle == nil {
		return false, fmt.Errorf("contract bundle is required")
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, req.ContractBundle)
	if err != nil {
		return false, err
	}
	req.ContractBundle = admittedSource.source
	instance := req.Instance
	stored, exists, err := am.workflowInstances.Load(ctx, instance.Route())
	if err != nil {
		return false, err
	}
	if !exists {
		if err := am.ActivateFlowInstance(ctx, req); err != nil {
			return false, err
		}
		return true, nil
	}
	if strings.TrimSpace(stored.WorkflowName) != strings.TrimSpace(instance.TemplateID) {
		return false, fmt.Errorf("standing flow instance %s belongs to template %s, not %s; run `swarm standing reset %s`",
			instance.InstancePath, stored.WorkflowName, instance.TemplateID, instance.InstanceID)
	}
	runID, err := exactFlowActivationRunID(ctx, req)
	if err != nil {
		return false, err
	}
	readinessPlan, err := am.reconcileEnsuredDynamicFlowRuntimeReadinessPlan(ctx, req, runID)
	if err != nil {
		return false, err
	}
	if err := am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, readinessPlan, req.ContractBundle); err != nil {
		am.signalDynamicFlowRuntimeReadiness()
		return false, err
	}
	return false, nil
}

func exactFlowActivationRunID(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (string, error) {
	contextRunID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	triggerRunID := strings.TrimSpace(req.TriggerEvent.RunID())
	if contextRunID != "" && triggerRunID != "" && contextRunID != triggerRunID {
		return "", fmt.Errorf("flow activation trigger run_id %s conflicts with context run_id %s", triggerRunID, contextRunID)
	}
	if contextRunID != "" {
		return contextRunID, nil
	}
	if triggerRunID != "" {
		return triggerRunID, nil
	}
	return "", fmt.Errorf("flow activation requires exact run_id")
}

func (am *AgentManager) flowInstanceAgentRecords(req runtimepipeline.FlowInstanceActivationRequest, schema runtimecontracts.FlowSchemaDocument, scope semanticview.FlowScope) ([]PersistedAgent, error) {
	instance := req.Instance
	vars := flowActivationVars(req)
	localEvents := flowLocalEventSet(schema, scope)
	agentKeys := make([]string, 0, len(scope.Agents))
	for key := range scope.Agents {
		if key = strings.TrimSpace(key); key != "" {
			agentKeys = append(agentKeys, key)
		}
	}
	sort.Strings(agentKeys)
	records := make([]PersistedAgent, 0, len(agentKeys))
	for _, key := range agentKeys {
		entry := scope.Agents[key]
		namePlan, err := semanticview.FlowAgentNamePlan(req.ContractBundle, scope, key)
		if err != nil {
			return nil, fmt.Errorf("flow agent %s declared name: %w", key, err)
		}
		cfg, err := buildFlowAgentConfig(req.ContractBundle, namePlan, instance.TemplateID, instance.InstanceID, instance.EntityID, instance.InstancePath, key, entry, vars, localEvents, req.Config)
		if err != nil {
			return nil, err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "flow-instance-activator",
			TemplateVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		}
		if err := am.resolveAgentModel(&rec.Config); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		return strings.TrimSpace(records[i].Config.ID) < strings.TrimSpace(records[j].Config.ID)
	})
	return records, nil
}

func (am *AgentManager) installFlowInstanceRoute(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	instance := req.Instance
	vars := flowActivationVars(req)
	request := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: instance.Route(), ActivationVariables: vars}
	if am.roles.RouteInstaller != nil {
		return am.roles.RouteInstaller.StageFlowInstanceRouteContext(ctx, request)
	}
	return fmt.Errorf("event bus does not support context-aware derived flow-instance routing for %s", instance.InstancePath)
}

func (am *AgentManager) logFlowInstanceActivationSideEffectFailure(req runtimepipeline.FlowInstanceActivationRequest, action, operation string, err error) {
	if am == nil || am.bus == nil || err == nil {
		return
	}
	_ = am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
		Level: "error", Message: "Flow instance runtime activation failed after commit",
		Component: "flow_activation", Action: strings.TrimSpace(action),
		EntityID: strings.TrimSpace(req.Instance.EntityID),
		Detail: map[string]any{
			"flow_path": strings.TrimSpace(req.Instance.InstancePath),
			"error":     err.Error(),
		},
		Failure: failureEnvelope(err, "flow_activation", strings.TrimSpace(operation)),
	})
}

var dynamicFlowCreationEventNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.dynamic-flow.creation-event.v1"))

func (am *AgentManager) buildDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
	agentRecords []PersistedAgent,
	schema runtimecontracts.FlowSchemaDocument,
	lineage events.EventLineage,
	occurredAt time.Time,
) (runtimepipeline.DynamicFlowRuntimeReadinessPlan, error) {
	bundleHash, bundleSource, err := dynamicFlowRuntimeReadinessSourceCoordinate(ctx)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	plan := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity:        req.Instance,
		RunID:           strings.TrimSpace(lineage.RunID),
		BundleHash:      bundleHash,
		BundleSource:    bundleSource,
		WorkflowVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		ExecutionMode:   lineage.ExecutionMode,
		Agents:          make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(agentRecords)),
	}
	for _, rec := range agentRecords {
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf(
				"derive dynamic flow agent identity %s: %w",
				rec.Config.ID,
				err,
			)
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, fmt.Errorf("derive dynamic flow agent revision %s: %w", rec.Config.ID, err)
		}
		plan.Agents = append(plan.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			Identity: identity, ConfigRevision: revision,
		})
	}
	creationEvent, err := buildDynamicFlowRuntimeCreationEventPlan(
		req.ContractBundle,
		schema,
		req.Instance.TemplateID,
		req.Instance.InstancePath,
		req.Instance.EntityID,
		lineage,
		req.Config,
		req.Context,
		occurredAt,
	)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessPlan{}, err
	}
	plan.CreationEvent = creationEvent
	return plan.Normalized()
}

func buildDynamicFlowRuntimeCreationEventPlan(
	source semanticview.Source,
	schema runtimecontracts.FlowSchemaDocument,
	templateID, flowPath, flowEntityID string,
	lineage events.EventLineage,
	config map[string]any,
	deliveryContext events.DeliveryContext,
	occurredAt time.Time,
) (*runtimepipeline.DynamicFlowRuntimeCreationEventPlan, error) {
	autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event)
	if autoEmit == "" {
		return nil, nil
	}
	if strings.TrimSpace(lineage.RunID) == "" || strings.TrimSpace(lineage.ParentEventID) == "" {
		return nil, fmt.Errorf("auto-emit %s requires exact trigger run_id and parent_event_id", autoEmit)
	}
	eventType := eventidentity.ExternalizeForFlow(flowPath, []string{autoEmit}, autoEmit)
	payload := map[string]any{}
	for key, value := range config {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		payload[key] = value
	}
	if err := validateAutoEmitPayload(source, templateID, autoEmit, payload); err != nil {
		return nil, fmt.Errorf("auto-emit %s: %w", autoEmit, err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode auto-emit payload %s: %w", autoEmit, err)
	}
	eventID := uuid.NewSHA1(dynamicFlowCreationEventNamespace, []byte(strings.Join([]string{
		strings.TrimSpace(lineage.RunID),
		strings.TrimSpace(lineage.ParentEventID),
		strings.Trim(strings.TrimSpace(flowPath), "/"),
		strings.TrimSpace(eventType),
	}, "\x00"))).String()
	return &runtimepipeline.DynamicFlowRuntimeCreationEventPlan{
		EventID: eventID, EventType: eventType, RunID: strings.TrimSpace(lineage.RunID),
		ParentEventID: strings.TrimSpace(lineage.ParentEventID), ExecutionMode: lineage.ExecutionMode,
		Payload: encoded, CreatedAt: occurredAt.UTC(), DeliveryContext: deliveryContext.Normalized(),
	}, nil
}

func validateAutoEmitPayload(source semanticview.Source, flowID, eventType string, payload map[string]any) error {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if source == nil || flowID == "" || eventType == "" {
		return nil
	}
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema {
		return nil
	}
	resolution := semanticview.ResolveEventSchema(source, flowID, eventType)
	if !resolution.HasSchema {
		return nil
	}
	if err := resolution.UnresolvedTypeError(); err != nil {
		return fmt.Errorf("%w for %s: %v", runtimebus.ErrPayloadValidation, proof.EventKey(), err)
	}
	schema := resolution.Schema
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, payload); err != nil {
		return fmt.Errorf("%w for %s: %v", runtimebus.ErrPayloadValidation, proof.EventKey(), err)
	}
	return nil
}

func (am *AgentManager) VerifyStaticFlowRequiredAgents(source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	records, err := StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return err
	}
	return am.verifyStaticAgentRecords(records)
}

func (am *AgentManager) VerifyStaticAgents(source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		return err
	}
	return am.verifyStaticAgentRecords(records)
}

func (am *AgentManager) verifyStaticAgentRecords(records []PersistedAgent) error {
	topology, err := am.staticTopologyAdmission()
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := am.resolveAgentModel(&rec.Config); err != nil {
			return err
		}
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return err
		}
		snapshot, ok := am.lifecycle.executionSnapshotByIdentity(identity)
		if !ok {
			return fmt.Errorf("static declaration %s was not hydrated by source-set reconciliation", identity.Description())
		}
		expectedRevision, err := lifecycleConfigRevision(rec)
		if err != nil {
			return err
		}
		actualRevision, err := lifecycleConfigRevision(PersistedAgent{Config: snapshot.Config})
		if err != nil {
			return err
		}
		state, ok := am.lifecycle.stateByIdentity(identity)
		if !ok {
			return fmt.Errorf("static declaration %s has no reconciled lifecycle state", identity.Description())
		}
		if expectedRevision != actualRevision {
			return fmt.Errorf(
				"static declaration %s config revision %s differs from reconciled execution %s",
				identity.Description(), expectedRevision, actualRevision,
			)
		}
		if !state.Topology.Equal(topology) {
			return fmt.Errorf("static declaration %s topology differs from reconciled execution", identity.Description())
		}
	}
	return nil
}

// StaticAgentMaterializationRecords derives the ordinary runtime static-agent
// materialization records without mutating the manager or persistence store.
func StaticAgentMaterializationRecords(source semanticview.Source) ([]PersistedAgent, error) {
	if source == nil {
		return nil, nil
	}
	standingFlows := standingActivatedFlowIDs(source)
	records := []PersistedAgent{}
	for _, scope := range source.ProjectScopes() {
		projectAgents := make(map[string]runtimecontracts.AgentRegistryEntry, len(scope.Agents))
		projectNamePlans := make(map[string]semanticview.AgentNamePlan, len(scope.Agents))
		packageFlowAgents := map[string]staticAgentFlowGroup{}
		for logicalID, entry := range scope.Agents {
			namePlan, err := semanticview.ProjectAgentNamePlan(source, scope, logicalID)
			if err != nil {
				return nil, fmt.Errorf("project scope %q agent %s declaration name: %w", scope.Key, logicalID, err)
			}
			proof := semanticview.ResolveAgentMemoryProof(source, semanticview.AgentMemoryLocator{
				AgentID:         logicalID,
				ProjectScopeKey: scope.Key,
			})
			if strings.TrimSpace(proof.OwningFlowID) != "" {
				if flowScope, ok := source.FlowScopeByID(proof.OwningFlowID); ok {
					if _, represented := flowScope.Agents[logicalID]; represented {
						flowPlan, flowPlanErr := semanticview.FlowAgentNamePlan(source, flowScope, logicalID)
						if flowPlanErr == nil && flowPlan.OwnerURI == namePlan.OwnerURI {
							continue
						}
					}
				}
				groupKey := staticAgentFlowGroupKey(proof.OwningFlowID, proof.FlowPath)
				group := packageFlowAgents[groupKey]
				group.FlowID = strings.TrimSpace(proof.OwningFlowID)
				group.FlowPath = strings.Trim(strings.TrimSpace(proof.FlowPath), "/")
				if group.Agents == nil {
					group.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
				}
				if group.NamePlans == nil {
					group.NamePlans = map[string]semanticview.AgentNamePlan{}
				}
				group.Agents[strings.TrimSpace(logicalID)] = entry
				group.NamePlans[strings.TrimSpace(logicalID)] = namePlan
				packageFlowAgents[groupKey] = group
				continue
			}
			projectAgents[strings.TrimSpace(logicalID)] = entry
			projectNamePlans[strings.TrimSpace(logicalID)] = namePlan
		}
		scopeRecords, err := staticAgentsForScope(source, "", "", projectAgents, projectNamePlans)
		if err != nil {
			return nil, err
		}
		records = append(records, scopeRecords...)
		groupKeys := make([]string, 0, len(packageFlowAgents))
		for key := range packageFlowAgents {
			groupKeys = append(groupKeys, key)
		}
		sort.Strings(groupKeys)
		for _, key := range groupKeys {
			group := packageFlowAgents[key]
			scopeRecords, err := staticAgentsForScope(source, group.FlowID, group.FlowPath, group.Agents, group.NamePlans)
			if err != nil {
				return nil, err
			}
			records = append(records, scopeRecords...)
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		if _, standing := standingFlows[flowID]; standing {
			continue
		}
		proof := semanticview.ResolveAgentMemoryProof(source, semanticview.AgentMemoryLocator{
			FlowID: flowID,
		})
		namePlans, err := flowAgentNamePlans(source, scope)
		if err != nil {
			return nil, err
		}
		scopeRecords, err := staticAgentsForScope(source, proof.OwningFlowID, proof.FlowPath, scope.Agents, namePlans)
		if err != nil {
			return nil, err
		}
		records = append(records, scopeRecords...)
	}
	return records, nil
}

// StaticFlowRequiredAgentMaterializationRecords derives the ordinary runtime
// required-flow-agent materialization records without mutating runtime state.
func StaticFlowRequiredAgentMaterializationRecords(source semanticview.Source) ([]PersistedAgent, error) {
	if source == nil {
		return nil, nil
	}
	standingFlows := standingActivatedFlowIDs(source)
	records := []PersistedAgent{}
	if rootScope, ok := runtimerequiredagents.RootScope(source); ok {
		projectScope, found := rootAgentProjectScope(source)
		if !found && len(rootScope.Required) > 0 {
			return nil, fmt.Errorf("root required agents have no exact project declaration scope")
		}
		namePlans, err := projectAgentNamePlans(source, projectScope)
		if err != nil {
			return nil, err
		}
		scopeRecords, err := staticRequiredAgentsForScope(source, "", "", rootScope.Agents, namePlans, rootScope.Required)
		if err != nil {
			return nil, err
		}
		records = append(records, scopeRecords...)
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		if _, standing := standingFlows[flowID]; standing {
			continue
		}
		namePlans, err := flowAgentNamePlans(source, scope)
		if err != nil {
			return nil, err
		}
		scopeRecords, err := staticRequiredAgentsForScope(source, flowID, strings.Trim(scope.Path, "/"), scope.Agents, namePlans, source.FlowRequiredAgents(flowID))
		if err != nil {
			return nil, err
		}
		records = append(records, scopeRecords...)
	}
	return records, nil
}

func standingActivatedFlowIDs(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return out
	}
	for _, pkg := range bundle.PackageTree {
		for _, ref := range pkg.Manifest.Flows {
			flowID := strings.TrimSpace(ref.ID)
			if flowID != "" && ref.HasStandingActivation() {
				out[flowID] = struct{}{}
			}
		}
	}
	return out
}

type staticAgentFlowGroup struct {
	FlowID    string
	FlowPath  string
	Agents    map[string]runtimecontracts.AgentRegistryEntry
	NamePlans map[string]semanticview.AgentNamePlan
}

func staticAgentFlowGroupKey(flowID, flowPath string) string {
	return strings.TrimSpace(flowID) + "\x00" + strings.Trim(strings.TrimSpace(flowPath), "/")
}

func projectAgentNamePlans(source semanticview.Source, scope semanticview.ProjectScope) (map[string]semanticview.AgentNamePlan, error) {
	plans := make(map[string]semanticview.AgentNamePlan, len(scope.Agents))
	for logicalID := range scope.Agents {
		plan, err := semanticview.ProjectAgentNamePlan(source, scope, logicalID)
		if err != nil {
			return nil, fmt.Errorf("project scope %q agent %s declaration name: %w", scope.Key, logicalID, err)
		}
		plans[strings.TrimSpace(logicalID)] = plan
	}
	return plans, nil
}

func flowAgentNamePlans(source semanticview.Source, scope semanticview.FlowScope) (map[string]semanticview.AgentNamePlan, error) {
	plans := make(map[string]semanticview.AgentNamePlan, len(scope.Agents))
	for logicalID := range scope.Agents {
		plan, err := semanticview.FlowAgentNamePlan(source, scope, logicalID)
		if err != nil {
			return nil, fmt.Errorf("flow scope %q agent %s declaration name: %w", scope.ID, logicalID, err)
		}
		plans[strings.TrimSpace(logicalID)] = plan
	}
	return plans, nil
}

func rootAgentProjectScope(source semanticview.Source) (semanticview.ProjectScope, bool) {
	for _, scope := range source.ProjectScopes() {
		if strings.TrimSpace(scope.OwningFlowID) == "" && scope.Depth == 0 {
			return scope, true
		}
	}
	for _, scope := range source.ProjectScopes() {
		if strings.TrimSpace(scope.OwningFlowID) == "" {
			return scope, true
		}
	}
	return semanticview.ProjectScope{}, false
}

func (am *AgentManager) DeactivateFlowInstanceModel(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	lease, err := am.beginWork(ctx, "flow instance deactivation")
	if err != nil {
		return err
	}
	defer func() { _ = lease.Done() }()
	ctx = lease.Context()
	if am.workflowInstances == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	instance := req.Instance
	templateID := strings.TrimSpace(instance.TemplateID)
	instanceID := strings.TrimSpace(instance.InstanceID)
	flowPath := strings.TrimSpace(instance.InstancePath)
	entityID := strings.TrimSpace(instance.EntityID)
	if templateID == "" || instanceID == "" || flowPath == "" || entityID == "" {
		return fmt.Errorf("template_id, instance_id, flow_path, and entity_id are required")
	}
	owner := am.roles.FlowTermination
	if owner == nil {
		return fmt.Errorf("flow instance terminalization requires selected pipeline mutation ownership")
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	termination, err := owner.CommitFlowInstanceTermination(ctx, runtimepipeline.FlowInstanceTerminationRequest{
		Route: instance.Route(), EntityID: identity.NormalizeEntityID(entityID), RunID: runID, TerminatedAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("persist flow instance terminalization %s: %w", flowPath, err)
	}
	plan, err := am.terminalFlowInstanceSideEffectPlan(ctx, req, termination)
	if err != nil {
		return err
	}
	if err := am.applyTerminalFlowInstanceSideEffects(ctx, plan); err != nil {
		am.logTerminalFlowInstanceSideEffectFailure(plan, err)
		return err
	}
	return nil
}

func (am *AgentManager) terminalFlowInstanceSideEffectPlan(
	ctx context.Context,
	req runtimepipeline.FlowInstanceDeactivationRequest,
	termination runtimepipeline.FlowInstanceTermination,
) (terminalFlowInstanceSideEffectPlan, error) {
	instance := req.Instance
	entityID := strings.TrimSpace(instance.EntityID)
	canonicalInstance := termination.Instance
	canonicalFlowPath := strings.TrimSpace(canonicalInstance.StorageRef)
	if canonicalFlowPath == "" {
		return terminalFlowInstanceSideEffectPlan{}, fmt.Errorf("canonical terminal flow instance missing storage_ref")
	}
	canonicalRoute := termination.Route
	configs := am.lifecycle.executionConfigs()
	agentIdentities := make([]runtimeagentidentity.Identity, 0, len(configs))
	for _, cfg := range configs {
		if cfg.CanonicalFlowPath() != canonicalFlowPath {
			continue
		}
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			return terminalFlowInstanceSideEffectPlan{}, fmt.Errorf("terminal flow instance agent %q identity: %w", cfg.ID, err)
		}
		agentIdentities = append(agentIdentities, identity)
	}
	sort.Slice(agentIdentities, func(i, j int) bool {
		if agentIdentities[i].AgentID() != agentIdentities[j].AgentID() {
			return agentIdentities[i].AgentID() < agentIdentities[j].AgentID()
		}
		return agentIdentities[i].FlowInstance() < agentIdentities[j].FlowInstance()
	})
	selfRetiringAgent, err := terminalFlowSelfRetiringAgent(ctx, agentIdentities)
	if err != nil {
		return terminalFlowInstanceSideEffectPlan{}, err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return terminalFlowInstanceSideEffectPlan{}, fmt.Errorf("flow instance terminalization requires canonical run_id for %s", canonicalFlowPath)
	}
	plan := terminalFlowInstanceSideEffectPlan{
		EntityID:          entityID,
		FlowPath:          canonicalFlowPath,
		AgentIdentities:   agentIdentities,
		SelfRetiringAgent: selfRetiringAgent,
		Route:             canonicalRoute,
		RunID:             runID,
		FinalState:        req.FinalState,
	}
	return plan, nil
}

func terminalFlowSelfRetiringAgent(ctx context.Context, candidates []runtimeagentidentity.Identity) (runtimeagentidentity.Identity, error) {
	evt, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok || evt.ProducerType() != events.EventProducerAgent {
		return runtimeagentidentity.Identity{}, nil
	}
	source := evt.RoutingSource().Route()
	if source.FlowInstance == "" {
		return runtimeagentidentity.Identity{}, nil
	}
	var match runtimeagentidentity.Identity
	for _, candidate := range candidates {
		if candidate.AgentID() != evt.SourceAgent() || candidate.FlowInstance() != source.FlowInstance {
			continue
		}
		if !match.IsZero() {
			return runtimeagentidentity.Identity{}, fmt.Errorf(
				"terminal event %s has ambiguous exact source agent %s at %s",
				evt.ID(), evt.SourceAgent(), source.FlowInstance,
			)
		}
		match = candidate
	}
	return match, nil
}

func (am *AgentManager) applyTerminalFlowInstanceSideEffects(ctx context.Context, plan terminalFlowInstanceSideEffectPlan) error {
	var agentErrs []error
	for _, identity := range plan.AgentIdentities {
		deferRouteRetirement := false
		if !plan.SelfRetiringAgent.IsZero() {
			deferRouteRetirement, _ = runtimeagentidentity.Equal(identity, plan.SelfRetiringAgent)
		}
		if err := am.teardownIdentityAfterTerminalEvent(ctx, identity, "flow_instance_terminal", deferRouteRetirement); err != nil && !errors.Is(err, ErrAgentNotFound) {
			agentErrs = append(agentErrs, fmt.Errorf(
				"teardown flow instance agent %s at %s: %w",
				identity.AgentID(),
				identity.FlowInstance(),
				err,
			))
		}
	}
	if len(agentErrs) > 0 {
		return errors.Join(agentErrs...)
	}
	return nil
}

func (am *AgentManager) logTerminalFlowInstanceSideEffectFailure(plan terminalFlowInstanceSideEffectPlan, err error) {
	if am == nil || am.bus == nil || err == nil {
		return
	}
	_ = am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
		Level:     "warn",
		Message:   "Terminal flow instance side-effect teardown failed after commit",
		Component: "flow_activation",
		Action:    "terminal_flow_instance_side_effects_failed",
		EntityID:  plan.EntityID,
		Detail: map[string]any{
			"flow_path":        plan.FlowPath,
			"agent_identities": append([]runtimeagentidentity.Identity(nil), plan.AgentIdentities...),
			"route":            plan.Route.InstancePath,
			"final_state":      plan.FinalState,
		},
		Failure: failureEnvelope(err, "flow_activation", "terminal_side_effects"),
	})
}

func buildFlowAgentConfig(
	source semanticview.Source,
	namePlan semanticview.AgentNamePlan,
	templateID string,
	instanceID string,
	entityID string,
	flowPath string,
	key string,
	entry runtimecontracts.AgentRegistryEntry,
	vars map[string]string,
	localEvents map[string]struct{},
	config map[string]any,
) (models.AgentConfig, error) {
	name, err := namePlan.Materialize()
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s declaration identity: %w", key, err)
	}
	agentID := name.AgentID
	route, err := runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s route identity: %w", key, err)
	}
	identity, err := runtimeagentidentity.New(name, route)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s concrete identity: %w", key, err)
	}
	subscriptions := make([]string, 0, len(entry.Subscriptions))
	subscriptions = append(subscriptions, entry.Subscriptions...)
	rendered := make([]string, 0, len(subscriptions))
	authorityPath := source.FlowPath(templateID)
	if strings.TrimSpace(authorityPath) == "" {
		authorityPath = templateID
	}
	for _, subscription := range subscriptions {
		subscription = strings.TrimSpace(renderFlowTemplate(subscription, vars))
		if subscription == "" {
			continue
		}
		admission := semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{
			ConsumerKind: semanticview.AuthoredSubscriptionConsumerAgent,
			ConsumerID:   agentID,
			FlowID:       templateID,
			FlowPath:     authorityPath,
			LocalEvents:  localEvents,
			Authored:     subscription,
		})
		if !admission.Admitted() {
			return models.AgentConfig{}, fmt.Errorf("flow agent %s: %s", key, admission.Message())
		}
		if admission.Class() == semanticview.AuthoredSubscriptionSameScopeAgentExact {
			subscription = eventidentity.Normalize(strings.Trim(flowPath, "/") + "/" + admission.LocalEvent())
		} else if _, ok := localEvents[subscription]; ok {
			subscription = eventidentity.ExternalizeForFlow(flowPath, localEventList(localEvents), subscription)
		}
		rendered = append(rendered, subscription)
	}
	rendered = dedupeStrings(rendered)

	cfgPayload := map[string]any{}
	for k, v := range config {
		k = strings.TrimSpace(k)
		if k != "" {
			cfgPayload[k] = v
		}
	}
	rawConfig, err := json.Marshal(cfgPayload)
	if err != nil {
		return models.AgentConfig{}, err
	}
	if err := models.ValidateNoAuthoredSystemPrompt(rawConfig); err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s config: %w", key, err)
	}
	prompt, err := assembleResolvedAgentPrompt(source, templateID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s intent: %w", key, err)
	}
	permissions, err := runtimetools.ResolveAgentPermissions(source, templateID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s permissions: %w", key, err)
	}

	cfg := models.AgentConfig{
		ID:              agentID,
		Identity:        identity,
		Type:            strings.TrimSpace(entry.Type),
		Role:            namePlan.EffectiveRole(entry),
		FlowID:          templateID,
		Model:           strings.TrimSpace(entry.Model),
		LLMBackend:      "",
		Memory:          entry.MemoryPlan,
		Mock:            entry.Mock,
		Intent:          entry.ResolvedIntent,
		Prompt:          prompt,
		MaxTurnsPerTask: entry.MaxTurnsPerTask,
		Subscriptions:   rendered,
		EmitEvents:      normalizedFlowAgentEmitEvents(entry.EmitEvents, vars, localEvents, strings.Trim(flowPath, "/"), templateID, instanceID),
		Tools:           normalizedConfiguredToolList(entry.ConfiguredTools()),
		Permissions:     permissions,
		NativeTools:     nativeToolConfigFromMap(normalizedConfiguredNativeTools(entry.NativeTools)),
		FlowDataAccess:  normalizedConfiguredToolList(entry.FlowDataAccess),
		Criteria:        normalizedConfiguredToolList(entry.Criteria),
		WorkspaceClass:  strings.TrimSpace(entry.WorkspaceClass),
		ManagerFallback: strings.TrimSpace(entry.ManagerFallback),
		FlowPath:        strings.Trim(flowPath, "/"),
		EntityID:        entityID,
		ParentAgent:     strings.TrimSpace(entry.ManagerFallback),
		Config:          rawConfig,
	}
	cfg.NormalizeRuntimeDescriptor()
	if _, err := admitAgentConfigSubscriptions(source, &cfg, localEvents); err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s: %w", key, err)
	}
	return cfg, nil
}

func staticRequiredAgentsForScope(
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
	namePlans map[string]semanticview.AgentNamePlan,
	required []runtimecontracts.FlowRequiredAgent,
) ([]PersistedAgent, error) {
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if len(required) == 0 {
		return nil, nil
	}
	localEvents := staticFlowLocalEventSet(agents)
	records := make([]PersistedAgent, 0, len(required))
	for _, requiredAgent := range required {
		logicalID, entry, ok := runtimerequiredagents.ResolveAgent(agents, requiredAgent)
		if !ok {
			return nil, fmt.Errorf("required agent %q missing from scope %q", strings.TrimSpace(requiredAgent.Role), flowID)
		}
		namePlan, exists := namePlans[logicalID]
		if !exists {
			return nil, fmt.Errorf("required agent %q in scope %q has no exact scoped declaration name", logicalID, flowID)
		}
		cfg, err := buildStaticFlowAgentConfig(source, namePlan, flowID, flowPath, logicalID, entry, localEvents)
		if err != nil {
			return nil, err
		}
		records = append(records, PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "static-flow-required-agent",
			TemplateVersion: "",
		})
	}
	return records, nil
}

func staticAgentsForScope(
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
	namePlans map[string]semanticview.AgentNamePlan,
) ([]PersistedAgent, error) {
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if len(agents) == 0 {
		return nil, nil
	}
	localEvents := staticFlowLocalEventSet(agents)
	logicalIDs := make([]string, 0, len(agents))
	for logicalID := range agents {
		logicalID = strings.TrimSpace(logicalID)
		if logicalID != "" {
			logicalIDs = append(logicalIDs, logicalID)
		}
	}
	sort.Strings(logicalIDs)
	records := make([]PersistedAgent, 0, len(logicalIDs))
	for _, logicalID := range logicalIDs {
		entry := agents[logicalID]
		namePlan, exists := namePlans[logicalID]
		if !exists {
			return nil, fmt.Errorf("static agent %q in scope %q has no exact scoped declaration name", logicalID, flowID)
		}
		cfg, err := buildStaticFlowAgentConfig(source, namePlan, flowID, flowPath, logicalID, entry, localEvents)
		if err != nil {
			return nil, err
		}
		records = append(records, PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "static-flow-agent",
			TemplateVersion: "",
		})
	}
	return records, nil
}

func buildStaticFlowAgentConfig(
	source semanticview.Source,
	namePlan semanticview.AgentNamePlan,
	flowID string,
	flowPath string,
	logicalID string,
	entry runtimecontracts.AgentRegistryEntry,
	localEvents map[string]struct{},
) (models.AgentConfig, error) {
	vars := map[string]string{
		"flow_id":   strings.TrimSpace(flowID),
		"flow_path": strings.Trim(strings.TrimSpace(flowPath), "/"),
	}
	name, err := namePlan.Materialize()
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s declaration identity: %w", logicalID, err)
	}
	agentID := name.AgentID
	route := runtimeagentidentity.RootRoute()
	if flowPath != "" {
		route, err = runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
		if err != nil {
			return models.AgentConfig{}, fmt.Errorf("static flow agent %s route identity: %w", logicalID, err)
		}
	}
	identity, err := runtimeagentidentity.New(name, route)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s concrete identity: %w", logicalID, err)
	}
	subscriptions := make([]string, 0, len(entry.Subscriptions))
	subscriptions = append(subscriptions, entry.Subscriptions...)
	rendered := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		subscription = strings.TrimSpace(renderFlowTemplate(subscription, vars))
		if subscription == "" {
			continue
		}
		subscription = eventidentity.ExternalizeForFlow(flowPath, localEventList(localEvents), subscription)
		rendered = append(rendered, subscription)
	}
	rendered = dedupeStrings(rendered)

	rawConfig, err := json.Marshal(map[string]any{})
	if err != nil {
		return models.AgentConfig{}, err
	}
	prompt, err := assembleResolvedAgentPrompt(source, flowID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s intent: %w", logicalID, err)
	}
	permissions, err := runtimetools.ResolveAgentPermissions(source, flowID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s permissions: %w", logicalID, err)
	}
	cfg := models.AgentConfig{
		ID:              agentID,
		Identity:        identity,
		Type:            strings.TrimSpace(entry.Type),
		Role:            namePlan.EffectiveRole(entry),
		FlowID:          flowID,
		Model:           strings.TrimSpace(entry.Model),
		LLMBackend:      "",
		Memory:          entry.MemoryPlan,
		Mock:            entry.Mock,
		Intent:          entry.ResolvedIntent,
		Prompt:          prompt,
		MaxTurnsPerTask: entry.MaxTurnsPerTask,
		Subscriptions:   rendered,
		EmitEvents:      normalizedStaticFlowEmitEvents(entry.EmitEvents, vars, localEvents, flowPath),
		Tools:           normalizedConfiguredToolList(entry.ConfiguredTools()),
		Permissions:     permissions,
		NativeTools:     nativeToolConfigFromMap(normalizedConfiguredNativeTools(entry.NativeTools)),
		FlowDataAccess:  normalizedConfiguredToolList(entry.FlowDataAccess),
		Criteria:        normalizedConfiguredToolList(entry.Criteria),
		WorkspaceClass:  strings.TrimSpace(entry.WorkspaceClass),
		ManagerFallback: strings.TrimSpace(entry.ManagerFallback),
		FlowPath:        flowPath,
		EntityID:        "",
		ParentAgent:     strings.TrimSpace(entry.ManagerFallback),
		Config:          rawConfig,
	}
	cfg.NormalizeRuntimeDescriptor()
	if _, err := admitAgentConfigSubscriptions(source, &cfg, localEvents); err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s: %w", logicalID, err)
	}
	return cfg, nil
}

func assembleResolvedAgentPrompt(source semanticview.Source, flowID string, entry runtimecontracts.AgentRegistryEntry) (runtimeagentintent.DerivedPrompt, error) {
	bundle, _ := semanticview.Bundle(source)
	return runtimecontracts.AssembleAgentPrompt(bundle, flowID, entry, nil)
}

func normalizedFlowAgentEmitEvents(events []string, vars map[string]string, localEvents map[string]struct{}, flowPath, templateID, instanceID string) []string {
	rendered := normalizedConfiguredEventList(events, vars)
	if len(rendered) == 0 {
		return nil
	}
	out := make([]string, 0, len(rendered))
	instancePath := strings.Trim(strings.TrimSpace(templateID)+"/"+strings.TrimSpace(instanceID), "/")
	for _, eventType := range rendered {
		out = append(out, eventidentity.ExternalizeForFlow(instancePath, localEventList(localEvents), eventType))
	}
	return dedupeStrings(out)
}

func normalizedStaticFlowEmitEvents(events []string, vars map[string]string, localEvents map[string]struct{}, flowPath string) []string {
	rendered := normalizedConfiguredEventList(events, vars)
	if len(rendered) == 0 {
		return nil
	}
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	out := make([]string, 0, len(rendered))
	for _, eventType := range rendered {
		out = append(out, eventidentity.ExternalizeForFlow(flowPath, localEventList(localEvents), eventType))
	}
	return dedupeStrings(out)
}

func localEventList(localEvents map[string]struct{}) []string {
	if len(localEvents) == 0 {
		return nil
	}
	out := make([]string, 0, len(localEvents))
	for eventType := range localEvents {
		if strings.TrimSpace(eventType) != "" {
			out = append(out, strings.TrimSpace(eventType))
		}
	}
	sort.Strings(out)
	return out
}

func staticFlowLocalEventSet(agents map[string]runtimecontracts.AgentRegistryEntry) map[string]struct{} {
	out := map[string]struct{}{}
	for _, entry := range agents {
		for _, eventType := range entry.Subscriptions {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !strings.Contains(eventType, "/") {
				out[eventType] = struct{}{}
			}
		}
		for _, eventType := range entry.EmitEvents {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !strings.Contains(eventType, "/") {
				out[eventType] = struct{}{}
			}
		}
	}
	return out
}

func flowActivationVars(req runtimepipeline.FlowInstanceActivationRequest) map[string]string {
	vars := map[string]string{}
	for key, value := range req.Config {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		vars[key] = stringifyPromptTemplateValue(value)
	}
	setFlowActivationBuiltin(vars, "entity_id", req.Instance.EntityID)
	setFlowActivationBuiltin(vars, "instance_id", req.Instance.InstanceID)
	setFlowActivationBuiltin(vars, "template_id", req.Instance.TemplateID)
	setFlowActivationBuiltin(vars, "flow_scope_key", req.Instance.ScopeKey)
	setFlowActivationBuiltin(vars, "flow_instance_path", req.Instance.InstancePath)
	return vars
}

func setFlowActivationBuiltin(vars map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	vars[key] = value
}

func cloneFlowConfig(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func normalizedConfiguredToolList(raw []string) []string {
	return dedupeStrings(raw)
}

func normalizedConfiguredNativeTools(raw map[string]any) map[string]bool {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]bool, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		flag, ok := value.(bool)
		if key == "" || !ok {
			continue
		}
		out[key] = flag
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizedConfiguredEventList(raw []string, vars map[string]string) []string {
	if len(raw) == 0 {
		return nil
	}
	rendered := make([]string, 0, len(raw))
	for _, eventType := range raw {
		eventType = strings.TrimSpace(renderFlowTemplate(eventType, vars))
		if eventType == "" {
			continue
		}
		rendered = append(rendered, eventType)
	}
	return dedupeStrings(rendered)
}

func flowLocalEventSet(schema runtimecontracts.FlowSchemaDocument, scope semanticview.FlowScope) map[string]struct{} {
	out := map[string]struct{}{}
	for _, eventType := range schema.Pins.Inputs.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range schema.Pins.Outputs.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for eventType := range scope.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	if autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event); autoEmit != "" {
		out[autoEmit] = struct{}{}
	}
	return out
}

func renderFlowTemplate(raw string, vars map[string]string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(vars) == 0 {
		return raw
	}
	replacer := make([]string, 0, len(vars)*4)
	for key, value := range vars {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		replacer = append(replacer, "{"+key+"}", value, "{{"+key+"}}", value)
	}
	return strings.NewReplacer(replacer...).Replace(raw)
}

func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
