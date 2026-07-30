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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerequiredagents "github.com/division-sh/swarm/internal/runtime/requiredagents"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type flowInstancePersistence interface {
	MaterializeInitialEntry(ctx context.Context, instance runtimepipeline.WorkflowInstance, occurredAt time.Time) (runtimepipeline.WorkflowInitialMaterializationResult, error)
	ArmInitialEntryTimers(ctx context.Context, instanceID string) error
	ReconcileInitialEntryTimers(ctx context.Context, instanceID string) error
	RetireInitialEntryTimerWakeups(ctx context.Context, instanceID string) error
	ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error)
	LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID, instanceID string) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
	ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error)
	ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error)
	MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error
	CommitDynamicFlowRuntimeCreationOccurrence(context.Context, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest, runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher) error
	MarkTerminated(ctx context.Context, storageRef string, terminatedAt time.Time) error
	Load(ctx context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error)
	LoadRouteRecoveryProjection(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstanceRouteRecoveryProjection, error)
}

type flowInstanceRouteContextInstaller interface {
	StageFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type flowInstanceRouteContextVerifier interface {
	HasFlowInstanceRoute(runtimeflowidentity.Route) bool
	VerifyFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error
}

type persistedFlowInstanceRouteRestorer interface {
	PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type publishedFlowInstanceRouteRetirer interface {
	RetirePublishedFlowInstanceRoute(runtimeflowidentity.Route) error
}

type flowInstanceRouteContextRemover interface {
	RemoveFlowInstanceRouteContext(context.Context, runtimeflowidentity.Route) error
}

type flowInstanceTerminalMutationOwner interface {
	RunPipelineMutation(context.Context, func(context.Context) error) error
}

type terminalFlowInstanceSideEffectPlan struct {
	EntityID        string
	FlowPath        string
	AgentIdentities []runtimeagentidentity.Identity
	Route           runtimeflowidentity.Route
	RunID           string
	FinalState      string
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
	if req.Context.Empty() {
		req.Context = events.DeliveryContextFromContext(ctx)
	}
	ctx = events.WithDeliveryContext(ctx, req.Context)
	if req.ContractBundle == nil {
		return fmt.Errorf("contract bundle is required")
	}
	if am.workflowInstances == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, req.ContractBundle)
	if err != nil {
		return err
	}
	req.ContractBundle = admittedSource.source
	instance := req.Instance
	templateID := strings.TrimSpace(instance.TemplateID)
	instanceID := strings.TrimSpace(instance.InstanceID)
	flowEntityID := strings.TrimSpace(instance.EntityID)
	flowPath := strings.TrimSpace(instance.InstancePath)
	if templateID == "" || instanceID == "" || flowEntityID == "" || flowPath == "" {
		return fmt.Errorf("template_id, instance_id, and entity_id are required")
	}
	scope, ok := semanticview.FlowScopeByID(req.ContractBundle, templateID)
	if !ok {
		return fmt.Errorf("flow contract view not found: %s", templateID)
	}
	schema, ok := req.ContractBundle.FlowSchemaByID(templateID)
	if !ok {
		return fmt.Errorf("flow schema not found: %s", templateID)
	}
	if flowEntityID == "" {
		return fmt.Errorf("derive flow entity id for %s", flowPath)
	}
	parentEntityID := strings.TrimSpace(instance.ParentEntityID)
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
			return fmt.Errorf(
				"auto-emit %s requires exact trigger run_id and parent_event_id",
				strings.TrimSpace(schema.AutoEmitOnCreate.Event),
			)
		}
		return err
	}
	autoEmitLineage := events.EventLineage{
		RunID:         autoEmitRunID,
		ParentEventID: triggerEventID,
		ExecutionMode: req.TriggerEvent.ExecutionMode(),
	}
	if strings.TrimSpace(schema.AutoEmitOnCreate.Event) != "" &&
		(strings.TrimSpace(autoEmitLineage.RunID) == "" || strings.TrimSpace(autoEmitLineage.ParentEventID) == "") {
		return fmt.Errorf(
			"auto-emit %s requires exact trigger run_id and parent_event_id",
			strings.TrimSpace(schema.AutoEmitOnCreate.Event),
		)
	}
	metadata := cloneFlowConfig(req.Metadata)
	for key, value := range flowInstanceActivationMetadata(instance, flowEntityID, instanceID, flowPath, parentEntityID) {
		metadata[key] = value
	}
	occurredAt := req.OccurredAt.UTC()
	if !req.TriggerEvent.CreatedAt().IsZero() {
		occurredAt = req.TriggerEvent.CreatedAt().UTC()
	}
	if occurredAt.IsZero() {
		return fmt.Errorf("flow activation requires an exact occurrence time")
	}
	agentRecords, err := am.flowInstanceAgentRecords(req, schema, scope)
	if err != nil {
		return err
	}
	readinessPlan, err := am.buildDynamicFlowRuntimeReadinessPlan(
		ctx,
		req,
		agentRecords,
		schema,
		autoEmitLineage,
		occurredAt,
	)
	if err != nil {
		return err
	}
	materialization, err := am.workflowInstances.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:       instanceID,
		StorageRef:       flowPath,
		WorkflowName:     templateID,
		WorkflowVersion:  strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		RuntimeReadiness: &readinessPlan,
		CurrentState:     initialState,
		Config:           cloneFlowConfig(req.Config),
		Metadata:         metadata,
	}, occurredAt)
	if err != nil {
		return fmt.Errorf("persist flow instance %s: %w", flowPath, err)
	}
	switch materialization {
	case runtimepipeline.WorkflowInitialMaterializationCreated:
	case runtimepipeline.WorkflowInitialMaterializationAlreadyExists:
	default:
		return fmt.Errorf("persist flow instance %s: unknown initial materialization result %d", flowPath, materialization)
	}
	if _, transactional := runtimepipeline.PipelineSQLTxFromContext(ctx); transactional {
		if err := am.installFlowInstanceRoute(ctx, req); err != nil {
			return fmt.Errorf("stage dynamic flow route %s: %w", flowPath, err)
		}
	}
	finalizeAfterCommit := runtimepipeline.QueuePipelinePostCommitAction(ctx, func(actionCtx context.Context) {
		postCommitCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(actionCtx))
		if err := am.reconcileDynamicFlowRuntimeReadinessPlan(postCommitCtx, readinessPlan, req.ContractBundle); err != nil {
			am.logFlowInstanceActivationSideEffectFailure(req, "runtime_readiness_failed", "finalize_runtime_readiness", err)
			am.signalDynamicFlowRuntimeReadiness()
			return
		}
		if err := am.launchDynamicFlowRuntimeAgentBacklogReplay(postCommitCtx, readinessPlan); err != nil {
			am.logFlowInstanceActivationSideEffectFailure(req, "runtime_backlog_failed", "replay_runtime_backlog", err)
			am.signalDynamicFlowRuntimeReadiness()
		}
	})
	if !finalizeAfterCommit {
		if err := am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, readinessPlan, req.ContractBundle); err != nil {
			am.signalDynamicFlowRuntimeReadiness()
			return err
		}
		if err := am.launchDynamicFlowRuntimeAgentBacklogReplay(ctx, readinessPlan); err != nil {
			am.signalDynamicFlowRuntimeReadiness()
			return err
		}
	}
	return nil
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
	stored, exists, err := am.workflowInstances.Load(ctx, instance.InstancePath)
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
	if _, transactional := runtimepipeline.PipelineSQLTxFromContext(ctx); transactional {
		if err := am.installFlowInstanceRoute(ctx, req); err != nil {
			return false, fmt.Errorf("stage dynamic flow route %s: %w", instance.InstancePath, err)
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(actionCtx context.Context) {
			postCommitCtx := runtimepipeline.WithoutPipelineSQLConnContext(runtimepipeline.WithoutPipelineSQLTxContext(actionCtx))
			if err := am.reconcileDynamicFlowRuntimeReadinessPlan(postCommitCtx, readinessPlan, req.ContractBundle); err != nil {
				am.logFlowInstanceActivationSideEffectFailure(req, "runtime_readiness_failed", "finalize_runtime_readiness", err)
				am.signalDynamicFlowRuntimeReadiness()
				return
			}
			if err := am.launchDynamicFlowRuntimeAgentBacklogReplay(postCommitCtx, readinessPlan); err != nil {
				am.logFlowInstanceActivationSideEffectFailure(req, "runtime_backlog_failed", "replay_runtime_backlog", err)
				am.signalDynamicFlowRuntimeReadiness()
			}
		}) {
			return false, fmt.Errorf("dynamic flow runtime readiness %s requires post-commit finalization owner", instance.InstancePath)
		}
		return false, nil
	}
	if err := am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, readinessPlan, req.ContractBundle); err != nil {
		am.signalDynamicFlowRuntimeReadiness()
		return false, err
	}
	if err := am.launchDynamicFlowRuntimeAgentBacklogReplay(ctx, readinessPlan); err != nil {
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
		cfg, err := buildFlowAgentConfig(req.ContractBundle, instance.TemplateID, instance.InstanceID, instance.EntityID, instance.InstancePath, key, entry, vars, localEvents, req.Config)
		if err != nil {
			return nil, err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "flow-instance-activator",
			TemplateVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		}
		if strings.TrimSpace(rec.Config.LLMBackend) == "" {
			rec.Config.LLMBackend = am.llmBackend
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
	if installer, ok := am.bus.(flowInstanceRouteContextInstaller); ok && installer != nil {
		return installer.StageFlowInstanceRouteContext(ctx, request)
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

func flowInstanceActivationMetadata(instance runtimeflowidentity.Instance, flowEntityID, instanceID, flowPath, parentEntityID string) map[string]any {
	metadata := map[string]any{
		"entity_id":        strings.TrimSpace(flowEntityID),
		"instance_id":      strings.TrimSpace(instanceID),
		"flow_path":        strings.Trim(strings.TrimSpace(flowPath), "/"),
		"parent_entity_id": strings.TrimSpace(parentEntityID),
	}
	parentRoute := instance.ParentRoute.Normalized()
	if strings.TrimSpace(parentRoute.FlowID) != "" {
		metadata["parent_flow_id"] = strings.TrimSpace(parentRoute.FlowID)
	}
	if strings.TrimSpace(parentRoute.FlowInstance) != "" {
		metadata["parent_flow_instance"] = strings.Trim(strings.TrimSpace(parentRoute.FlowInstance), "/")
	}
	if strings.TrimSpace(parentRoute.EntityID) != "" {
		metadata["parent_entity_id"] = strings.TrimSpace(parentRoute.EntityID)
	}
	return metadata
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

func (am *AgentManager) EnsureStaticFlowRequiredAgents(ctx context.Context, source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	records, err := StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return err
	}
	return am.spawnStaticAgentRecords(ctx, records)
}

func (am *AgentManager) EnsureStaticAgents(ctx context.Context, source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		return err
	}
	return am.spawnStaticAgentRecords(ctx, records)
}

func (am *AgentManager) spawnStaticAgentRecords(ctx context.Context, records []PersistedAgent) error {
	for _, rec := range records {
		if err := am.spawnAgentInternal(ctx, rec, true); err != nil && !errors.Is(err, ErrAgentAlreadyExists) {
			return err
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
		packageFlowAgents := map[string]staticAgentFlowGroup{}
		for logicalID, entry := range scope.Agents {
			proof := semanticview.ResolveAgentMemoryProof(source, semanticview.AgentMemoryLocator{
				AgentID:         logicalID,
				ProjectScopeKey: scope.Key,
			})
			if strings.TrimSpace(proof.OwningFlowID) != "" {
				if flowScopeContainsStaticAgent(source, proof.OwningFlowID, logicalID, entry) {
					continue
				}
				groupKey := staticAgentFlowGroupKey(proof.OwningFlowID, proof.FlowPath)
				group := packageFlowAgents[groupKey]
				group.FlowID = strings.TrimSpace(proof.OwningFlowID)
				group.FlowPath = strings.Trim(strings.TrimSpace(proof.FlowPath), "/")
				if group.Agents == nil {
					group.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
				}
				group.Agents[strings.TrimSpace(logicalID)] = entry
				packageFlowAgents[groupKey] = group
				continue
			}
			projectAgents[strings.TrimSpace(logicalID)] = entry
		}
		scopeRecords, err := staticAgentsForScope(source, "", "", projectAgents)
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
			scopeRecords, err := staticAgentsForScope(source, group.FlowID, group.FlowPath, group.Agents)
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
		scopeRecords, err := staticAgentsForScope(source, proof.OwningFlowID, proof.FlowPath, scope.Agents)
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
		scopeRecords, err := staticRequiredAgentsForScope(source, "", "", rootScope.Agents, rootScope.Required)
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
		scopeRecords, err := staticRequiredAgentsForScope(source, flowID, strings.Trim(scope.Path, "/"), scope.Agents, source.FlowRequiredAgents(flowID))
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
	FlowID   string
	FlowPath string
	Agents   map[string]runtimecontracts.AgentRegistryEntry
}

func staticAgentFlowGroupKey(flowID, flowPath string) string {
	return strings.TrimSpace(flowID) + "\x00" + strings.Trim(strings.TrimSpace(flowPath), "/")
}

func flowScopeContainsStaticAgent(source semanticview.Source, flowID, logicalID string, entry runtimecontracts.AgentRegistryEntry) bool {
	flowID = strings.TrimSpace(flowID)
	logicalID = strings.TrimSpace(logicalID)
	if source == nil || flowID == "" || logicalID == "" {
		return false
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return false
	}
	if scopedEntry, ok := scope.Agents[logicalID]; ok {
		return scopedEntry.ID == entry.ID
	}
	entryID := strings.TrimSpace(entry.ID)
	if entryID == "" {
		return false
	}
	for scopedLogicalID, scopedEntry := range scope.Agents {
		if strings.TrimSpace(scopedLogicalID) == logicalID || strings.TrimSpace(scopedEntry.ID) == entryID {
			return true
		}
	}
	return false
}

func (am *AgentManager) DeactivateFlowInstance(ctx context.Context, templateID, instanceID, flowPath, entityID string) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	if canonicalEntityID := runtimeflowidentity.EntityID(flowPath); canonicalEntityID != "" {
		entityID = canonicalEntityID
	}
	return am.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
		Instance: runtimeflowidentity.Stored(nil, templateID, flowPath, instanceID, entityID, ""),
	})
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
	if _, active := runtimepipeline.PipelineSQLTxFromContext(ctx); active {
		return am.deactivateFlowInstanceModelInMutation(ctx, req)
	}
	owner, ok := am.workflowInstances.(flowInstanceTerminalMutationOwner)
	if !ok || owner == nil {
		return fmt.Errorf("flow instance terminalization requires selected pipeline mutation ownership")
	}
	return owner.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if _, active := runtimepipeline.PipelineSQLTxFromContext(txctx); !active {
			return fmt.Errorf("flow instance terminalization mutation did not provide selected transaction")
		}
		return am.deactivateFlowInstanceModelInMutation(txctx, req)
	})
}

func (am *AgentManager) deactivateFlowInstanceModelInMutation(
	ctx context.Context,
	req runtimepipeline.FlowInstanceDeactivationRequest,
) error {
	instance := req.Instance
	entityID := strings.TrimSpace(instance.EntityID)
	flowPath := strings.TrimSpace(instance.InstancePath)
	if err := am.workflowInstances.MarkTerminated(ctx, flowPath, time.Now().UTC()); err != nil {
		return fmt.Errorf("persist flow instance terminal state %s: %w", flowPath, err)
	}
	canonicalInstance, ok, err := am.workflowInstances.Load(ctx, flowPath)
	if err != nil {
		return fmt.Errorf("load canonical terminal flow instance %s: %w", flowPath, err)
	}
	if !ok {
		return fmt.Errorf("load canonical terminal flow instance %s: not found", flowPath)
	}
	if strings.TrimSpace(canonicalInstance.Status) != "terminated" || canonicalInstance.TerminatedAt.IsZero() {
		return fmt.Errorf("canonical terminal flow instance %s not persisted", flowPath)
	}
	canonicalFlowPath := strings.TrimSpace(canonicalInstance.StorageRef)
	if canonicalFlowPath == "" {
		return fmt.Errorf("canonical terminal flow instance %s missing storage_ref", flowPath)
	}
	canonicalRoute := runtimeflowidentity.StoredRoute("", "", canonicalFlowPath)
	if !canonicalRoute.Valid() {
		return fmt.Errorf("derive canonical route identity for flow path %s", canonicalFlowPath)
	}
	configs := am.lifecycle.executionConfigs()
	agentIdentities := make([]runtimeagentidentity.Identity, 0, len(configs))
	for _, cfg := range configs {
		if cfg.CanonicalFlowPath() != canonicalFlowPath {
			continue
		}
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			return fmt.Errorf("terminal flow instance agent %q identity: %w", cfg.ID, err)
		}
		agentIdentities = append(agentIdentities, identity)
	}
	sort.Slice(agentIdentities, func(i, j int) bool {
		if agentIdentities[i].AgentID() != agentIdentities[j].AgentID() {
			return agentIdentities[i].AgentID() < agentIdentities[j].AgentID()
		}
		return agentIdentities[i].FlowInstance() < agentIdentities[j].FlowInstance()
	})
	remover, ok := am.bus.(flowInstanceRouteContextRemover)
	if !ok || remover == nil {
		return fmt.Errorf("event bus does not support derived flow-instance route removal for %s", canonicalFlowPath)
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("derived flow-instance route removal requires canonical run_id for %s", canonicalFlowPath)
	}
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := remover.RemoveFlowInstanceRouteContext(ctx, canonicalRoute); err != nil {
		return fmt.Errorf("stage exact terminal flow-instance route topology %s: %w", canonicalFlowPath, err)
	}
	plan := terminalFlowInstanceSideEffectPlan{
		EntityID:        entityID,
		FlowPath:        canonicalFlowPath,
		AgentIdentities: agentIdentities,
		Route:           canonicalRoute,
		RunID:           runID,
		FinalState:      req.FinalState,
	}
	if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(actionCtx context.Context) {
		if err := am.applyTerminalFlowInstanceSideEffects(actionCtx, plan); err != nil {
			am.logTerminalFlowInstanceSideEffectFailure(plan, err)
		}
	}) {
		return fmt.Errorf("terminal flow-instance side effects require selected mutation post-commit ownership")
	}
	return nil
}

func (am *AgentManager) applyTerminalFlowInstanceSideEffects(ctx context.Context, plan terminalFlowInstanceSideEffectPlan) error {
	var agentErrs []error
	for _, identity := range plan.AgentIdentities {
		if err := am.teardownIdentity(ctx, identity, "flow_instance_terminal"); err != nil && !errors.Is(err, ErrAgentNotFound) {
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
	agentID := strings.TrimSpace(renderFlowTemplate(strings.TrimSpace(entry.ID), vars))
	if agentID == "" {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s resolved empty id", key)
	}
	declarationOwner, ok := semanticview.AgentDeclarationOwner(source, templateID, key)
	if !ok {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s missing scoped declaration owner", key)
	}
	name, err := runtimeagentidentity.DeclaredName(agentID, declarationOwner)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s declaration identity: %w", key, err)
	}
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
	for _, subscription := range subscriptions {
		subscription = strings.TrimSpace(renderFlowTemplate(subscription, vars))
		if subscription == "" {
			continue
		}
		if _, ok := localEvents[subscription]; ok {
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
	permissions, err := runtimetools.ResolveAgentPermissions(source, templateID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s permissions: %w", key, err)
	}

	cfg := models.AgentConfig{
		ID:              agentID,
		Identity:        identity,
		Type:            strings.TrimSpace(entry.Type),
		Role:            strings.TrimSpace(entry.Role),
		FlowID:          templateID,
		Model:           strings.TrimSpace(entry.Model),
		LLMBackend:      "",
		Memory:          entry.MemoryPlan,
		Mock:            entry.Mock,
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

func (am *AgentManager) ensureStaticRequiredAgentsForScope(
	ctx context.Context,
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
	required []runtimecontracts.FlowRequiredAgent,
) error {
	records, err := staticRequiredAgentsForScope(source, flowID, flowPath, agents, required)
	if err != nil {
		return err
	}
	return am.spawnStaticAgentRecords(ctx, records)
}

func staticRequiredAgentsForScope(
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
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
		cfg, err := buildStaticFlowAgentConfig(source, flowID, flowPath, logicalID, entry, localEvents)
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

func (am *AgentManager) ensureStaticAgentsForScope(
	ctx context.Context,
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
) error {
	records, err := staticAgentsForScope(source, flowID, flowPath, agents)
	if err != nil {
		return err
	}
	return am.spawnStaticAgentRecords(ctx, records)
}

func staticAgentsForScope(
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
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
		cfg, err := buildStaticFlowAgentConfig(source, flowID, flowPath, logicalID, entry, localEvents)
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
	agentID := strings.TrimSpace(renderFlowTemplate(strings.TrimSpace(entry.ID), vars))
	if agentID == "" {
		agentID = strings.TrimSpace(logicalID)
	}
	if agentID == "" {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s resolved empty id", logicalID)
	}
	declarationOwner, ok := semanticview.AgentDeclarationOwner(source, flowID, logicalID)
	if !ok {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s missing scoped declaration owner", logicalID)
	}
	name, err := runtimeagentidentity.DeclaredName(agentID, declarationOwner)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s declaration identity: %w", logicalID, err)
	}
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

	cfgPayload := map[string]any{}
	if _, ok := cfgPayload["system_prompt"]; !ok {
		role := strings.TrimSpace(entry.Role)
		if role == "" {
			role = strings.TrimSpace(logicalID)
		}
		if role == "" {
			role = "agent"
		}
		if flowID != "" {
			cfgPayload["system_prompt"] = fmt.Sprintf("Handle %s events for static flow %s.", role, flowID)
		} else {
			cfgPayload["system_prompt"] = fmt.Sprintf("Handle %s events.", role)
		}
	}
	rawConfig, err := json.Marshal(cfgPayload)
	if err != nil {
		return models.AgentConfig{}, err
	}
	permissions, err := runtimetools.ResolveAgentPermissions(source, flowID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("static flow agent %s permissions: %w", logicalID, err)
	}
	role := strings.TrimSpace(entry.Role)
	if role == "" {
		role = strings.TrimSpace(logicalID)
	}
	cfg := models.AgentConfig{
		ID:              agentID,
		Identity:        identity,
		Type:            strings.TrimSpace(entry.Type),
		Role:            role,
		FlowID:          flowID,
		Model:           strings.TrimSpace(entry.Model),
		LLMBackend:      "",
		Memory:          entry.MemoryPlan,
		Mock:            entry.Mock,
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
