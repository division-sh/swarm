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
	Create(ctx context.Context, instance runtimepipeline.WorkflowInstance) error
	MarkTerminated(ctx context.Context, storageRef string, terminatedAt time.Time) error
	Load(ctx context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error)
}

type flowInstanceRouteInstaller interface {
	AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type flowInstanceRouteContextInstaller interface {
	AddFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) error
}

type flowInstanceRouteRemover interface {
	RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error
}

type terminalFlowInstanceSideEffectPlan struct {
	EntityID   string
	FlowPath   string
	AgentIDs   []string
	Route      runtimeflowidentity.Route
	Remover    flowInstanceRouteRemover
	FinalState string
}

func (am *AgentManager) ActivateFlowInstance(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
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
	autoEmitLineage := events.EventLineage{
		RunID:         runtimecorrelation.RunIDFromContext(ctx),
		ParentEventID: strings.TrimSpace(req.TriggerEvent.ID()),
	}
	autoEmitEvent, autoEmitName, err := buildAutoEmitOnCreateEvent(req.ContractBundle, schema, templateID, flowPath, flowEntityID, autoEmitLineage, req.Config)
	if err != nil {
		return err
	}
	metadata := cloneFlowConfig(req.Metadata)
	for key, value := range flowInstanceActivationMetadata(instance, flowEntityID, instanceID, flowPath, parentEntityID) {
		metadata[key] = value
	}
	if err := am.workflowInstances.Create(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      instanceID,
		StorageRef:      flowPath,
		WorkflowName:    templateID,
		WorkflowVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		CurrentState:    initialState,
		Config:          cloneFlowConfig(req.Config),
		Metadata:        metadata,
	}); err != nil {
		return fmt.Errorf("persist flow instance %s: %w", flowPath, err)
	}
	if _, inMutation := runtimepipeline.PipelineSQLTxFromContext(ctx); inMutation {
		if err := am.installFlowInstanceRoute(ctx, req); err != nil {
			return err
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
			postCommitCtx := runtimepipeline.WithoutPipelineSQLTxContext(context.WithoutCancel(ctx))
			if err := am.installFlowInstanceAgents(postCommitCtx, req, schema, scope); err != nil {
				am.logFlowInstanceActivationSideEffectFailure(req, err)
			}
		}) {
			return fmt.Errorf("flow instance %s requires post-commit agent activation", flowPath)
		}
	} else if err := am.installFlowInstanceRuntime(ctx, req, schema, scope); err != nil {
		return err
	}
	if strings.TrimSpace(autoEmitName) != "" {
		autoEmitCtx := events.WithDeliveryContext(context.Background(), req.Context)
		publishAutoEmit := func() {
			if err := am.bus.Publish(autoEmitCtx, autoEmitEvent); err != nil {
				am.bus.LogRuntime(autoEmitCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "warn",
					Message:   "Auto-emitting the flow activation event failed",
					Component: "flow_activation",
					Action:    "auto_emit_failed",
					EventType: autoEmitName,
					EntityID:  flowEntityID,
					Detail: map[string]any{
						"flow_path": flowPath,
					},
					Failure: failureEnvelope(err, "flow_activation", "auto_emit"),
				})
			}
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, publishAutoEmit) {
			if err := am.bus.Publish(ctx, autoEmitEvent); err != nil {
				return fmt.Errorf("auto-emit %s: %w", autoEmitName, err)
			}
		}
	}
	return nil
}

func (am *AgentManager) EnsureFlowInstance(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (bool, error) {
	if am == nil || am.workflowInstances == nil {
		return false, fmt.Errorf("workflow instance store is required")
	}
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
	if strings.TrimSpace(stored.WorkflowName) != strings.TrimSpace(instance.TemplateID) ||
		strings.TrimSpace(stored.WorkflowVersion) != strings.TrimSpace(req.ContractBundle.WorkflowVersion()) {
		return false, fmt.Errorf("standing flow instance %s belongs to %s@%s, not %s@%s; explicit reset or migration is required",
			instance.InstancePath, stored.WorkflowName, stored.WorkflowVersion, instance.TemplateID, req.ContractBundle.WorkflowVersion())
	}
	scope, ok := semanticview.FlowScopeByID(req.ContractBundle, instance.TemplateID)
	if !ok {
		return false, fmt.Errorf("flow contract view not found: %s", instance.TemplateID)
	}
	schema, ok := req.ContractBundle.FlowSchemaByID(instance.TemplateID)
	if !ok {
		return false, fmt.Errorf("flow schema not found: %s", instance.TemplateID)
	}
	if err := am.installFlowInstanceRuntime(ctx, req, schema, scope); err != nil {
		return false, err
	}
	return false, nil
}

func (am *AgentManager) installFlowInstanceRuntime(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest, schema runtimecontracts.FlowSchemaDocument, scope semanticview.FlowScope) error {
	if err := am.installFlowInstanceAgents(ctx, req, schema, scope); err != nil {
		return err
	}
	return am.installFlowInstanceRoute(ctx, req)
}

func (am *AgentManager) installFlowInstanceAgents(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest, schema runtimecontracts.FlowSchemaDocument, scope semanticview.FlowScope) error {
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
	for _, key := range agentKeys {
		entry := scope.Agents[key]
		cfg, err := buildFlowAgentConfig(req.ContractBundle, instance.TemplateID, instance.InstanceID, instance.EntityID, instance.InstancePath, key, entry, vars, localEvents, req.Config)
		if err != nil {
			return err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "flow-instance-activator",
			TemplateVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		}
		if err := am.spawnAgentInternal(ctx, rec, true); err != nil && !errors.Is(err, ErrAgentAlreadyExists) {
			return err
		}
	}
	return nil
}

func (am *AgentManager) installFlowInstanceRoute(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	instance := req.Instance
	vars := flowActivationVars(req)
	request := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: instance.Route(), ActivationVariables: vars}
	if installer, ok := am.bus.(flowInstanceRouteContextInstaller); ok && installer != nil {
		return installer.AddFlowInstanceRouteContext(ctx, request)
	}
	if installer, ok := am.bus.(flowInstanceRouteInstaller); ok && installer != nil {
		return installer.AddFlowInstanceRoute(request)
	}
	return fmt.Errorf("event bus does not support derived flow-instance routing for %s", instance.InstancePath)
}

func (am *AgentManager) logFlowInstanceActivationSideEffectFailure(req runtimepipeline.FlowInstanceActivationRequest, err error) {
	if am == nil || am.bus == nil || err == nil {
		return
	}
	_ = am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
		Level: "error", Message: "Flow instance agent activation failed after commit",
		Component: "flow_activation", Action: "agent_activation_failed",
		EntityID: strings.TrimSpace(req.Instance.EntityID),
		Detail:   map[string]any{"flow_path": strings.TrimSpace(req.Instance.InstancePath)},
		Failure:  failureEnvelope(err, "flow_activation", "install_agents"),
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

func buildAutoEmitOnCreateEvent(source semanticview.Source, schema runtimecontracts.FlowSchemaDocument, templateID, flowPath, flowEntityID string, lineage events.EventLineage, config map[string]any) (events.Event, string, error) {
	autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event)
	if autoEmit == "" {
		return events.EmptyEvent(), "", nil
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
		return events.EmptyEvent(), autoEmit, fmt.Errorf("auto-emit %s: %w", autoEmit, err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return events.EmptyEvent(), autoEmit, fmt.Errorf("encode auto-emit payload %s: %w", autoEmit, err)
	}
	return events.NewChildEventWithLineage(uuid.NewString(), events.EventType(eventType), "flow-instance-activator", "", encoded, 0, lineage, events.EventEnvelope{
		EntityID:     flowEntityID,
		FlowInstance: flowPath,
	}, time.Now().UTC()), autoEmit, nil
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
	records := []PersistedAgent{}
	for _, scope := range source.ProjectScopes() {
		projectAgents := make(map[string]runtimecontracts.AgentRegistryEntry, len(scope.Agents))
		packageFlowAgents := map[string]staticAgentFlowGroup{}
		for logicalID, entry := range scope.Agents {
			proof := semanticview.ResolveAgentSessionScopeProof(source, semanticview.AgentSessionScopeLocator{
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
		proof := semanticview.ResolveAgentSessionScopeProof(source, semanticview.AgentSessionScopeLocator{
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
		scopeRecords, err := staticRequiredAgentsForScope(source, flowID, strings.Trim(scope.Path, "/"), scope.Agents, source.FlowRequiredAgents(flowID))
		if err != nil {
			return nil, err
		}
		records = append(records, scopeRecords...)
	}
	return records, nil
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
	am.mu.RLock()
	agentIDs := make([]string, 0, len(am.agentCfg))
	for agentID, cfg := range am.agentCfg {
		if cfg.CanonicalFlowPath() != canonicalFlowPath {
			continue
		}
		agentIDs = append(agentIDs, agentID)
	}
	am.mu.RUnlock()
	sort.Strings(agentIDs)
	remover, ok := am.bus.(flowInstanceRouteRemover)
	if !ok || remover == nil {
		return fmt.Errorf("event bus does not support derived flow-instance route removal for %s", canonicalFlowPath)
	}
	plan := terminalFlowInstanceSideEffectPlan{
		EntityID:   entityID,
		FlowPath:   canonicalFlowPath,
		AgentIDs:   agentIDs,
		Route:      canonicalRoute,
		Remover:    remover,
		FinalState: req.FinalState,
	}
	if runtimepipeline.QueuePipelinePostCommitAction(ctx, func() {
		if err := am.applyTerminalFlowInstanceSideEffects(plan); err != nil {
			am.logTerminalFlowInstanceSideEffectFailure(plan, err)
		}
	}) {
		return nil
	}
	return am.applyTerminalFlowInstanceSideEffects(plan)
}

func (am *AgentManager) applyTerminalFlowInstanceSideEffects(plan terminalFlowInstanceSideEffectPlan) error {
	var agentErrs []error
	for _, agentID := range plan.AgentIDs {
		if err := am.TeardownAgent(agentID); err != nil && !errors.Is(err, ErrAgentNotFound) {
			agentErrs = append(agentErrs, fmt.Errorf("teardown flow instance agent %s: %w", agentID, err))
		}
	}
	if len(agentErrs) > 0 {
		return errors.Join(agentErrs...)
	}
	if plan.Remover == nil {
		return fmt.Errorf("event bus does not support derived flow-instance route removal for %s", plan.FlowPath)
	} else if err := plan.Remover.RemoveFlowInstanceRoute(plan.Route); err != nil {
		return fmt.Errorf("remove flow instance route %s: %w", plan.FlowPath, err)
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
			"flow_path":   plan.FlowPath,
			"agent_ids":   append([]string(nil), plan.AgentIDs...),
			"route":       plan.Route.InstancePath,
			"final_state": plan.FinalState,
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
		ID:               agentID,
		Type:             strings.TrimSpace(entry.Type),
		Role:             strings.TrimSpace(entry.Role),
		Mode:             templateID,
		Model:            strings.TrimSpace(entry.Model),
		LLMBackend:       "",
		ConversationMode: strings.TrimSpace(entry.ConversationMode),
		SessionScope:     strings.TrimSpace(entry.SessionScope),
		MaxTurnsPerTask:  entry.MaxTurnsPerTask,
		Subscriptions:    rendered,
		EmitEvents:       normalizedFlowAgentEmitEvents(entry.EmitEvents, vars, localEvents, strings.Trim(flowPath, "/"), templateID, instanceID),
		Tools:            normalizedConfiguredToolList(entry.ConfiguredTools()),
		Permissions:      permissions,
		NativeTools:      nativeToolConfigFromMap(normalizedConfiguredNativeTools(entry.NativeTools)),
		FlowDataAccess:   normalizedConfiguredToolList(entry.FlowDataAccess),
		Criteria:         normalizedConfiguredToolList(entry.Criteria),
		WorkspaceClass:   strings.TrimSpace(entry.WorkspaceClass),
		ManagerFallback:  strings.TrimSpace(entry.ManagerFallback),
		FlowPath:         strings.Trim(flowPath, "/"),
		EntityID:         entityID,
		ParentAgent:      strings.TrimSpace(entry.ManagerFallback),
		Config:           rawConfig,
	}
	cfg.NormalizeRuntimeDescriptor()
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
		ID:               agentID,
		Type:             strings.TrimSpace(entry.Type),
		Role:             role,
		Mode:             flowID,
		Model:            strings.TrimSpace(entry.Model),
		LLMBackend:       "",
		ConversationMode: strings.TrimSpace(entry.ConversationMode),
		SessionScope:     strings.TrimSpace(entry.SessionScope),
		MaxTurnsPerTask:  entry.MaxTurnsPerTask,
		Subscriptions:    rendered,
		EmitEvents:       normalizedStaticFlowEmitEvents(entry.EmitEvents, vars, localEvents, flowPath),
		Tools:            normalizedConfiguredToolList(entry.ConfiguredTools()),
		Permissions:      permissions,
		NativeTools:      nativeToolConfigFromMap(normalizedConfiguredNativeTools(entry.NativeTools)),
		FlowDataAccess:   normalizedConfiguredToolList(entry.FlowDataAccess),
		Criteria:         normalizedConfiguredToolList(entry.Criteria),
		WorkspaceClass:   strings.TrimSpace(entry.WorkspaceClass),
		ManagerFallback:  strings.TrimSpace(entry.ManagerFallback),
		FlowPath:         flowPath,
		EntityID:         "",
		ParentAgent:      strings.TrimSpace(entry.ManagerFallback),
		Config:           rawConfig,
	}
	cfg.NormalizeRuntimeDescriptor()
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
