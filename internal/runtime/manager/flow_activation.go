package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

type flowInstancePersistence interface {
	Upsert(ctx context.Context, instance runtimepipeline.WorkflowInstance) error
}

type flowInstanceRouteInstaller interface {
	AddFlowInstance(template runtimecontracts.SystemNodeContract, instancePath string) error
}

type flowInstanceRouteRemover interface {
	RemoveFlowInstance(templateID, instanceID string) error
}

func (am *AgentManager) ActivateFlowInstance(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	if req.ContractBundle == nil {
		return fmt.Errorf("contract bundle is required")
	}
	templateID := strings.TrimSpace(req.TemplateID)
	instanceID := strings.TrimSpace(req.InstanceID)
	entityID := strings.TrimSpace(req.EntityID)
	if templateID == "" || instanceID == "" || entityID == "" {
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
	flowPath := strings.Trim(strings.TrimSpace(req.FlowPath), "/")
	identity := runtimepipeline.DeriveFlowInstanceIdentity(req.ContractBundle, templateID, instanceID)
	if flowPath == "" {
		flowPath = identity.InstancePath
	}
	sourceEntityID := entityID
	flowEntityID := strings.TrimSpace(identity.EntityID)
	if strings.TrimSpace(flowPath) != strings.TrimSpace(identity.InstancePath) {
		flowEntityID = strings.TrimSpace(runtimepipeline.FlowInstanceEntityID(flowPath))
	}
	if flowEntityID == "" {
		return fmt.Errorf("derive flow entity id for %s", flowPath)
	}
	if am.workflowInstances != nil {
		initialState := strings.TrimSpace(schema.InitialState)
		if initialState == "" {
			initialState = strings.TrimSpace(req.InitialState)
		}
		if initialState == "" {
			initialState = "pending"
		}
		if err := am.workflowInstances.Upsert(ctx, runtimepipeline.WorkflowInstance{
			InstanceID:      instanceID,
			SubjectID:       strings.TrimSpace(sourceEntityID),
			StorageRef:      flowPath,
			WorkflowName:    templateID,
			WorkflowVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
			CurrentState:    initialState,
			Config:          cloneFlowConfig(req.Config),
			Metadata: map[string]any{
				"entity_id":        flowEntityID,
				"instance_id":      identity.InstanceID,
				"flow_path":        flowPath,
				"subject_id":       strings.TrimSpace(sourceEntityID),
				"parent_entity_id": strings.TrimSpace(sourceEntityID),
			},
		}); err != nil {
			return fmt.Errorf("persist flow instance %s: %w", flowPath, err)
		}
	}

	vars := flowActivationVars(req)
	localEvents := flowLocalEventSet(schema, scope)
	agentKeys := make([]string, 0, len(scope.Agents))
	for key := range scope.Agents {
		key = strings.TrimSpace(key)
		if key != "" {
			agentKeys = append(agentKeys, key)
		}
	}
	sort.Strings(agentKeys)

	for _, key := range agentKeys {
		entry := scope.Agents[key]
		cfg, err := buildFlowAgentConfig(req.ContractBundle, templateID, instanceID, flowEntityID, flowPath, key, entry, vars, localEvents, req.Config)
		if err != nil {
			return err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "flow-instance-activator",
			TemplateVersion: strings.TrimSpace(req.ContractBundle.WorkflowVersion()),
		}
		if err := am.spawnAgentInternal(ctx, rec, true); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	if installer, ok := am.bus.(flowInstanceRouteInstaller); ok && installer != nil {
		if err := installer.AddFlowInstance(runtimecontracts.SystemNodeContract{}, flowPath); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("event bus does not support derived flow-instance routing for %s", flowPath)
	}
	if autoEmit := strings.TrimSpace(schema.AutoEmitOnCreate.Event); autoEmit != "" {
		eventType := eventidentity.ExternalizeForFlow(flowPath, []string{autoEmit}, autoEmit)
		payload := map[string]any{
			"entity_id":        flowEntityID,
			"instance_id":      identity.InstanceID,
			"template_id":      templateID,
			"flow_path":        flowPath,
			"subject_id":       strings.TrimSpace(sourceEntityID),
			"parent_entity_id": strings.TrimSpace(sourceEntityID),
		}
		for key, value := range req.Config {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, exists := payload[key]; !exists {
				payload[key] = value
			}
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode auto-emit payload %s: %w", autoEmit, err)
		}
		autoEmitEvent := (events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType(eventType),
			SourceAgent: "flow-instance-activator",
			Payload:     encoded,
			CreatedAt:   time.Now().UTC(),
		}).WithEntityID(flowEntityID)
		publishAutoEmit := func() {
			if err := am.bus.Publish(context.Background(), autoEmitEvent); err != nil {
				am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
					Level:     "warn",
					Message:   "Auto-emitting the flow activation event failed",
					Component: "flow_activation",
					Action:    "auto_emit_failed",
					EventType: autoEmit,
					EntityID:  flowEntityID,
					Detail: map[string]any{
						"flow_path": flowPath,
					},
					Error: err.Error(),
				})
			}
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, publishAutoEmit) {
			if err := am.bus.Publish(ctx, autoEmitEvent); err != nil {
				return fmt.Errorf("auto-emit %s: %w", autoEmit, err)
			}
		}
	}
	return nil
}

func (am *AgentManager) EnsureStaticFlowRequiredAgents(ctx context.Context, source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	for _, scope := range source.ProjectScopes() {
		if err := am.ensureStaticRequiredAgentsForScope(ctx, source, "", "", scope.Agents, source.RequiredAgents()); err != nil {
			return err
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		if err := am.ensureStaticRequiredAgentsForScope(ctx, source, flowID, strings.Trim(scope.Path, "/"), scope.Agents, source.FlowRequiredAgents(flowID)); err != nil {
			return err
		}
	}
	return nil
}

func (am *AgentManager) EnsureStaticAgents(ctx context.Context, source semanticview.Source) error {
	if am == nil || source == nil {
		return nil
	}
	for _, scope := range source.ProjectScopes() {
		projectAgents := make(map[string]runtimecontracts.AgentRegistryEntry, len(scope.Agents))
		for logicalID, entry := range scope.Agents {
			agentID := strings.TrimSpace(entry.ID)
			if agentID == "" {
				agentID = strings.TrimSpace(logicalID)
			}
			if sourceInfo, ok := source.AgentContractSource(agentID); ok && strings.TrimSpace(sourceInfo.FlowID) != "" {
				continue
			}
			projectAgents[strings.TrimSpace(logicalID)] = entry
		}
		if err := am.ensureStaticAgentsForScope(ctx, source, "", "", projectAgents); err != nil {
			return err
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		if err := am.ensureStaticAgentsForScope(ctx, source, flowID, strings.Trim(scope.Path, "/"), scope.Agents); err != nil {
			return err
		}
	}
	return nil
}

func (am *AgentManager) DeactivateFlowInstance(ctx context.Context, templateID, instanceID, flowPath, entityID string) error {
	if am == nil {
		return fmt.Errorf("agent manager is required")
	}
	templateID = strings.TrimSpace(templateID)
	instanceID = strings.TrimSpace(instanceID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	entityID = strings.TrimSpace(entityID)
	if templateID == "" || instanceID == "" || flowPath == "" || entityID == "" {
		return fmt.Errorf("template_id, instance_id, flow_path, and entity_id are required")
	}
	flowEntityID := strings.TrimSpace(runtimepipeline.FlowInstanceEntityID(flowPath))
	scopeKey := strings.TrimSpace(runtimeflowidentity.SemanticScopeFromInstancePath(flowPath))
	if scopeKey == "" {
		return fmt.Errorf("derive scope key for flow path %s", flowPath)
	}
	am.mu.RLock()
	agentIDs := make([]string, 0, len(am.agentCfg))
	for agentID, cfg := range am.agentCfg {
		if strings.TrimSpace(cfg.EntityID) != flowEntityID {
			continue
		}
		agentIDs = append(agentIDs, agentID)
	}
	am.mu.RUnlock()
	sort.Strings(agentIDs)
	for _, agentID := range agentIDs {
		if err := am.TeardownAgent(agentID); err != nil && !strings.Contains(err.Error(), "not found") {
			return err
		}
	}
	if remover, ok := am.bus.(flowInstanceRouteRemover); ok && remover != nil {
		return remover.RemoveFlowInstance(scopeKey, instanceID)
	}
	return fmt.Errorf("event bus does not support derived flow-instance route removal for %s", flowPath)
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
	subscriptions := make([]string, 0, len(entry.Subscriptions)+len(entry.SubscriptionsBootstrap)+len(entry.SubscribesTo))
	subscriptions = append(subscriptions, entry.Subscriptions...)
	subscriptions = append(subscriptions, entry.SubscriptionsBootstrap...)
	subscriptions = append(subscriptions, entry.SubscribesTo...)
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
	if workspaceClass := strings.TrimSpace(entry.WorkspaceClass); workspaceClass != "" {
		cfgPayload["workspace_class"] = workspaceClass
	}
	if flowPath != "" {
		cfgPayload["flow_path"] = strings.Trim(flowPath, "/")
	}
	if managerFallback := strings.TrimSpace(entry.ManagerFallback); managerFallback != "" {
		cfgPayload["manager_fallback"] = managerFallback
	}
	if modelTier := strings.TrimSpace(entry.ModelTier); modelTier != "" {
		cfgPayload["model_tier"] = modelTier
	}
	if conversationMode := strings.TrimSpace(entry.ConversationMode); conversationMode != "" {
		cfgPayload["conversation_mode"] = conversationMode
	}
	if maxTurns := entry.MaxTurnsPerTask; maxTurns > 0 {
		cfgPayload["max_turns_per_task"] = maxTurns
	}
	if tools := normalizedConfiguredToolList(entry.ConfiguredTools()); len(tools) > 0 {
		cfgPayload["tools"] = append([]string{}, tools...)
	}
	if nativeTools := normalizedConfiguredNativeTools(entry.NativeTools); len(nativeTools) > 0 {
		cfgPayload["native_tools"] = nativeTools
	}
	if emitEvents := normalizedFlowAgentEmitEvents(entry.EmitEvents, vars, localEvents, strings.Trim(flowPath, "/"), templateID, instanceID); len(emitEvents) > 0 {
		cfgPayload["emit_events"] = append([]string{}, emitEvents...)
	}
	rawConfig, err := json.Marshal(cfgPayload)
	if err != nil {
		return models.AgentConfig{}, err
	}
	permissions, err := runtimetools.ResolveAgentPermissions(source, templateID, entry)
	if err != nil {
		return models.AgentConfig{}, fmt.Errorf("flow agent %s permissions: %w", key, err)
	}

	return models.AgentConfig{
		ID:            agentID,
		Type:          strings.TrimSpace(entry.Type),
		Role:          strings.TrimSpace(entry.Role),
		Mode:          templateID,
		LLMBackend:    "",
		Permissions:   permissions,
		EntityID:      entityID,
		Subscriptions: rendered,
		Config:        rawConfig,
	}, nil
}

func (am *AgentManager) ensureStaticRequiredAgentsForScope(
	ctx context.Context,
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
	required []runtimecontracts.FlowRequiredAgent,
) error {
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if len(required) == 0 || len(agents) == 0 {
		return nil
	}
	localEvents := staticFlowLocalEventSet(agents)
	for _, requiredAgent := range required {
		logicalID, entry, ok := resolveRequiredAgentEntry(agents, requiredAgent)
		if !ok {
			return fmt.Errorf("required agent %q missing from scope %q", strings.TrimSpace(requiredAgent.Role), flowID)
		}
		cfg, err := buildStaticFlowAgentConfig(source, flowID, flowPath, logicalID, entry, localEvents)
		if err != nil {
			return err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "static-flow-required-agent",
			TemplateVersion: "",
		}
		if err := am.spawnAgentInternal(ctx, rec, true); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}

func (am *AgentManager) ensureStaticAgentsForScope(
	ctx context.Context,
	source semanticview.Source,
	flowID string,
	flowPath string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
) error {
	flowID = strings.TrimSpace(flowID)
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	if len(agents) == 0 {
		return nil
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
	for _, logicalID := range logicalIDs {
		entry := agents[logicalID]
		cfg, err := buildStaticFlowAgentConfig(source, flowID, flowPath, logicalID, entry, localEvents)
		if err != nil {
			return err
		}
		rec := PersistedAgent{
			Config:          cfg,
			Status:          "active",
			HiredBy:         "static-flow-agent",
			TemplateVersion: "",
		}
		if err := am.spawnAgentInternal(ctx, rec, true); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
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
	subscriptions := make([]string, 0, len(entry.Subscriptions)+len(entry.SubscriptionsBootstrap)+len(entry.SubscribesTo))
	subscriptions = append(subscriptions, entry.Subscriptions...)
	subscriptions = append(subscriptions, entry.SubscriptionsBootstrap...)
	subscriptions = append(subscriptions, entry.SubscribesTo...)
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
	if flowPath != "" {
		cfgPayload["flow_path"] = flowPath
	}
	if workspaceClass := strings.TrimSpace(entry.WorkspaceClass); workspaceClass != "" {
		cfgPayload["workspace_class"] = workspaceClass
	}
	if managerFallback := strings.TrimSpace(entry.ManagerFallback); managerFallback != "" {
		cfgPayload["manager_fallback"] = managerFallback
	}
	if modelTier := strings.TrimSpace(entry.ModelTier); modelTier != "" {
		cfgPayload["model_tier"] = modelTier
	}
	if conversationMode := strings.TrimSpace(entry.ConversationMode); conversationMode != "" {
		cfgPayload["conversation_mode"] = conversationMode
	}
	if maxTurns := entry.MaxTurnsPerTask; maxTurns > 0 {
		cfgPayload["max_turns_per_task"] = maxTurns
	}
	if tools := normalizedConfiguredToolList(entry.ConfiguredTools()); len(tools) > 0 {
		cfgPayload["tools"] = append([]string{}, tools...)
	}
	if nativeTools := normalizedConfiguredNativeTools(entry.NativeTools); len(nativeTools) > 0 {
		cfgPayload["native_tools"] = nativeTools
	}
	if emitEvents := normalizedStaticFlowEmitEvents(entry.EmitEvents, vars, localEvents, flowPath); len(emitEvents) > 0 {
		cfgPayload["emit_events"] = append([]string{}, emitEvents...)
	}
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
	return models.AgentConfig{
		ID:            agentID,
		Type:          strings.TrimSpace(entry.Type),
		Role:          role,
		Mode:          flowID,
		LLMBackend:    "",
		Permissions:   permissions,
		EntityID:      "",
		Subscriptions: rendered,
		Config:        rawConfig,
	}, nil
}

func resolveRequiredAgentEntry(agents map[string]runtimecontracts.AgentRegistryEntry, required runtimecontracts.FlowRequiredAgent) (string, runtimecontracts.AgentRegistryEntry, bool) {
	role := strings.TrimSpace(required.Role)
	for logicalID, entry := range agents {
		if strings.EqualFold(strings.TrimSpace(logicalID), role) || strings.EqualFold(strings.TrimSpace(entry.Role), role) || strings.EqualFold(strings.TrimSpace(entry.ID), role) {
			return strings.TrimSpace(logicalID), entry, true
		}
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
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
		for _, eventType := range append(append([]string{}, entry.Subscriptions...), append(entry.SubscriptionsBootstrap, entry.SubscribesTo...)...) {
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
	vars := map[string]string{
		"entity_id":   strings.TrimSpace(req.EntityID),
		"instance_id": strings.TrimSpace(req.InstanceID),
	}
	for key, value := range req.Config {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		vars[key] = stringifyPromptTemplateValue(value)
	}
	return vars
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
