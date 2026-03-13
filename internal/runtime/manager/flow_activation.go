package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	models "empireai/internal/runtime/actors"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
)

type flowInstanceRouteInstaller interface {
	AddFlowInstance(template runtimecontracts.SystemNodeContract, instancePath string) error
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
	if am.workspaces != nil {
		if err := am.workspaces.EnsureEntityWorkspace(ctx, entityID); err != nil {
			return fmt.Errorf("ensure entity workspace: %w", err)
		}
	}
	if am.store != nil {
		if err := am.store.EnsureEntitySchema(ctx, entityID); err != nil {
			return fmt.Errorf("ensure entity schema: %w", err)
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
		cfg, err := buildFlowAgentConfig(templateID, instanceID, entityID, key, entry, vars, localEvents, req.Config)
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
		if err := installer.AddFlowInstance(runtimecontracts.SystemNodeContract{}, req.FlowPath); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("event bus does not support derived flow-instance routing for %s", req.FlowPath)
}

func buildFlowAgentConfig(
	templateID string,
	instanceID string,
	verticalID string,
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
			subscription = templateID + "/" + instanceID + "/" + subscription
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
	if managerFallback := strings.TrimSpace(entry.ManagerFallback); managerFallback != "" {
		cfgPayload["manager_fallback"] = managerFallback
	}
	rawConfig, err := json.Marshal(cfgPayload)
	if err != nil {
		return models.AgentConfig{}, err
	}

	return models.AgentConfig{
		ID:            agentID,
		Type:          strings.TrimSpace(entry.Type),
		Role:          strings.TrimSpace(entry.Role),
		Mode:          templateID,
		EntityID:      verticalID,
		Subscriptions: rendered,
		Config:        rawConfig,
	}, nil
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
