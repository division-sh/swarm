package packs

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// GenerationID identifies the complete compiled channel plan. Durable private
// targets pin this value so project reload cannot reinterpret an admitted
// request through a replacement trigger, connector, schema, or projection.
func (p SatisfactionPlan) GenerationID() (string, error) {
	operations := make(map[string]any, len(p.Operations))
	for _, name := range sortedKeys(p.Operations) {
		operation := p.Operations[name]
		tool, err := p.OperationTool(name)
		if err != nil {
			return "", err
		}
		operations[name] = map[string]any{
			"name":            operation.Name,
			"tool":            operation.Tool,
			"tool_schema":     channelToolGenerationValue(tool),
			"input":           channelMappingGenerationValue(operation.Input),
			"output":          channelMappingGenerationValue(operation.Output),
			"interface":       channelInterfaceOperationGenerationValue(operation.Interface),
			"input_topology":  channelTopologyGenerationValue(operation.InputTopology),
			"output_topology": channelTopologyGenerationValue(operation.OutputTopology),
		}
	}
	events := make(map[string]any, len(p.Events))
	for _, name := range sortedKeys(p.Events) {
		event := p.Events[name]
		events[name] = map[string]any{
			"name":       event.Name,
			"event":      event.Event,
			"fields":     event.Fields,
			"descriptor": event.Descriptor,
		}
	}
	return canonicaljson.Hash(map[string]any{
		"interface_ref":         strings.TrimSpace(p.InterfaceRef),
		"channel":               p.Channel,
		"trigger":               p.Trigger,
		"connector":             p.Connector,
		"provider":              strings.TrimSpace(p.Provider),
		"trigger_generation_id": strings.TrimSpace(p.TriggerGenerationID),
		"schemas":               channelSchemaMapGenerationValue(p.Schemas),
		"opaque_types":          channelSchemaMapGenerationValue(p.OpaqueTypes),
		"constraints":           channelSchemaMapGenerationValue(p.Constraints),
		"operations":            operations,
		"events":                events,
	})
}

func channelSchemaMapGenerationValue(schemas map[string]runtimecontracts.ToolInputSchema) map[string]any {
	out := make(map[string]any, len(schemas))
	for name, schema := range schemas {
		out[name] = runtimecontracts.ToolInputSchemaJSONSchema(schema)
	}
	return out
}

func channelToolGenerationValue(tool runtimecontracts.ToolSchemaEntry) map[string]any {
	value := map[string]any{
		"category":            strings.TrimSpace(tool.Category),
		"description":         strings.TrimSpace(tool.Description),
		"handler_type":        strings.TrimSpace(tool.HandlerType),
		"effect_class":        strings.TrimSpace(tool.EffectClass),
		"permission":          strings.TrimSpace(tool.Permission),
		"required_permission": strings.TrimSpace(tool.RequiredPermission),
		"rate_limit":          strings.TrimSpace(tool.RateLimit),
		"rate_limit_max_wait": strings.TrimSpace(tool.RateLimitMaxWait),
		"input_schema":        runtimecontracts.ToolInputSchemaJSONSchema(tool.InputSchema),
		"output_schema":       runtimecontracts.ToolInputSchemaJSONSchema(tool.OutputSchema),
		"response_mapping":    tool.ResponseMapping,
		"credentials":         append([]string(nil), tool.Credentials...),
	}
	if tool.HTTP != nil {
		value["http"] = map[string]any{
			"method": tool.HTTP.Method, "url": tool.HTTP.URL, "headers": tool.HTTP.Headers,
			"body": tool.HTTP.Body, "timeout_seconds": tool.HTTP.TimeoutSeconds,
		}
	}
	if tool.ResponseSuccess != nil {
		value["response_success"] = map[string]any{
			"kind": tool.ResponseSuccess.Kind, "path": tool.ResponseSuccess.Path, "equals": tool.ResponseSuccess.Equals,
		}
	}
	if tool.ManagedCredential != nil {
		value["managed_credential"] = map[string]any{
			"key": tool.ManagedCredential.Key, "header": tool.ManagedCredential.Header,
			"prefix": tool.ManagedCredential.Prefix, "grant_type": tool.ManagedCredential.GrantType,
			"scopes": append([]string(nil), tool.ManagedCredential.Scopes...), "grant_model": tool.ManagedCredential.GrantModel,
			"token_request": tool.ManagedCredential.TokenRequest, "installation_id_input": tool.ManagedCredential.InstallationIDInput,
		}
	}
	if tool.CompiledResult != nil {
		value["compiled_result"] = map[string]any{
			"fields":        tool.CompiledResult.Fields,
			"output_schema": runtimecontracts.ToolInputSchemaJSONSchema(tool.CompiledResult.OutputSchema),
		}
	}
	return value
}

func channelMappingGenerationValue(mappings map[string]ChannelMapping) map[string]any {
	out := make(map[string]any, len(mappings))
	for target, mapping := range mappings {
		items := make([]any, 0, len(mapping.Item))
		for _, item := range mapping.Item {
			items = append(items, channelMappingGenerationValue(item))
		}
		out[target] = map[string]any{
			"from": mapping.From, "convert": mapping.Convert, "each": mapping.Each, "item": items,
		}
	}
	return out
}

func channelInterfaceOperationGenerationValue(operation runtimecontracts.PackInterfaceOperation) map[string]any {
	return map[string]any{
		"effect_class": operation.EffectClass,
		"input":        channelInterfaceFieldGenerationValue(operation.Input),
		"context":      channelInterfaceFieldGenerationValue(operation.Context),
		"output":       channelInterfaceFieldGenerationValue(operation.Output),
	}
}

func channelInterfaceFieldGenerationValue(fields map[string]runtimecontracts.PackInterfaceField) map[string]any {
	out := make(map[string]any, len(fields))
	for name, field := range fields {
		out[name] = map[string]any{"schema": field.Schema, "opaque": field.Opaque}
	}
	return out
}

func channelTopologyGenerationValue(topology compiledChannelMappingTopology) map[string]any {
	itemTargets := make(map[string]any, len(topology.ItemTargets))
	for target, paths := range topology.ItemTargets {
		itemTargets[target] = append([]string(nil), paths...)
	}
	return map[string]any{
		"targets": append([]string(nil), topology.Targets...), "item_targets": itemTargets,
	}
}

func (p OutboundBindingPlan) RuntimeActivityTarget(operation string) (string, string, error) {
	operation = strings.TrimSpace(operation)
	if _, ok := p.Structural.Operations[operation]; !ok {
		return "", "", fmt.Errorf("channel operation %q is not compiled", operation)
	}
	generation, err := p.Structural.GenerationID()
	if err != nil {
		return "", "", fmt.Errorf("compute channel plan generation: %w", err)
	}
	identity := strings.TrimPrefix(generation, "sha256:")
	return "platform.channel_activity." + strings.TrimSpace(p.ID) + "." + operation + ".g" + identity, generation, nil
}
