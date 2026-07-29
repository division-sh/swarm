package packs

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
)

// Generation identifies the complete compiled channel plan. Durable private
// targets pin this value so project reload cannot reinterpret an admitted
// request through a replacement trigger, connector, schema, or projection.
func (p SatisfactionPlan) Generation() (plangeneration.Generation, error) {
	if err := validateSatisfactionPlanGenerationInputs(p); err != nil {
		return plangeneration.Generation{}, err
	}
	operations := make(map[string]any, len(p.operations))
	for _, name := range sortedKeys(p.operations) {
		operation := p.operations[name]
		tool, err := p.OperationTool(name)
		if err != nil {
			return plangeneration.Generation{}, err
		}
		toolValue, err := tool.CanonicalValue()
		if err != nil {
			return plangeneration.Generation{}, fmt.Errorf("channel generation operation %q tool: %w", name, err)
		}
		operations[name] = map[string]any{
			"name":            operation.name,
			"tool":            operation.tool,
			"tool_schema":     toolValue,
			"input":           channelMappingGenerationValue(operation.input),
			"output":          channelMappingGenerationValue(operation.output),
			"interface":       channelInterfaceOperationGenerationValue(operation),
			"input_topology":  channelTopologyGenerationValue(operation.inputTopology),
			"output_topology": channelTopologyGenerationValue(operation.outputTopology),
		}
	}
	events := make(map[string]any, len(p.events))
	for _, name := range sortedKeys(p.events) {
		event := p.events[name]
		events[name] = map[string]any{
			"name":       event.name,
			"event":      event.event,
			"fields":     event.fields,
			"descriptor": channelTriggerEventGenerationValue(event.descriptor),
		}
	}
	return plangeneration.FromCanonicalValue(map[string]any{
		"interface_ref":      p.interfaceRef,
		"channel":            p.channel,
		"trigger":            p.trigger,
		"connector":          p.connector,
		"provider":           p.provider,
		"trigger_generation": p.triggerGeneration.Diagnostic(),
		"schemas":            channelSchemaMapGenerationValue(p.schemas),
		"opaque_types":       channelSchemaMapGenerationValue(p.opaqueTypes),
		"constraints":        channelSchemaMapGenerationValue(p.constraints),
		"operations":         operations,
		"events":             events,
	})
}

func validateSatisfactionPlanGenerationInputs(plan SatisfactionPlan) error {
	for family, schemas := range map[string]map[string]runtimecontracts.ToolInputSchema{
		"schema": plan.schemas, "opaque type": plan.opaqueTypes, "constraint": plan.constraints,
	} {
		for name, schema := range schemas {
			if err := schema.ValidateDefinition(); err != nil {
				return fmt.Errorf("channel generation %s %q: %w", family, name, err)
			}
		}
	}
	for name, operation := range plan.operations {
		if err := operation.toolSchema.Validate(); err != nil {
			return fmt.Errorf("channel generation operation %q tool: %w", name, err)
		}
	}
	for eventName, event := range plan.events {
		for fieldName, field := range event.descriptor.Fields {
			if err := field.Schema.ValidateDefinition(); err != nil {
				return fmt.Errorf("channel generation event %q field %q: %w", eventName, fieldName, err)
			}
		}
	}
	return nil
}

func channelTriggerEventGenerationValue(event TriggerEvent) map[string]any {
	fields := make(map[string]any, len(event.Fields))
	for name, field := range event.Fields {
		fields[name] = map[string]any{
			"required": field.Required,
			"schema":   mustChannelSchemaProjection(field.Schema),
		}
	}
	return map[string]any{"name": event.Name, "fields": fields}
}

func channelSchemaMapGenerationValue(schemas map[string]runtimecontracts.ToolInputSchema) map[string]any {
	out := make(map[string]any, len(schemas))
	for name, schema := range schemas {
		out[name] = mustChannelSchemaProjection(schema)
	}
	return out
}

func mustChannelSchemaProjection(schema runtimecontracts.ToolInputSchema) map[string]any {
	projected, err := schema.Project()
	if err != nil {
		panic(err)
	}
	return projected
}

func channelMappingGenerationValue(mappings map[string]ChannelMapping) map[string]any {
	out := make(map[string]any, len(mappings))
	for target, mapping := range mappings {
		items := make([]any, 0, len(mapping.Item))
		for _, item := range mapping.Item {
			items = append(items, channelMappingGenerationValue(item))
		}
		out[target] = map[string]any{
			"from": mapping.From, "each": mapping.Each, "item": items,
		}
	}
	return out
}

func channelInterfaceOperationGenerationValue(operation compiledChannelOperation) map[string]any {
	return map[string]any{
		"effect_class": string(operation.effect),
		"input":        channelInterfaceFieldGenerationValue(operation.interfaceValue.Input),
		"context":      channelInterfaceFieldGenerationValue(operation.interfaceValue.Context),
		"output":       channelInterfaceFieldGenerationValue(operation.interfaceValue.Output),
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

type PrivateActivityTargetIdentity struct {
	toolID     string
	generation plangeneration.Generation
}

func (t PrivateActivityTargetIdentity) ToolID() string {
	return t.toolID
}

func (t PrivateActivityTargetIdentity) Generation() plangeneration.Generation {
	return t.generation
}

func (p OutboundBindingPlan) RuntimeActivityTarget(operation string) (PrivateActivityTargetIdentity, error) {
	operation = strings.TrimSpace(operation)
	if _, ok := p.structural.operations[operation]; !ok {
		return PrivateActivityTargetIdentity{}, fmt.Errorf("channel operation %q is not compiled", operation)
	}
	generation, err := p.structural.Generation()
	if err != nil {
		return PrivateActivityTargetIdentity{}, fmt.Errorf("compute channel plan generation: %w", err)
	}
	identity := strings.TrimPrefix(generation.Diagnostic(), "sha256:")
	return PrivateActivityTargetIdentity{
		toolID:     runtimecontracts.PrivateChannelActivityPrefix + p.id + "." + operation + ".g" + identity,
		generation: generation,
	}, nil
}
