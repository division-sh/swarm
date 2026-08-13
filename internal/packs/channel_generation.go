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
	if !p.generation.Valid() {
		return plangeneration.Generation{}, fmt.Errorf("channel plan generation is missing")
	}
	return p.generation, nil
}

// Generation identifies the complete structural plan behind this configured
// binding. Destination and binding identity are carried separately by the
// effective-source projection.
func (p OutboundBindingPlan) Generation() (plangeneration.Generation, error) {
	return p.structural.Generation()
}

func compileSatisfactionPlanGeneration(p SatisfactionPlan) (plangeneration.Generation, error) {
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
			"name":           operation.name.String(),
			"tool":           operation.tool.String(),
			"tool_schema":    toolValue,
			"input":          compiledChannelMappingGenerationValue(operation.input),
			"output":         compiledChannelMappingGenerationValue(operation.output),
			"input_schema":   mustChannelSchemaProjection(operation.inputSchema),
			"context_schema": mustChannelSchemaProjection(operation.contextSchema),
			"output_schema":  mustChannelSchemaProjection(operation.outputSchema),
		}
	}
	events := make(map[string]any, len(p.events))
	for _, name := range sortedKeys(p.events) {
		event := p.events[name]
		fields := make(map[string]string, len(event.fields))
		for source, target := range event.fields {
			fields[source] = target.String()
		}
		events[name] = map[string]any{
			"name":       event.name.String(),
			"event":      event.event.String(),
			"fields":     fields,
			"descriptor": compiledChannelEventGenerationValue(event),
		}
	}
	var registration any
	if p.registration != nil {
		registration = p.registration.generationValue()
	}
	return plangeneration.FromCanonicalValue(map[string]any{
		"interface_ref":      p.interfaceRef.String(),
		"channel":            p.channel,
		"trigger":            p.trigger,
		"connector":          p.connector,
		"provider":           p.provider.String(),
		"trigger_generation": p.triggerGeneration.Diagnostic(),
		"schemas":            channelSchemaMapGenerationValue(p.schemas),
		"opaque_types":       channelSchemaMapGenerationValue(p.opaqueTypes),
		"constraints":        channelSchemaMapGenerationValue(p.constraints),
		"operations":         operations,
		"events":             events,
		"registration":       registration,
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
		for schemaName, schema := range map[string]runtimecontracts.ToolInputSchema{
			"input": operation.inputSchema, "context": operation.contextSchema, "output": operation.outputSchema,
		} {
			if err := schema.ValidateDefinition(); err != nil {
				return fmt.Errorf("channel generation operation %q %s schema: %w", name, schemaName, err)
			}
		}
	}
	for eventName, event := range plan.events {
		for fieldName, schema := range event.fieldSchema {
			if err := schema.ValidateDefinition(); err != nil {
				return fmt.Errorf("channel generation event %q field %q: %w", eventName, fieldName, err)
			}
		}
	}
	if plan.registration != nil {
		for _, operation := range []compiledRegistrationOperation{plan.registration.identify, plan.registration.apply, plan.registration.readback} {
			if err := operation.tool.Validate(); err != nil {
				return fmt.Errorf("channel generation registration %q tool: %w", operation.name, err)
			}
		}
	}
	return nil
}

func compiledChannelEventGenerationValue(event compiledChannelEvent) map[string]any {
	fields := make(map[string]any, len(event.fieldSchema))
	for name, schema := range event.fieldSchema {
		fields[name] = map[string]any{
			"required": event.required[name],
			"schema":   mustChannelSchemaProjection(schema),
		}
	}
	return map[string]any{"name": event.event.String(), "fields": fields}
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

func compiledChannelMappingGenerationValue(mappings []compiledChannelMapping) map[string]any {
	out := make(map[string]any, len(mappings))
	for _, mapping := range mappings {
		items := []any{}
		if len(mapping.item) > 0 {
			items = append(items, compiledChannelMappingGenerationValue(mapping.item))
		}
		out[mapping.target.syntax] = map[string]any{
			"from": mapping.source.syntax, "each": mapping.each.syntax, "item": items,
		}
	}
	return out
}

type PrivateActivityTargetIdentity struct {
	toolID         channelPlanIdentity
	generation     plangeneration.Generation
	credentialKeys map[string]string
}

func (t PrivateActivityTargetIdentity) ToolID() string {
	return t.toolID.String()
}

func (t PrivateActivityTargetIdentity) Generation() plangeneration.Generation {
	return t.generation
}

func (t PrivateActivityTargetIdentity) CredentialStoreKeys() map[string]string {
	return cloneChannelStringMap(t.credentialKeys)
}

func (p OutboundBindingPlan) RuntimeActivityTarget(operation string) (PrivateActivityTargetIdentity, error) {
	operation = strings.TrimSpace(operation)
	target, ok := p.activityTargets[operation]
	if !ok {
		return PrivateActivityTargetIdentity{}, fmt.Errorf("channel operation %q is not compiled", operation)
	}
	target.credentialKeys = cloneChannelStringMap(target.credentialKeys)
	return target, nil
}
