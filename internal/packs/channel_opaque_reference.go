package packs

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// RestoreOpaqueReference restores value type from the exact admitted slot, never
// from the carrier's appearance. It does not confer identity or effect authority.
func (p SatisfactionPlan) RestoreOpaqueReference(slot, stored string) (any, error) {
	if _, err := p.Generation(); err != nil {
		return nil, err
	}
	schema, found := p.opaqueTypes[slot]
	if !found {
		return nil, fmt.Errorf("channel opaque slot %q is absent from the admitted plan", slot)
	}
	if strings.TrimSpace(stored) == "" {
		return nil, fmt.Errorf("channel opaque slot %q has an empty stored reference", slot)
	}
	var value any = stored
	if schema.Kind() == runtimecontracts.ToolSchemaObject {
		decoded, err := canonicaljson.Decode([]byte(stored))
		if err != nil {
			return nil, fmt.Errorf("channel opaque slot %q: %w", slot, err)
		}
		canonical, err := canonicaljson.Encode(decoded)
		if err != nil || string(canonical) != stored {
			return nil, fmt.Errorf("channel opaque slot %q requires canonical object JSON", slot)
		}
		value = decoded.Interface()
	} else if schema.Kind() != runtimecontracts.ToolSchemaString {
		return nil, fmt.Errorf("channel opaque slot %q has no admitted reference schema", slot)
	}
	if err := schema.Validate(value); err != nil {
		return nil, fmt.Errorf("channel opaque slot %q: %w", slot, err)
	}
	return value, nil
}

func (p OutboundBindingPlan) RestoreOpaqueReference(slot, stored string) (any, error) {
	return p.structural.RestoreOpaqueReference(slot, stored)
}

func (p SatisfactionPlan) LearnedDestination(storedConversation string) (any, error) {
	if p.onboarding == nil || len(p.onboarding.learnedDestination) == 0 {
		return nil, fmt.Errorf("channel plan has no compiled learned_destination relation")
	}
	conversation, err := p.RestoreOpaqueReference("conversation_reference", storedConversation)
	if err != nil {
		return nil, err
	}
	projected, err := projectChannelOperationMappings("learned_destination", p.onboarding.learnedDestination, map[string]any{"conversation_reference": conversation})
	if err != nil {
		return nil, err
	}
	destination := projected["destination"]
	if err := p.opaqueTypes["destination"].Validate(destination); err != nil {
		return nil, fmt.Errorf("channel learned destination: %w", err)
	}
	return destination, nil
}

func compileLearnedDestination(mappings map[string]ChannelMapping, opaque map[string]runtimecontracts.ToolInputSchema) ([]compiledChannelMapping, error) {
	if len(mappings) == 0 {
		return nil, fmt.Errorf("explicit learned_destination relation is required")
	}
	topology, err := compileChannelMappingTopology("learned_destination", mappings)
	if err != nil {
		return nil, err
	}
	source := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"conversation_reference": opaque["conversation_reference"]}),
		runtimecontracts.ToolSchemaRequired("conversation_reference"), runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false))
	target := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"destination": opaque["destination"]}),
		runtimecontracts.ToolSchemaRequired("destination"), runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false))
	used := newChannelPathCardinality("learned_destination source")
	for _, path := range topology.Targets {
		mapping := mappings[path]
		if mapping.Each != "" || len(mapping.Item) != 0 {
			return nil, fmt.Errorf("learned_destination %q permits only scalar-leaf projection", path)
		}
		if err := validateChannelTargetAndMapping("learned_destination", path, mapping); err != nil {
			return nil, err
		}
		from, sourceOK := schemaAt(source, strings.Split(mapping.From, "."))
		to, targetOK := schemaAt(target, strings.Split(path, "."))
		if !sourceOK || !targetOK {
			return nil, fmt.Errorf("learned_destination %q <- %q must resolve declared destination and conversation_reference leaves", path, mapping.From)
		}
		if from.Kind() == runtimecontracts.ToolSchemaObject || from.Kind() == runtimecontracts.ToolSchemaArray || to.Kind() == runtimecontracts.ToolSchemaObject || to.Kind() == runtimecontracts.ToolSchemaArray {
			return nil, fmt.Errorf("learned_destination %q <- %q permits only scalar-leaf projection", path, mapping.From)
		}
		if err := validateDirectionalRelation("learned_destination "+path+" <- "+mapping.From, from, to); err != nil {
			return nil, err
		}
		if err := used.add(mapping.From); err != nil {
			return nil, err
		}
	}
	if err := validateRequiredPathCardinality("learned_destination source", schemaRequiredLeafPaths("", source), used.values()); err != nil {
		return nil, err
	}
	if err := validateRequiredPathCardinality("learned_destination target", schemaRequiredLeafPaths("", target), topology.Targets); err != nil {
		return nil, err
	}
	return compileAdmittedChannelMappings(mappings, topology, target)
}
