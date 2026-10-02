package engine

import (
	"fmt"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func seedConstructorAssignment(source semanticview.Source, flowID, input string, contract entityruntime.Contract, analysis *EntityAssignmentAnalysis) error {
	flow, found := source.FlowSchemaByID(flowID)
	if !found || flow.Instance.Empty() {
		return fmt.Errorf("flow %s has a keyless no-argument constructor, not input %s", flowID, input)
	}
	if !source.FlowHasInputEvent(flowID, input) {
		return fmt.Errorf("%s is not a declared input of flow %s", input, flowID)
	}
	compiled := semanticview.ResolveEventSchema(source, flowID, input)
	if !compiled.HasStructural {
		return fmt.Errorf("constructor %s of flow %s has no structural event schema", input, flowID)
	}
	if !compiled.HasSchema {
		return fmt.Errorf("constructor %s of flow %s has no admitted event schema", input, flowID)
	}
	properties, _ := compiled.Schema.Schema["properties"].(map[string]any)
	key := flow.Instance.Path()
	keyType, found := analysis.entity.Field(key)
	if !found {
		return fmt.Errorf("constructor %s of flow %s has no declared key field %s", input, flowID, key)
	}
	// Key presence is justified by receiver resolution, not by projection of a
	// payload field. Runtime separately checks a supplied same-name key agrees.
	analysis.constructorKey = key
	analysis.initial.Assign(key, keyType.Type)
	for _, field := range compiled.StructuralType.Fields {
		target, found := analysis.entity.Field(field.Name)
		if !found {
			continue
		}
		if contract.Entity.Fields[field.Name].Initial != nil {
			return fmt.Errorf("%s in constructor %s of flow %s collides with internal %s; rename one or make it supplied", field.Name, input, flowID, field.Name)
		}
		if !c.StructuralCatalogTypeAssignable(field.Type, target.Type) {
			return fmt.Errorf("%s in constructor %s of flow %s is incompatible with state field %s", field.Name, input, flowID, field.Name)
		}
		supplied, found := properties[field.Name].(map[string]any)
		if !found {
			return fmt.Errorf("constructor %s of flow %s has no admitted schema for %s", input, flowID, field.Name)
		}
		if err := c.ValidateCatalogFieldSupply(field.Name, supplied, contract.Entity.Fields[field.Name], contract.Types); err != nil {
			return fmt.Errorf("%s in constructor %s of flow %s is incompatible with state field %s: %w", field.Name, input, flowID, field.Name, err)
		}
		if field.Name != key {
			analysis.constructorFields = append(analysis.constructorFields, field.Name)
			if !field.IsOptional {
				analysis.initial.Assign(field.Name, field.Type)
			}
		}
	}
	return nil
}
