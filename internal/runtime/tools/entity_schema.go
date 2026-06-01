package tools

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type entityToolSchema struct {
	Defined  bool
	Contract entityruntime.Contract
}

func entityToolSchemaForActor(source semanticview.Source, actorID string) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	contract, ok := entityruntime.ResolveForActor(source, actorID)
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for actor %s", strings.TrimSpace(actorID))
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func entityToolSchemaForReadTarget(source semanticview.Source, actorID string, payload map[string]any) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	target := strings.TrimSpace(asString(payload["entity_type"]))
	contract, ok, err := entityruntime.ResolveForReadTarget(source, actorID, target)
	if err != nil {
		return entityToolSchema{}, err
	}
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for actor %s", strings.TrimSpace(actorID))
	}
	if !entityReadContractOwnedByActor(source, actorID, contract) {
		targetLabel := target
		if targetLabel == "" {
			targetLabel = entityruntime.CanonicalReadTargetName(contract)
		}
		return entityToolSchema{}, fmt.Errorf("entity_type %q resolves outside caller flow scope", targetLabel)
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func entityToolSchemaForEntityRow(source semanticview.Source, row map[string]any) (entityToolSchema, error) {
	if source == nil {
		return entityToolSchema{}, fmt.Errorf("workflow source is not configured")
	}
	contract, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return entityToolSchema{}, fmt.Errorf("flow-owned entity contract is not available for entity flow_instance %s", strings.TrimSpace(asString(row["flow_instance"])))
	}
	return entityToolSchema{Defined: true, Contract: contract}, nil
}

func (s entityToolSchema) field(name string) (entityruntime.Field, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return entityruntime.Field{}, fmt.Errorf("field is required")
	}
	field, err := entityruntime.ResolveLeafField(s.Contract, name)
	if err != nil {
		return entityruntime.Field{}, fmt.Errorf("%w: %v", ErrUnknownEntityField, err)
	}
	return field, nil
}

func (s entityToolSchema) declaredField(name string) (entityruntime.Field, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return entityruntime.Field{}, fmt.Errorf("field is required")
	}
	field, err := entityruntime.ResolveFieldPath(s.Contract, name)
	if err != nil {
		return entityruntime.Field{}, fmt.Errorf("%w: %v", ErrUnknownEntityField, err)
	}
	return field, nil
}

func normalizeEntityFieldValue(schema entityToolSchema, field entityruntime.Field, value any) (any, error) {
	return entityruntime.NormalizeFieldValue(schema.Contract, field.Path, value)
}

func defaultEntitySearchLimit(value int) int {
	if value <= 0 {
		return 100
	}
	if value > 1000 {
		return 1000
	}
	return value
}
