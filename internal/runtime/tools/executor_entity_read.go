package tools

import (
	"context"

	models "swarm/internal/runtime/core/actors"
)

func (e *Executor) execGetEntity(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_entity.entity_id", false, err.Error())
	}
	entity, found, err := loadEntityState(ctx, db, entityID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_entity.lookup", true, err, "load entity %s", entityID)
	}
	if !found {
		return nil, NewRuntimeError("not_found", "tool-executor", "exec_get_entity.lookup", false, "entity %s not found", entityID)
	}
	if err := enforceEntityReadOwnership(source, actor, entityID, entity, "entity_read_ownership.enforce"); err != nil {
		return nil, err
	}
	if flowInstance := normalizeEntityToolFlowInstance(asString(payload["flow_instance"])); flowInstance != "" {
		if !entityToolExistingFlowInstanceMatches(source, flowInstance, asString(entity["flow_instance"])) {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_entity.flow_instance", false, "flow_instance does not match entity ownership")
		}
	}
	materialized, err := materializeEntityStateRow(source, entity)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_entity.materialize", false, err, "materialize entity %s", entityID)
	}
	return materialized, nil
}
