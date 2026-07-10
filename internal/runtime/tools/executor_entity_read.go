package tools

import (
	"context"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func (e *Executor) execGetEntity(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_get_entity.entity_id", nil)
	}
	entity, found, err := loadEntityState(ctx, store, entityID)
	if err != nil {
		return nil, failures.WrapDetail("query_failed", "tool-executor", "exec_get_entity.lookup", map[string]any{"entity_id": entityID}, err)
	}
	if !found {
		return nil, failures.NewDetail("not_found", "tool-executor", "exec_get_entity.lookup", map[string]any{"entity_id": entityID})
	}
	if err := enforceEntityReadOwnership(source, actor, entityID, entity, "entity_read_ownership.enforce"); err != nil {
		return nil, err
	}
	if flowInstance := normalizeEntityToolFlowInstance(asString(payload["flow_instance"])); flowInstance != "" {
		if !entityToolExistingFlowInstanceMatches(source, flowInstance, asString(entity["flow_instance"])) {
			return nil, failures.New(
				failures.ClassAuthorizationDenied,
				"entity_flow_ownership_mismatch",
				"tool-executor",
				"exec_get_entity.flow_instance",
				map[string]any{"action": "entity_read", "entity_id": entityID, "requested_flow_path": flowInstance, "owner_flow_path": asString(entity["flow_instance"])},
			)
		}
	}
	materialized, err := materializeEntityStateRow(source, entity)
	if err != nil {
		return nil, failures.Wrap(failures.ClassInternalFailure, "entity_materialization_failed", "tool-executor", "exec_get_entity.materialize", map[string]any{"entity_id": entityID}, err)
	}
	return materialized, nil
}
