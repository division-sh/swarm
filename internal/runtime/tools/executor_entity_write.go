package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const entityFieldPostCommitErrorCode = "entity_field_write_post_commit_failure"

func (e *Executor) execSaveEntityField(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", nil)
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_save_entity_field.run_context", nil, err)
	}
	if err := enforceEntityWriteOwnership(ctx, store, source, actor, entityID, e.runtimeLogSink()); err != nil {
		return nil, err
	}
	row, found, err := loadEntityState(ctx, store, entityID)
	if err != nil {
		return nil, failures.WrapDetail("query_failed", "tool-executor", "exec_save_entity_field.lookup", map[string]any{"entity_id": entityID}, err)
	}
	if !found {
		return nil, failures.NewDetail("not_found", "tool-executor", "exec_save_entity_field.lookup", map[string]any{"entity_id": entityID})
	}
	if flowInstance := normalizeEntityToolFlowInstance(asString(payload["flow_instance"])); flowInstance != "" {
		if !entityToolExistingFlowInstanceMatches(source, flowInstance, asString(row["flow_instance"])) {
			return nil, failures.New(failures.ClassAuthorizationDenied, "entity_flow_ownership_mismatch", "tool-executor", "exec_save_entity_field.flow_instance", map[string]any{"action": "entity_write", "entity_id": entityID, "requested_flow_path": flowInstance, "owner_flow_path": asString(row["flow_instance"])})
		}
	}
	schema, err := entityToolSchemaForEntityRow(source, row)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.schema", nil, err)
	}
	fieldName := strings.TrimSpace(asString(payload["field"]))
	if fieldName == "" {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": "field"})
	}
	field, err := schema.declaredField(fieldName)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.field", map[string]any{"field": fieldName}, err)
	}
	if strings.TrimSpace(field.FieldDecl.MaterializeFrom) != "" {
		return nil, failures.New(failures.ClassAuthorizationDenied, "runtime_materialized_field_write_forbidden", "tool-executor", "exec_save_entity_field.field", map[string]any{"action": "entity_write", "field": fieldName})
	}
	currentFields := entityRowFieldMap(row)
	candidate, err := entityruntime.ApplyMutations(schema.Contract, currentFields, []entityruntime.Mutation{{Target: "entity." + field.Path, Value: payload["value"]}})
	if err != nil {
		if errors.Is(err, entityruntime.ErrImmutableMutation) {
			return nil, failures.Wrap(failures.ClassAuthorizationDenied, "immutable_field_write_forbidden", "tool-executor", "exec_save_entity_field.field", map[string]any{"action": "entity_write", "field": fieldName}, err)
		}
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.value", map[string]any{"field": fieldName}, err)
	}
	value, _ := entityruntime.PathValue(candidate, field.Path)

	result, err := store.SaveEntityField(ctx, EntityFieldUpdate{
		RunID:     identity.RunID,
		EntityID:  identity.EntityID,
		FieldPath: field.Path,
		Value:     value,
		Source:    source,
		Writer: EntityMutationWriter{
			Type:        "agent",
			ID:          strings.TrimSpace(actor.ID),
			HandlerStep: "save_entity_field",
		},
	})
	if !result.Acknowledged {
		if err == nil {
			err = fmt.Errorf("entity field write was not acknowledged")
		}
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_save_entity_field.update", map[string]any{"entity_id": entityID, "field": fieldName}, err)
	}
	response := map[string]any{
		"entity_id": entityID,
		"field":     field.Path,
		"revision":  result.Revision,
	}
	if err != nil {
		response["status"] = "committed_with_post_commit_error"
		response["write_committed"] = true
		response["retry_write"] = false
		response["post_commit_error_code"] = entityFieldPostCommitErrorCode
		if logger := e.runtimeLogSink(); logger != nil {
			_ = logger.LogRuntime(toolExecutorRuntimeLogContext(ctx), runtimepipeline.RuntimeLogEntry{
				Level: "warn", Message: "Entity field write committed with a post-commit failure",
				Component: "tool-executor", Action: "entity_field_write_post_commit_failure",
				AgentID: strings.TrimSpace(actor.ID), EntityID: entityID,
				Detail: map[string]any{
					"field": field.Path, "revision": result.Revision,
					"post_commit_error": err.Error(),
				},
			})
		}
	}
	return response, nil
}

func enforceEntityWriteOwnership(ctx context.Context, store EntityPersistence, source semanticview.Source, actor models.AgentConfig, entityID string, logger runtimeToolLogSink) error {
	flowRoot := actorFlowOwnershipRoot(source, actor)
	if flowRoot == "" || store == nil {
		return nil
	}
	row, found, err := loadEntityState(ctx, store, entityID)
	if err != nil {
		return failures.WrapDetail("query_failed", "tool-executor", "entity_write_ownership.lookup", map[string]any{"entity_id": entityID}, err)
	}
	if !found {
		return nil
	}
	targetFlow := strings.Trim(asString(row["flow_instance"]), "/")
	if targetFlow == "" || entityFlowOwnedBy(flowRoot, targetFlow) {
		return nil
	}
	denial := failures.NewDetail(
		"cross_flow_write_forbidden",
		"tool-executor",
		"entity_write_ownership.enforce",
		map[string]any{
			"action":          "entity_write",
			"actor_id":        strings.TrimSpace(actor.ID),
			"entity_id":       entityID,
			"owner_flow_path": targetFlow,
		},
	)
	if logger != nil {
		failure, _ := failures.EnvelopeFromError(denial)
		logger.LogRuntime(toolExecutorRuntimeLogContext(ctx), runtimepipeline.RuntimeLogEntry{
			Level:     "warn",
			Message:   "Entity write was denied because the target entity belongs to a different flow",
			Component: "tool-executor",
			Action:    "entity_write_denied",
			AgentID:   strings.TrimSpace(actor.ID),
			EntityID:  strings.TrimSpace(entityID),
			Detail: map[string]any{
				"denial_layer":       "executor",
				"denial_reason":      "cross_flow_write_forbidden",
				"tool_name":          "save_entity_field",
				"actor_id":           strings.TrimSpace(actor.ID),
				"actor_role":         strings.TrimSpace(actor.Role),
				"turn_flow":          strings.TrimSpace(flowRoot),
				"entity_owner_flow":  targetFlow,
				"write_target_id":    strings.TrimSpace(entityID),
				"ownership_relation": "foreign",
			},
			Failure: failures.CloneEnvelope(&failure),
		})
	}
	return denial
}

func actorFlowOwnershipRoot(source semanticview.Source, actor models.AgentConfig) string {
	if source == nil || strings.TrimSpace(actor.ID) == "" {
		return ""
	}
	projection, ok := semanticview.ResolveAgentContractProjection(source, actor)
	if !ok {
		return ""
	}
	flowID := strings.TrimSpace(projection.OwnerFlowID)
	if flowID == "" {
		return ""
	}
	return runtimeflowidentity.ScopeKey(source, flowID)
}

func entityFlowOwnedBy(flowRoot, targetFlow string) bool {
	return runtimeflowidentity.OwnedByScope(flowRoot, targetFlow)
}
