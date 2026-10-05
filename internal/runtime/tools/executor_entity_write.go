package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const entityFieldPostCommitErrorCode = "entity_field_write_post_commit_failure"

func (e *Executor) execSaveEntityField(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	writer := e.entityWriter
	e.mu.RUnlock()
	if writer == nil {
		return nil, failures.NewDetail("dependency_unavailable", "tool-executor", "exec_save_entity_field.writer", map[string]any{"dependency": "entity_field_mutation_writer"})
	}
	entityID, err := parseEntityID(payload["entity_id"])
	if err != nil {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.entity_id", nil)
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return nil, failures.WrapDetail("write_failed", "tool-executor", "exec_save_entity_field.run_context", nil, err)
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
	actorContract, ok := entityruntime.ResolveForActor(source, actor)
	if !ok {
		return nil, failures.New(failures.ClassAuthorizationDenied, "role_scoped_tool_forbidden", "tool-executor", "exec_save_entity_field.actor", map[string]any{"action": "entity_write", "actor_id": strings.TrimSpace(actor.ID)})
	}
	owner, err := enforceEntityWriteOwnership(ctx, source, actor, identity.RunID, identity.EntityID, schema.Contract.FlowID, row, e.runtimeLogSink())
	if err != nil {
		return nil, err
	}
	if err := roleScopedEntityMatchesContract(source, row, actorContract); err != nil {
		return nil, err
	}
	root, _, _ := strings.Cut(field.Path, ".")
	writable := false
	for _, allowed := range roleScopedWritableFields(source, actor, actorContract) {
		if root == allowed {
			writable = true
			break
		}
	}
	if !writable {
		return nil, failures.New(failures.ClassAuthorizationDenied, "role_scoped_tool_forbidden", "tool-executor", "exec_save_entity_field.ownership", map[string]any{"action": "entity_write", "actor_id": strings.TrimSpace(actor.ID), "field": field.Path})
	}
	mutation, err := entityToolLiteralMutation(schema.Contract, field.Path, payload)
	if err != nil {
		return nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "exec_save_entity_field.operation", map[string]any{"field": fieldName}, err)
	}
	// State-dependent admission belongs to the fresh locked writer evaluation.
	result, err := writer.ApplyEntityFieldMutation(ctx, runtimepipeline.EntityFieldMutation{
		RunID:    identity.RunID,
		EntityID: identity.EntityID,
		Owner:    owner,
		FlowID:   schema.Contract.FlowID,
		Mutation: mutation,
		Source:   source,
		Writer: mutationlog.Writer{
			Type:        "agent",
			ID:          strings.TrimSpace(actor.ID),
			HandlerStep: "save_entity_field",
		},
	})
	if !result.Acknowledged {
		if errors.Is(err, entityruntime.ErrImmutableMutation) {
			return nil, failures.Wrap(failures.ClassAuthorizationDenied, "immutable_field_write_forbidden", "tool-executor", "exec_save_entity_field.field", map[string]any{"action": "entity_write", "field": fieldName}, err)
		}
		if failure, typed := failures.EnvelopeFromError(err); typed && (failure.Class == failures.ClassSchemaInvalid || failure.Class == failures.ClassAuthorizationDenied) {
			return nil, err
		}
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

func entityToolMutationOwner(source semanticview.Source, actor models.AgentConfig, runID, entityID, flowID string, row map[string]any) (runtimeflowidentity.RunScopedFlowInstance, error) {
	agent, err := actor.ConcreteIdentity()
	if err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, err
	}
	if agent.RunID != runID || strings.TrimSpace(actor.FlowID) != flowID || asString(row["entity_id"]) != entityID ||
		(actor.EffectiveEntityID() != "" && actor.EffectiveEntityID() != entityID) {
		return runtimeflowidentity.RunScopedFlowInstance{}, fmt.Errorf("entity write requires its exact actor run, entity and declaring flow")
	}
	owner := runtimeflowidentity.RunScopedFlowInstance{RunID: agent.RunID}
	if agent.Route.Presence == agentidentity.RouteRoot {
		root, err := semanticview.AdmitRootExecutionCoordinate(source, runID)
		if err != nil || !root.Matches(flowID, runID) {
			return runtimeflowidentity.RunScopedFlowInstance{}, fmt.Errorf("root entity write requires its admitted execution coordinate")
		}
		owner.Route = runtimeflowidentity.Route{ScopeKey: root.FlowID(), InstanceID: root.RunID(), InstancePath: root.RunID()}
	} else {
		owner.Route = runtimeflowidentity.Route{ScopeKey: agent.Route.ScopeKey, InstanceID: agent.Route.InstanceID, InstancePath: agent.Route.InstancePath}
	}
	if err := owner.Validate(); err != nil {
		return runtimeflowidentity.RunScopedFlowInstance{}, err
	}
	if owner.Route.InstancePath != asString(row["flow_instance"]) {
		return runtimeflowidentity.RunScopedFlowInstance{}, fmt.Errorf("entity write route disagrees with stored entity owner")
	}
	return owner, nil
}

func entityToolLiteralMutation(contract entityruntime.Contract, field string, payload map[string]any) (entityruntime.Mutation, error) {
	value, hasValue := payload["value"]
	if !hasValue {
		return entityruntime.Mutation{}, fmt.Errorf("value is required")
	}
	op := ""
	if raw, present := payload["op"]; present {
		var ok bool
		op, ok = raw.(string)
		if !ok || (op != entityruntime.ContainedOperationSet && op != entityruntime.ContainedOperationAppend && op != entityruntime.ContainedOperationUpdate) {
			return entityruntime.Mutation{}, fmt.Errorf("op must be set, append, or update")
		}
	}
	key, hasKey := payload["key"]
	index, hasIndex := payload["index"]
	mutation := entityruntime.Mutation{Operation: op, Target: "entity." + field, Value: value, Key: key, HasKey: hasKey, Index: index, HasIndex: hasIndex}
	if op == "" || (op == entityruntime.ContainedOperationSet && !hasKey) {
		if hasKey || hasIndex {
			return entityruntime.Mutation{}, fmt.Errorf("whole-value set must not declare key or index")
		}
		// The shared catalog spells whole/scalar assignment as an empty operation.
		mutation.Operation = ""
		if _, err := entityruntime.NormalizeFieldValue(contract, field, value); err != nil {
			return entityruntime.Mutation{}, err
		}
		return mutation, nil
	}
	if hasKey && op != entityruntime.ContainedOperationSet {
		return entityruntime.Mutation{}, fmt.Errorf("only map-key set accepts key")
	}
	if hasIndex {
		switch index.(type) {
		case int, int64, float64:
		default:
			return entityruntime.Mutation{}, fmt.Errorf("index must be a non-negative integer")
		}
		if _, err := entityruntime.NormalizeContainedOperationIndex(index); err != nil {
			return entityruntime.Mutation{}, err
		}
	}
	target, err := entityruntime.ResolveContainedOperationTarget(contract, mutation.Target, op, hasKey, hasIndex)
	if err != nil {
		return entityruntime.Mutation{}, err
	}
	if hasKey {
		if _, err := entityruntime.NormalizeContainedOperationKey(contract, target.MapKeyType, key); err != nil {
			return entityruntime.Mutation{}, err
		}
	}
	if _, err := entityruntime.NormalizeContainedOperationValue(contract, target, op, value); err != nil {
		return entityruntime.Mutation{}, err
	}
	return mutation, nil
}

func enforceEntityWriteOwnership(ctx context.Context, source semanticview.Source, actor models.AgentConfig, runID, entityID, flowID string, row map[string]any, logger runtimeToolLogSink) (runtimeflowidentity.RunScopedFlowInstance, error) {
	owner, err := entityToolMutationOwner(source, actor, runID, entityID, flowID, row)
	if err == nil {
		return owner, nil
	}
	targetFlow := asString(row["flow_instance"])
	denial := failures.Wrap(
		failures.ClassAuthorizationDenied,
		"cross_flow_write_forbidden",
		"tool-executor",
		"entity_write_ownership.enforce",
		map[string]any{
			"action":          "entity_write",
			"actor_id":        strings.TrimSpace(actor.ID),
			"entity_id":       entityID,
			"owner_flow_path": targetFlow,
		},
		err,
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
				"turn_flow":          actor.CanonicalFlowPath(),
				"entity_owner_flow":  targetFlow,
				"write_target_id":    strings.TrimSpace(entityID),
				"ownership_relation": "foreign",
			},
			Failure: failures.CloneEnvelope(&failure),
		})
	}
	return runtimeflowidentity.RunScopedFlowInstance{}, denial
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
