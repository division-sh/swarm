package tools

import (
	"context"
	"sort"
	"strings"
	"time"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

func (e *Executor) execGetEntity(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
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

func (e *Executor) execGetSubjectStatus(ctx context.Context, _ models.AgentConfig, input any) (any, error) {
	db, _, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	subjectID, err := parseEntityID(payload["subject_id"])
	if err != nil {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "exec_get_subject_status.subject_id", false, err.Error())
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	rows, err := queryEntityStateRows(ctx, db, " WHERE subject_id = $1::uuid", subjectID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "exec_get_subject_status.query", true, err, "query subject status")
	}
	subjectStatusSortRows(source, rows)
	entities := make([]map[string]any, 0, len(rows))
	allTerminal := true
	latestFlow := ""
	latestState := ""
	for idx, row := range rows {
		flowInstance := strings.Trim(strings.TrimSpace(asString(row["flow_instance"])), "/")
		flowID := subjectStatusFlowID(source, flowInstance)
		state := strings.TrimSpace(asString(row["current_state"]))
		terminal := subjectStatusTerminal(source, flowID, state)
		if !terminal {
			allTerminal = false
		}
		if idx == 0 {
			latestFlow = flowID
			latestState = state
		}
		entities = append(entities, map[string]any{
			"entity_id": strings.TrimSpace(asString(row["entity_id"])),
			"flow":      flowID,
			"state":     state,
			"terminal":  terminal,
		})
	}
	return map[string]any{
		"subject_id":   subjectID,
		"entities":     entities,
		"all_terminal": allTerminal,
		"latest_flow":  latestFlow,
		"latest_state": latestState,
	}, nil
}

func subjectStatusTerminal(source semanticview.Source, flowID, state string) bool {
	state = strings.TrimSpace(state)
	if source == nil || flowID == "" || state == "" {
		return false
	}
	for _, terminal := range source.FlowTerminalStages(flowID) {
		if strings.EqualFold(strings.TrimSpace(terminal), state) {
			return true
		}
	}
	return false
}

func subjectStatusSortRows(source semanticview.Source, rows []map[string]any) {
	flowDepth := subjectStatusFlowDepths(source)
	sort.SliceStable(rows, func(i, j int) bool {
		left := subjectStatusSortKey(source, flowDepth, rows[i])
		right := subjectStatusSortKey(source, flowDepth, rows[j])
		if !left.entered.Equal(right.entered) {
			return left.entered.After(right.entered)
		}
		if left.depth != right.depth {
			return left.depth > right.depth
		}
		if !left.updated.Equal(right.updated) {
			return left.updated.After(right.updated)
		}
		if !left.created.Equal(right.created) {
			return left.created.After(right.created)
		}
		return left.entityID > right.entityID
	})
}

type subjectStatusKey struct {
	entered  time.Time
	updated  time.Time
	created  time.Time
	depth    int
	entityID string
}

func subjectStatusSortKey(source semanticview.Source, flowDepth map[string]int, row map[string]any) subjectStatusKey {
	flowInstance := strings.Trim(strings.TrimSpace(asString(row["flow_instance"])), "/")
	flowID := subjectStatusFlowID(source, flowInstance)
	state := strings.TrimSpace(asString(row["current_state"]))
	entered := subjectStatusTime(row["entered_state_at"])
	if subjectStatusTerminal(source, flowID, state) {
		entered = time.Time{}
	}
	return subjectStatusKey{
		entered:  entered,
		updated:  subjectStatusTime(row["updated_at"]),
		created:  subjectStatusTime(row["created_at"]),
		depth:    flowDepth[flowID],
		entityID: strings.TrimSpace(asString(row["entity_id"])),
	}
}

func subjectStatusTime(value any) time.Time {
	raw := strings.TrimSpace(asString(value))
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func subjectStatusFlowDepths(source semanticview.Source) map[string]int {
	if source == nil {
		return nil
	}
	scopes := source.FlowScopes()
	out := make(map[string]int, len(scopes))
	for idx, scope := range scopes {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		out[flowID] = idx
	}
	return out
}
