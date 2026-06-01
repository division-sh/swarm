package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

var entityStateTopLevelFields = map[string]struct{}{
	"entity_id":        {},
	"run_id":           {},
	"flow_instance":    {},
	"entity_type":      {},
	"name":             {},
	"current_state":    {},
	"gates":            {},
	"fields":           {},
	"accumulator":      {},
	"revision":         {},
	"entered_state_at": {},
	"created_at":       {},
	"updated_at":       {},
}

func (e *Executor) entityToolDependencies(input any) (EntityPersistence, semanticview.Source, map[string]any, error) {
	store, err := e.entityStoreDependency()
	if err != nil {
		return nil, nil, nil, NewRuntimeError("dependency_unavailable", "tool-executor", "entity_tool.store", true, "entity persistence store is not configured")
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, nil, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.decode", false, err, "decode entity tool input")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return store, source, payload, nil
}

func parseEntityID(raw any) (string, error) {
	entityID := strings.TrimSpace(asString(raw))
	if entityID == "" {
		return "", fmt.Errorf("entity_id is required")
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return "", fmt.Errorf("entity_id must be uuid")
	}
	return entityID, nil
}

func loadEntityState(ctx context.Context, store EntityPersistence, entityID string) (map[string]any, bool, error) {
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return nil, false, err
	}
	if store == nil {
		return nil, false, fmt.Errorf("entity persistence store is not configured")
	}
	return store.LoadEntityState(ctx, EntityIdentity{
		RunID:    identity.RunID,
		EntityID: identity.EntityID,
	})
}

func materializeEntityStateRow(source semanticview.Source, row map[string]any) (map[string]any, error) {
	contract, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return row, nil
	}
	fields := entityRowFieldMap(row)
	materialized, err := entityruntime.Materialize(contract, entityruntime.DeclaredValues(contract, fields))
	if err != nil {
		return nil, err
	}
	cloned := cloneEntityRows([]map[string]any{row})[0]
	cloned["fields"] = materialized
	if strings.TrimSpace(asString(cloned["entity_type"])) == "" {
		cloned["entity_type"] = contract.EntityType
	}
	return cloned, nil
}

func materializeEntityStateRows(source semanticview.Source, rows []map[string]any) ([]map[string]any, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		materialized, err := materializeEntityStateRow(source, row)
		if err != nil {
			return nil, err
		}
		out = append(out, materialized)
	}
	return out, nil
}

func mapKeys(values map[string]any) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

func orderedEntityFieldNamesFromInput(names []string) []string {
	out := append([]string{}, names...)
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	sort.Strings(out)
	deduped := out[:0]
	for _, name := range out {
		if name == "" {
			continue
		}
		if len(deduped) > 0 && deduped[len(deduped)-1] == name {
			continue
		}
		deduped = append(deduped, name)
	}
	return deduped
}

func numericEntityValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func decodeEntityJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func subjectStatusFlowID(source semanticview.Source, flowInstance string) string {
	return entityruntime.ResolveFlowIDForInstance(source, flowInstance)
}
