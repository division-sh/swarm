package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
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
		return nil, nil, nil, failures.NewDetail("dependency_unavailable", "tool-executor", "entity_tool.store", map[string]any{"dependency": "entity_persistence"})
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, nil, nil, failures.WrapDetail("invalid_tool_input", "tool-executor", "entity_tool.decode", nil, err)
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
	projected := projectAgentEntityStateRow(row)
	contract, ok := entityruntime.ResolveForEntityRow(source, projected)
	if !ok {
		return projected, nil
	}
	fields := entityRowFieldMap(projected)
	materialized, err := entityruntime.Materialize(contract, entityruntime.DeclaredValues(contract, fields))
	if err != nil {
		return nil, err
	}
	projected["fields"] = materialized
	if strings.TrimSpace(asString(projected["entity_type"])) == "" {
		projected["entity_type"] = contract.EntityType
	}
	return projected, nil
}

func projectAgentEntityStateRow(row map[string]any) map[string]any {
	projected := make(map[string]any, len(entityStateTopLevelFields))
	for field := range entityStateTopLevelFields {
		if value, ok := row[field]; ok {
			projected[field] = deepCloneJSONValue(value)
		}
	}
	return projected
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
