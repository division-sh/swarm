package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/semanticview"
)

var entityStateTopLevelFields = map[string]struct{}{
	"entity_id":        {},
	"subject_id":       {},
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

func (e *Executor) entityToolDependencies(input any) (*sql.DB, entityToolSchema, map[string]any, error) {
	db, err := e.sqlDBDependency()
	if err != nil {
		return nil, entityToolSchema{}, nil, NewRuntimeError("dependency_unavailable", "tool-executor", "entity_tool.db", true, "sql database is not configured")
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, entityToolSchema{}, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.decode", false, err, "decode entity tool input")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	schema, err := entityToolSchemaFromSource(source)
	if err != nil {
		return nil, entityToolSchema{}, nil, WrapRuntimeError("invalid_tool_input", "tool-executor", "entity_tool.schema", false, err, "resolve entity schema")
	}
	return db, schema, payload, nil
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

func loadEntityState(ctx context.Context, db *sql.DB, entityID string) (map[string]any, bool, error) {
	row := db.QueryRowContext(ctx, entityStateRowQuery(" WHERE entity_id = $1::uuid"), entityID)
	var raw []byte
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	entity, err := decodeEntityJSONMap(raw)
	if err != nil {
		return nil, false, err
	}
	return entity, true, nil
}

func entityStateRowQuery(suffix string) string {
	return `
		SELECT row_to_json(t)
		FROM (
			SELECT
				entity_id::text AS entity_id,
				subject_id::text AS subject_id,
				COALESCE(flow_instance, '') AS flow_instance,
				COALESCE(entity_type, '') AS entity_type,
				name,
				current_state,
				COALESCE(gates, '{}'::jsonb) AS gates,
				COALESCE(fields, '{}'::jsonb) AS fields,
				COALESCE(accumulator, '{}'::jsonb) AS accumulator,
				revision,
				entered_state_at,
				created_at,
				updated_at
			FROM entity_state` + suffix + `
		) AS t
	`
}

func queryEntityStateRows(ctx context.Context, db *sql.DB, suffix string, args ...any) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT entity_id::text, subject_id::text, COALESCE(flow_instance, ''), COALESCE(entity_type, ''), name, current_state,
		       COALESCE(gates, '{}'::jsonb), COALESCE(fields, '{}'::jsonb), COALESCE(accumulator, '{}'::jsonb),
		       revision, entered_state_at, created_at, updated_at
		FROM entity_state`+suffix, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var entityID, flowInstance, entityType, currentState string
		var subjectID sql.NullString
		var name sql.NullString
		var gatesRaw, fieldsRaw, accumulatorRaw []byte
		var revision int
		var enteredStateAt, createdAt, updatedAt time.Time
		if err := rows.Scan(
			&entityID,
			&subjectID,
			&flowInstance,
			&entityType,
			&name,
			&currentState,
			&gatesRaw,
			&fieldsRaw,
			&accumulatorRaw,
			&revision,
			&enteredStateAt,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		gates, err := decodeEntityJSONMap(gatesRaw)
		if err != nil {
			return nil, err
		}
		fields, err := decodeEntityJSONMap(fieldsRaw)
		if err != nil {
			return nil, err
		}
		accumulator, err := decodeEntityJSONMap(accumulatorRaw)
		if err != nil {
			return nil, err
		}
		row := map[string]any{
			"entity_id":        entityID,
			"subject_id":       nullStringValue(subjectID),
			"flow_instance":    flowInstance,
			"entity_type":      entityType,
			"name":             nullStringValue(name),
			"current_state":    currentState,
			"gates":            gates,
			"fields":           fields,
			"accumulator":      accumulator,
			"revision":         revision,
			"entered_state_at": enteredStateAt.Format(time.RFC3339Nano),
			"created_at":       createdAt.Format(time.RFC3339Nano),
			"updated_at":       updatedAt.Format(time.RFC3339Nano),
		}
		out = append(out, row)
	}
	return out, rows.Err()
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

func nullStringValue(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return strings.TrimSpace(value.String)
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
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" || source == nil {
		return flowInstance
	}
	if _, ok := source.FlowScopeByID(flowInstance); ok {
		return flowInstance
	}
	for flowID := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		if strings.Trim(source.FlowPath(flowID), "/") == flowInstance {
			return flowID
		}
	}
	return flowInstance
}
