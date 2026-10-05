package entitystore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

var _ runtimetools.EntityPersistence = (*EntityPostgresOwner)(nil)
var _ runtimetools.EntityPersistence = (*EntitySQLiteOwner)(nil)

func (s *EntityPostgresOwner) LoadEntityState(ctx context.Context, identity runtimetools.EntityIdentity) (map[string]any, bool, error) {
	if s == nil || s.backend == nil {
		return nil, false, fmt.Errorf("postgres entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(identity)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(`run_id = $1::uuid AND entity_id = $2::uuid`), runID, entityID)
	if err != nil {
		return nil, false, fmt.Errorf("load postgres entity state: %w", err)
	}
	defer rows.Close()
	items, err := ScanToolEntityRows(rows)
	if err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		return nil, false, nil
	}
	return items[0], true, nil
}

func (s *EntitySQLiteOwner) LoadEntityState(ctx context.Context, identity runtimetools.EntityIdentity) (map[string]any, bool, error) {
	if s == nil || s.backend == nil {
		return nil, false, fmt.Errorf("sqlite entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(identity)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(`run_id = ? AND entity_id = ?`), runID, entityID)
	if err != nil {
		return nil, false, fmt.Errorf("load sqlite entity state: %w", err)
	}
	defer rows.Close()
	items, err := ScanToolEntityRows(rows)
	if err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		return nil, false, nil
	}
	return items[0], true, nil
}

func (s *EntityPostgresOwner) QueryEntityStates(ctx context.Context, query runtimetools.EntityStateQuery) ([]map[string]any, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres entity persistence store is required")
	}
	where, args, err := postgresToolEntityWhere(query)
	if err != nil {
		return nil, err
	}
	order := ""
	if query.OrderByCreatedDesc {
		order = " ORDER BY created_at DESC"
	}
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(where)+order, args...)
	if err != nil {
		return nil, fmt.Errorf("query postgres entity state: %w", err)
	}
	defer rows.Close()
	return ScanToolEntityRows(rows)
}

func (s *EntitySQLiteOwner) QueryEntityStates(ctx context.Context, query runtimetools.EntityStateQuery) ([]map[string]any, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite entity persistence store is required")
	}
	where, args, err := sqliteToolEntityWhere(query)
	if err != nil {
		return nil, err
	}
	order := ""
	if query.OrderByCreatedDesc {
		order = " ORDER BY created_at DESC"
	}
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(where)+order, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite entity state: %w", err)
	}
	defer rows.Close()
	return ScanToolEntityRows(rows)
}

func toolEntitySelectSQL(where string) string {
	return `
		SELECT entity_id, run_id, COALESCE(flow_instance, ''), COALESCE(entity_type, ''), name, current_state,
		       COALESCE(gates, '{}'), COALESCE(fields, '{}'), COALESCE(bookkeeping, '{}'), COALESCE(accumulator, '{}'),
		       revision, entered_state_at, created_at, updated_at
		FROM entity_state
		WHERE ` + where
}

func ScanToolEntityRows(rows *sql.Rows) ([]map[string]any, error) {
	out := make([]map[string]any, 0)
	for rows.Next() {
		var entityID, runID, flowInstance, entityType, currentState string
		var name sql.NullString
		var gatesRaw, fieldsRaw, bookkeepingRaw, accumulatorRaw any
		var revision int
		var enteredStateAtRaw, createdAtRaw, updatedAtRaw any
		if err := rows.Scan(
			&entityID,
			&runID,
			&flowInstance,
			&entityType,
			&name,
			&currentState,
			&gatesRaw,
			&fieldsRaw,
			&bookkeepingRaw,
			&accumulatorRaw,
			&revision,
			&enteredStateAtRaw,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scan entity state: %w", err)
		}
		gates, err := DecodeJSONMap(gatesRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity gates: %w", err)
		}
		fields, err := DecodeJSONMap(fieldsRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity fields: %w", err)
		}
		bookkeeping, err := DecodeJSONMap(bookkeepingRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity bookkeeping: %w", err)
		}
		accumulator, err := DecodeJSONMap(accumulatorRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity accumulator: %w", err)
		}
		loops, err := loopruntime.PublicActivations(accumulator)
		if err != nil {
			return nil, fmt.Errorf("decode entity loop state: %w", err)
		}
		accumulator = loopruntime.PublicStateBuckets(accumulator)
		enteredStateAt, err := toolTimeString(enteredStateAtRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity entered_state_at: %w", err)
		}
		createdAt, err := toolTimeString(createdAtRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity created_at: %w", err)
		}
		updatedAt, err := toolTimeString(updatedAtRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity updated_at: %w", err)
		}
		row := map[string]any{
			"entity_id":        strings.TrimSpace(entityID),
			"run_id":           strings.TrimSpace(runID),
			"flow_instance":    strings.TrimSpace(flowInstance),
			"entity_type":      strings.TrimSpace(entityType),
			"name":             nil,
			"current_state":    strings.TrimSpace(currentState),
			"gates":            gates,
			"fields":           fields,
			"bookkeeping":      bookkeeping,
			"accumulator":      accumulator,
			"loops":            loops,
			"revision":         revision,
			"entered_state_at": enteredStateAt,
			"created_at":       createdAt,
			"updated_at":       updatedAt,
		}
		if name.Valid {
			row["name"] = strings.TrimSpace(name.String)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entity states: %w", err)
	}
	return out, nil
}

func postgresToolEntityWhere(query runtimetools.EntityStateQuery) (string, []any, error) {
	runID := strings.TrimSpace(query.RunID)
	if _, err := uuid.Parse(runID); err != nil {
		return "", nil, fmt.Errorf("run_id must be uuid")
	}
	clauses := []string{"run_id = $1::uuid"}
	args := []any{runID}
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	appendFlowScope := func(scope runtimetools.EntityFlowScope) {
		root := strings.Trim(strings.TrimSpace(scope.Root), "/")
		if root == "" {
			return
		}
		if scope.IncludeDescendants {
			eq := addArg(root)
			like := addArg(root + "/%")
			clauses = append(clauses, fmt.Sprintf("(flow_instance = %s OR flow_instance LIKE %s)", eq, like))
			return
		}
		clauses = append(clauses, "flow_instance = "+addArg(root))
	}
	appendFlowScope(query.FlowScope)
	appendFlowScope(query.RequestedFlowScope)
	if exact := strings.Trim(strings.TrimSpace(query.RequestedFlowExact), "/"); exact != "" {
		clauses = append(clauses, "flow_instance = "+addArg(exact))
	}
	if state := strings.TrimSpace(query.CurrentState); state != "" {
		clauses = append(clauses, "current_state = "+addArg(state))
	}
	for _, filter := range query.FieldEquals {
		path := strings.TrimSpace(filter.Path)
		if path == "" {
			return "", nil, fmt.Errorf("entity field filter path is required")
		}
		valueJSON, err := json.Marshal(filter.Value)
		if err != nil {
			return "", nil, fmt.Errorf("marshal entity field filter %s: %w", path, err)
		}
		clauses = append(clauses, fmt.Sprintf("%s = %s::jsonb", postgresToolEntityFieldExpr(path), addArg(string(valueJSON))))
	}
	return strings.Join(clauses, " AND "), args, nil
}

func sqliteToolEntityWhere(query runtimetools.EntityStateQuery) (string, []any, error) {
	runID := strings.TrimSpace(query.RunID)
	if _, err := uuid.Parse(runID); err != nil {
		return "", nil, fmt.Errorf("run_id must be uuid")
	}
	clauses := []string{"run_id = ?"}
	args := []any{runID}
	appendFlowScope := func(scope runtimetools.EntityFlowScope) {
		root := strings.Trim(strings.TrimSpace(scope.Root), "/")
		if root == "" {
			return
		}
		if scope.IncludeDescendants {
			clauses = append(clauses, "(flow_instance = ? OR flow_instance LIKE ?)")
			args = append(args, root, root+"/%")
			return
		}
		clauses = append(clauses, "flow_instance = ?")
		args = append(args, root)
	}
	appendFlowScope(query.FlowScope)
	appendFlowScope(query.RequestedFlowScope)
	if exact := strings.Trim(strings.TrimSpace(query.RequestedFlowExact), "/"); exact != "" {
		clauses = append(clauses, "flow_instance = ?")
		args = append(args, exact)
	}
	if state := strings.TrimSpace(query.CurrentState); state != "" {
		clauses = append(clauses, "current_state = ?")
		args = append(args, state)
	}
	for _, filter := range query.FieldEquals {
		path := strings.TrimSpace(filter.Path)
		if path == "" {
			return "", nil, fmt.Errorf("entity field filter path is required")
		}
		clause, clauseArgs, err := sqliteToolEntityFieldEqualsClause(path, filter.Value)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, clause)
		args = append(args, clauseArgs...)
	}
	return strings.Join(clauses, " AND "), args, nil
}

func postgresToolEntityFieldExpr(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return `COALESCE(fields, '{}'::jsonb)`
	}
	segments := strings.Split(path, ".")
	if len(segments) == 1 {
		return fmt.Sprintf("COALESCE(fields, '{}'::jsonb) -> %s", postgresStringLiteral(segments[0]))
	}
	sqlPath := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment != "" {
			sqlPath = append(sqlPath, postgresStringLiteral(segment))
		}
	}
	return fmt.Sprintf("COALESCE(fields, '{}'::jsonb) #> ARRAY[%s]", strings.Join(sqlPath, ", "))
}

func postgresStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqliteToolJSONPath(path string) string {
	segments := strings.Split(strings.TrimSpace(path), ".")
	out := "$"
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		out += "." + segment
	}
	return out
}

func sqliteToolEntityFieldEqualsClause(path string, value any) (string, []any, error) {
	jsonPath := sqliteToolJSONPath(path)
	if sqliteToolStructuredJSONCompareValue(value) {
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return "", nil, fmt.Errorf("marshal sqlite entity field filter %s: %w", path, err)
		}
		return "json(json_extract(COALESCE(fields, '{}'), ?)) = json(?)", []any{jsonPath, string(valueJSON)}, nil
	}
	return "json_extract(COALESCE(fields, '{}'), ?) = ?", []any{jsonPath, sqliteToolJSONCompareValue(value)}, nil
}

func sqliteToolStructuredJSONCompareValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any, []any:
		return true
	case json.RawMessage:
		trimmed := strings.TrimSpace(string(typed))
		return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
	default:
		return false
	}
}

func sqliteToolJSONCompareValue(value any) any {
	switch v := value.(type) {
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return v
	}
}

func normalizeToolEntityIdentity(identity runtimetools.EntityIdentity) (string, string, error) {
	runID := strings.TrimSpace(identity.RunID)
	entityID := strings.TrimSpace(identity.EntityID)
	if _, err := uuid.Parse(runID); err != nil {
		return "", "", fmt.Errorf("run_id must be uuid")
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return "", "", fmt.Errorf("entity_id must be uuid")
	}
	return runID, entityID, nil
}

func DecodeJSONMap(raw any) (map[string]any, error) {
	data := jsonRawMessageValue(raw)
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	if strings.TrimSpace(string(data)) == "" || strings.TrimSpace(string(data)) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(data, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func toolTimeString(raw any) (string, error) {
	if at, ok, err := sqliteTimeValue(raw); err != nil {
		return "", err
	} else if ok {
		return at.Format(time.RFC3339Nano), nil
	}
	return "", nil
}

func jsonRawMessageValue(raw any) json.RawMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), value...)
	case []byte:
		return json.RawMessage(append([]byte(nil), value...))
	case string:
		return json.RawMessage([]byte(value))
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return encoded
	}
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if value.IsZero() {
			return time.Time{}, false, nil
		}
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTimeString(value)
	case []byte:
		return parseSQLiteTimeString(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTimeString(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}
