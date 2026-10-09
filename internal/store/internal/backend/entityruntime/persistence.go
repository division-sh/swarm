package entitystore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
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
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(`es.run_id = $1::uuid AND es.entity_id = $2::uuid`), runID, entityID)
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
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(`es.run_id = ? AND es.entity_id = ?`), runID, entityID)
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
		order = " ORDER BY es.created_at DESC"
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
		order = " ORDER BY es.created_at DESC"
	}
	rows, err := s.backend.QueryContext(ctx, toolEntitySelectSQL(where)+order, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite entity state: %w", err)
	}
	defer rows.Close()
	return ScanToolEntityRows(rows)
}

func toolEntitySelectSQL(where string) string {
	return `SELECT ` + workflowheader.DataReadColumns + ` ` + workflowheader.DataReadFrom + ` WHERE ` + where
}

func ScanToolEntityRows(rows *sql.Rows) ([]map[string]any, error) {
	out := make([]map[string]any, 0)
	seen := map[string]bool{}
	for rows.Next() {
		full, err := workflowheader.ScanData(rows, DecodeEntityTime)
		if err != nil {
			return nil, fmt.Errorf("scan entity state: %w", err)
		}
		item := full.Entity
		key := item.RunID + "/" + item.EntityID
		if seen[key] {
			return nil, fmt.Errorf("entity has conflicting constructed header owners")
		}
		seen[key] = true
		row := map[string]any{
			"entity_id":            item.EntityID,
			"run_id":               item.RunID,
			"flow_instance":        item.FlowInstance,
			"entity_type":          item.EntityType,
			"name":                 nil,
			"stage_defined":        nil,
			"current_state":        nil,
			"gates":                nil,
			"fields":               full.Fields,
			"bookkeeping":          nil,
			"accumulator":          nil,
			"loops":                nil,
			"revision":             nil,
			"entered_state_at":     nil,
			"lifecycle_created_at": nil,
			"lifecycle_updated_at": nil,
			"created_at":           item.CreatedAt.Format(time.RFC3339Nano),
			"updated_at":           item.UpdatedAt.Format(time.RFC3339Nano),
		}
		if item.StageDefined != nil {
			row["stage_defined"], row["revision"] = *item.StageDefined, *item.Revision
			row["gates"], row["bookkeeping"], row["accumulator"], row["loops"] = full.Gates, full.Bookkeeping, full.Accumulated, full.Loops
			row["lifecycle_created_at"], row["lifecycle_updated_at"] = item.LifecycleCreatedAt.Format(time.RFC3339Nano), item.LifecycleUpdatedAt.Format(time.RFC3339Nano)
			if item.CurrentState != nil {
				row["current_state"], row["entered_state_at"] = *item.CurrentState, item.EnteredStateAt.Format(time.RFC3339Nano)
			}
		}
		if item.Name != "" {
			row["name"] = item.Name
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
	clauses := []string{"es.run_id = $1::uuid"}
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
			clauses = append(clauses, fmt.Sprintf("(es.flow_instance = %s OR es.flow_instance LIKE %s)", eq, like))
			return
		}
		clauses = append(clauses, "es.flow_instance = "+addArg(root))
	}
	appendFlowScope(query.FlowScope)
	appendFlowScope(query.RequestedFlowScope)
	if exact := strings.Trim(strings.TrimSpace(query.RequestedFlowExact), "/"); exact != "" {
		clauses = append(clauses, "es.flow_instance = "+addArg(exact))
	}
	if state := strings.TrimSpace(query.CurrentState); state != "" {
		clauses = append(clauses, "(fi.stage_defined AND fi.current_state = "+addArg(state)+")")
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
	clauses := []string{"es.run_id = ?"}
	args := []any{runID}
	appendFlowScope := func(scope runtimetools.EntityFlowScope) {
		root := strings.Trim(strings.TrimSpace(scope.Root), "/")
		if root == "" {
			return
		}
		if scope.IncludeDescendants {
			clauses = append(clauses, "(es.flow_instance = ? OR es.flow_instance LIKE ?)")
			args = append(args, root, root+"/%")
			return
		}
		clauses = append(clauses, "es.flow_instance = ?")
		args = append(args, root)
	}
	appendFlowScope(query.FlowScope)
	appendFlowScope(query.RequestedFlowScope)
	if exact := strings.Trim(strings.TrimSpace(query.RequestedFlowExact), "/"); exact != "" {
		clauses = append(clauses, "es.flow_instance = ?")
		args = append(args, exact)
	}
	if state := strings.TrimSpace(query.CurrentState); state != "" {
		clauses = append(clauses, "(fi.stage_defined AND fi.current_state = ?)")
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
		return `es.fields`
	}
	segments := strings.Split(path, ".")
	if len(segments) == 1 {
		return fmt.Sprintf("es.fields -> %s", postgresStringLiteral(segments[0]))
	}
	sqlPath := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment != "" {
			sqlPath = append(sqlPath, postgresStringLiteral(segment))
		}
	}
	return fmt.Sprintf("es.fields #> ARRAY[%s]", strings.Join(sqlPath, ", "))
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
		return "json(json_extract(es.fields, ?)) = json(?)", []any{jsonPath, string(valueJSON)}, nil
	}
	return "json_extract(es.fields, ?) = ?", []any{jsonPath, sqliteToolJSONCompareValue(value)}, nil
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

func DecodeEntityTime(raw any) (time.Time, bool, error) {
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
