package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

var _ runtimetools.EntityPersistence = (*PostgresStore)(nil)
var _ runtimetools.EntityPersistence = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) LoadEntityState(ctx context.Context, identity runtimetools.EntityIdentity) (map[string]any, bool, error) {
	if s == nil || s.DB == nil {
		return nil, false, fmt.Errorf("postgres entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(identity)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.DB.QueryContext(ctx, toolEntitySelectSQL(`run_id = $1::uuid AND entity_id = $2::uuid`), runID, entityID)
	if err != nil {
		return nil, false, fmt.Errorf("load postgres entity state: %w", err)
	}
	defer rows.Close()
	items, err := scanToolEntityRows(rows)
	if err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		return nil, false, nil
	}
	return items[0], true, nil
}

func (s *SQLiteRuntimeStore) LoadEntityState(ctx context.Context, identity runtimetools.EntityIdentity) (map[string]any, bool, error) {
	if s == nil || s.DB == nil {
		return nil, false, fmt.Errorf("sqlite entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(identity)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.DB.QueryContext(ctx, toolEntitySelectSQL(`run_id = ? AND entity_id = ?`), runID, entityID)
	if err != nil {
		return nil, false, fmt.Errorf("load sqlite entity state: %w", err)
	}
	defer rows.Close()
	items, err := scanToolEntityRows(rows)
	if err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		return nil, false, nil
	}
	return items[0], true, nil
}

func (s *PostgresStore) QueryEntityStates(ctx context.Context, query runtimetools.EntityStateQuery) ([]map[string]any, error) {
	if s == nil || s.DB == nil {
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
	rows, err := s.DB.QueryContext(ctx, toolEntitySelectSQL(where)+order, args...)
	if err != nil {
		return nil, fmt.Errorf("query postgres entity state: %w", err)
	}
	defer rows.Close()
	return scanToolEntityRows(rows)
}

func (s *SQLiteRuntimeStore) QueryEntityStates(ctx context.Context, query runtimetools.EntityStateQuery) ([]map[string]any, error) {
	if s == nil || s.DB == nil {
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
	rows, err := s.DB.QueryContext(ctx, toolEntitySelectSQL(where)+order, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite entity state: %w", err)
	}
	defer rows.Close()
	return scanToolEntityRows(rows)
}

func (s *PostgresStore) SaveEntityField(ctx context.Context, update runtimetools.EntityFieldUpdate) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres entity persistence store is required")
	}
	runID, entityID, segments, valueJSON, err := normalizeToolEntityFieldUpdate(update)
	if err != nil {
		return 0, err
	}
	var revision int
	err = s.runAuthorActivityMutation(ctx, "postgres entity field update", func(txctx context.Context, tx *sql.Tx) error {
		pathArray := pq.Array(segments)
		var oldValue []byte
		if err := tx.QueryRowContext(txctx, `
		SELECT COALESCE(COALESCE(fields, '{}'::jsonb) #> $3::text[], 'null'::jsonb)
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		FOR UPDATE
	`, runID, entityID, pathArray).Scan(&oldValue); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("entity not found: %s", entityID)
			}
			return fmt.Errorf("load postgres entity field: %w", err)
		}
		if err := tx.QueryRowContext(txctx, `
		UPDATE entity_state
		SET
			fields = jsonb_set(COALESCE(fields, '{}'::jsonb), $2::text[], $3::jsonb, true),
			revision = revision + 1,
			updated_at = now()
		WHERE entity_id = $1::uuid
		  AND run_id = $4::uuid
		RETURNING revision
	`, entityID, pathArray, string(valueJSON), runID).Scan(&revision); err != nil {
			return fmt.Errorf("update postgres entity field: %w", err)
		}
		if err := runtimemutationlog.InsertEntityStateDiff(txctx, tx, entityID, runtimemutationlog.EntityStateProjection{
			Fields: map[string]any{update.FieldPath: toolNullableJSONBytes(oldValue)},
		}, runtimemutationlog.EntityStateProjection{
			Fields: map[string]any{update.FieldPath: json.RawMessage(valueJSON)},
		}, mutationWriter(update.Writer)); err != nil {
			return fmt.Errorf("record postgres entity mutation: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return revision, nil
}

func (s *SQLiteRuntimeStore) SaveEntityField(ctx context.Context, update runtimetools.EntityFieldUpdate) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("sqlite entity persistence store is required")
	}
	runID, entityID, segments, valueJSON, err := normalizeToolEntityFieldUpdate(update)
	if err != nil {
		return 0, err
	}
	var revision int
	if err := s.runAuthorActivityMutation(ctx, "sqlite entity field update", func(txctx context.Context, tx *sql.Tx) error {
		var fieldsRaw any
		if err := tx.QueryRowContext(txctx, `
			SELECT COALESCE(fields, '{}')
			FROM entity_state
			WHERE run_id = ? AND entity_id = ?
		`, runID, entityID).Scan(&fieldsRaw); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("entity not found: %s", entityID)
			}
			return fmt.Errorf("load sqlite entity fields: %w", err)
		}
		fields, err := toolDecodeJSONMap(fieldsRaw)
		if err != nil {
			return fmt.Errorf("decode sqlite entity fields: %w", err)
		}
		oldValue, _ := toolPathValue(fields, segments)
		newValue, err := toolDecodeJSONValue(valueJSON)
		if err != nil {
			return fmt.Errorf("decode sqlite entity field value: %w", err)
		}
		toolSetPath(fields, segments, newValue)
		fieldsJSON, err := json.Marshal(fields)
		if err != nil {
			return fmt.Errorf("marshal sqlite entity fields: %w", err)
		}
		now := s.now()
		if _, err := tx.ExecContext(txctx, `
			UPDATE entity_state
			SET fields = ?, revision = revision + 1, updated_at = ?
			WHERE run_id = ? AND entity_id = ?
		`, string(fieldsJSON), now, runID, entityID); err != nil {
			return fmt.Errorf("update sqlite entity field: %w", err)
		}
		if err := tx.QueryRowContext(txctx, `
			SELECT revision
			FROM entity_state
			WHERE run_id = ? AND entity_id = ?
		`, runID, entityID).Scan(&revision); err != nil {
			return fmt.Errorf("load sqlite entity revision: %w", err)
		}
		if err := insertSQLiteEntityStateDiff(txctx, tx, runID, entityID, runtimemutationlog.EntityStateProjection{
			Fields: map[string]any{update.FieldPath: oldValue},
		}, runtimemutationlog.EntityStateProjection{
			Fields: map[string]any{update.FieldPath: newValue},
		}, mutationWriter(update.Writer), now); err != nil {
			return fmt.Errorf("record sqlite entity mutation: %w", err)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return revision, nil
}

func (s *PostgresStore) CreateEntity(ctx context.Context, rec runtimetools.EntityCreateRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres entity persistence store is required")
	}
	rec, fields, err := normalizeToolEntityCreateRecord(rec)
	if err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "postgres entity create", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''),
			$6, '{}'::jsonb, $7::jsonb, '{}'::jsonb, 1,
			$8, $8, $8
		)
	`, rec.RunID, rec.EntityID, rec.FlowInstance, rec.EntityType, rec.Name, rec.CurrentState, string(rec.FieldsJSON), rec.CreatedAt); err != nil {
			return fmt.Errorf("insert postgres entity: %w", err)
		}
		if err := runtimemutationlog.InsertEntityStateDiff(txctx, tx, rec.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
			CurrentState: rec.CurrentState,
			Fields:       fields,
		}, mutationWriter(rec.Writer)); err != nil {
			return fmt.Errorf("record postgres entity create mutation: %w", err)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) CreateEntity(ctx context.Context, rec runtimetools.EntityCreateRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite entity persistence store is required")
	}
	rec, fields, err := normalizeToolEntityCreateRecord(rec)
	if err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "sqlite entity create", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, '{}', ?, '{}', 1, ?, ?, ?)
		`, rec.RunID, rec.EntityID, rec.FlowInstance, rec.EntityType, sqliteNullString(rec.Name),
			rec.CurrentState, string(rec.FieldsJSON), rec.CreatedAt, rec.CreatedAt, rec.CreatedAt); err != nil {
			return fmt.Errorf("insert sqlite entity: %w", err)
		}
		if err := insertSQLiteEntityStateDiff(txctx, tx, rec.RunID, rec.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
			CurrentState: rec.CurrentState,
			Fields:       fields,
		}, mutationWriter(rec.Writer), rec.CreatedAt); err != nil {
			return fmt.Errorf("record sqlite entity create mutation: %w", err)
		}
		return nil
	})
}

func toolEntitySelectSQL(where string) string {
	return `
		SELECT entity_id, run_id, COALESCE(flow_instance, ''), COALESCE(entity_type, ''), name, current_state,
		       COALESCE(gates, '{}'), COALESCE(fields, '{}'), COALESCE(accumulator, '{}'),
		       revision, entered_state_at, created_at, updated_at
		FROM entity_state
		WHERE ` + where
}

func scanToolEntityRows(rows *sql.Rows) ([]map[string]any, error) {
	out := make([]map[string]any, 0)
	for rows.Next() {
		var entityID, runID, flowInstance, entityType, currentState string
		var name sql.NullString
		var gatesRaw, fieldsRaw, accumulatorRaw any
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
			&accumulatorRaw,
			&revision,
			&enteredStateAtRaw,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scan entity state: %w", err)
		}
		gates, err := toolDecodeJSONMap(gatesRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity gates: %w", err)
		}
		fields, err := toolDecodeJSONMap(fieldsRaw)
		if err != nil {
			return nil, fmt.Errorf("decode entity fields: %w", err)
		}
		accumulator, err := toolDecodeJSONMap(accumulatorRaw)
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

func normalizeToolEntityFieldUpdate(update runtimetools.EntityFieldUpdate) (string, string, []string, json.RawMessage, error) {
	runID, entityID, err := normalizeToolEntityIdentity(runtimetools.EntityIdentity{RunID: update.RunID, EntityID: update.EntityID})
	if err != nil {
		return "", "", nil, nil, err
	}
	segments := append([]string(nil), update.PathSegments...)
	if len(segments) == 0 {
		segments = strings.Split(strings.TrimSpace(update.FieldPath), ".")
	}
	clean := segments[:0]
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return "", "", nil, nil, fmt.Errorf("field path segment is required")
		}
		clean = append(clean, segment)
	}
	valueJSON := json.RawMessage(strings.TrimSpace(string(update.ValueJSON)))
	if len(valueJSON) == 0 {
		valueJSON = json.RawMessage("null")
	}
	return runID, entityID, clean, valueJSON, nil
}

func normalizeToolEntityCreateRecord(rec runtimetools.EntityCreateRecord) (runtimetools.EntityCreateRecord, map[string]any, error) {
	runID, entityID, err := normalizeToolEntityIdentity(runtimetools.EntityIdentity{RunID: rec.RunID, EntityID: rec.EntityID})
	if err != nil {
		return runtimetools.EntityCreateRecord{}, nil, err
	}
	rec.RunID = runID
	rec.EntityID = entityID
	rec.FlowInstance = strings.Trim(strings.TrimSpace(rec.FlowInstance), "/")
	rec.EntityType = strings.TrimSpace(rec.EntityType)
	rec.Name = strings.TrimSpace(rec.Name)
	rec.CurrentState = strings.TrimSpace(rec.CurrentState)
	if rec.FlowInstance == "" || rec.EntityType == "" || rec.CurrentState == "" {
		return runtimetools.EntityCreateRecord{}, nil, fmt.Errorf("flow_instance, entity_type, and current_state are required")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	} else {
		rec.CreatedAt = rec.CreatedAt.UTC()
	}
	if len(rec.FieldsJSON) == 0 {
		rec.FieldsJSON = json.RawMessage("{}")
	}
	fields, err := toolDecodeJSONMap(rec.FieldsJSON)
	if err != nil {
		return runtimetools.EntityCreateRecord{}, nil, fmt.Errorf("decode entity fields: %w", err)
	}
	normalized, err := json.Marshal(fields)
	if err != nil {
		return runtimetools.EntityCreateRecord{}, nil, fmt.Errorf("marshal entity fields: %w", err)
	}
	rec.FieldsJSON = json.RawMessage(normalized)
	return rec, fields, nil
}

func mutationWriter(writer runtimetools.EntityMutationWriter) runtimemutationlog.Writer {
	return runtimemutationlog.Writer{
		Type:        strings.TrimSpace(writer.Type),
		ID:          strings.TrimSpace(writer.ID),
		HandlerStep: strings.TrimSpace(writer.HandlerStep),
	}
}

func toolDecodeJSONMap(raw any) (map[string]any, error) {
	data := jsonRawMessageValue(raw)
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	if strings.TrimSpace(string(data)) == "" || strings.TrimSpace(string(data)) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func toolDecodeJSONValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
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

func toolNullableJSONBytes(raw []byte) any {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	return json.RawMessage(append([]byte(nil), raw...))
}

func toolPathValue(fields map[string]any, segments []string) (any, bool) {
	var current any = fields
	for _, segment := range segments {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = next[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func toolSetPath(fields map[string]any, segments []string, value any) {
	if len(segments) == 0 {
		return
	}
	current := fields
	for _, segment := range segments[:len(segments)-1] {
		next, _ := current[segment].(map[string]any)
		if next == nil {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[segments[len(segments)-1]] = value
}

func insertSQLiteEntityStateDiff(ctx context.Context, tx *sql.Tx, runID string, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, createdAt time.Time) error {
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	causedByEvent := ""
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		causedByEvent = nullUUIDString(inbound.ID())
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	for _, rec := range records {
		if strings.TrimSpace(rec.Field) == "current_state" {
			if err := runtimeauthoractivity.Require(ctx); err != nil {
				return err
			}
		}
		oldValue, err := toolJSONSQLArg(rec.OldValue)
		if err != nil {
			return err
		}
		newValue, err := toolJSONSQLArg(rec.NewValue)
		if err != nil {
			return err
		}
		mutationID := uuid.NewString()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_mutations (
				mutation_id, run_id, entity_id, field, old_value, new_value,
				caused_by_event, writer_type, writer_id, handler_step, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, mutationID, runID, rec.EntityID, rec.Field, oldValue, newValue,
			sqliteNullUUID(causedByEvent), rec.WriterType, rec.WriterID, sqliteNullString(rec.HandlerStep), createdAt.UTC()); err != nil {
			return fmt.Errorf("insert sqlite entity mutation: %w", err)
		}
		draft, admitted, err := runtimemutationlog.AuthorActivityDraft(runID, mutationID, rec, createdAt)
		if err != nil {
			return err
		}
		if admitted {
			if err := runtimeauthoractivity.Record(ctx, draft); err != nil {
				return err
			}
		}
	}
	return nil
}

func toolJSONSQLArg(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case json.RawMessage:
		if len(typed) == 0 {
			return nil, nil
		}
		return string(typed), nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}
}

func toolIntValue(raw any) int {
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		n, _ := v.Int64()
		return int(n)
	case string:
		var n int
		_, _ = fmt.Sscanf(strings.TrimSpace(v), "%d", &n)
		return n
	default:
		return 0
	}
}
