package entitystore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	privatemutationprotocol "github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
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

func (s *EntityPostgresOwner) SaveEntityField(ctx context.Context, update runtimetools.EntityFieldUpdate) (runtimetools.EntityFieldWriteResult, error) {
	if s == nil || s.backend == nil {
		return runtimetools.EntityFieldWriteResult{}, fmt.Errorf("postgres entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(runtimetools.EntityIdentity{RunID: update.RunID, EntityID: update.EntityID})
	if err != nil {
		return runtimetools.EntityFieldWriteResult{}, err
	}
	if s.schemaGuard == nil {
		return runtimetools.EntityFieldWriteResult{}, fmt.Errorf("entity postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimetools.EntityFieldWriteResult{}, err
	}
	result := privatemutationprotocol.RunPostgres[int](ctx, s.backend, privatemutationprotocol.Story, privatemutationprotocol.Ordinary, nil, nil, func(txctx context.Context, mutation *privatemutationprotocol.Attempt) (int, error) {
		var revision int
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			sourceFact, err := storerunstate.RequirePostgresActiveSourceTx(txctx, tx, runID)
			if err != nil {
				return err
			}
			var fieldsRaw []byte
			var flowInstance, entityType string
			if err := tx.QueryRowContext(txctx, `
		SELECT fields, flow_instance, entity_type
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		FOR UPDATE
	`, runID, entityID).Scan(&fieldsRaw, &flowInstance, &entityType); err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("entity not found: %s", entityID)
				}
				return fmt.Errorf("load postgres entity field: %w", err)
			}
			before, after, fieldsJSON, err := applyToolEntityFieldUpdate(update, sourceFact, flowInstance, entityType, fieldsRaw)
			if err != nil {
				return err
			}
			if err := tx.QueryRowContext(txctx, `
		UPDATE entity_state
		SET
			fields = $2::jsonb,
			revision = revision + 1,
			updated_at = now()
		WHERE entity_id = $1::uuid
		  AND run_id = $3::uuid
		RETURNING revision
	`, entityID, string(fieldsJSON), runID).Scan(&revision); err != nil {
				return fmt.Errorf("update postgres entity field: %w", err)
			}
			if err := insertPostgresEntityStateDiff(txctx, mutation, tx, entityID, runtimemutationlog.EntityStateProjection{
				Fields: before,
			}, runtimemutationlog.EntityStateProjection{
				Fields: after,
			}, MutationWriter(update.Writer)); err != nil {
				return fmt.Errorf("record postgres entity mutation: %w", err)
			}
			return nil
		})
		return revision, err
	})
	return committedEntityFieldRevision(result)
}

func (s *EntitySQLiteOwner) SaveEntityField(ctx context.Context, update runtimetools.EntityFieldUpdate) (runtimetools.EntityFieldWriteResult, error) {
	if s == nil || s.backend == nil {
		return runtimetools.EntityFieldWriteResult{}, fmt.Errorf("sqlite entity persistence store is required")
	}
	runID, entityID, err := normalizeToolEntityIdentity(runtimetools.EntityIdentity{RunID: update.RunID, EntityID: update.EntityID})
	if err != nil {
		return runtimetools.EntityFieldWriteResult{}, err
	}
	if s.schemaGuard == nil {
		return runtimetools.EntityFieldWriteResult{}, fmt.Errorf("entity sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimetools.EntityFieldWriteResult{}, err
	}
	result := privatemutationprotocol.RunSQLite[int](ctx, s.backend, "sqlite entity field update", privatemutationprotocol.Story, privatemutationprotocol.Ordinary, nil, nil, func(txctx context.Context, mutation *privatemutationprotocol.Attempt) (int, error) {
		var revision int
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			sourceFact, err := storerunstate.RequireSQLiteActiveSourceTx(txctx, tx, runID)
			if err != nil {
				return err
			}
			var fieldsRaw any
			var flowInstance, entityType string
			if err := tx.QueryRowContext(txctx, `
			SELECT fields, flow_instance, entity_type
			FROM entity_state
			WHERE run_id = ? AND entity_id = ?
		`, runID, entityID).Scan(&fieldsRaw, &flowInstance, &entityType); err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("entity not found: %s", entityID)
				}
				return fmt.Errorf("load sqlite entity fields: %w", err)
			}
			before, after, fieldsJSON, err := applyToolEntityFieldUpdate(update, sourceFact, flowInstance, entityType, fieldsRaw)
			if err != nil {
				return err
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
			if err := insertSQLiteEntityStateDiffAttempt(txctx, mutation, tx, runID, entityID, runtimemutationlog.EntityStateProjection{
				Fields: before,
			}, runtimemutationlog.EntityStateProjection{
				Fields: after,
			}, MutationWriter(update.Writer), now); err != nil {
				return fmt.Errorf("record sqlite entity mutation: %w", err)
			}
			return nil
		})
		return revision, err
	})
	return committedEntityFieldRevision(result)
}

func committedEntityFieldRevision(result privatemutationprotocol.Result[int]) (runtimetools.EntityFieldWriteResult, error) {
	revision, acknowledged := result.Value()
	if !acknowledged {
		return runtimetools.EntityFieldWriteResult{}, result.Err()
	}
	return runtimetools.EntityFieldWriteResult{Revision: revision, Acknowledged: true}, result.Err()
}

func (s *EntityPostgresOwner) CreateEntity(ctx context.Context, rec runtimetools.EntityCreateRecord) (runtimetools.EntityCreateResult, error) {
	if s == nil || s.backend == nil {
		return runtimetools.EntityCreateResult{}, fmt.Errorf("postgres entity persistence store is required")
	}
	rec, fields, err := normalizeToolEntityCreateRecord(rec)
	if err != nil {
		return runtimetools.EntityCreateResult{}, err
	}
	if s.schemaGuard == nil {
		return runtimetools.EntityCreateResult{}, fmt.Errorf("entity postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimetools.EntityCreateResult{}, err
	}
	result := privatemutationprotocol.RunPostgres[string](ctx, s.backend, privatemutationprotocol.Story, privatemutationprotocol.Ordinary, nil, nil, func(txctx context.Context, mutation *privatemutationprotocol.Attempt) (string, error) {
		var storedEntityID string
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			sourceFact, err := storerunstate.RequirePostgresActiveSourceTx(txctx, tx, rec.RunID)
			if err != nil {
				return err
			}
			fields, err = initializeToolEntityRecord(&rec, sourceFact, fields)
			if err != nil {
				return err
			}
			var storedRunID string
			if err := tx.QueryRowContext(txctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''),
			$6, '{}'::jsonb, $7::jsonb, '{}'::jsonb, '{}'::jsonb, 1,
			$8, $8, $8
		)
		RETURNING run_id::text, entity_id::text
	`, rec.RunID, rec.EntityID, rec.FlowInstance, rec.EntityType, rec.Name, rec.CurrentState, string(rec.FieldsJSON), rec.CreatedAt).Scan(&storedRunID, &storedEntityID); err != nil {
				return fmt.Errorf("insert postgres entity: %w", err)
			}
			if err := mutation.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID); err != nil {
				return err
			}
			if err := insertPostgresEntityStateDiff(txctx, mutation, tx, storedEntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
				CurrentState: rec.CurrentState,
				Fields:       fields,
			}, MutationWriter(rec.Writer)); err != nil {
				return fmt.Errorf("record postgres entity create mutation: %w", err)
			}
			return nil
		})
		return storedEntityID, err
	})
	return committedEntityCreate(result)
}

func (s *EntitySQLiteOwner) CreateEntity(ctx context.Context, rec runtimetools.EntityCreateRecord) (runtimetools.EntityCreateResult, error) {
	if s == nil || s.backend == nil {
		return runtimetools.EntityCreateResult{}, fmt.Errorf("sqlite entity persistence store is required")
	}
	rec, fields, err := normalizeToolEntityCreateRecord(rec)
	if err != nil {
		return runtimetools.EntityCreateResult{}, err
	}
	if s.schemaGuard == nil {
		return runtimetools.EntityCreateResult{}, fmt.Errorf("entity sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimetools.EntityCreateResult{}, err
	}
	result := privatemutationprotocol.RunSQLite[string](ctx, s.backend, "sqlite entity create", privatemutationprotocol.Story, privatemutationprotocol.Ordinary, nil, nil, func(txctx context.Context, mutation *privatemutationprotocol.Attempt) (string, error) {
		err := mutation.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			sourceFact, err := storerunstate.RequireSQLiteActiveSourceTx(txctx, tx, rec.RunID)
			if err != nil {
				return err
			}
			fields, err = initializeToolEntityRecord(&rec, sourceFact, fields)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, name,
				current_state, gates, fields, bookkeeping, accumulator, revision,
				entered_state_at, created_at, updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, '{}', ?, '{}', '{}', 1, ?, ?, ?)
		`, rec.RunID, rec.EntityID, rec.FlowInstance, rec.EntityType, sqliteNullString(rec.Name),
				rec.CurrentState, string(rec.FieldsJSON), rec.CreatedAt, rec.CreatedAt, rec.CreatedAt); err != nil {
				return fmt.Errorf("insert sqlite entity: %w", err)
			}
			if err := mutation.AddFact(rec.RunID, privaterunforkrevision.FamilyEntityMetadata, rec.EntityID); err != nil {
				return err
			}
			if err := insertSQLiteEntityStateDiffAttempt(txctx, mutation, tx, rec.RunID, rec.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
				CurrentState: rec.CurrentState,
				Fields:       fields,
			}, MutationWriter(rec.Writer), rec.CreatedAt); err != nil {
				return fmt.Errorf("record sqlite entity create mutation: %w", err)
			}
			return nil
		})
		return rec.EntityID, err
	})
	return committedEntityCreate(result)
}

func committedEntityCreate(result privatemutationprotocol.Result[string]) (runtimetools.EntityCreateResult, error) {
	entityID, acknowledged := result.Value()
	if !acknowledged {
		return runtimetools.EntityCreateResult{}, result.Err()
	}
	return runtimetools.EntityCreateResult{EntityID: entityID, Acknowledged: true}, result.Err()
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

func toolEntityMutationContract(source semanticview.Source, sourceFact runtimecorrelation.SourceArtifactFact, flowInstance, entityType string) (entityruntime.Contract, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.SourceArtifact == nil {
		return entityruntime.Contract{}, fmt.Errorf("entity mutation requires its admitted source artifact")
	}
	if err := sourceFact.Validate(); err != nil {
		return entityruntime.Contract{}, err
	}
	if bundle.SourceArtifact.BundleHash() != sourceFact.BundleHash() {
		return entityruntime.Contract{}, fmt.Errorf("entity mutation source does not match active run source")
	}
	contract, ok := entityruntime.ResolveForFlowInstance(source, flowInstance)
	if !ok || contract.EntityType != entityType {
		return entityruntime.Contract{}, fmt.Errorf("entity mutation contract does not own %s (%s)", flowInstance, entityType)
	}
	return contract, nil
}

func applyToolEntityFieldUpdate(update runtimetools.EntityFieldUpdate, sourceFact runtimecorrelation.SourceArtifactFact, flowInstance, entityType string, fieldsRaw any) (map[string]any, map[string]any, []byte, error) {
	contract, err := toolEntityMutationContract(update.Source, sourceFact, flowInstance, entityType)
	if err != nil {
		return nil, nil, nil, err
	}
	before, err := DecodeJSONMap(fieldsRaw)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode current entity fields: %w", err)
	}
	after, err := entityruntime.ApplyMutations(contract, before, []entityruntime.Mutation{{Target: "entity." + update.FieldPath, Value: update.Value}})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("apply entity field mutation: %w", err)
	}
	encoded, err := canonicaljson.MarshalPreservingNumberKinds(after)
	if err != nil {
		return nil, nil, nil, err
	}
	beforeField, afterField := map[string]any{}, map[string]any{}
	if value, present := entityruntime.PathValue(before, update.FieldPath); present {
		beforeField[update.FieldPath] = value
	}
	if value, present := entityruntime.PathValue(after, update.FieldPath); present {
		afterField[update.FieldPath] = value
	}
	return beforeField, afterField, encoded, nil
}

func initializeToolEntityRecord(rec *runtimetools.EntityCreateRecord, sourceFact runtimecorrelation.SourceArtifactFact, supplied map[string]any) (map[string]any, error) {
	contract, err := toolEntityMutationContract(rec.Source, sourceFact, rec.FlowInstance, rec.EntityType)
	if err != nil {
		return nil, err
	}
	fields, err := entityruntime.Initialize(contract, supplied)
	if err != nil {
		return nil, err
	}
	rec.FieldsJSON, err = canonicaljson.MarshalPreservingNumberKinds(fields)
	return fields, err
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
	fields, err := DecodeJSONMap(rec.FieldsJSON)
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

func MutationWriter(writer runtimetools.EntityMutationWriter) runtimemutationlog.Writer {
	return runtimemutationlog.Writer{
		Type:        strings.TrimSpace(writer.Type),
		ID:          strings.TrimSpace(writer.ID),
		HandlerStep: strings.TrimSpace(writer.HandlerStep),
	}
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

func insertPostgresEntityStateDiff(ctx context.Context, mutation *privatemutationprotocol.Attempt, tx *sql.Tx, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer) error {
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(entityID, before, after, writer)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if strings.TrimSpace(rec.EntityID) == "" || strings.TrimSpace(rec.WriterType) == "" || strings.TrimSpace(rec.WriterID) == "" {
			return runtimemutationlog.ErrInvalidMutationLogWriter("entity_id, writer_type, and writer_id are required")
		}
		if err := runtimemutationlog.ValidateDomainPath(rec.Domain, rec.Path); err != nil {
			return err
		}
		runID, err := runtimecurrentstate.RequireRunID(ctx)
		if err != nil {
			return runtimemutationlog.ErrInvalidMutationLogWriter(err.Error())
		}
		runFact, err := storerunstate.RequirePostgresActiveSourceTx(ctx, tx, runID)
		if err != nil {
			return err
		}
		contextFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
		if !ok {
			return fmt.Errorf("mutation log bundle source fact is required")
		}
		if !runFact.Matches(contextFact) {
			return fmt.Errorf("mutation log bundle source fact does not match active run")
		}
		oldValue, err := toolJSONSQLArg(rec.OldValue)
		if err != nil {
			return err
		}
		newValue, err := toolJSONSQLArg(rec.NewValue)
		if err != nil {
			return err
		}
		causedByEvent := ""
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			causedByEvent = nullUUIDString(inbound.ID())
		}
		mutationID := uuid.NewString()
		occurredAt := time.Now().UTC()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_mutations (
				mutation_id, run_id, entity_id, domain, path, old_value, new_value,
				caused_by_event, writer_type, writer_id, handler_step, created_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4, $5, $6::jsonb, $7::jsonb,
				NULLIF($8, '')::uuid, $9, $10, NULLIF($11, ''), $12
			)
		`, mutationID, runID, strings.TrimSpace(rec.EntityID), string(rec.Domain), strings.TrimSpace(rec.Path), oldValue, newValue,
			causedByEvent, strings.TrimSpace(rec.WriterType), strings.TrimSpace(rec.WriterID), strings.TrimSpace(rec.HandlerStep), occurredAt); err != nil {
			return err
		}
		if err := mutation.AddFact(runID, privaterunforkrevision.FamilyEntityMutations, mutationID); err != nil {
			return err
		}
		draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, occurredAt)
		if err != nil {
			return err
		}
		if admitted {
			if err := mutation.Record(ctx, draft); err != nil {
				return err
			}
		}
	}
	return nil
}

func insertSQLiteEntityStateDiffAttempt(ctx context.Context, mutation *privatemutationprotocol.Attempt, tx *sql.Tx, runID string, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, createdAt time.Time) error {
	return insertSQLiteEntityStateDiff(ctx, tx, runID, entityID, before, after, writer, createdAt, mutation.AddFact, mutation.Record)
}

func InsertSQLiteEntityStateDiff(ctx context.Context, mutation *privatemutationprotocol.Attempt, runID string, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, createdAt time.Time) error {
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return insertSQLiteEntityStateDiffAttempt(ctx, mutation, tx, runID, entityID, before, after, writer, createdAt)
	})
}

func insertSQLiteEntityStateDiff(ctx context.Context, tx *sql.Tx, runID string, entityID string, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, createdAt time.Time, addFact func(string, privaterunforkrevision.Family, string) error, record func(context.Context, runtimeauthoractivity.Draft) error) error {
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
				mutation_id, run_id, entity_id, domain, path, old_value, new_value,
				caused_by_event, writer_type, writer_id, handler_step, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, mutationID, runID, rec.EntityID, rec.Domain, rec.Path, oldValue, newValue,
			sqliteNullUUID(causedByEvent), rec.WriterType, rec.WriterID, sqliteNullString(rec.HandlerStep), createdAt.UTC()); err != nil {
			return fmt.Errorf("insert sqlite entity mutation: %w", err)
		}
		if err := addFact(runID, privaterunforkrevision.FamilyEntityMutations, mutationID); err != nil {
			return err
		}
		draft, admitted, err := runtimemutationlog.AuthorActivityDraft(ctx, runID, mutationID, rec, createdAt)
		if err != nil {
			return err
		}
		if admitted {
			if err := record(ctx, draft); err != nil {
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
	case []byte:
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

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any {
	raw = nullUUIDString(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
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
