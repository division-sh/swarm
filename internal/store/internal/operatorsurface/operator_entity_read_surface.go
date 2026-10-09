package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
	"github.com/google/uuid"
)

type entityPositionCursor struct {
	Kind      string `json:"kind"`
	UpdatedAt string `json:"updated_at"`
	EntityID  string `json:"entity_id"`
	RunID     string `json:"run_id"`
}

type entityAggregateGroup struct {
	Expr string
}

var entityAggregateFieldPattern = regexp.MustCompile(`^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$`)

func (s *EntityPostgres) requireOperatorEntityAccess() error {
	return s.requireCurrentSchema()
}

func (s *EntitySQLite) requireOperatorEntityAccess() error {
	return s.requireCurrentSchema()
}

func (s *EntityPostgres) ListOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityListResult{}, err
	}
	opts, err := defaultOperatorEntityListOptions(opts)
	if err != nil {
		return operatorread.OperatorEntityListResult{}, err
	}
	args := make([]any, 0, 12)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.RunID != "" {
		n := add(opts.RunID)
		where = append(where, fmt.Sprintf("es.run_id = $%d::uuid", n))
	}
	if opts.EntityID != "" {
		n := add(opts.EntityID)
		where = append(where, fmt.Sprintf("es.entity_id = $%d::uuid", n))
	}
	if opts.Flow != "" {
		n := add(opts.Flow)
		where = append(where, fmt.Sprintf("(es.flow_instance = $%d OR es.flow_instance LIKE $%d || '/%%')", n, n))
	}
	if opts.Type != "" {
		n := add(opts.Type)
		where = append(where, fmt.Sprintf("es.entity_type = $%d", n))
	}
	if opts.CurrentState != "" {
		n := add(opts.CurrentState)
		where = append(where, fmt.Sprintf("fi.stage_defined AND fi.current_state = $%d", n))
	}
	if opts.Cursor != "" {
		cursor, err := decodeEntityPositionCursor(opts.Cursor, "entity.list")
		if err != nil {
			return operatorread.OperatorEntityListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.EntityID) == "" || strings.TrimSpace(cursor.RunID) == "" {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.EntityID); err != nil {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.RunID); err != nil {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		nTime := add(updatedAt.UTC())
		nEntity := add(cursor.EntityID)
		nRun := add(cursor.RunID)
		where = append(where, fmt.Sprintf(`(
			es.updated_at < $%d
			OR (
				es.updated_at = $%d
				AND (
					es.entity_id::text > $%d
					OR (es.entity_id::text = $%d AND es.run_id::text > $%d)
				)
			)
		)`, nTime, nTime, nEntity, nEntity, nRun))
	}
	limitArg := add(opts.Limit + 1)
	rows, err := s.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(`
		ORDER BY es.updated_at DESC, es.entity_id::text ASC, es.run_id::text ASC
		LIMIT $%d
	`, limitArg), args...)
	if err != nil {
		return operatorread.OperatorEntityListResult{}, fmt.Errorf("list operator entities: %w", err)
	}
	defer rows.Close()
	entities := []operatorread.OperatorEntitySummary{}
	for rows.Next() {
		item, err := workflowheader.ScanData(rows, sqliteTimeValue)
		if err != nil {
			return operatorread.OperatorEntityListResult{}, fmt.Errorf("scan operator entity summary: %w", err)
		}
		entities = append(entities, item.Entity)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityListResult{}, fmt.Errorf("read operator entity summaries: %w", err)
	}
	nextCursor := ""
	if len(entities) > opts.Limit {
		entities = entities[:opts.Limit]
		last := entities[len(entities)-1]
		nextCursor = encodeEntityPositionCursor(entityPositionCursor{
			Kind:      "entity.list",
			UpdatedAt: last.UpdatedAt.UTC().Format(time.RFC3339Nano),
			EntityID:  last.EntityID,
			RunID:     last.RunID,
		})
	}
	if entities == nil {
		entities = []operatorread.OperatorEntitySummary{}
	}
	return operatorread.OperatorEntityListResult{Entities: entities, NextCursor: nextCursor}, nil
}

func (s *EntitySQLite) ListOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityListResult{}, err
	}
	opts, err := defaultOperatorEntityListOptions(opts)
	if err != nil {
		return operatorread.OperatorEntityListResult{}, err
	}
	args := make([]any, 0, 12)
	where := []string{"1=1"}
	add := func(value any) {
		args = append(args, value)
	}
	if opts.RunID != "" {
		add(opts.RunID)
		where = append(where, "es.run_id = ?")
	}
	if opts.EntityID != "" {
		add(opts.EntityID)
		where = append(where, "es.entity_id = ?")
	}
	if opts.Flow != "" {
		add(opts.Flow)
		add(opts.Flow + "/%")
		where = append(where, "(es.flow_instance = ? OR es.flow_instance LIKE ?)")
	}
	if opts.Type != "" {
		add(opts.Type)
		where = append(where, "es.entity_type = ?")
	}
	if opts.CurrentState != "" {
		add(opts.CurrentState)
		where = append(where, "fi.stage_defined AND fi.current_state = ?")
	}
	if opts.Cursor != "" {
		cursor, err := decodeEntityPositionCursor(opts.Cursor, "entity.list")
		if err != nil {
			return operatorread.OperatorEntityListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.EntityID) == "" || strings.TrimSpace(cursor.RunID) == "" {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.EntityID); err != nil {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.RunID); err != nil {
			return operatorread.OperatorEntityListResult{}, operatorread.ErrInvalidEntityCursor
		}
		add(updatedAt.UTC())
		add(updatedAt.UTC())
		add(cursor.EntityID)
		add(cursor.EntityID)
		add(cursor.RunID)
		where = append(where, `(
			es.updated_at < ?
			OR (
				es.updated_at = ?
				AND (
					es.entity_id > ?
					OR (es.entity_id = ? AND es.run_id > ?)
				)
			)
		)`)
	}
	add(opts.Limit + 1)
	rows, err := s.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY es.updated_at DESC, es.entity_id ASC, es.run_id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return operatorread.OperatorEntityListResult{}, fmt.Errorf("list sqlite operator entities: %w", err)
	}
	defer rows.Close()
	entities := []operatorread.OperatorEntitySummary{}
	for rows.Next() {
		item, err := workflowheader.ScanData(rows, sqliteTimeValue)
		if err != nil {
			return operatorread.OperatorEntityListResult{}, fmt.Errorf("scan sqlite operator entity summary: %w", err)
		}
		entities = append(entities, item.Entity)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityListResult{}, fmt.Errorf("read sqlite operator entity summaries: %w", err)
	}
	nextCursor := ""
	if len(entities) > opts.Limit {
		entities = entities[:opts.Limit]
		last := entities[len(entities)-1]
		nextCursor = encodeEntityPositionCursor(entityPositionCursor{
			Kind:      "entity.list",
			UpdatedAt: last.UpdatedAt.UTC().Format(time.RFC3339Nano),
			EntityID:  last.EntityID,
			RunID:     last.RunID,
		})
	}
	if entities == nil {
		entities = []operatorread.OperatorEntitySummary{}
	}
	return operatorread.OperatorEntityListResult{Entities: entities, NextCursor: nextCursor}, nil
}

func (s *EntityPostgres) LoadOperatorEntity(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	entityID = strings.TrimSpace(entityID)
	runID = strings.TrimSpace(runID)
	if entityID == "" {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return operatorread.OperatorEntityFull{}, &operatorread.EntityReadParamError{Field: "entity_id", Reason: "must be a UUID"}
	}
	if runID != "" {
		if _, err := uuid.Parse(runID); err != nil {
			return operatorread.OperatorEntityFull{}, &operatorread.EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
		return s.loadOperatorEntityRow(ctx, entityID, runID)
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT es.run_id::text
		FROM entity_state es
		WHERE es.entity_id = $1::uuid
		ORDER BY es.updated_at DESC, es.run_id::text ASC
		LIMIT 2
	`, entityID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("resolve operator entity run scope: %w", err)
	}
	defer rows.Close()
	matches := []string{}
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return operatorread.OperatorEntityFull{}, fmt.Errorf("scan operator entity run scope: %w", err)
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("read operator entity run scopes: %w", err)
	}
	switch len(matches) {
	case 0:
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	case 1:
		return s.loadOperatorEntityRow(ctx, entityID, matches[0])
	default:
		return operatorread.OperatorEntityFull{}, operatorread.ErrAmbiguousEntityRunID
	}
}

func (s *EntitySQLite) LoadOperatorEntity(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	entityID = strings.TrimSpace(entityID)
	runID = strings.TrimSpace(runID)
	if entityID == "" {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return operatorread.OperatorEntityFull{}, &operatorread.EntityReadParamError{Field: "entity_id", Reason: "must be a UUID"}
	}
	if runID != "" {
		if _, err := uuid.Parse(runID); err != nil {
			return operatorread.OperatorEntityFull{}, &operatorread.EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
		return s.loadSQLiteOperatorEntityRow(ctx, entityID, runID)
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT COALESCE(es.run_id, '')
		FROM entity_state es
		WHERE es.entity_id = ?
		ORDER BY es.updated_at DESC, es.run_id ASC
		LIMIT 2
	`, entityID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("resolve sqlite operator entity run scope: %w", err)
	}
	defer rows.Close()
	matches := []string{}
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return operatorread.OperatorEntityFull{}, fmt.Errorf("scan sqlite operator entity run scope: %w", err)
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("read sqlite operator entity run scopes: %w", err)
	}
	switch len(matches) {
	case 0:
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	case 1:
		return s.loadSQLiteOperatorEntityRow(ctx, entityID, matches[0])
	default:
		return operatorread.OperatorEntityFull{}, operatorread.ErrAmbiguousEntityRunID
	}
}

func (s *EntityPostgres) AggregateOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	opts, err := defaultOperatorEntityAggregateOptions(opts)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	args := make([]any, 0, 6)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.RunID != "" {
		n := add(opts.RunID)
		where = append(where, fmt.Sprintf("es.run_id = $%d::uuid", n))
	}
	if opts.Type != "" {
		n := add(opts.Type)
		where = append(where, fmt.Sprintf("es.entity_type = $%d", n))
	}
	group, err := operatorEntityAggregateGroup(opts.GroupBy, add)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT grouped.*, COUNT(*) OVER (PARTITION BY bucket) AS bucket_count
		FROM (SELECT `+workflowheader.DataReadColumns+`, `+group.Expr+` AS bucket
		`+workflowheader.DataReadFrom+`
		WHERE `+strings.Join(where, " AND ")+`
		) grouped
		ORDER BY bucket_count DESC, (bucket IS NULL), bucket ASC
	`, args...)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("aggregate operator entities: %w", err)
	}
	defer rows.Close()
	return scanOperatorEntityAggregateRows(rows)
}

func (s *EntitySQLite) AggregateOperatorEntities(ctx context.Context, opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error) {
	if err := s.requireOperatorEntityAccess(); err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	opts, err := defaultOperatorEntityAggregateOptions(opts)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	args := make([]any, 0, 6)
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	group, err := sqliteOperatorEntityAggregateGroup(opts.GroupBy, add)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, err
	}
	where := []string{"1=1"}
	if opts.RunID != "" {
		args = append(args, opts.RunID)
		where = append(where, "es.run_id = ?")
	}
	if opts.Type != "" {
		args = append(args, opts.Type)
		where = append(where, "es.entity_type = ?")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT grouped.*, COUNT(*) OVER (PARTITION BY bucket) AS bucket_count
		FROM (SELECT `+workflowheader.DataReadColumns+`, `+group.Expr+` AS bucket
		`+workflowheader.DataReadFrom+`
		WHERE `+strings.Join(where, " AND ")+`
		) grouped
		ORDER BY bucket_count DESC, (bucket IS NULL), bucket ASC
	`, args...)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("aggregate sqlite operator entities: %w", err)
	}
	defer rows.Close()
	return scanOperatorEntityAggregateRows(rows)
}

func (s *EntityPostgres) loadOperatorEntityRow(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	rows, err := s.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE es.entity_id = $1::uuid AND es.run_id = $2::uuid`, entityID, runID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("load operator entity: %w", err)
	}
	defer rows.Close()
	return scanOperatorEntityRow(rows)
}

func (s *EntitySQLite) loadSQLiteOperatorEntityRow(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	rows, err := s.backend.QueryContext(ctx, `SELECT `+workflowheader.DataReadColumns+` `+workflowheader.DataReadFrom+`
		WHERE es.entity_id = ? AND es.run_id = ?`, entityID, runID)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("load sqlite operator entity: %w", err)
	}
	defer rows.Close()
	return scanOperatorEntityRow(rows)
}

func scanOperatorEntityRow(rows *sql.Rows) (operatorread.OperatorEntityFull, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return operatorread.OperatorEntityFull{}, err
		}
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	item, err := workflowheader.ScanData(rows, sqliteTimeValue)
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if rows.Next() {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("entity has conflicting constructed header owners")
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	return item, nil
}

func scanOperatorEntityAggregateRows(rows *sql.Rows) (operatorread.OperatorEntityAggregateResult, error) {
	out := operatorread.OperatorEntityAggregateResult{Counts: map[string]int{}}
	for rows.Next() {
		var key sql.NullString
		var count int
		// Window counts retain SQL grouping while every member consumes exact
		// companion admission in the same query snapshot.
		if _, err := workflowheader.ScanData(rows, sqliteTimeValue, &key, &count); err != nil {
			return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("scan operator entity aggregate: %w", err)
		}
		if key.Valid {
			out.Counts[key.String] = count
		} else {
			out.AbsentCount = count
		}
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("read operator entity aggregate: %w", err)
	}
	return out, nil
}

func defaultOperatorEntityListOptions(opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.EntityID = strings.TrimSpace(opts.EntityID)
	opts.Flow = strings.Trim(strings.TrimSpace(opts.Flow), "/")
	opts.Type = strings.TrimSpace(opts.Type)
	opts.CurrentState = strings.TrimSpace(opts.CurrentState)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return operatorread.OperatorEntityListOptions{}, &operatorread.EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
	}
	if opts.EntityID != "" {
		if _, err := uuid.Parse(opts.EntityID); err != nil {
			return operatorread.OperatorEntityListOptions{}, &operatorread.EntityReadParamError{Field: "entity_id", Reason: "must be a UUID"}
		}
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts, nil
}

func defaultOperatorEntityAggregateOptions(opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Type = strings.TrimSpace(opts.Type)
	opts.GroupBy = strings.TrimSpace(opts.GroupBy)
	if opts.GroupBy == "" {
		opts.GroupBy = "current_state"
	}
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return operatorread.OperatorEntityAggregateOptions{}, &operatorread.EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
	}
	return opts, nil
}

func operatorEntityAggregateGroup(groupBy string, add func(any) int) (entityAggregateGroup, error) {
	switch strings.TrimSpace(groupBy) {
	case "current_state":
		return entityAggregateGroup{Expr: "CASE WHEN fi.stage_defined THEN fi.current_state END"}, nil
	case "flow", "flow_instance":
		return entityAggregateGroup{Expr: "NULLIF(es.flow_instance, '')"}, nil
	case "workflow_name":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.flow_template, '')",
		}, nil
	case "workflow_version":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.config->>'workflow_version', '')",
		}, nil
	case "type", "entity_type":
		return entityAggregateGroup{Expr: "NULLIF(es.entity_type, '')"}, nil
	case "slug":
		return entityAggregateGroup{Expr: "NULLIF(es.slug, '')"}, nil
	case "name":
		return entityAggregateGroup{Expr: "NULLIF(es.name, '')"}, nil
	default:
		if path, ok := strings.CutPrefix(strings.TrimSpace(groupBy), "fields."); ok && entityAggregateFieldPattern.MatchString(path) {
			n := add(path)
			return entityAggregateGroup{Expr: fmt.Sprintf("NULLIF(es.fields #>> string_to_array($%d, '.'), '')", n)}, nil
		}
		return entityAggregateGroup{}, &operatorread.EntityReadParamError{Field: "group_by", Reason: "unsupported entity aggregate group_by"}
	}
}

func sqliteOperatorEntityAggregateGroup(groupBy string, add func(any) int) (entityAggregateGroup, error) {
	switch strings.TrimSpace(groupBy) {
	case "current_state":
		return entityAggregateGroup{Expr: "CASE WHEN fi.stage_defined THEN fi.current_state END"}, nil
	case "flow", "flow_instance":
		return entityAggregateGroup{Expr: "NULLIF(es.flow_instance, '')"}, nil
	case "workflow_name":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.flow_template, '')",
		}, nil
	case "workflow_version":
		return entityAggregateGroup{
			Expr: "NULLIF(CAST(json_extract(COALESCE(fi.config, '{}'), '$.workflow_version') AS TEXT), '')",
		}, nil
	case "type", "entity_type":
		return entityAggregateGroup{Expr: "NULLIF(es.entity_type, '')"}, nil
	case "slug":
		return entityAggregateGroup{Expr: "NULLIF(es.slug, '')"}, nil
	case "name":
		return entityAggregateGroup{Expr: "NULLIF(es.name, '')"}, nil
	default:
		if path, ok := strings.CutPrefix(strings.TrimSpace(groupBy), "fields."); ok && entityAggregateFieldPattern.MatchString(path) {
			add(sqliteToolJSONPath(path))
			return entityAggregateGroup{Expr: "NULLIF(CAST(json_extract(COALESCE(es.fields, '{}'), ?) AS TEXT), '')"}, nil
		}
		return entityAggregateGroup{}, &operatorread.EntityReadParamError{Field: "group_by", Reason: "unsupported entity aggregate group_by"}
	}
}

func encodeEntityPositionCursor(cursor entityPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeEntityPositionCursor(raw string, kind string) (entityPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return entityPositionCursor{}, operatorread.ErrInvalidEntityCursor
	}
	var cursor entityPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return entityPositionCursor{}, operatorread.ErrInvalidEntityCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return entityPositionCursor{}, operatorread.ErrInvalidEntityCursor
	}
	return cursor, nil
}
