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
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
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
	Join string
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
		where = append(where, fmt.Sprintf("es.current_state = $%d", n))
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
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			es.entity_id::text,
			es.run_id::text,
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			COALESCE(es.current_state, ''),
			COALESCE(es.revision, 0),
			es.created_at,
			es.updated_at,
			COALESCE(es.slug, ''),
			COALESCE(es.name, '')
		FROM entity_state es
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
		var item operatorread.OperatorEntitySummary
		if err := rows.Scan(
			&item.EntityID,
			&item.RunID,
			&item.FlowInstance,
			&item.EntityType,
			&item.CurrentState,
			&item.Revision,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.Slug,
			&item.Name,
		); err != nil {
			return operatorread.OperatorEntityListResult{}, fmt.Errorf("scan operator entity summary: %w", err)
		}
		entities = append(entities, item)
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
		where = append(where, "es.current_state = ?")
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
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			COALESCE(es.entity_id, ''),
			COALESCE(es.run_id, ''),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			COALESCE(es.current_state, ''),
			COALESCE(es.revision, 0),
			es.created_at,
			es.updated_at,
			COALESCE(es.slug, ''),
			COALESCE(es.name, '')
		FROM entity_state es
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
		item, err := scanSQLiteOperatorEntitySummary(rows)
		if err != nil {
			return operatorread.OperatorEntityListResult{}, fmt.Errorf("scan sqlite operator entity summary: %w", err)
		}
		entities = append(entities, item)
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
		SELECT COALESCE(`+group.Expr+`, 'unknown') AS bucket, COUNT(*)::int
		FROM entity_state es
		`+group.Join+`
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY bucket
		ORDER BY COUNT(*) DESC, bucket ASC
	`, args...)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("aggregate operator entities: %w", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var (
			key   string
			count int
		)
		if err := rows.Scan(&key, &count); err != nil {
			return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("scan operator entity aggregate: %w", err)
		}
		counts[key] = count
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("read operator entity aggregate: %w", err)
	}
	return operatorread.OperatorEntityAggregateResult{Counts: counts}, nil
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
		SELECT COALESCE(`+group.Expr+`, 'unknown') AS bucket, COUNT(*)
		FROM entity_state es
		`+group.Join+`
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY bucket
		ORDER BY COUNT(*) DESC, bucket ASC
	`, args...)
	if err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("aggregate sqlite operator entities: %w", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var (
			key   string
			count int
		)
		if err := rows.Scan(&key, &count); err != nil {
			return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("scan sqlite operator entity aggregate: %w", err)
		}
		counts[key] = count
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorEntityAggregateResult{}, fmt.Errorf("read sqlite operator entity aggregate: %w", err)
	}
	return operatorread.OperatorEntityAggregateResult{Counts: counts}, nil
}

func (s *EntityPostgres) loadOperatorEntityRow(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	row := s.backend.QueryRowContext(ctx, `
		SELECT
			es.entity_id::text,
			es.run_id::text,
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			COALESCE(es.current_state, ''),
			COALESCE(es.revision, 0),
			es.created_at,
			es.updated_at,
			COALESCE(es.slug, ''),
			COALESCE(es.name, ''),
			COALESCE(es.fields, '{}'::jsonb),
			COALESCE(es.gates, '{}'::jsonb),
			COALESCE(es.accumulator, '{}'::jsonb)
		FROM entity_state es
		WHERE es.entity_id = $1::uuid
		  AND es.run_id = $2::uuid
	`, entityID, runID)
	var (
		out     operatorread.OperatorEntityFull
		fields  []byte
		gates   []byte
		accum   []byte
		summary = &out.Entity
	)
	if err := row.Scan(
		&summary.EntityID,
		&summary.RunID,
		&summary.FlowInstance,
		&summary.EntityType,
		&summary.CurrentState,
		&summary.Revision,
		&summary.CreatedAt,
		&summary.UpdatedAt,
		&summary.Slug,
		&summary.Name,
		&fields,
		&gates,
		&accum,
	); err == sql.ErrNoRows {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	} else if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("load operator entity: %w", err)
	}
	decodedFields, err := decodeStoreJSONMap(fields)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode operator entity fields: %w", err)
	}
	decodedGates, err := decodeStoreJSONBoolMap(gates)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode operator entity gates: %w", err)
	}
	decodedAccumulated, err := decodeStoreJSONMap(accum)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode operator entity accumulated: %w", err)
	}
	loops, err := loopruntime.PublicActivations(decodedAccumulated)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode operator entity loops: %w", err)
	}
	out.Fields = decodedFields
	out.Gates = decodedGates
	out.Accumulated = loopruntime.PublicStateBuckets(decodedAccumulated)
	out.Loops = loops
	return out, nil
}

func (s *EntitySQLite) loadSQLiteOperatorEntityRow(ctx context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	row := s.backend.QueryRowContext(ctx, `
		SELECT
			COALESCE(es.entity_id, ''),
			COALESCE(es.run_id, ''),
			COALESCE(es.flow_instance, ''),
			COALESCE(es.entity_type, ''),
			COALESCE(es.current_state, ''),
			COALESCE(es.revision, 0),
			es.created_at,
			es.updated_at,
			COALESCE(es.slug, ''),
			COALESCE(es.name, ''),
			COALESCE(es.fields, '{}'),
			COALESCE(es.gates, '{}'),
			COALESCE(es.accumulator, '{}')
		FROM entity_state es
		WHERE es.entity_id = ?
		  AND es.run_id = ?
	`, entityID, runID)
	var (
		out     operatorread.OperatorEntityFull
		fields  any
		gates   any
		accum   any
		summary = &out.Entity
	)
	item, err := scanSQLiteOperatorEntitySummaryWithTail(row, &fields, &gates, &accum)
	if err == sql.ErrNoRows {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("load sqlite operator entity: %w", err)
	}
	*summary = item
	decodedFields, err := decodeStoreJSONMap([]byte(sqliteJSONRawMessage(fields)))
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode sqlite operator entity fields: %w", err)
	}
	decodedGates, err := decodeStoreJSONBoolMap([]byte(sqliteJSONRawMessage(gates)))
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode sqlite operator entity gates: %w", err)
	}
	decodedAccumulated, err := decodeStoreJSONMap([]byte(sqliteJSONRawMessage(accum)))
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode sqlite operator entity accumulated: %w", err)
	}
	loops, err := loopruntime.PublicActivations(decodedAccumulated)
	if err != nil {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("decode sqlite operator entity loops: %w", err)
	}
	out.Fields = decodedFields
	out.Gates = decodedGates
	out.Accumulated = loopruntime.PublicStateBuckets(decodedAccumulated)
	out.Loops = loops
	return out, nil
}

type sqliteOperatorEntityScanner interface {
	Scan(dest ...any) error
}

func scanSQLiteOperatorEntitySummary(scanner sqliteOperatorEntityScanner) (operatorread.OperatorEntitySummary, error) {
	return scanSQLiteOperatorEntitySummaryWithTail(scanner)
}

func scanSQLiteOperatorEntitySummaryWithTail(scanner sqliteOperatorEntityScanner, tail ...any) (operatorread.OperatorEntitySummary, error) {
	var (
		item       operatorread.OperatorEntitySummary
		createdRaw any
		updatedRaw any
	)
	dest := []any{
		&item.EntityID,
		&item.RunID,
		&item.FlowInstance,
		&item.EntityType,
		&item.CurrentState,
		&item.Revision,
		&createdRaw,
		&updatedRaw,
		&item.Slug,
		&item.Name,
	}
	dest = append(dest, tail...)
	if err := scanner.Scan(dest...); err != nil {
		return operatorread.OperatorEntitySummary{}, err
	}
	createdAt, ok, err := sqliteTimeValue(createdRaw)
	if err != nil {
		return operatorread.OperatorEntitySummary{}, fmt.Errorf("decode created_at: %w", err)
	}
	if !ok {
		return operatorread.OperatorEntitySummary{}, fmt.Errorf("created_at is required")
	}
	updatedAt, ok, err := sqliteTimeValue(updatedRaw)
	if err != nil {
		return operatorread.OperatorEntitySummary{}, fmt.Errorf("decode updated_at: %w", err)
	}
	if !ok {
		return operatorread.OperatorEntitySummary{}, fmt.Errorf("updated_at is required")
	}
	item.CreatedAt = createdAt
	item.UpdatedAt = updatedAt
	return item, nil
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
		return entityAggregateGroup{Expr: "NULLIF(es.current_state, '')"}, nil
	case "flow", "flow_instance":
		return entityAggregateGroup{Expr: "NULLIF(es.flow_instance, '')"}, nil
	case "workflow_name":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.flow_template, '')",
			Join: "LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance",
		}, nil
	case "workflow_version":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.config->>'workflow_version', '')",
			Join: "LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance",
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
		return entityAggregateGroup{Expr: "NULLIF(es.current_state, '')"}, nil
	case "flow", "flow_instance":
		return entityAggregateGroup{Expr: "NULLIF(es.flow_instance, '')"}, nil
	case "workflow_name":
		return entityAggregateGroup{
			Expr: "NULLIF(fi.flow_template, '')",
			Join: "LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance",
		}, nil
	case "workflow_version":
		return entityAggregateGroup{
			Expr: "NULLIF(CAST(json_extract(COALESCE(fi.config, '{}'), '$.workflow_version') AS TEXT), '')",
			Join: "LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance",
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

func decodeStoreJSONBoolMap(raw []byte) (map[string]bool, error) {
	if len(raw) == 0 {
		return map[string]bool{}, nil
	}
	var out map[string]bool
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]bool{}
	}
	return out, nil
}
