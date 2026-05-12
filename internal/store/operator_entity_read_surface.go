package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type OperatorEntityListOptions struct {
	RunID        string
	EntityID     string
	Flow         string
	Type         string
	CurrentState string
	Limit        int
	Cursor       string
}

type OperatorEntityListResult struct {
	Entities   []OperatorEntitySummary `json:"entities"`
	NextCursor string                  `json:"next_cursor,omitempty"`
}

type OperatorEntitySummary struct {
	EntityID     string    `json:"entity_id"`
	RunID        string    `json:"run_id"`
	FlowInstance string    `json:"flow_instance"`
	EntityType   string    `json:"entity_type"`
	CurrentState string    `json:"current_state"`
	Revision     int       `json:"revision"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	Slug         string    `json:"slug,omitempty"`
	Name         string    `json:"name,omitempty"`
}

type OperatorEntityFull struct {
	Entity      OperatorEntitySummary `json:"entity"`
	Fields      map[string]any        `json:"fields"`
	Gates       map[string]bool       `json:"gates"`
	Accumulated map[string]any        `json:"accumulated"`
}

type OperatorEntityAggregateOptions struct {
	RunID   string
	GroupBy string
	Type    string
}

type OperatorEntityAggregateResult struct {
	Counts map[string]int `json:"counts"`
}

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

func (s *PostgresStore) requireOperatorEntityCapabilities(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.EntityState != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("entity_state", caps.EntityState)
	case !caps.EntityRunID:
		return fmt.Errorf("operator entity read surface requires canonical entity_state.run_id")
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	required := []string{
		"run_id", "entity_id", "flow_instance", "entity_type", "slug", "name",
		"current_state", "gates", "fields", "accumulator", "revision",
		"created_at", "updated_at",
	}
	if !catalog.hasColumns("entity_state", required...) {
		return fmt.Errorf("operator entity read surface requires entity_state columns %v", required)
	}
	return nil
}

func (s *PostgresStore) ListOperatorEntities(ctx context.Context, opts OperatorEntityListOptions) (OperatorEntityListResult, error) {
	if err := s.requireOperatorEntityCapabilities(ctx); err != nil {
		return OperatorEntityListResult{}, err
	}
	opts, err := defaultOperatorEntityListOptions(opts)
	if err != nil {
		return OperatorEntityListResult{}, err
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
			return OperatorEntityListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.EntityID) == "" || strings.TrimSpace(cursor.RunID) == "" {
			return OperatorEntityListResult{}, ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.EntityID); err != nil {
			return OperatorEntityListResult{}, ErrInvalidEntityCursor
		}
		if _, err := uuid.Parse(cursor.RunID); err != nil {
			return OperatorEntityListResult{}, ErrInvalidEntityCursor
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
	rows, err := s.DB.QueryContext(ctx, `
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
		return OperatorEntityListResult{}, fmt.Errorf("list operator entities: %w", err)
	}
	defer rows.Close()
	entities := []OperatorEntitySummary{}
	for rows.Next() {
		var item OperatorEntitySummary
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
			return OperatorEntityListResult{}, fmt.Errorf("scan operator entity summary: %w", err)
		}
		entities = append(entities, item)
	}
	if err := rows.Err(); err != nil {
		return OperatorEntityListResult{}, fmt.Errorf("read operator entity summaries: %w", err)
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
		entities = []OperatorEntitySummary{}
	}
	return OperatorEntityListResult{Entities: entities, NextCursor: nextCursor}, nil
}

func (s *PostgresStore) LoadOperatorEntity(ctx context.Context, entityID, runID string) (OperatorEntityFull, error) {
	if err := s.requireOperatorEntityCapabilities(ctx); err != nil {
		return OperatorEntityFull{}, err
	}
	entityID = strings.TrimSpace(entityID)
	runID = strings.TrimSpace(runID)
	if entityID == "" {
		return OperatorEntityFull{}, ErrEntityNotFound
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return OperatorEntityFull{}, &EntityReadParamError{Field: "entity_id", Reason: "must be a UUID"}
	}
	if runID != "" {
		if _, err := uuid.Parse(runID); err != nil {
			return OperatorEntityFull{}, &EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
		return s.loadOperatorEntityRow(ctx, entityID, runID)
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT es.run_id::text
		FROM entity_state es
		WHERE es.entity_id = $1::uuid
		ORDER BY es.updated_at DESC, es.run_id::text ASC
		LIMIT 2
	`, entityID)
	if err != nil {
		return OperatorEntityFull{}, fmt.Errorf("resolve operator entity run scope: %w", err)
	}
	defer rows.Close()
	matches := []string{}
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return OperatorEntityFull{}, fmt.Errorf("scan operator entity run scope: %w", err)
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return OperatorEntityFull{}, fmt.Errorf("read operator entity run scopes: %w", err)
	}
	switch len(matches) {
	case 0:
		return OperatorEntityFull{}, ErrEntityNotFound
	case 1:
		return s.loadOperatorEntityRow(ctx, entityID, matches[0])
	default:
		return OperatorEntityFull{}, ErrAmbiguousEntityRunID
	}
}

func (s *PostgresStore) AggregateOperatorEntities(ctx context.Context, opts OperatorEntityAggregateOptions) (OperatorEntityAggregateResult, error) {
	if err := s.requireOperatorEntityCapabilities(ctx); err != nil {
		return OperatorEntityAggregateResult{}, err
	}
	opts, err := defaultOperatorEntityAggregateOptions(opts)
	if err != nil {
		return OperatorEntityAggregateResult{}, err
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
		return OperatorEntityAggregateResult{}, err
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(`+group.Expr+`, 'unknown') AS bucket, COUNT(*)::int
		FROM entity_state es
		`+group.Join+`
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY bucket
		ORDER BY COUNT(*) DESC, bucket ASC
	`, args...)
	if err != nil {
		return OperatorEntityAggregateResult{}, fmt.Errorf("aggregate operator entities: %w", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var (
			key   string
			count int
		)
		if err := rows.Scan(&key, &count); err != nil {
			return OperatorEntityAggregateResult{}, fmt.Errorf("scan operator entity aggregate: %w", err)
		}
		counts[key] = count
	}
	if err := rows.Err(); err != nil {
		return OperatorEntityAggregateResult{}, fmt.Errorf("read operator entity aggregate: %w", err)
	}
	return OperatorEntityAggregateResult{Counts: counts}, nil
}

func (s *PostgresStore) loadOperatorEntityRow(ctx context.Context, entityID, runID string) (OperatorEntityFull, error) {
	row := s.DB.QueryRowContext(ctx, `
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
		out     OperatorEntityFull
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
		return OperatorEntityFull{}, ErrEntityNotFound
	} else if err != nil {
		return OperatorEntityFull{}, fmt.Errorf("load operator entity: %w", err)
	}
	decodedFields, err := decodeStoreJSONMap(fields)
	if err != nil {
		return OperatorEntityFull{}, fmt.Errorf("decode operator entity fields: %w", err)
	}
	decodedGates, err := decodeStoreJSONBoolMap(gates)
	if err != nil {
		return OperatorEntityFull{}, fmt.Errorf("decode operator entity gates: %w", err)
	}
	decodedAccumulated, err := decodeStoreJSONMap(accum)
	if err != nil {
		return OperatorEntityFull{}, fmt.Errorf("decode operator entity accumulated: %w", err)
	}
	out.Fields = decodedFields
	out.Gates = decodedGates
	out.Accumulated = decodedAccumulated
	return out, nil
}

func defaultOperatorEntityListOptions(opts OperatorEntityListOptions) (OperatorEntityListOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.EntityID = strings.TrimSpace(opts.EntityID)
	opts.Flow = strings.Trim(strings.TrimSpace(opts.Flow), "/")
	opts.Type = strings.TrimSpace(opts.Type)
	opts.CurrentState = strings.TrimSpace(opts.CurrentState)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return OperatorEntityListOptions{}, &EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
		}
	}
	if opts.EntityID != "" {
		if _, err := uuid.Parse(opts.EntityID); err != nil {
			return OperatorEntityListOptions{}, &EntityReadParamError{Field: "entity_id", Reason: "must be a UUID"}
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

func defaultOperatorEntityAggregateOptions(opts OperatorEntityAggregateOptions) (OperatorEntityAggregateOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Type = strings.TrimSpace(opts.Type)
	opts.GroupBy = strings.TrimSpace(opts.GroupBy)
	if opts.GroupBy == "" {
		opts.GroupBy = "current_state"
	}
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return OperatorEntityAggregateOptions{}, &EntityReadParamError{Field: "run_id", Reason: "must be a UUID"}
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
		return entityAggregateGroup{}, &EntityReadParamError{Field: "group_by", Reason: "unsupported entity aggregate group_by"}
	}
}

func encodeEntityPositionCursor(cursor entityPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeEntityPositionCursor(raw string, kind string) (entityPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return entityPositionCursor{}, ErrInvalidEntityCursor
	}
	var cursor entityPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return entityPositionCursor{}, ErrInvalidEntityCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return entityPositionCursor{}, ErrInvalidEntityCursor
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
