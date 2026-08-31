package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeworkspace "github.com/division-sh/swarm/internal/runtime/workspace"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type Postgres struct{ backend *postgresbackend.Backend }
type SQLite struct{ backend *sqlitebackend.Backend }

func NewPostgres(backend *postgresbackend.Backend) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres workspace owner requires backend")
	}
	return &Postgres{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite workspace owner requires backend")
	}
	return &SQLite{backend: backend}, nil
}

func (o *Postgres) LookupEntity(ctx context.Context, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	return scanEntity(o.backend.QueryRowContext(ctx, `SELECT slug FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, identity.RunID, identity.EntityID), identity)
}

func (o *SQLite) LookupEntity(ctx context.Context, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	return scanEntity(o.backend.QueryRowContext(ctx, `SELECT slug FROM entity_state WHERE run_id = ? AND entity_id = ?`, identity.RunID, identity.EntityID), identity)
}

type rowScanner interface{ Scan(...any) error }

func scanEntity(row rowScanner, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	var slug string
	if err := row.Scan(&slug); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimeworkspace.WorkspaceEntityLookup{}, fmt.Errorf("workspace entity %s is not present in run %s", identity.EntityID, identity.RunID)
		}
		return runtimeworkspace.WorkspaceEntityLookup{}, fmt.Errorf("lookup workspace entity: %w", err)
	}
	return runtimeworkspace.WorkspaceEntityLookup{Slug: strings.TrimSpace(slug)}, nil
}

func (o *Postgres) ListContainers(ctx context.Context, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	rows, err := o.backend.QueryContext(ctx, `
		SELECT DISTINCT es.slug
		FROM entity_state es
		JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
		WHERE es.run_id = $1::uuid AND fi.config->>'instance_kind' = 'entity'
		ORDER BY 1
	`, strings.TrimSpace(runID))
	if err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, fmt.Errorf("list postgres runtime workspace entities: %w", err)
	}
	defer rows.Close()
	return scanContainers(rows)
}

func (o *SQLite) ListContainers(ctx context.Context, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	rows, err := o.backend.QueryContext(ctx, `
		SELECT DISTINCT es.slug
		FROM entity_state es
		JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance
		WHERE es.run_id = ? AND json_extract(fi.config, '$.instance_kind') = 'entity'
		ORDER BY 1
	`, strings.TrimSpace(runID))
	if err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, fmt.Errorf("list sqlite runtime workspace entities: %w", err)
	}
	defer rows.Close()
	return scanContainers(rows)
}

type rowIterator interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanContainers(rows rowIterator) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	result := runtimeworkspace.RuntimeWorkspaceContainerSet{}
	for rows.Next() {
		var slug string
		if err := rows.Scan(&slug); err != nil {
			return runtimeworkspace.RuntimeWorkspaceContainerSet{}, fmt.Errorf("scan runtime workspace entity slug: %w", err)
		}
		result.EntitySlugs = append(result.EntitySlugs, strings.TrimSpace(slug))
	}
	if err := rows.Err(); err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, fmt.Errorf("iterate runtime workspace entity slugs: %w", err)
	}
	return result, nil
}
