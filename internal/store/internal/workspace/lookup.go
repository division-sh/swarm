package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeworkspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func LookupEntityPostgres(ctx context.Context, db *sql.DB, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	return lookupEntity(ctx, db, `
		SELECT slug
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, "PostgreSQL", identity)
}

func LookupEntitySQLite(ctx context.Context, db *sql.DB, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	return lookupEntity(ctx, db, `
		SELECT slug
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, "SQLite", identity)
}

func lookupEntity(ctx context.Context, db *sql.DB, query, backend string, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	var slug string
	if err := db.QueryRowContext(ctx, query, identity.RunID, identity.EntityID).Scan(&slug); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimeworkspace.WorkspaceEntityLookup{}, fmt.Errorf("workspace entity %s is not present in run %s", identity.EntityID, identity.RunID)
		}
		return runtimeworkspace.WorkspaceEntityLookup{}, fmt.Errorf("lookup %s workspace entity: %w", backend, err)
	}
	return runtimeworkspace.WorkspaceEntityLookup{Slug: strings.TrimSpace(slug)}, nil
}

func ListContainersPostgres(ctx context.Context, db *sql.DB, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	return listContainers(ctx, db, `
		SELECT DISTINCT es.slug
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid
		  AND fi.config->>'instance_kind' = 'entity'
		ORDER BY 1
	`, "PostgreSQL", runID)
}

func ListContainersSQLite(ctx context.Context, db *sql.DB, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	return listContainers(ctx, db, `
		SELECT DISTINCT es.slug
		FROM entity_state es
		JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
		  AND json_extract(fi.config, '$.instance_kind') = 'entity'
		ORDER BY 1
	`, "SQLite", runID)
}

func listContainers(ctx context.Context, db *sql.DB, query, backend, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	rows, err := db.QueryContext(ctx, query, strings.TrimSpace(runID))
	if err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, fmt.Errorf("list %s runtime workspace entities: %w", backend, err)
	}
	defer rows.Close()

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
