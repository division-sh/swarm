package runtimepersistence

import (
	"context"
	"errors"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeworkspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func (s *PostgresStore) LookupWorkspaceEntity(ctx context.Context, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	if s == nil || s.workspaceLookups == nil {
		return runtimeworkspace.WorkspaceEntityLookup{}, errors.New("PostgreSQL workspace lookup store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkspace.WorkspaceEntityLookup{}, err
	}
	return s.workspaceLookups.LookupEntity(ctx, identity)
}

func (s *SQLiteRuntimeStore) LookupWorkspaceEntity(ctx context.Context, identity runtimecurrentstate.Identity) (runtimeworkspace.WorkspaceEntityLookup, error) {
	if s == nil || s.workspaceLookups == nil {
		return runtimeworkspace.WorkspaceEntityLookup{}, errors.New("SQLite workspace lookup store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkspace.WorkspaceEntityLookup{}, err
	}
	return s.workspaceLookups.LookupEntity(ctx, identity)
}

func (s *PostgresStore) ListRuntimeWorkspaceContainers(ctx context.Context, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	if s == nil || s.workspaceLookups == nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, errors.New("PostgreSQL workspace lookup store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, err
	}
	return s.workspaceLookups.ListContainers(ctx, runID)
}

func (s *SQLiteRuntimeStore) ListRuntimeWorkspaceContainers(ctx context.Context, runID string) (runtimeworkspace.RuntimeWorkspaceContainerSet, error) {
	if s == nil || s.workspaceLookups == nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, errors.New("SQLite workspace lookup store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkspace.RuntimeWorkspaceContainerSet{}, err
	}
	return s.workspaceLookups.ListContainers(ctx, runID)
}
