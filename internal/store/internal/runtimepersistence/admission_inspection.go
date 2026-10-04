package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

func (s *PostgresStore) InspectSnapshot(ctx context.Context, inspect func(context.Context) error) error {
	return s.backend.InspectSnapshot(ctx, inspect)
}

func (s *SQLiteRuntimeStore) InspectSnapshot(ctx context.Context, inspect func(context.Context) error) error {
	return s.backend.InspectSnapshot(ctx, inspect)
}

func (s *PostgresStore) InspectSchema(ctx context.Context, request SchemaBootstrapRequest) (SchemaInspection, error) {
	return s.schemaOwner.InspectSchema(ctx, request)
}

func (s *SQLiteRuntimeStore) InspectSchema(ctx context.Context, request SchemaBootstrapRequest) (SchemaInspection, error) {
	return s.schema.InspectSchema(ctx, request)
}

func (s *PostgresStore) InspectPendingResetOperations(ctx context.Context) ([]destructivereset.Operation, error) {
	return s.startupPostgresOwner.InspectPendingResetOperations(ctx)
}

func (s *SQLiteRuntimeStore) InspectPendingResetOperations(ctx context.Context) ([]destructivereset.Operation, error) {
	return s.startupSQLiteOwner.InspectPendingResetOperations(ctx)
}
