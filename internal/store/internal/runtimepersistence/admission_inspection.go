package runtimepersistence

import (
	"context"
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
