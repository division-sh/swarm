package store

import "context"

type SchemaDialect string

const (
	SchemaDialectPostgres SchemaDialect = "postgres"
	SchemaDialectSQLite   SchemaDialect = "sqlite"
)

type SchemaBootstrapper interface {
	EnsureSchemaTables(context.Context, []SchemaTableDDL) error
	ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error)
}
