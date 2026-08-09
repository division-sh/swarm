package runtimepersistence

import (
	"context"

	storeschema "github.com/division-sh/swarm/internal/store/internal/schemastore"
)

type RuntimeStoreOrigin = storeschema.RuntimeStoreOrigin
type SchemaBootstrapRequest = storeschema.SchemaBootstrapRequest
type SchemaBootstrapper = storeschema.SchemaBootstrapper
type SchemaCompatibilityError = storeschema.SchemaCompatibilityError
type SchemaDialect = storeschema.SchemaDialect
type SchemaTableDDL = storeschema.SchemaTableDDL
type SQLiteSchemaStore = storeschema.SQLite

const (
	RuntimeStoreMetadataTable = storeschema.RuntimeStoreMetadataTable
	SchemaDialectPostgres     = storeschema.SchemaDialectPostgres
	SchemaDialectSQLite       = storeschema.SchemaDialectSQLite
)

var (
	FlattenSchemaTableDDLs     = storeschema.FlattenSchemaTableDDLs
	GenerateNodeStateTableDDLs = storeschema.GenerateNodeStateTableDDLs
	GeneratePlatformTableDDLs  = storeschema.GeneratePlatformTableDDLs
	NodeStateFieldTypeToDDL    = storeschema.NodeStateFieldTypeToDDL
	QuoteIdent                 = storeschema.QuoteIdent
	SchemaFieldTypeToDDL       = storeschema.SchemaFieldTypeToDDL
	shellQuote                 = storeschema.ShellQuote
	SQLiteStatementsForPlan    = storeschema.SQLiteStatementsForPlan
	StatementsForSchemaDialect = storeschema.StatementsForSchemaDialect
	postgresPublicTables       = storeschema.PostgresPublicTables
	readRuntimeStoreOrigin     = storeschema.ReadRuntimeStoreOrigin
	sqliteUserTables           = storeschema.SQLiteUserTables
)

func (s *PostgresStore) acceptCurrentSchemaForTest() {
	if s != nil && s.schemaOwner != nil {
		s.schemaOwner.AcceptCurrentForTest()
	}
}

func (s *PostgresStore) BootstrapSchema(ctx context.Context, request SchemaBootstrapRequest) error {
	if s == nil || s.schemaOwner == nil {
		return storeschema.NewMissingPostgresOwnerError()
	}
	return s.schemaOwner.BootstrapSchema(ctx, request)
}

func (s *PostgresStore) requireCurrentSchema() error {
	if s == nil || s.schemaOwner == nil {
		return storeschema.NewMissingPostgresOwnerError()
	}
	return s.schemaOwner.RequireCurrent()
}

func (s *SQLiteRuntimeStore) requireCurrentSchema() error {
	if s == nil || s.schema == nil {
		return storeschema.NewMissingSQLiteOwnerError()
	}
	return s.schema.RequireCurrent()
}
