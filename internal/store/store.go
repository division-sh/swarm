package store

import (
	"github.com/division-sh/swarm/internal/store/construction"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type PostgresStore = private.PostgresStore
type RuntimeStoreOrigin = private.RuntimeStoreOrigin
type SQLiteRuntimeStore = private.SQLiteRuntimeStore
type SchemaBootstrapRequest = private.SchemaBootstrapRequest
type SchemaBootstrapper = private.SchemaBootstrapper
type SchemaCompatibilityError = private.SchemaCompatibilityError
type SchemaDialect = private.SchemaDialect
type SchemaTableDDL = private.SchemaTableDDL

var DSNFromConfig = private.DSNFromConfig
var FlattenSchemaTableDDLs = private.FlattenSchemaTableDDLs
var GenerateNodeStateTableDDLs = private.GenerateNodeStateTableDDLs
var GeneratePlatformTableDDLs = private.GeneratePlatformTableDDLs
var NewPostgresStore = construction.NewPostgres
var NewSQLiteRuntimeStore = construction.NewSQLiteRuntime
var NodeStateFieldTypeToDDL = private.NodeStateFieldTypeToDDL
var QuoteIdent = private.QuoteIdent
var ResolveDatabasePassword = private.ResolveDatabasePassword
var SQLiteStatementsForPlan = private.SQLiteStatementsForPlan
var SchemaFieldTypeToDDL = private.SchemaFieldTypeToDDL
var StatementsForSchemaDialect = private.StatementsForSchemaDialect

const (
	RuntimeStoreMetadataTable = private.RuntimeStoreMetadataTable
	SchemaDialectPostgres     = private.SchemaDialectPostgres
	SchemaDialectSQLite       = private.SchemaDialectSQLite
)

var (
	ErrUnknownSchemaType = private.ErrUnknownSchemaType
)
