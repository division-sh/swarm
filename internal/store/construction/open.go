// Package construction owns the raw database handles used while assembling a
// serve process. Runtime consumers receive only the typed store returned with
// each handle.
package construction

import (
	"database/sql"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func OpenPostgres(dsn string) (*private.PostgresStore, *sql.DB, error) {
	return private.OpenPostgresStore(dsn)
}

func OpenSQLiteRuntime(path string) (*private.SQLiteRuntimeStore, *sql.DB, error) {
	return private.OpenSQLiteRuntimeStore(path)
}
