// Package authoractivityfixture defines the dialect used by event fixtures.
package authoractivityfixture

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)
