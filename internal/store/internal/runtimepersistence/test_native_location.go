package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/tests/support/testutil"
)

// PostgresFixtureLocationForTest retains the sandbox's native location, never
// a running store's pool or transaction authority.
func PostgresFixtureLocationForTest(t *testing.T) string {
	t.Helper()
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return dsn
}
