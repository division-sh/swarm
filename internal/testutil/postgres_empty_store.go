package testutil

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testpostgres"
)

// EmptyPostgresStore retains administrative reset authority without lending a pool.
type EmptyPostgresStore struct{ sandbox *testpostgres.Sandbox }

func StartEmptyPostgresStore(t *testing.T) (string, EmptyPostgresStore) {
	t.Helper()
	sandbox, _ := startPostgresSandbox(t, false)
	dsn, err := sandbox.Connection.String()
	if err != nil {
		t.Fatal(err)
	}
	return dsn, EmptyPostgresStore{sandbox: sandbox}
}

func (s EmptyPostgresStore) PublicTableCount(ctx context.Context) (int, error) {
	return s.sandbox.PublicTableCount(ctx)
}

// ResetPublicSchema is only for a stopped disposable fixture, not a runtime reset.
func (s EmptyPostgresStore) ResetPublicSchema(ctx context.Context) error {
	return s.sandbox.ResetPublicSchema(ctx)
}
