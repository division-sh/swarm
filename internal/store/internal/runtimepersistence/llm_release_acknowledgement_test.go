package runtimepersistence

import (
	"context"
	"database/sql"

	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"

	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

type releaseAcknowledgementTestStore interface {
	completionSettlementTestStore
	runtimesessions.Registry
}

func TestSessionReleaseOutcomeAcknowledgesPostcommitHandoffErrorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store releaseAcknowledgementTestStore
			var db *sql.DB
			sqlite := backend == "sqlite"
			if sqlite {
				selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				store, db = selected, selected.backend.ConstructionHandle()
			} else {
				_, postgresDB, _ := testutil.StartPostgres(t)
				store, db = admitTestPostgresStore(t, postgresDB), postgresDB
			}
			fixture := newCompletionSettlementFixture(t, store, db, sqlite)
			ctx := runtimeeffects.WithDifferentOwner(fixture.context, runtimeeffects.OwnerBuildTestInfrastructure)
			identity := fixture.authority.Target.AgentIdentity
			lease := &runtimesessions.Lease{SessionID: fixture.sessionID, GrantID: fixture.grantID, Identity: identity, LockOwner: fixture.leaseHolder}
			result, err := store.ReleaseOutcome(ctx, lease)
			if !result.Acknowledged || err != nil {
				t.Fatalf("release result=%+v err=%v", result, err)
			}
			if holder := releaseTestLeaseHolder(t, ctx, db, sqlite, fixture.sessionID); holder != "" {
				t.Fatalf("acknowledged release kept lease holder %q", holder)
			}
			result, err = store.ReleaseOutcome(ctx, lease)
			if result.Acknowledged || err == nil {
				t.Fatalf("duplicate release result=%+v err=%v", result, err)
			}
		})
	}
}

func releaseTestLeaseHolder(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, sessionID string) string {
	t.Helper()
	query := `SELECT COALESCE(lease_holder, '') FROM agent_sessions WHERE session_id=?`
	if !sqlite {
		query = `SELECT COALESCE(lease_holder, '') FROM agent_sessions WHERE session_id=$1::uuid`
	}
	var holder string
	if err := db.QueryRowContext(ctx, query, sessionID).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	return holder
}
