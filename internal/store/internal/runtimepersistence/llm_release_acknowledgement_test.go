package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

type releaseAcknowledgementTestStore interface {
	completionSettlementTestStore
	runtimesessions.Registry
	runtimerunlifecycle.CandidateRegistrar
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
			ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
			identity := fixture.authority.Target.AgentIdentity
			bundleHash := releaseTestBundleHash(t, ctx, db, sqlite, identity.RunID)
			injected := errors.New("injected session release postcommit handoff failure")
			submits := 0
			registration, err := store.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
				submits++
				if candidate.RunID != identity.RunID {
					t.Errorf("wrong candidate run: %+v", candidate)
				}
				return injected
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			lease := &runtimesessions.Lease{SessionID: fixture.sessionID, Identity: identity, LockOwner: fixture.leaseHolder}
			result, err := store.ReleaseOutcome(ctx, lease)
			if !result.Acknowledged || !errors.Is(err, injected) || submits != 1 {
				t.Fatalf("release result=%+v err=%v submissions=%d", result, err, submits)
			}
			if holder := releaseTestLeaseHolder(t, ctx, db, sqlite, fixture.sessionID); holder != "" {
				t.Fatalf("acknowledged release kept lease holder %q", holder)
			}
			result, err = store.ReleaseOutcome(ctx, lease)
			if result.Acknowledged || err == nil || submits != 1 {
				t.Fatalf("duplicate release result=%+v err=%v submissions=%d", result, err, submits)
			}
		})
	}
}

func releaseTestBundleHash(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, runID string) string {
	t.Helper()
	query := `SELECT bundle_hash FROM runs WHERE run_id=?`
	if !sqlite {
		query = `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	}
	var bundleHash string
	if err := db.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
		t.Fatal(err)
	}
	return bundleHash
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
