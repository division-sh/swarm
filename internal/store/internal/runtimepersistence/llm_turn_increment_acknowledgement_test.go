package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSessionTurnIncrementAcknowledgesPostcommitHandoffAndSurvivesRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store releaseAcknowledgementTestStore
			var db *sql.DB
			var sqlitePath, postgresDSN string
			sqlite := backend == "sqlite"
			if sqlite {
				sqlitePath = filepath.Join(t.TempDir(), "runtime.db")
				selected := newBootstrappedSQLiteRuntimeStoreForPath(t, sqlitePath)
				store, db = selected, selected.backend.ConstructionHandle()
			} else {
				dsn, postgresDB, _ := testutil.StartPostgres(t)
				store, db, postgresDSN = admitTestPostgresStore(t, postgresDB), postgresDB, dsn
			}
			fixture := newCompletionSettlementFixture(t, store, db, sqlite)
			ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
			identity := fixture.authority.Target.AgentIdentity
			bundleHash := releaseTestBundleHash(t, ctx, db, sqlite, identity.RunID)
			injected := errors.New("injected session turn postcommit handoff failure")
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
			result, err := store.IncrementTurnOutcome(ctx, identity, fixture.sessionID)
			if !result.Acknowledged || !errors.Is(err, injected) || submits != 1 {
				t.Fatalf("turn result=%+v err=%v submissions=%d", result, err, submits)
			}
			var restartedDB *sql.DB
			if sqlite {
				restarted, err := NewSQLiteRuntimeStore(sqlitePath)
				if err != nil {
					t.Fatal(err)
				}
				defer restarted.Close()
				restartedDB = restarted.backend.ConstructionHandle()
			} else {
				restarted, err := NewPostgresStore(postgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer restarted.Close()
				restartedDB = restarted.backend.ConstructionHandle()
			}
			query := `SELECT turn_count FROM agent_sessions WHERE session_id=?`
			if !sqlite {
				query = `SELECT turn_count FROM agent_sessions WHERE session_id=$1::uuid`
			}
			var turns int
			if err := restartedDB.QueryRowContext(context.Background(), query, fixture.sessionID).Scan(&turns); err != nil {
				t.Fatal(err)
			}
			if turns != 1 {
				t.Fatalf("restarted turn count=%d, want exactly one", turns)
			}
		})
	}
}
