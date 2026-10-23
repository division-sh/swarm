package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

const admissionTestRunID = "00000000-0000-4000-8000-000000000001"
const admissionTestBundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const admissionTestRevisedBundle = "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func admissionTestSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func admissionTestNativeAttempt(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock, postgres bool, candidates *runhandoff.CandidateCoordinator, observe func(context.Context, *mutationprotocol.Attempt) error) {
	t.Helper()
	rollback := errors.New("run admission observations complete")
	mock.ExpectRollback()
	write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		if err := observe(ctx, attempt); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, rollback
	}
	var result mutationprotocol.Result[struct{}]
	if postgres {
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		result = mutationprotocol.RunPostgres(context.Background(), backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, candidates, write)
	} else {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		result = mutationprotocol.RunSQLite(context.Background(), backend, "run admission statement counts", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, candidates, write)
	}
	if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
		t.Fatalf("native result = acknowledged %t, error %v; want observation rollback", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("exact SQL statement counts: %v", err)
	}
}

func admissionTestLoadSource(ctx context.Context, tx *sql.Tx, postgres, readOnly, requireActive bool) (runtimecorrelation.SourceArtifactFact, error) {
	if postgres {
		return (postgresRunLifecycleMutation{tx: tx, readOnly: readOnly}).loadSource(ctx, admissionTestRunID, requireActive)
	}
	return (sqliteRunLifecycleMutation{tx: tx, readOnly: readOnly}).loadSource(ctx, admissionTestRunID, requireActive)
}

func admissionTestRunSQL(postgres, readOnly bool) string {
	if !postgres {
		return "SELECT status, bundle_hash FROM runs WHERE run_id = ?"
	}
	query := "SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid"
	if !readOnly {
		query += " FOR UPDATE"
	}
	return query
}

func admissionTestArtifactSQL(postgres bool) string {
	if postgres {
		return "SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)"
	}
	return "SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)"
}

func admissionTestSource(t *testing.T, bundle string) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func admissionTestDialect(postgres bool) string {
	if postgres {
		return "postgres"
	}
	return "sqlite"
}
