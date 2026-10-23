package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

const admissionTestRunID = "00000000-0000-4000-8000-000000000001"
const admissionTestBundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRunAdmissionNonMutableSourceCannotMint(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, mode := range []struct {
			name          string
			readOnly      bool
			requireActive bool
			withoutNative bool
		}{
			{name: "read_only", readOnly: true, requireActive: true},
			{name: "present"},
			{name: "without_native_context", requireActive: true, withoutNative: true},
		} {
			t.Run(admissionTestDialect(postgres)+"/"+mode.name, func(t *testing.T) {
				db, mock, original := admissionTestSQLMock(t, postgres)
				fact := admissionTestSource(t, admissionTestBundle)
				mock.ExpectBegin()
				expectAdmissionTestGeneration(mock, postgres, false)
				// Non-minting reads remain fresh even after a mutable admission.
				for range 2 {
					mock.ExpectQuery(admissionTestRunSQL(postgres, mode.readOnly)).WithArgs(admissionTestRunID).
						WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
					mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
						WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				}
				mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery(admissionTestRunSQL(postgres, mode.readOnly)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				for range 2 {
					mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
						WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				}
				admissionTestNativeAttempt(t, db, mock, postgres, original, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						readCtx := ctx
						if mode.withoutNative {
							readCtx = runtimecorrelation.WithSourceArtifactFact(context.Background(), fact)
						}
						for range 2 {
							got, err := admissionTestLoadSource(readCtx, tx, postgres, mode.readOnly, mode.requireActive)
							if err != nil {
								return err
							}
							if !got.Matches(fact) {
								t.Fatalf("non-mutable source = %q, want %q", got.BundleHash(), fact.BundleHash())
							}
							if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, got); err == nil {
								t.Fatal("non-mutable source read minted native admission")
							}
						}
						got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
						if err != nil {
							return err
						}
						if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, got); err != nil {
							return err
						}
						if _, err := admissionTestLoadSource(readCtx, tx, postgres, mode.readOnly, mode.requireActive); err != nil {
							return err
						}
						_, err = admissionTestLoadSource(ctx, tx, postgres, false, true)
						return err
					})
				})
			})
		}
	}
}

func admissionTestIdentity(postgres bool) processbinding.LeaseIdentity {
	backend := processbinding.SQLiteLeaseBackend
	if postgres {
		backend = processbinding.PostgresLeaseBackend
	}
	return processbinding.LeaseIdentity{
		LeaseID: "00000000-0000-4000-8000-000000000011", OwnerID: "source-reader-original",
		BootID: "00000000-0000-4000-8000-000000000012", RuntimeInstanceID: "00000000-0000-4000-8000-000000000013", Backend: backend,
	}
}

const admissionTestGenerationSQL = `SELECT generation, CAST(lease_id AS TEXT), owner_id, CAST(boot_id AS TEXT),
		CAST(runtime_instance_id AS TEXT), backend
		FROM runtime_process_generation WHERE singleton_id = 1`

func expectAdmissionTestGeneration(mock sqlmock.Sqlmock, postgres, acquisition bool) {
	query := admissionTestGenerationSQL
	if postgres {
		if acquisition {
			mock.ExpectExec("SELECT pg_advisory_xact_lock(1398231629, 1)").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE runtime_process_generation SET generation = generation WHERE singleton_id = 1").WillReturnResult(sqlmock.NewResult(0, 1))
			query += " FOR UPDATE"
		} else {
			mock.ExpectExec("SELECT pg_advisory_xact_lock_shared(1398231629, 1)").WillReturnResult(sqlmock.NewResult(0, 1))
			query += " FOR SHARE"
		}
	}
	rows := sqlmock.NewRows([]string{"generation", "lease_id", "owner_id", "boot_id", "runtime_instance_id", "backend"})
	if acquisition {
		rows.AddRow(0, nil, nil, nil, nil, nil)
	} else {
		identity := admissionTestIdentity(postgres)
		rows.AddRow(1, identity.LeaseID, identity.OwnerID, identity.BootID, identity.RuntimeInstanceID, identity.Backend)
	}
	mock.ExpectQuery(query).WillReturnRows(rows)
}

func admissionTestSQLMock(t *testing.T, postgres bool) (*sql.DB, sqlmock.Sqlmock, context.Context) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	var acquire func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)
	if postgres {
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		const key = "source-reader-original"
		mock.ExpectExec("SELECT pg_advisory_lock(hashtext($1))").WithArgs(key).WillReturnResult(sqlmock.NewResult(0, 1))
		lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, backend, key)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			mock.ExpectQuery("SELECT pg_advisory_unlock(hashtext($1))").WithArgs(key).
				WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
			if err := lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
		acquire = lease.RunProcessAcquisitionOutcome
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
			AND classid::bigint = CASE WHEN hashtext($1) < 0 THEN 4294967295::bigint ELSE 0::bigint END
			AND objid::bigint = (hashtext($1)::bigint & 4294967295::bigint) AND objsubid = 1
		)`).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"held"}).AddRow(true))
	} else {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		acquire = backend.RunProcessAcquisitionOutcome
		mock.ExpectBegin()
	}
	expectAdmissionTestGeneration(mock, postgres, true)
	identity := admissionTestIdentity(postgres)
	mock.ExpectExec(`UPDATE runtime_process_generation
		SET generation = $1, lease_id = $2, owner_id = $3, boot_id = $4, runtime_instance_id = $5, backend = $6
		WHERE singleton_id = 1 AND generation = $7`).
		WithArgs(int64(1), identity.LeaseID, identity.OwnerID, identity.BootID, identity.RuntimeInstanceID, identity.Backend, int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	var original processbinding.LeaseStamp
	acknowledged, err := acquire(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(txctx, tx, identity, !postgres)
		return err
	})
	if err != nil || !acknowledged {
		t.Fatalf("original source-reader acquisition: acknowledged=%v err=%v", acknowledged, err)
	}
	ctx, err = generationauthority.MutationContext(ctx, original, false)
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, ctx
}

func admissionTestNativeAttempt(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock, postgres bool, original context.Context, observe func(context.Context, *mutationprotocol.Attempt) error) {
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
		result = mutationprotocol.RunPostgres(original, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	} else {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		result = mutationprotocol.RunSQLite(original, backend, "run admission observations", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
		t.Fatalf("native result = acknowledged %t, error %v; want observation rollback", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("source-reader entry expectations: %v", err)
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
		query += " FOR SHARE"
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
