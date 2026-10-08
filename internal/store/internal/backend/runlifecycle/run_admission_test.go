package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

const admissionTestRunID = "00000000-0000-4000-8000-000000000001"
const admissionTestBundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const admissionTestRevisedBundle = "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestRunAdmissionMutableSourceStatementCount(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			fact := admissionTestSource(t, admissionTestBundle)
			mock.ExpectBegin()
			// Three independent SQL loans share one run-row query, not the artifact checks.
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
			for range 3 {
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				for range 3 {
					if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
						if err != nil {
							return err
						}
						if !got.Matches(fact) {
							t.Fatalf("source = %q, want %q", got.BundleHash(), fact.BundleHash())
						}
						return attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, got)
					}); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
}

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
				db, mock := admissionTestSQLMock(t)
				fact := admissionTestSource(t, admissionTestBundle)
				mock.ExpectBegin()
				// Two non-minting reads, one mutable admission, then a non-mutable read
				// despite existing admission: four run queries and five artifact checks.
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
				admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
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

func TestRunAdmissionMissingArtifactAfterAdmission(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			mock.ExpectBegin()
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
			for _, exists := range []bool{true, false} {
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
			}
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					fact, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					if err != nil {
						return err
					}
					if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, fact); err != nil {
						return err
					}
					got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
					if !errors.As(err, &unavailable) || unavailable.BundleHash != admissionTestBundle || unavailable.Cause != "missing_source_artifact" {
						t.Fatalf("cached source with missing artifact = %v, want exact unavailable error", err)
					}
					if got.Validate() == nil {
						t.Fatal("missing artifact returned a valid source")
					}
					return nil
				})
			})
		})
	}
}

func TestRunAdmissionMissingArtifactCannotMint(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			fact := admissionTestSource(t, admissionTestBundle)
			mock.ExpectBegin()
			// A failed first admission must leave the second use reading the run again.
			for _, exists := range []bool{false, true} {
				mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
			}
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
					if !errors.As(err, &unavailable) || unavailable.BundleHash != admissionTestBundle {
						t.Fatalf("first source read = %v, want unavailable artifact", err)
					}
					if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, fact); err == nil {
						t.Fatal("missing artifact minted admission")
					}
					got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					if err != nil {
						return err
					}
					return attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, got)
				})
			})
		})
	}
}

func TestRunAdmissionCanonicalSourceRevisionReloads(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			candidates := runhandoff.NewCandidateCoordinator()
			revised := admissionTestSource(t, admissionTestRevisedBundle)
			selectedNow := time.Unix(1700000000, 0).UTC()
			mock.ExpectBegin()
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("paused", admissionTestBundle))
			for range 2 {
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			if postgres {
				mock.ExpectExec("LOCK TABLE runs IN ROW EXCLUSIVE MODE").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestRevisedBundle).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			if postgres {
				mock.ExpectExec("UPDATE runs SET bundle_hash = $2 WHERE run_id = $1::uuid AND status IN ('running', 'paused')").
					WithArgs(admissionTestRunID, admissionTestRevisedBundle).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT LOWER(status), bundle_hash, completion_due_at, completion_revision, clock_timestamp() FROM runs WHERE run_id = $1::uuid FOR UPDATE").
					WithArgs(admissionTestRunID).WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash", "completion_due_at", "completion_revision", "now"}).
					AddRow("paused", admissionTestRevisedBundle, nil, 0, selectedNow))
			} else {
				mock.ExpectExec("UPDATE runs SET bundle_hash = ? WHERE run_id = ? AND status IN ('running', 'paused')").
					WithArgs(admissionTestRevisedBundle, admissionTestRunID).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("SELECT LOWER(status), bundle_hash, completion_due_at, completion_revision FROM runs WHERE run_id = ?").
					WithArgs(admissionTestRunID).WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash", "completion_due_at", "completion_revision"}).
					AddRow("paused", admissionTestRevisedBundle, nil, 0))
			}
			// The canonical writer must invalidate the old admission; only the reload mints the new one.
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("paused", admissionTestRevisedBundle))
			for range 2 {
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestRevisedBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			admissionTestNativeAttempt(t, db, mock, postgres, candidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					original, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					if err != nil {
						return err
					}
					if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, original); err != nil {
						return err
					}
					request := runtimerunlifecycle.SourceRevisionRequest{RunID: admissionTestRunID, Source: revised}
					var disposition runtimerunlifecycle.MutationDisposition
					if postgres {
						backend, backendErr := postgresbackend.New(db)
						if backendErr != nil {
							return backendErr
						}
						owner, ownerErr := NewPostgres(backend, func() error { return nil }, candidates)
						if ownerErr != nil {
							return ownerErr
						}
						disposition, err = (postgresRunLifecycleMutation{store: owner, tx: tx, attempt: attempt}).ReviseSource(ctx, request)
					} else {
						backend, backendErr := sqlitebackend.New(db)
						if backendErr != nil {
							return backendErr
						}
						owner, ownerErr := NewSQLite(backend, func() error { return nil }, candidates, func() time.Time { return selectedNow })
						if ownerErr != nil {
							return ownerErr
						}
						disposition, err = (sqliteRunLifecycleMutation{store: owner, tx: tx, attempt: attempt}).ReviseSource(ctx, request)
					}
					if err != nil {
						return err
					}
					if disposition != runtimerunlifecycle.MutationApplied {
						t.Fatalf("source revision = %q, want applied", disposition)
					}
					for _, fact := range []runtimecorrelation.SourceArtifactFact{original, revised} {
						if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, fact); err == nil {
							t.Fatalf("source %q admitted before canonical reload", fact.BundleHash())
						}
					}
					for range 2 {
						got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
						if err != nil {
							return err
						}
						if !got.Matches(revised) {
							t.Fatalf("source after revision = %q, want %q", got.BundleHash(), revised.BundleHash())
						}
						if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, got); err != nil {
							return err
						}
					}
					if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, original); err == nil {
						t.Fatal("old source remained admitted after canonical reload")
					}
					return nil
				})
			})
		})
	}
}

func TestRunAdmissionInvalidationReloadsTerminalState(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, state := range []runtimerunlifecycle.State{runtimerunlifecycle.StateCompleted, runtimerunlifecycle.StateCancelled} {
			t.Run(admissionTestDialect(postgres)+"/"+string(state), func(t *testing.T) {
				db, mock := admissionTestSQLMock(t)
				mock.ExpectBegin()
				mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow(string(state), admissionTestBundle))
				admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						fact, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
						if err != nil {
							return err
						}
						if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, fact); err != nil {
							return err
						}
						if err := mutationprotocol.InvalidateActiveRunSource(ctx, tx, admissionTestRunID); err != nil {
							return err
						}
						got, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
						var inactive *runtimerunlifecycle.RunNotActiveError
						if !errors.As(err, &inactive) || inactive.RunID != admissionTestRunID || inactive.State != state {
							t.Fatalf("source after invalidation = %v, want exact %s state rejection", err, state)
						}
						if got.Validate() == nil {
							t.Fatal("terminal run returned a valid source")
						}
						if err := attempt.RequireActiveRunSourceAdmission(ctx, admissionTestRunID, fact); err == nil {
							t.Fatal("terminal run retained active admission")
						}
						return nil
					})
				})
			})
		}
	}
}

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
