package runlifecycle

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func TestRunAdmissionGenericQueryTransactionStaysFreshAndCannotMint(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			mock.ExpectBegin()
			for range 2 {
				mock.ExpectQuery(admissionTestRunSQL(postgres, true)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
			mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			for range 2 {
				mock.ExpectQuery(admissionTestRunSQL(postgres, true)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					query := func() error {
						// Deliberately erase the typed transaction capability.
						var q runstate.RowQueryer = tx
						if postgres {
							return runstate.RequirePostgresActiveQuery(ctx, q, admissionTestRunID)
						}
						return runstate.RequireSQLiteActiveQuery(ctx, q, admissionTestRunID)
					}
					for range 2 {
						if err := query(); err != nil {
							return err
						}
						if _, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, admissionTestRunID); err != nil || cached {
							t.Fatalf("generic query minted admission: cached=%t err=%v", cached, err)
						}
					}
					if _, err := admissionTestLoadSource(ctx, tx, postgres, false, true); err != nil {
						return err
					}
					for range 2 {
						if err := query(); err != nil {
							return err
						}
					}
					_, err := admissionTestLoadSource(ctx, tx, postgres, false, true)
					return err
				})
			})
		})
	}
}

func TestRunAdmissionNonlockingMutationConsumesOnlyExistingAdmission(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			mock.ExpectBegin()
			for range 2 {
				mock.ExpectQuery(admissionTestRunSQL(postgres, true)).WithArgs(admissionTestRunID).
					WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			mock.ExpectQuery(admissionTestRunSQL(postgres, false)).WithArgs(admissionTestRunID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", admissionTestBundle))
			for range 3 {
				mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(admissionTestBundle).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			}
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					guard := func() error {
						if postgres {
							return runstate.RequirePostgresActiveNonlockingTx(ctx, tx, admissionTestRunID)
						}
						return runstate.RequireSQLiteActiveNonlockingTx(ctx, tx, admissionTestRunID)
					}
					for range 2 {
						if err := guard(); err != nil {
							return err
						}
						if _, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, admissionTestRunID); err != nil || cached {
							t.Fatalf("nonlocking first check minted admission: cached=%t err=%v", cached, err)
						}
					}
					if _, err := admissionTestLoadSource(ctx, tx, postgres, false, true); err != nil {
						return err
					}
					for range 2 {
						if err := guard(); err != nil {
							return err
						}
					}
					return nil
				})
			})
		})
	}
}
