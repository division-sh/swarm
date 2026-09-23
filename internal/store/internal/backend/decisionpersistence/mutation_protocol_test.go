package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type unusedCandidateWriter struct{}

func (unusedCandidateWriter) WriteCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time) (runtimerunlifecycle.CandidateRequestResult, error) {
	panic("candidate writer must not run before decision schema admission")
}

func TestDecisionMutationGuardPrecedesTransactionBothStores(t *testing.T) {
	guardErr := errors.New("decision schema not ready")
	for _, dialect := range []string{"postgres", "sqlite"} {
		t.Run(dialect, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			candidates := runhandoff.NewCandidateCoordinator()
			var create func() error
			var supersede func() error
			if dialect == "postgres" {
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				owner, err := NewPostgres(backend, func() error { return guardErr }, unusedCandidateWriter{}, candidates)
				if err != nil {
					t.Fatal(err)
				}
				create = func() error { return owner.CreateDecisionCard(context.Background(), decisioncard.Card{}) }
				supersede = func() error {
					return owner.SupersedeDecisionCardsForStage(context.Background(), "run", "entity", "activation", "stop", time.Now())
				}
			} else {
				backend, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				owner, err := NewSQLite(backend, func() error { return guardErr }, unusedCandidateWriter{}, candidates, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				create = func() error { return owner.CreateDecisionCard(context.Background(), decisioncard.Card{}) }
				supersede = func() error {
					return owner.SupersedeDecisionCardsForStage(context.Background(), "run", "entity", "activation", "stop", time.Now())
				}
			}
			if err := create(); !errors.Is(err, guardErr) {
				t.Fatalf("create guard error = %v", err)
			}
			if err := supersede(); !errors.Is(err, guardErr) {
				t.Fatalf("supersede guard error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComposedDecisionWritersRefuseInactiveAttempt(t *testing.T) {
	ctx := context.Background()
	attempt := &mutationprotocol.Attempt{}
	postgres := &DecisionPostgresOwner{}
	sqlite := &DecisionSQLiteOwner{}
	for _, test := range []struct {
		name  string
		write func() error
	}{
		{"postgres/card", func() error { return postgres.InsertTx(ctx, attempt, decisioncard.Card{}) }},
		{"sqlite/card", func() error { return sqlite.InsertTx(ctx, attempt, decisioncard.Card{}) }},
		{"postgres/proposed", func() error {
			return postgres.InsertProposedEffectTx(ctx, attempt, decisioncard.Card{}, decisioncard.ProposedEffectContinuation{})
		}},
		{"sqlite/proposed", func() error {
			return sqlite.InsertProposedEffectTx(ctx, attempt, decisioncard.Card{}, decisioncard.ProposedEffectContinuation{})
		}},
		{"postgres/run", func() error { return postgres.SupersedeRunTx(ctx, attempt, "run", "stop", time.Now(), false) }},
		{"sqlite/run", func() error { return sqlite.SupersedeRunTx(ctx, attempt, "run", "stop", time.Now(), false) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.write(); err == nil {
				t.Fatal("inactive attempt was accepted")
			}
		})
	}
}

func TestDecisionDraftExpiryAttemptNoopAndRollbackBothStores(t *testing.T) {
	for _, dialect := range []string{"postgres", "sqlite"} {
		for _, failure := range []bool{false, true} {
			name := dialect + "/no_op"
			if failure {
				name = dialect + "/rollback"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				mock.ExpectBegin()
				if dialect == "postgres" {
					mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
				} else {
					mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("UPDATE author_activity_order SET last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
				}
				read := mock.ExpectQuery("SELECT input_draft_id, run_id, card_id, expires_at FROM decision_card_input_drafts")
				readErr := errors.New("draft read failed")
				if failure {
					read.WillReturnError(readErr)
					mock.ExpectRollback()
				} else {
					read.WillReturnRows(sqlmock.NewRows([]string{"input_draft_id", "run_id", "card_id", "expires_at"}))
					mock.ExpectCommit()
				}
				var count int
				var callErr error
				if dialect == "postgres" {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					owner, err := NewPostgres(backend, func() error { return nil }, unusedCandidateWriter{}, runhandoff.NewCandidateCoordinator())
					if err != nil {
						t.Fatal(err)
					}
					count, callErr = owner.ExpireDecisionCardInputDrafts(context.Background(), time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC))
				} else {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					owner, err := NewSQLite(backend, func() error { return nil }, unusedCandidateWriter{}, runhandoff.NewCandidateCoordinator(), time.Now)
					if err != nil {
						t.Fatal(err)
					}
					count, callErr = owner.ExpireDecisionCardInputDrafts(context.Background(), time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC))
				}
				if count != 0 {
					t.Fatalf("draft count = %d, want 0", count)
				}
				if failure && !errors.Is(callErr, readErr) {
					t.Fatalf("rollback error = %v, want %v", callErr, readErr)
				}
				if !failure && callErr != nil {
					t.Fatalf("no-op error = %v", callErr)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
