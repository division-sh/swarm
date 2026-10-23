package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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

func (unusedCandidateWriter) RequireActiveSourceTx(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error) {
	panic("source admission must not run before decision schema admission")
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
		{"postgres/card", func() error { _, err := postgres.InsertTx(ctx, attempt, decisioncard.Card{}); return err }},
		{"sqlite/card", func() error { _, err := sqlite.InsertTx(ctx, attempt, decisioncard.Card{}); return err }},
		{"postgres/proposed", func() error {
			_, err := postgres.InsertProposedEffectTx(ctx, attempt, decisioncard.Card{}, decisioncard.ProposedEffectContinuation{})
			return err
		}},
		{"sqlite/proposed", func() error {
			_, err := sqlite.InsertProposedEffectTx(ctx, attempt, decisioncard.Card{}, decisioncard.ProposedEffectContinuation{})
			return err
		}},
		{"postgres/run", func() error {
			_, err := postgres.SupersedeRunTx(ctx, attempt, "run", "stop", time.Now(), false)
			return err
		}},
		{"sqlite/run", func() error {
			_, err := sqlite.SupersedeRunTx(ctx, attempt, "run", "stop", time.Now(), false)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.write(); err == nil {
				t.Fatal("inactive attempt was accepted")
			}
		})
	}
}
