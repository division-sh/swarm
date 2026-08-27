package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type CompletionCandidateRequester interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type DecisionPostgresOwner struct {
	backend           *postgresbackend.Backend
	requireCurrent    func() error
	candidateRequests CompletionCandidateRequester
}

type DecisionSQLiteOwner struct {
	backend           *sqlitebackend.Backend
	requireCurrent    func() error
	candidateRequests CompletionCandidateRequester
	nowFn             func() time.Time
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, candidates CompletionCandidateRequester) (*DecisionPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || candidates == nil {
		return nil, errors.New("decision-card PostgreSQL owner dependencies are required")
	}
	return &DecisionPostgresOwner{backend: backend, requireCurrent: requireCurrent, candidateRequests: candidates}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, candidates CompletionCandidateRequester, now func() time.Time) (*DecisionSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || candidates == nil {
		return nil, errors.New("decision-card SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &DecisionSQLiteOwner{backend: backend, requireCurrent: requireCurrent, candidateRequests: candidates, nowFn: now}, nil
}

func (s *DecisionPostgresOwner) runPrivateAuthorActivityMutation(ctx context.Context, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if s == nil || s.backend == nil || s.requireCurrent == nil {
		return errors.New("decision-card PostgreSQL owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func (s *DecisionSQLiteOwner) runPrivateAuthorActivityMutation(ctx context.Context, label string, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
	if s == nil || s.backend == nil || s.requireCurrent == nil {
		return errors.New("decision-card SQLite owner is required")
	}
	if err := s.requireCurrent(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, label, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := operation(txctx, tx, story); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) runtimeauthoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

func (s *DecisionPostgresOwner) requestCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, handoff *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error) {
	return s.candidateRequests.RequestCompletionCandidateTx(ctx, tx, runID, dueAt, handoff)
}

func (s *DecisionSQLiteOwner) requestCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, dueAt *time.Time, handoff *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error) {
	return s.candidateRequests.RequestCompletionCandidateTx(ctx, tx, runID, dueAt, handoff)
}
