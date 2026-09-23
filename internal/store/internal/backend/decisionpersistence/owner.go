package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type DecisionPostgresOwner struct {
	backend           *postgresbackend.Backend
	requireCurrent    func() error
	candidateRequests mutationprotocol.CandidateWriter
	candidates        *runhandoff.CandidateCoordinator
}

type DecisionSQLiteOwner struct {
	backend           *sqlitebackend.Backend
	requireCurrent    func() error
	candidateRequests mutationprotocol.CandidateWriter
	candidates        *runhandoff.CandidateCoordinator
	nowFn             func() time.Time
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, candidateWriter mutationprotocol.CandidateWriter, candidates *runhandoff.CandidateCoordinator) (*DecisionPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || candidateWriter == nil || candidates == nil {
		return nil, errors.New("decision-card PostgreSQL owner dependencies are required")
	}
	return &DecisionPostgresOwner{backend: backend, requireCurrent: requireCurrent, candidateRequests: candidateWriter, candidates: candidates}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, candidateWriter mutationprotocol.CandidateWriter, candidates *runhandoff.CandidateCoordinator, now func() time.Time) (*DecisionSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || candidateWriter == nil || candidates == nil {
		return nil, errors.New("decision-card SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &DecisionSQLiteOwner{backend: backend, requireCurrent: requireCurrent, candidateRequests: candidateWriter, candidates: candidates, nowFn: now}, nil
}

func postgresDecisionMutation[T any](ctx context.Context, s *DecisionPostgresOwner, withCandidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) mutationprotocol.Result[T] {
	if s == nil || s.backend == nil || s.requireCurrent == nil {
		return mutationprotocol.Reject[T](errors.New("decision-card PostgreSQL owner is required"))
	}
	if err := s.requireCurrent(); err != nil {
		return mutationprotocol.Reject[T](err)
	}
	var candidates *runhandoff.CandidateCoordinator
	if withCandidates {
		candidates = s.candidates
	}
	return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, write)
}

func sqliteDecisionMutation[T any](ctx context.Context, s *DecisionSQLiteOwner, label string, withCandidates bool, write func(context.Context, *mutationprotocol.Attempt) (T, error)) mutationprotocol.Result[T] {
	if s == nil || s.backend == nil || s.requireCurrent == nil {
		return mutationprotocol.Reject[T](errors.New("decision-card SQLite owner is required"))
	}
	if err := s.requireCurrent(); err != nil {
		return mutationprotocol.Reject[T](err)
	}
	var candidates *runhandoff.CandidateCoordinator
	if withCandidates {
		candidates = s.candidates
	}
	return mutationprotocol.RunSQLite(ctx, s.backend, label, mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, write)
}

func withDecisionSQL[T any](ctx context.Context, attempt *mutationprotocol.Attempt, write func(context.Context, *sql.Tx) (T, error)) (T, error) {
	var value T
	err := attempt.WithSQL(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		value, err = write(txctx, tx)
		return err
	})
	return value, err
}

func writePostgresDecision(ctx context.Context, s *DecisionPostgresOwner, withCandidates bool, write func(context.Context, *mutationprotocol.Attempt) error) error {
	return postgresDecisionMutation(ctx, s, withCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}).Err()
}

func writeSQLiteDecision(ctx context.Context, s *DecisionSQLiteOwner, label string, withCandidates bool, write func(context.Context, *mutationprotocol.Attempt) error) error {
	return sqliteDecisionMutation(ctx, s, label, withCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}).Err()
}
