package delivery

import (
	"context"
	"database/sql"
	"errors"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type CompletionCandidateRequester interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type DeliveryPostgresOwner struct {
	*DeadLetterPostgresOwner
	backend           *postgresbackend.Backend
	candidateRequests CompletionCandidateRequester
}

type DeliverySQLiteOwner struct {
	*DeadLetterSQLiteOwner
	backend           *sqlitebackend.Backend
	candidateRequests CompletionCandidateRequester
	nowFn             func() time.Time
}

func NewDeliveryPostgresOwner(deadLetters *DeadLetterPostgresOwner, candidates CompletionCandidateRequester) (*DeliveryPostgresOwner, error) {
	if deadLetters == nil || deadLetters.backend == nil || candidates == nil {
		return nil, errors.New("delivery PostgreSQL owner dependencies are required")
	}
	return &DeliveryPostgresOwner{DeadLetterPostgresOwner: deadLetters, backend: deadLetters.backend, candidateRequests: candidates}, nil
}

func NewDeliverySQLiteOwner(deadLetters *DeadLetterSQLiteOwner, candidates CompletionCandidateRequester, now func() time.Time) (*DeliverySQLiteOwner, error) {
	if deadLetters == nil || deadLetters.backend == nil || candidates == nil {
		return nil, errors.New("delivery SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &DeliverySQLiteOwner{DeadLetterSQLiteOwner: deadLetters, backend: deadLetters.backend, candidateRequests: candidates, nowFn: now}, nil
}
