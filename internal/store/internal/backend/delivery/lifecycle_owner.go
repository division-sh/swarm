package delivery

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

type CompletionCandidateRequester interface {
	RequestCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time, *runhandoff.CandidateHandoff) (runtimerunlifecycle.CandidateRequestResult, error)
}

type ReceiverExecutionAdmission interface {
	ReceiverExecutionReadyTx(context.Context, *sql.Tx, events.DeliveryRoute, deliverylifecycle.ExecutionAuthority, bool) (bool, error)
}

type ReceiverTargetPersistence interface {
	ReceiverMaterializedTx(context.Context, *sql.Tx, events.DeliveryRoute) (bool, error)
}

func (s *DeliveryPostgresOwner) BindReceiverTargetPersistence(owner ReceiverTargetPersistence) error {
	if s == nil || owner == nil || s.receiverAdapter.receiverTarget != nil {
		return errors.New("receiver target persistence must be bound exactly once")
	}
	s.receiverAdapter.receiverTarget = owner
	return nil
}

func (s *DeliverySQLiteOwner) BindReceiverTargetPersistence(owner ReceiverTargetPersistence) error {
	if s == nil || owner == nil || s.receiverAdapter.receiverTarget != nil {
		return errors.New("receiver target persistence must be bound exactly once")
	}
	s.receiverAdapter.receiverTarget = owner
	return nil
}

type DeliveryPostgresOwner struct {
	*DeadLetterPostgresOwner
	backend           *postgresbackend.Backend
	candidateRequests CompletionCandidateRequester
	receiverAdapter   *Adapter
}

type DeliverySQLiteOwner struct {
	*DeadLetterSQLiteOwner
	backend           *sqlitebackend.Backend
	candidateRequests CompletionCandidateRequester
	nowFn             func() time.Time
	receiverAdapter   *Adapter
}

func NewDeliveryPostgresOwner(deadLetters *DeadLetterPostgresOwner, candidates CompletionCandidateRequester, receiver ReceiverExecutionAdmission) (*DeliveryPostgresOwner, error) {
	if deadLetters == nil || deadLetters.backend == nil || candidates == nil || receiver == nil {
		return nil, errors.New("delivery PostgreSQL owner dependencies are required")
	}
	adapter := &Adapter{dialect: DialectPostgres, receiverExecution: receiver}
	return &DeliveryPostgresOwner{DeadLetterPostgresOwner: deadLetters, backend: deadLetters.backend, candidateRequests: candidates, receiverAdapter: adapter}, nil
}

func NewDeliverySQLiteOwner(deadLetters *DeadLetterSQLiteOwner, candidates CompletionCandidateRequester, receiver ReceiverExecutionAdmission, now func() time.Time) (*DeliverySQLiteOwner, error) {
	if deadLetters == nil || deadLetters.backend == nil || candidates == nil || receiver == nil {
		return nil, errors.New("delivery SQLite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	adapter := &Adapter{dialect: DialectSQLite, receiverExecution: receiver}
	return &DeliverySQLiteOwner{DeadLetterSQLiteOwner: deadLetters, backend: deadLetters.backend, candidateRequests: candidates, nowFn: now, receiverAdapter: adapter}, nil
}
