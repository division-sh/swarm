package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type workflowStoreDialect string

const (
	workflowStoreDialectPostgres workflowStoreDialect = "postgres"
	workflowStoreDialectSQLite   workflowStoreDialect = "sqlite"
)

type runtimeMutationRunner interface {
	RunRuntimeMutationContext(context.Context, func(context.Context) error) error
}

type workflowPersistenceFixture struct {
	db      *sql.DB
	dialect workflowStoreDialect
	runner  runtimeMutationRunner
}

var workflowPersistenceFixtures sync.Map

func registerWorkflowPersistenceFixture(store *workflowInstanceStore, db *sql.DB, dialect workflowStoreDialect, runner runtimeMutationRunner) *workflowInstanceStore {
	if store != nil {
		workflowPersistenceFixtures.Store(store, workflowPersistenceFixture{db: db, dialect: dialect, runner: runner})
	}
	return store
}

func workflowStoreForRecordingRunner(runner *recordingRuntimeMutationRunner) *workflowInstanceStore {
	dialect := runner.dialect
	if dialect == "" {
		dialect = workflowStoreDialectSQLite
	}
	store := newWorkflowPersistenceFixtureStore(runner)
	store.decisionCards = runner.decisionCards
	return registerWorkflowPersistenceFixture(store, runner.db, dialect, runner)
}

func (s *workflowInstanceStore) testFixture() workflowPersistenceFixture {
	if s == nil {
		return workflowPersistenceFixture{}
	}
	value, _ := workflowPersistenceFixtures.Load(s)
	fixture, _ := value.(workflowPersistenceFixture)
	return fixture
}

func (s *workflowInstanceStore) testDB() *sql.DB { return s.testFixture().db }

func (s *workflowInstanceStore) testDialect() workflowStoreDialect { return s.testFixture().dialect }

func (s *workflowInstanceStore) testRuntimeMutation() runtimeMutationRunner {
	return s.testFixture().runner
}

func (s *workflowInstanceStore) isSQLite() bool {
	return s != nil && s.testDialect() == workflowStoreDialectSQLite
}

func (s *workflowInstanceStore) requireActiveWorkflowRun(ctx context.Context, _ *sql.Tx) (string, error) {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return "", err
	}
	if s == nil || s.runLifecycle == nil {
		return "", errors.New("workflow run lifecycle owner is required")
	}
	if err := s.runLifecycle.RequireActiveRun(ctx, runID); err != nil {
		return "", err
	}
	return runID, nil
}

func (s *workflowInstanceStore) runPipelineMutation(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error { return fn(txctx) })
}

func (s *workflowInstanceStore) runInPipelineTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return nil
	}
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		if authoractivityfixture.InMutation(ctx, tx) {
			return fn(ctx, tx)
		}
		if !authoractivityfixture.FinalizedMutation(ctx, tx) {
			return fmt.Errorf("pipeline fixture entered from a raw transaction without author activity ownership")
		}
		ctx = WithoutPipelineSQLTxContext(ctx)
	}
	runner := s.testRuntimeMutation()
	if runner == nil {
		return errors.New("pipeline fixture requires its selected mutation owner")
	}
	return runner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return errors.New("pipeline fixture mutation did not provide a transaction")
		}
		return fn(txctx, tx)
	})
}

func (s *workflowInstanceStore) upsert(ctx context.Context, instance WorkflowInstance) error {
	return s.persistFixtureWorkflowInstance(ctx, instance, false)
}

func (s *workflowInstanceStore) create(ctx context.Context, instance WorkflowInstance) error {
	return s.persistFixtureWorkflowInstance(ctx, instance, true)
}

func (s *workflowInstanceStore) persistFixtureWorkflowInstance(ctx context.Context, instance WorkflowInstance, createOnly bool) error {
	if s == nil || s.engineMutations == nil {
		return errors.New("pipeline fixture requires the workflow engine mutation owner")
	}
	instance, identity, ok, err := normalizeWorkflowInstanceForPersistence(instance)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("pipeline fixture requires canonical workflow identity")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	expectedState := ""
	expectedRevision := int64(0)
	target, err := s.LoadTargetPersistence(ctx, identity.Instance.Route(), runtimeidentity.NormalizeEntityID(identity.RowID()))
	if err != nil {
		return err
	}
	if target.Presence != WorkflowTargetPersistenceAbsent {
		if createOnly {
			return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-store", "create", map[string]any{"flow_instance": identity.StorageRef})
		}
		if !target.Presence.HasState() {
			return errors.New("pipeline fixture rejects lifecycle companion without state")
		}
		expectedState = target.State.CurrentState
		expectedRevision = target.State.Revision
	}
	transition, err := WorkflowEngineStateTransitionForPresence(target.Presence)
	if err != nil {
		return err
	}
	updatedAt := time.Now().UTC()
	if updatedAt.Before(instance.CreatedAt) {
		updatedAt = instance.CreatedAt
	}
	state, err := workflowEngineStateRecord(runID, identity.Instance.Route(), instance, expectedState, expectedRevision, transition, updatedAt)
	if err != nil {
		return err
	}
	_, err = s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: state})
	return err
}

func (s *workflowInstanceStore) requestRunCompletionCandidate(ctx context.Context, runID string) error {
	if s == nil || s.runLifecycle == nil {
		return errors.New("workflow run lifecycle owner is required")
	}
	_, err := s.runLifecycle.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID))
	return err
}

func (s *workflowInstanceStore) queueDeliveryContinuationSignal(ctx context.Context) error {
	if !queuePipelineTransactionPostCommitAction(ctx, func(context.Context) { s.signalDeliveryContinuations() }) {
		return errors.New("delivery continuation signal requires transaction-owned post-commit authority")
	}
	return nil
}
