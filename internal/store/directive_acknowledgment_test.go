package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type directivePersistenceFault string

const (
	directiveFaultFinalizeFailure directivePersistenceFault = "finalize_failure"
	directiveFaultRecordResult    directivePersistenceFault = "record_result"
	directiveFaultFinalizeSuccess directivePersistenceFault = "finalize_success"
	directiveFaultReconcile       directivePersistenceFault = "reconcile"
)

type directiveFaultMode string

const (
	directiveFaultBeforeCommit directiveFaultMode = "before_commit"
	directiveFaultAfterCommit  directiveFaultMode = "after_commit"
)

var errInjectedDirectivePersistence = errors.New("injected directive persistence acknowledgment failure")

type directiveIntegrationStore interface {
	runtimebus.EventStore
	runtimeagentcontrol.DirectiveOperationStore
	runtimemanager.ManagerPersistence
}

type faultingDirectiveIntegrationStore struct {
	directiveIntegrationStore

	mu        sync.Mutex
	fault     directivePersistenceFault
	mode      directiveFaultMode
	remaining int
}

func (s *faultingDirectiveIntegrationStore) setFault(fault directivePersistenceFault, mode directiveFaultMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = fault
	s.mode = mode
	s.remaining = 1
}

func (s *faultingDirectiveIntegrationStore) takeFault(fault directivePersistenceFault) (directiveFaultMode, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fault != fault || s.remaining == 0 {
		return "", false
	}
	s.remaining--
	return s.mode, true
}

func (s *faultingDirectiveIntegrationStore) FinalizeDirectiveFailure(ctx context.Context, operationID, ownerID string, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	mode, inject := s.takeFault(directiveFaultFinalizeFailure)
	if inject && mode == directiveFaultBeforeCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	op, err := s.directiveIntegrationStore.FinalizeDirectiveFailure(ctx, operationID, ownerID, failure, now, ttl)
	if err == nil && inject && mode == directiveFaultAfterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	return op, err
}

func (s *faultingDirectiveIntegrationStore) RecordDirectiveExecuted(ctx context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	mode, inject := s.takeFault(directiveFaultRecordResult)
	if inject && mode == directiveFaultBeforeCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	op, err := s.directiveIntegrationStore.RecordDirectiveExecuted(ctx, operationID, ownerID, response, now)
	if err == nil && inject && mode == directiveFaultAfterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	return op, err
}

func (s *faultingDirectiveIntegrationStore) FinalizeDirectiveSuccess(ctx context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	mode, inject := s.takeFault(directiveFaultFinalizeSuccess)
	if inject && mode == directiveFaultBeforeCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	op, err := s.directiveIntegrationStore.FinalizeDirectiveSuccess(ctx, operationID, now, ttl)
	if err == nil && inject && mode == directiveFaultAfterCommit {
		return runtimeagentcontrol.DirectiveOperation{}, errInjectedDirectivePersistence
	}
	return op, err
}

func (s *faultingDirectiveIntegrationStore) ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	mode, inject := s.takeFault(directiveFaultReconcile)
	if inject && mode == directiveFaultBeforeCommit {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, errInjectedDirectivePersistence
	}
	result, err := s.directiveIntegrationStore.ReconcileDirectiveOperations(ctx, now, ttl)
	if err == nil && inject && mode == directiveFaultAfterCommit {
		return runtimeagentcontrol.DirectiveOperationReconcileResult{}, errInjectedDirectivePersistence
	}
	return result, err
}

func (s *faultingDirectiveIntegrationStore) ResolveAgentDirectiveRunTarget(ctx context.Context, agentID, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	if explicitRunID == "" {
		return runtimeagentcontrol.RunTargetResolution{}, &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrRunNotFound, AgentID: agentID}
	}
	var status string
	err := scanDirectiveTestRow(
		s.directiveDB(),
		`SELECT status FROM runs WHERE run_id = ?`,
		`SELECT status FROM runs WHERE run_id = $1::uuid`,
		[]any{explicitRunID},
		&status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeagentcontrol.RunTargetResolution{}, &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrRunNotFound, AgentID: agentID, RunID: explicitRunID}
	}
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	if status != "running" && status != "paused" {
		return runtimeagentcontrol.RunTargetResolution{}, &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrRunAlreadyTerminal, AgentID: agentID, RunID: explicitRunID, CurrentStatus: status}
	}
	return runtimeagentcontrol.RunTargetResolution{RunID: explicitRunID, Mode: runtimeagentcontrol.RunResolutionSpecified}, nil
}

func (s *faultingDirectiveIntegrationStore) directiveDB() *sql.DB {
	switch store := s.directiveIntegrationStore.(type) {
	case *PostgresStore:
		return store.DB
	case *SQLiteRuntimeStore:
		return store.DB
	default:
		return nil
	}
}

type directiveAmbiguityAgent struct {
	id       string
	response string
	err      error
	calls    atomic.Int32
}

func (a *directiveAmbiguityAgent) ID() string                      { return a.id }
func (*directiveAmbiguityAgent) Type() string                      { return "test" }
func (*directiveAmbiguityAgent) Subscriptions() []events.EventType { return nil }
func (*directiveAmbiguityAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *directiveAmbiguityAgent) BoardStep(context.Context, runtimeagentcontrol.BoardDirective) (string, error) {
	a.calls.Add(1)
	return a.response, a.err
}

type directiveAmbiguityBackend struct {
	name  string
	store directiveIntegrationStore
	db    *sql.DB
}

func forEachDirectiveAmbiguityBackend(t *testing.T, run func(*testing.T, directiveAmbiguityBackend)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForPath(t, filepath.Join(t.TempDir(), "directive-ambiguity.db"))
		run(t, directiveAmbiguityBackend{name: "sqlite", store: store, db: store.DB})
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		store := &PostgresStore{DB: db}
		run(t, directiveAmbiguityBackend{name: "postgres", store: store, db: db})
	})
}

func TestDirectiveFailureFinalizationAcknowledgmentMatrix(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		for _, mode := range []directiveFaultMode{directiveFaultBeforeCommit, directiveFaultAfterCommit} {
			t.Run(string(mode), func(t *testing.T) {
				h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "failure-agent", err: errors.New("provider failed")})
				h.faults.setFault(directiveFaultFinalizeFailure, mode)

				_, err := h.manager.SendDirective(context.Background(), h.request)
				assertImmediateDirectiveFailure(t, err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedDetail)
				op := h.loadOperation(t)
				if got := h.agent.calls.Load(); got != 1 {
					t.Fatalf("BoardStep calls = %d, want 1", got)
				}
				if mode == directiveFaultBeforeCommit {
					assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuting, false, false)
					assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
					if _, err := h.manager.SendDirective(context.Background(), h.request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
						t.Fatalf("same-key retry before expiry = %v, want in progress", err)
					}
					h.expireLease(t, op.OperationID)
					if _, err := h.manager.SendDirective(context.Background(), h.request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
						t.Fatalf("same-key retry after expiry = %v, want indeterminate", err)
					}
					op = h.loadOperation(t)
					assertDirectiveOperationFailure(t, op, runtimeagentcontrol.DirectiveOperationIndeterminate, runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail)
					assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "error", op.Failure)
				} else {
					assertDirectiveOperationFailure(t, op, runtimeagentcontrol.DirectiveOperationFailed, runtimeagentcontrol.DirectiveBoardStepFailedDetail)
					assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "error", op.Failure)
					if _, err := h.manager.SendDirective(context.Background(), h.request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveExecutionFailed) {
						t.Fatalf("same-key committed-failure replay = %v, want execution failed", err)
					}
					if err := h.manager.ReconcileDirectiveOperations(context.Background()); err != nil {
						t.Fatalf("startup reconciliation: %v", err)
					}
				}
				assertFailureDetailAbsentFromDurableOperation(t, op, runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedDetail)
				if got := h.agent.calls.Load(); got != 1 {
					t.Fatalf("BoardStep calls after convergence = %d, want 1", got)
				}
			})
		}
	})
}

func TestDirectiveResultRecordingAcknowledgmentMatrix(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		for _, mode := range []directiveFaultMode{directiveFaultBeforeCommit, directiveFaultAfterCommit} {
			for _, convergence := range []string{"same_key", "startup"} {
				t.Run(string(mode)+"/"+convergence, func(t *testing.T) {
					h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "result-agent", response: "accepted"})
					h.faults.setFault(directiveFaultRecordResult, mode)

					_, err := h.manager.SendDirective(context.Background(), h.request)
					assertImmediateDirectiveFailure(t, err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedDetail)
					op := h.loadOperation(t)
					assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
					if mode == directiveFaultBeforeCommit {
						assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuting, false, false)
						h.expireLease(t, op.OperationID)
						if convergence == "startup" {
							if err := h.manager.ReconcileDirectiveOperations(context.Background()); err != nil {
								t.Fatalf("startup reconciliation: %v", err)
							}
						} else if _, err := h.manager.SendDirective(context.Background(), h.request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
							t.Fatalf("same-key expiry convergence = %v, want indeterminate", err)
						}
						op = h.loadOperation(t)
						assertDirectiveOperationFailure(t, op, runtimeagentcontrol.DirectiveOperationIndeterminate, runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail)
						assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "error", op.Failure)
					} else {
						assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuted, true, false)
						if convergence == "startup" {
							if err := h.manager.ReconcileDirectiveOperations(context.Background()); err != nil {
								t.Fatalf("startup reconciliation: %v", err)
							}
						} else {
							result, err := h.manager.SendDirective(context.Background(), h.request)
							if err != nil || !result.OK || result.Response != "accepted" {
								t.Fatalf("same-key result convergence = %#v err=%v", result, err)
							}
						}
						op = h.loadOperation(t)
						assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationSucceeded, true, false)
						assertDirectiveSuccessSettlement(t, backend.db, op)
					}
					assertFailureDetailAbsentFromDurableOperation(t, op, runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedDetail)
					if got := h.agent.calls.Load(); got != 1 {
						t.Fatalf("BoardStep calls after convergence = %d, want 1", got)
					}
				})
			}
		}
	})
}

func TestDirectiveSuccessFinalizationAcknowledgmentMatrix(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		for _, mode := range []directiveFaultMode{directiveFaultBeforeCommit, directiveFaultAfterCommit} {
			for _, convergence := range []string{"same_key", "startup"} {
				t.Run(string(mode)+"/"+convergence, func(t *testing.T) {
					h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "success-agent", response: "accepted"})
					h.faults.setFault(directiveFaultFinalizeSuccess, mode)

					_, err := h.manager.SendDirective(context.Background(), h.request)
					assertImmediateDirectiveCompletionPending(t, err)
					op := h.loadOperation(t)
					if mode == directiveFaultBeforeCommit {
						assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuted, true, false)
						assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
						assertDirectiveProjection(t, backend.db, op.OperationID, false)
					} else {
						assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationSucceeded, true, false)
						assertDirectiveSuccessSettlement(t, backend.db, op)
					}
					if convergence == "startup" {
						if err := h.manager.ReconcileDirectiveOperations(context.Background()); err != nil {
							t.Fatalf("startup reconciliation: %v", err)
						}
					} else {
						result, err := h.manager.SendDirective(context.Background(), h.request)
						if err != nil || !result.OK || result.Response != "accepted" {
							t.Fatalf("same-key success convergence = %#v err=%v", result, err)
						}
					}
					op = h.loadOperation(t)
					assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationSucceeded, true, false)
					assertDirectiveSuccessSettlement(t, backend.db, op)
					if got := h.agent.calls.Load(); got != 1 {
						t.Fatalf("BoardStep calls after convergence = %d, want 1", got)
					}
				})
			}
		}
	})
}

func TestDirectiveReconciliationAcknowledgmentMatrix(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		for _, producer := range []string{"expired_execution", "keyless_prepared"} {
			for _, mode := range []directiveFaultMode{directiveFaultBeforeCommit, directiveFaultAfterCommit} {
				t.Run(producer+"/"+string(mode), func(t *testing.T) {
					h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "recovery-agent"})
					var op runtimeagentcontrol.DirectiveOperation
					if producer == "expired_execution" {
						h.faults.setFault(directiveFaultRecordResult, directiveFaultBeforeCommit)
						_, err := h.manager.SendDirective(context.Background(), h.request)
						assertImmediateDirectiveFailure(t, err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedDetail)
						op = h.loadOperation(t)
						h.expireLease(t, op.OperationID)
					} else {
						op = h.reserveKeylessPrepared(t)
					}
					h.faults.setFault(directiveFaultReconcile, mode)
					if err := h.manager.ReconcileDirectiveOperations(context.Background()); !errors.Is(err, errInjectedDirectivePersistence) {
						t.Fatalf("first reconciliation error = %v, want injected failure", err)
					}
					persisted := h.loadOperationID(t, op.OperationID)
					if mode == directiveFaultBeforeCommit {
						wantState := runtimeagentcontrol.DirectiveOperationExecuting
						if producer == "keyless_prepared" {
							wantState = runtimeagentcontrol.DirectiveOperationPrepared
						}
						assertDirectiveOperationEvidence(t, persisted, wantState, false, false)
						assertDirectiveReceipt(t, backend.db, persisted.DirectiveEventID, "", nil)
					} else {
						assertReconciledDirectiveProducer(t, backend.db, producer, persisted)
					}
					if err := h.manager.ReconcileDirectiveOperations(context.Background()); err != nil {
						t.Fatalf("second reconciliation: %v", err)
					}
					persisted = h.loadOperationID(t, op.OperationID)
					assertReconciledDirectiveProducer(t, backend.db, producer, persisted)
					if got := h.agent.calls.Load(); got > 1 {
						t.Fatalf("BoardStep calls = %d, want at most 1", got)
					}
				})
			}
		}
	})
}

func TestDirectiveFailureFinalizationRollsBackReceiptAndOperationTogether(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "atomic-failure-agent", err: errors.New("provider failed")})
		dropTrigger := installDirectiveRejectStateTrigger(t, backend, runtimeagentcontrol.DirectiveOperationFailed)
		_, err := h.manager.SendDirective(context.Background(), h.request)
		assertImmediateDirectiveFailure(t, err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedDetail)
		dropTrigger()

		op := h.loadOperation(t)
		assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuting, false, false)
		assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
		if got := h.agent.calls.Load(); got != 1 {
			t.Fatalf("BoardStep calls = %d, want 1", got)
		}
		h.expireLease(t, op.OperationID)
		if _, err := h.manager.SendDirective(context.Background(), h.request); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
			t.Fatalf("same-key convergence error = %v, want indeterminate", err)
		}
		op = h.loadOperation(t)
		assertDirectiveOperationFailure(t, op, runtimeagentcontrol.DirectiveOperationIndeterminate, runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail)
		assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "error", op.Failure)
		if got := h.agent.calls.Load(); got != 1 {
			t.Fatalf("BoardStep calls after convergence = %d, want 1", got)
		}
	})
}

func TestDirectiveMalformedTypedBoardStepFailureCannotBecomeDurable(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		malformed := &runtimefailures.Error{Failure: runtimefailures.Envelope{
			Class:  runtimefailures.ClassAuthenticationNeeded,
			Detail: runtimefailures.Detail{Code: "provider_unauthorized"},
		}}
		h := newDirectiveAmbiguityHarness(t, backend, &directiveAmbiguityAgent{id: "malformed-agent", err: malformed})

		_, err := h.manager.SendDirective(context.Background(), h.request)
		assertImmediateDirectiveFailure(t, err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedDetail)
		op := h.loadOperation(t)
		assertDirectiveOperationEvidence(t, op, runtimeagentcontrol.DirectiveOperationExecuting, false, false)
		assertDirectiveReceipt(t, backend.db, op.DirectiveEventID, "", nil)
		if got := h.agent.calls.Load(); got != 1 {
			t.Fatalf("BoardStep calls = %d, want 1", got)
		}
	})
}

func TestDirectiveOperationDatabaseEnforcesStateEvidenceEquivalence(t *testing.T) {
	forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
		seedDirectiveAmbiguityRun(t, backend.db, "00000000-0000-0000-0000-000000001000")
		failureRaw, err := runtimefailures.MarshalEnvelope(runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure())
		if err != nil {
			t.Fatal(err)
		}
		responseRaw := []byte(`{"ok":true}`)
		tests := []struct {
			name     string
			state    runtimeagentcontrol.DirectiveOperationState
			response []byte
			failure  []byte
		}{
			{name: "prepared with response", state: runtimeagentcontrol.DirectiveOperationPrepared, response: responseRaw},
			{name: "prepared with failure", state: runtimeagentcontrol.DirectiveOperationPrepared, failure: failureRaw},
			{name: "executing with response", state: runtimeagentcontrol.DirectiveOperationExecuting, response: responseRaw},
			{name: "executing with failure", state: runtimeagentcontrol.DirectiveOperationExecuting, failure: failureRaw},
			{name: "executed without response", state: runtimeagentcontrol.DirectiveOperationExecuted},
			{name: "executed with failure", state: runtimeagentcontrol.DirectiveOperationExecuted, response: responseRaw, failure: failureRaw},
			{name: "succeeded without response", state: runtimeagentcontrol.DirectiveOperationSucceeded},
			{name: "succeeded with failure", state: runtimeagentcontrol.DirectiveOperationSucceeded, response: responseRaw, failure: failureRaw},
			{name: "failed without failure", state: runtimeagentcontrol.DirectiveOperationFailed},
			{name: "failed with response", state: runtimeagentcontrol.DirectiveOperationFailed, response: responseRaw, failure: failureRaw},
			{name: "indeterminate without failure", state: runtimeagentcontrol.DirectiveOperationIndeterminate},
			{name: "indeterminate with response", state: runtimeagentcontrol.DirectiveOperationIndeterminate, response: responseRaw, failure: failureRaw},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				req := directiveOperationReservationForTest(t, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), time.Now().UTC())
				reserved, err := backend.store.ReserveDirectiveOperation(context.Background(), req)
				if err != nil {
					t.Fatalf("reserve directive: %v", err)
				}
				if err := updateDirectiveOperationEvidence(t, backend, reserved.Operation.OperationID, test.state, test.response, test.failure); err == nil {
					t.Fatalf("database accepted invalid %s evidence", test.state)
				}
				persisted, ok, err := backend.store.LoadDirectiveOperation(context.Background(), reserved.Operation.OperationID)
				if err != nil || !ok {
					t.Fatalf("reload directive ok=%v err=%v", ok, err)
				}
				assertDirectiveOperationEvidence(t, persisted, runtimeagentcontrol.DirectiveOperationPrepared, false, false)
			})
		}
	})
}

type directiveAmbiguityHarness struct {
	backend directiveAmbiguityBackend
	faults  *faultingDirectiveIntegrationStore
	manager *runtimemanager.AgentManager
	agent   *directiveAmbiguityAgent
	request runtimeagentcontrol.SendDirectiveRequest
}

func newDirectiveAmbiguityHarness(t *testing.T, backend directiveAmbiguityBackend, agent *directiveAmbiguityAgent) *directiveAmbiguityHarness {
	t.Helper()
	runID := uuid.NewString()
	seedDirectiveAmbiguityRun(t, backend.db, runID)
	faults := &faultingDirectiveIntegrationStore{directiveIntegrationStore: backend.store}
	bus, err := runtimebus.NewEventBus(faults)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	manager := runtimemanager.NewAgentManager(bus, func(runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, faults)
	if err := manager.RegisterEphemeralAgentForExecution(context.Background(), runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: agent.id, Role: "test"},
		Status: "active",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return &directiveAmbiguityHarness{
		backend: backend,
		faults:  faults,
		manager: manager,
		agent:   agent,
		request: runtimeagentcontrol.SendDirectiveRequest{
			AgentID:        agent.id,
			Directive:      "run ambiguity proof",
			RunID:          runID,
			Source:         runtimeagentcontrol.DirectiveSourceV1RPC,
			ActorTokenID:   "operator-token",
			IdempotencyKey: uuid.NewString(),
			RequestHash:    uuid.NewString(),
		},
	}
}

func (h *directiveAmbiguityHarness) loadOperation(t *testing.T) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	op, ok, err := h.backend.store.LoadDirectiveOperationByKey(context.Background(), runtimeagentcontrol.DirectiveOperationMethod, h.request.ActorTokenID, h.request.IdempotencyKey)
	if err != nil || !ok {
		t.Fatalf("load directive operation by key ok=%v err=%v", ok, err)
	}
	return op
}

func (h *directiveAmbiguityHarness) loadOperationID(t *testing.T, operationID string) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	op, ok, err := h.backend.store.LoadDirectiveOperation(context.Background(), operationID)
	if err != nil || !ok {
		t.Fatalf("load directive operation %s ok=%v err=%v", operationID, ok, err)
	}
	return op
}

func (h *directiveAmbiguityHarness) expireLease(t *testing.T, operationID string) {
	t.Helper()
	if _, err := h.backend.db.Exec(`UPDATE agent_directive_operations SET execution_lease_expires_at = ? WHERE operation_id = ?`, time.Now().UTC().Add(-time.Minute), operationID); err == nil {
		return
	}
	if _, err := h.backend.db.Exec(`UPDATE agent_directive_operations SET execution_lease_expires_at = $1 WHERE operation_id = $2::uuid`, time.Now().UTC().Add(-time.Minute), operationID); err != nil {
		t.Fatalf("expire directive lease: %v", err)
	}
}

func (h *directiveAmbiguityHarness) reserveKeylessPrepared(t *testing.T) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	now := time.Now().UTC()
	seedDirectiveAmbiguityRun(t, h.backend.db, "00000000-0000-0000-0000-000000001000")
	req := directiveOperationReservationForTest(t, uuid.NewString(), uuid.NewString(), "", uuid.NewString(), now)
	reserved, err := h.backend.store.ReserveDirectiveOperation(context.Background(), req)
	if err != nil {
		t.Fatalf("reserve keyless directive: %v", err)
	}
	return reserved.Operation
}

func seedDirectiveAmbiguityRun(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO runs (run_id, status) VALUES (?, 'running') ON CONFLICT(run_id) DO NOTHING`, runID); err == nil {
		return
	}
	if _, err := db.Exec(`INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running') ON CONFLICT(run_id) DO NOTHING`, runID); err != nil {
		t.Fatalf("seed directive run: %v", err)
	}
}

func installDirectiveRejectStateTrigger(t *testing.T, backend directiveAmbiguityBackend, state runtimeagentcontrol.DirectiveOperationState) func() {
	t.Helper()
	if backend.name == "sqlite" {
		const triggerName = "fail_directive_terminal_state"
		if _, err := backend.db.Exec(`CREATE TRIGGER ` + triggerName + ` BEFORE UPDATE OF state ON agent_directive_operations WHEN NEW.state = '` + string(state) + `' BEGIN SELECT RAISE(ABORT, 'injected terminal transition failure'); END`); err != nil {
			t.Fatalf("create SQLite directive trigger: %v", err)
		}
		return func() {
			if _, err := backend.db.Exec(`DROP TRIGGER ` + triggerName); err != nil {
				t.Fatalf("drop SQLite directive trigger: %v", err)
			}
		}
	}
	const functionName = "fail_directive_terminal_state_fn"
	const triggerName = "fail_directive_terminal_state"
	if _, err := backend.db.Exec(`CREATE FUNCTION ` + functionName + `() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected terminal transition failure'; END; $$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create Postgres directive trigger function: %v", err)
	}
	if _, err := backend.db.Exec(`CREATE TRIGGER ` + triggerName + ` BEFORE UPDATE OF state ON agent_directive_operations FOR EACH ROW WHEN (NEW.state = '` + string(state) + `') EXECUTE FUNCTION ` + functionName + `()`); err != nil {
		t.Fatalf("create Postgres directive trigger: %v", err)
	}
	return func() {
		if _, err := backend.db.Exec(`DROP TRIGGER ` + triggerName + ` ON agent_directive_operations`); err != nil {
			t.Fatalf("drop Postgres directive trigger: %v", err)
		}
		if _, err := backend.db.Exec(`DROP FUNCTION ` + functionName + `()`); err != nil {
			t.Fatalf("drop Postgres directive trigger function: %v", err)
		}
	}
}

func updateDirectiveOperationEvidence(t *testing.T, backend directiveAmbiguityBackend, operationID string, state runtimeagentcontrol.DirectiveOperationState, response, failure []byte) error {
	t.Helper()
	var responseValue, failureValue any
	if len(response) > 0 {
		responseValue = string(response)
	}
	if len(failure) > 0 {
		failureValue = string(failure)
	}
	if backend.name == "sqlite" {
		_, err := backend.db.Exec(`UPDATE agent_directive_operations SET state = ?, response = ?, failure = ? WHERE operation_id = ?`, string(state), responseValue, failureValue, operationID)
		return err
	}
	_, err := backend.db.Exec(`UPDATE agent_directive_operations SET state = $1, response = $2::jsonb, failure = $3::jsonb WHERE operation_id = $4::uuid`, string(state), responseValue, failureValue, operationID)
	return err
}

func assertImmediateDirectiveFailure(t *testing.T, err error, sentinel error, detail string) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("directive error = %v, want %v", err, sentinel)
	}
	var operationErr *runtimeagentcontrol.DirectiveOperationError
	if !errors.As(err, &operationErr) || operationErr.Operation.Failure == nil || operationErr.Operation.Failure.Detail.Code != detail {
		t.Fatalf("directive immediate failure = %#v, want detail %s", operationErr, detail)
	}
}

func assertImmediateDirectiveCompletionPending(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, runtimeagentcontrol.ErrDirectiveCompletionPending) {
		t.Fatalf("directive error = %v, want completion pending", err)
	}
	var operationErr *runtimeagentcontrol.DirectiveOperationError
	if !errors.As(err, &operationErr) || operationErr.Operation.Failure != nil {
		t.Fatalf("completion-pending operation = %#v, want no failure", operationErr)
	}
}

func assertDirectiveOperationEvidence(t *testing.T, op runtimeagentcontrol.DirectiveOperation, state runtimeagentcontrol.DirectiveOperationState, response, failure bool) {
	t.Helper()
	if op.State != state || (len(op.Response) > 0) != response || (op.Failure != nil) != failure {
		t.Fatalf("operation evidence = state:%s response:%t failure:%#v, want state:%s response:%t failure:%t", op.State, len(op.Response) > 0, op.Failure, state, response, failure)
	}
	if err := runtimeagentcontrol.ValidateDirectiveOperationEvidence(op); err != nil {
		t.Fatalf("validate operation evidence: %v", err)
	}
}

func assertDirectiveOperationFailure(t *testing.T, op runtimeagentcontrol.DirectiveOperation, state runtimeagentcontrol.DirectiveOperationState, detail string) {
	t.Helper()
	assertDirectiveOperationEvidence(t, op, state, false, true)
	if op.Failure.Detail.Code != detail {
		t.Fatalf("operation failure detail = %s, want %s", op.Failure.Detail.Code, detail)
	}
}

func assertFailureDetailAbsentFromDurableOperation(t *testing.T, op runtimeagentcontrol.DirectiveOperation, detail string) {
	t.Helper()
	if op.Failure != nil && op.Failure.Detail.Code == detail {
		t.Fatalf("response-local failure %s was persisted", detail)
	}
}

func assertDirectiveReceipt(t *testing.T, db *sql.DB, eventID, status string, failure *runtimefailures.Envelope) {
	t.Helper()
	var outcome string
	var raw sql.NullString
	err := scanDirectiveTestRow(
		db,
		`SELECT outcome, failure FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`,
		`SELECT outcome, failure FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`,
		[]any{eventID},
		&outcome, &raw,
	)
	if status == "" {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("receipt lookup error = %v, want absent", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("load directive receipt: %v", err)
	}
	wantOutcome := "dead_letter"
	if status == "processed" {
		wantOutcome = "success"
	}
	if outcome != wantOutcome {
		t.Fatalf("receipt outcome = %s, want %s", outcome, wantOutcome)
	}
	if failure == nil {
		if raw.Valid && raw.String != "" && raw.String != "null" {
			t.Fatalf("processed receipt failure = %s, want null", raw.String)
		}
		return
	}
	if !raw.Valid {
		t.Fatal("error receipt failure is null")
	}
	want, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		t.Fatalf("marshal expected receipt failure: %v", err)
	}
	var gotValue, wantValue any
	if err := json.Unmarshal([]byte(raw.String), &gotValue); err != nil {
		t.Fatalf("decode receipt failure: %v", err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode expected receipt failure: %v", err)
	}
	gotCanonical, _ := json.Marshal(gotValue)
	wantCanonical, _ := json.Marshal(wantValue)
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("receipt failure = %s, want %s", gotCanonical, wantCanonical)
	}
}

func assertDirectiveProjection(t *testing.T, db *sql.DB, operationID string, want bool) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE resource_id = ?`, operationID).Scan(&count); err != nil {
		if err := db.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE resource_id = $1`, operationID).Scan(&count); err != nil {
			t.Fatalf("count directive projection: %v", err)
		}
	}
	wantCount := 0
	if want {
		wantCount = 1
	}
	if count != wantCount {
		t.Fatalf("directive projection count = %d, want %d", count, wantCount)
	}
}

func scanDirectiveTestRow(db *sql.DB, sqliteQuery, postgresQuery string, args []any, destinations ...any) error {
	err := db.QueryRow(sqliteQuery, args...).Scan(destinations...)
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return db.QueryRow(postgresQuery, args...).Scan(destinations...)
}

func assertDirectiveSuccessSettlement(t *testing.T, db *sql.DB, op runtimeagentcontrol.DirectiveOperation) {
	t.Helper()
	assertDirectiveReceipt(t, db, op.DirectiveEventID, "processed", nil)
	assertDirectiveProjection(t, db, op.OperationID, true)
}

func assertReconciledDirectiveProducer(t *testing.T, db *sql.DB, producer string, op runtimeagentcontrol.DirectiveOperation) {
	t.Helper()
	wantDetail := runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail
	wantState := runtimeagentcontrol.DirectiveOperationIndeterminate
	if producer == "keyless_prepared" {
		wantDetail = runtimeagentcontrol.DirectiveExecutionNotAdmittedDetail
		wantState = runtimeagentcontrol.DirectiveOperationFailed
	}
	assertDirectiveOperationFailure(t, op, wantState, wantDetail)
	assertDirectiveReceipt(t, db, op.DirectiveEventID, "error", op.Failure)
}
