package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type spawnReadbackFault struct {
	AgentLifecycleStateReader
	failNext bool
}

func (r *spawnReadbackFault) LoadAgentLifecycleState(ctx context.Context, identity agentidentity.Identity) (AgentLifecycleState, bool, error) {
	if err := ctx.Err(); err != nil {
		return AgentLifecycleState{}, false, err
	}
	if r.failNext {
		r.failNext = false
		return AgentLifecycleState{}, false, errors.New("injected committed spawn read failure")
	}
	return r.AgentLifecycleStateReader.LoadAgentLifecycleState(ctx, identity)
}

type spawnCommitFault struct {
	*agentRowResponseLoss
	afterCommit func()
}

func (s *spawnCommitFault) CommitAgentLifecycleTransition(ctx context.Context, request AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	result, err := s.agentRowResponseLoss.CommitAgentLifecycleTransition(ctx, request)
	if s.afterCommit != nil {
		callback := s.afterCommit
		s.afterCommit = nil
		callback()
	}
	return result, err
}

func ProveAgentRowNativeSpawnAdoption(t *testing.T, store AgentLifecyclePersistence, reader AgentLifecycleStateReader, outcomes AgentLifecycleOutcomeReader, rec PersistedAgent, snapshot func() any, retireProcess func(), cut string) {
	t.Helper()
	binding := store.(processExecutionBindingProvider)
	fault := &spawnCommitFault{agentRowResponseLoss: &agentRowResponseLoss{AgentLifecyclePersistence: store, binding: binding, runOwner: store.(RunExecutionOwner)}}
	readback := &spawnReadbackFault{AgentLifecycleStateReader: reader}
	bus := newProjectionTestBus()
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{LLMBackend: rec.Config.LLMBackend, LifecycleStore: fault, PersistenceRoles: PersistenceRoles{LifecycleState: readback, LifecycleOutcome: outcomes}})
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	builds := 0
	build := func() (Agent, error) {
		builds++
		return reconfigureTestAgent{id: rec.Config.ID}, nil
	}
	register := func(ctx context.Context, record PersistedAgent) error {
		return am.lifecycle.registerExecutionCandidate(ctx, record, true, build, testManagerSubscriptionAdmission(t, record.Config), record.Topology, nil)
	}
	cut = strings.TrimPrefix(cut, "reintroduction_")
	t.Cleanup(func() {
		for _, cell := range am.lifecycle.cells {
			if cell.execution != nil {
				cell.execution.cancelGeneration()
				for _, done := range []<-chan struct{}{cell.execution.loopDone, cell.execution.loopSettled} {
					if done != nil {
						select {
						case <-done:
						default:
							t.Error("spawn adoption retained an unjoined loop")
						}
					}
				}
				if len(cell.execution.leases) != 0 || cell.execution.routeToken.Valid() {
					t.Error("spawn candidate acquired unjoined execution resources")
				}
			}
		}
	})
	if cut == "shutdown_during_factory" {
		runCtx := beginCoordinatorRun(t, am.lifecycle, ctx, AgentRunModeStandard)
		if err := am.startShutdownWatcher(runCtx, "spawn proof shutdown watcher"); err != nil {
			t.Fatal(err)
		}
		held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		build = func() (Agent, error) {
			builds++
			close(held)
			<-release
			return reconfigureTestAgent{id: rec.Config.ID}, nil
		}
		before := snapshot()
		go func() { done <- register(ctx, rec) }()
		<-held
		shutdownErr := am.Shutdown()
		close(release)
		if err := <-done; err == nil || shutdownErr != nil {
			t.Fatalf("registration crossed completed shutdown: registration=%v shutdown=%v", err, shutdownErr)
		}
		if fault.commits != 0 || len(am.lifecycle.cells) != 0 || !reflect.DeepEqual(before, snapshot()) {
			t.Fatal("rejected factory cut created a row, outcome or execution")
		}
		return
	}
	if cut == "reset_during_stopped_factory" {
		held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		build = func() (Agent, error) {
			builds++
			close(held)
			<-release
			return reconfigureTestAgent{id: rec.Config.ID}, nil
		}
		before := snapshot()
		preparedRun, preparedReset := am.lifecycle.runGeneration, am.lifecycle.resetOccurrence
		go func() { done <- register(ctx, rec) }()
		<-held
		resetErr := am.ResetRuntimeState()
		resetPhase, resetRun := am.lifecycle.phaseSnapshot(), am.lifecycle.runGeneration
		close(release)
		registrationErr := <-done
		if resetErr != nil || resetPhase != runtimeLifecycleStopped || resetRun != preparedRun || am.lifecycle.resetOccurrence != preparedReset+1 || ctx.Err() != nil {
			t.Fatalf("stopped reset cut: reset=%v phase=%s run=%d/%d caller=%v", resetErr, resetPhase, resetRun, preparedRun, ctx.Err())
		}
		var refusal *runtimefailures.Error
		if !errors.As(registrationErr, &refusal) || refusal.Failure.Class != runtimefailures.ClassLifecycleConflict || refusal.Failure.Detail.Code != "lifecycle_transition_conflict" {
			t.Fatalf("registration crossed completed stopped reset: %v", registrationErr)
		}
		if fault.commits != 0 || len(am.lifecycle.cells) != 0 || len(am.lifecycle.registrations) != 0 || !reflect.DeepEqual(before, snapshot()) {
			t.Fatal("stale stopped factory created a row, outcome, cell or execution")
		}
		if len(bus.routes) != 0 || len(bus.history) != 0 {
			t.Fatal("stale stopped factory published a route")
		}
		build = func() (Agent, error) {
			builds++
			return reconfigureTestAgent{id: rec.Config.ID}, nil
		}
		if err := register(ctx, rec); err != nil || fault.commits != 1 || builds != 2 {
			t.Fatalf("fresh post-reset registration: commits=%d builds=%d error=%v", fault.commits, builds, err)
		}
		proveSpawnAdoptionJoinedExecution(t, am)
		return
	}
	switch cut {
	case "lost_ack", "historical", "retired_process":
		fault.loseNext = true
	case "rollback":
		fault.failNext = true
	case "cancel_after_commit":
		fault.afterCommit = cancel
	case "read_after_commit":
		fault.afterCommit = func() { readback.failNext = true }
	default:
		t.Fatalf("unknown spawn proof cut %s", cut)
	}
	firstErr := register(ctx, rec)
	if cut == "rollback" {
		if firstErr == nil || fault.commits != 0 || len(am.lifecycle.cells) != 0 {
			t.Fatal("uncommitted spawn published execution")
		}
		if err := register(context.Background(), rec); err != nil || fault.commits != 1 || builds != 2 {
			t.Fatalf("exact rolled-back spawn retry: commits=%d builds=%d error=%v", fault.commits, builds, err)
		}
		return
	}
	view, found, err := reader.LoadAgentLifecycleState(context.Background(), rec.Config.Identity)
	if err != nil || !found || view.Phase != AgentLifecycleRegistered || fault.commits != 1 {
		t.Fatalf("spawn did not reach native commit: found=%v view=%+v commits=%d err=%v registration=%v", found, view, fault.commits, err, firstErr)
	}
	if cut == "cancel_after_commit" {
		if firstErr != nil || builds != 1 || am.lifecycle.cells[rec.Config.Identity] == nil {
			t.Fatalf("known committed spawn was stranded by caller cancellation: %v", firstErr)
		}
		proveSpawnAdoptionJoinedExecution(t, am)
		return
	}
	if firstErr == nil {
		t.Fatal("injected commit/readback loss was reported successful")
	}
	afterCommit := snapshot()
	conflicting := rec
	conflicting.Config.MaxTurnsPerTask++
	if err := register(context.Background(), conflicting); err == nil || builds != 1 || !reflect.DeepEqual(afterCommit, snapshot()) {
		t.Fatalf("conflicting pending spawn rebuilt or mutated: %v", err)
	}
	if cut == "historical" {
		terminal := AgentLifecycleTransition{OperationID: uuid.NewString(), OperationKind: "start", Trigger: "start", RequestHash: uuid.NewString(), Identity: view.Identity, AgentID: view.AgentID,
			ExpectedEpoch: view.RuntimeEpoch, ExpectedGeneration: view.Generation, ExpectedPhase: view.Phase,
			TargetEpoch: view.RuntimeEpoch, TargetGeneration: view.Generation + 1, TargetPhase: AgentLifecycleRunning,
			ConfigRevision: view.ConfigRevision, RunMode: AgentRunModeStandard, Topology: view.Topology, Now: time.Now().UTC(),
			DiagnosticOrigin: LifecycleDiagnosticOrigin{Owner: LifecycleDiagnosticNormal, Causality: LifecycleDiagnosticObservation}}
		if _, err := store.CommitAgentLifecycleTransition(TerminalPanicFixtureContext(), terminal); err != nil {
			t.Fatal(err)
		}
		afterCommit = snapshot()
		if err := register(context.Background(), rec); err == nil || builds != 1 || len(am.lifecycle.cells) != 0 || !reflect.DeepEqual(afterCommit, snapshot()) {
			t.Fatalf("historical spawn was adopted: %v", err)
		}
		return
	}
	if cut == "retired_process" {
		retireProcess()
		afterRetirement := snapshot()
		if err := register(context.Background(), rec); err == nil || builds != 1 || len(am.lifecycle.cells) != 0 || !reflect.DeepEqual(afterRetirement, snapshot()) {
			t.Fatalf("retired process adopted a pending spawn: %v", err)
		}
		return
	}
	if err := register(context.Background(), rec); err != nil {
		t.Fatalf("exact spawn outcome adoption: %v", err)
	}
	if fault.commits != 1 || builds != 1 || !reflect.DeepEqual(afterCommit, snapshot()) {
		t.Fatal("spawn adoption repeated factory, native mutation or durable evidence")
	}
	cell := am.lifecycle.cells[rec.Config.Identity]
	if cell == nil || cell.execution == nil || cell.view.LastTransitionID != view.LastTransitionID || cell.execution.token.Generation != view.Generation {
		t.Fatal("spawn adoption published something other than the exact current outcome")
	}
	proveSpawnAdoptionJoinedExecution(t, am)
}

func proveSpawnAdoptionJoinedExecution(t *testing.T, am *AgentManager) {
	t.Helper()
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("run exact adopted spawn: %v", err)
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("join exact adopted spawn: %v", err)
	}
}
