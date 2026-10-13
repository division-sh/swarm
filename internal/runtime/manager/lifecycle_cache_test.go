package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestAgentRowCacheRequiresDurableOwnerAndExactHydration(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	record := lifecycleTestPersistedAgent(t)
	for _, persist := range []bool{false, true} {
		coordinator := newAgentLifecycleCoordinator(nil, nil, nil, nil)
		if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, persist); err == nil {
			t.Fatalf("store-less registration accepted, persist=%v", persist)
		}
		if len(coordinator.cells) != 0 {
			t.Fatal("missing durable owner published a cell")
		}
	}
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, probe, nil)
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, false); err == nil {
		t.Fatal("descriptor without its durable row hydrated")
	}
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, true); err != nil {
		t.Fatal(err)
	}
	current, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	coordinator.mu.Lock()
	delete(coordinator.cells, record.Config.Identity)
	coordinator.mu.Unlock()
	record.LifecycleEpoch, record.LifecycleGeneration = 99, 88
	record.LifecyclePhase, record.LifecycleRunMode = AgentLifecycleFailed, AgentRunModeStandard
	record.ProcessBinding = ProcessExecutionBinding{}
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, false); err != nil {
		t.Fatal(err)
	}
	adopted, found := coordinator.stateByIdentity(record.Config.Identity)
	if !found || !reflect.DeepEqual(current, adopted) {
		t.Fatalf("descriptor replaced durable currentness: current=%#v adopted=%#v", current, adopted)
	}
}

func TestAgentRowCacheHistoricalReplayCannotResurrectExecution(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, probe, nil)
	record := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, coordinator, ctx, AgentRunModeStandard)
	oldOperation := uuid.NewString()
	_, oldToken, oldDone, err := replaceCoordinatorLoop(coordinator, ctx, record, "restart", oldOperation, nil, sessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = releaseCoordinatorLoop(coordinator, oldToken, oldDone) }()
	loop, currentToken, currentDone, err := replaceCoordinatorLoop(coordinator, ctx, record, "restart", uuid.NewString(), nil, sessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	before, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	_, _, _, err = replaceCoordinatorLoop(coordinator, ctx, record, "restart", oldOperation, nil, sessions.LifecycleMutationPlan{})
	var failure *runtimefailures.Error
	if !errors.As(err, &failure) || failure.Failure.Detail.Code != "lifecycle_outcome_not_current" {
		t.Fatalf("old replay execution result = %v", err)
	}
	after, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("historical replay changed the current row")
	}
	select {
	case <-loop.Done():
		t.Fatal("historical replay canceled the current execution")
	default:
	}
	if token, current := coordinator.tokenIdentity(record.Config.Identity); !current || token != currentToken {
		t.Fatalf("historical replay replaced the current token: %+v current=%v", token, current)
	}
	coordinator.cancelShutdownWork()
	if err := releaseCoordinatorLoop(coordinator, currentToken, currentDone); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRowCacheFailedJoinDoesNotInventDurablePhase(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, probe, nil)
	record := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, coordinator, ctx, AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(coordinator, ctx, record, "start", uuid.NewString(), nil, sessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	probe.failNext = errors.New("exact settlement rollback")
	if err := releaseCoordinatorLoop(coordinator, token, done); err == nil {
		t.Fatal("failed settlement reported success")
	}
	view, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	cached, _ := coordinator.stateByIdentity(record.Config.Identity)
	if view.Phase != AgentLifecycleRunning || !reflect.DeepEqual(view, cached) {
		t.Fatalf("failed cleanup invented a durable phase: row=%#v cache=%#v", view, cached)
	}
	if token, live := coordinator.tokenIdentity(record.Config.Identity); live || token != (runtimeeffects.LifecycleToken{}) {
		t.Fatal("failed resource settlement advertised a live execution")
	}
}

func TestAgentRowCacheLostAckFinalizerCannotMutateSuccessor(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, probe, nil)
	record := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, coordinator, ctx, record, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, coordinator, ctx, AgentRunModeStandard)
	_, old, done, err := replaceCoordinatorLoop(coordinator, ctx, record, "start", uuid.NewString(), nil, sessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	patch := record
	patch.Config.Tools = []string{"replacement-tool"}
	probe.failAfter = errors.New("lost native replacement acknowledgement")
	if _, _, _, err := replaceCoordinatorLoop(coordinator, ctx, record, "reconfigure", "", &patch, sessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("lost acknowledgment was reported as success")
	}
	committed, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if err := releaseCoordinatorLoop(coordinator, old, done); err != nil {
		t.Fatalf("old physical completion attempted successor mutation: %v", err)
	}
	after, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if !reflect.DeepEqual(committed, after) || len(probe.requestsFor("self_release")) != 0 {
		t.Fatal("old physical completion changed the committed successor")
	}
	_, current, currentDone, err := replaceCoordinatorLoop(coordinator, ctx, record, "reconfigure", "", &patch, sessions.LifecycleMutationPlan{})
	if err != nil || current.Generation != committed.Generation {
		t.Fatalf("exact outcome adoption after old completion: token=%+v err=%v", current, err)
	}
	coordinator.cancelShutdownWork()
	if err := releaseCoordinatorLoop(coordinator, current, currentDone); err != nil {
		t.Fatal(err)
	}
}
