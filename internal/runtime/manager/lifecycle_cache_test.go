package manager

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestManagerFixtureRowOwnerSelectionIsExplicit(t *testing.T) {
	unit := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{})
	model, ok := unit.lifecycle.persistence().(*ephemeralLifecyclePersistenceProbe)
	if !ok || unit.lifecycle.stateReader != model || unit.lifecycle.outcomeReader != model {
		t.Fatal("storeless unit fixture did not select its explicit typed row/outcome model")
	}
	owner := newLifecyclePersistenceProbe()
	supplied := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{LifecycleStore: owner, PersistenceRoles: PersistenceRoles{LifecycleState: owner, LifecycleOutcome: owner}})
	if supplied.lifecycle.persistence() != owner || supplied.lifecycle.stateReader != owner || supplied.lifecycle.outcomeReader != owner {
		t.Fatal("fixture constructor replaced the supplied row/outcome roles")
	}
	roles := PersistenceRoles{LifecycleOutcome: owner}
	projectManagerTestPersistenceRoles(&roles, model)
	if roles.LifecycleOutcome != owner {
		t.Fatal("role projection overwrote the supplied immutable-outcome reader")
	}
	roles.LifecycleOutcome = nil
	projectManagerTestPersistenceRoles(&roles, owner)
	if roles.LifecycleOutcome != owner {
		t.Fatal("missing outcome role did not consume its exact supplied candidate")
	}
}

func TestAgentCurrentnessHasOneMutationAndReadOwner(t *testing.T) {
	cell := reflect.TypeOf(agentLifecycleCell{})
	view, found := cell.FieldByName("view")
	if !found || view.Type != reflect.TypeOf(AgentLifecycleState{}) {
		t.Fatal("agent cell does not cache the complete typed row")
	}
	for _, retired := range []string{"epoch", "generation", "phase", "configRevision", "runMode", "topology", "processBinding"} {
		if _, found := cell.FieldByName(retired); found {
			t.Fatalf("agent cell retains independent currentness field %s", retired)
		}
	}
	if _, found := reflect.TypeOf(AgentManagerOptions{}).FieldByName("SessionLifecycle"); found {
		t.Fatal("Manager exposes the retired optional projection owner")
	}
	retired := map[string]bool{"ApplyLifecycleProjection": true, "LifecycleProjectionRequest": true, "LifecycleProjectionRegistry": true}
	inspect := func(file *ast.File) bool {
		found := false
		ast.Inspect(file, func(node ast.Node) bool {
			if name, ok := node.(*ast.Ident); ok && retired[name.Name] {
				found = true
			}
			return true
		})
		return found
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if inspect(file) {
			t.Fatalf("production consumer %s restored a session currentness projection", entry.Name())
		}
	}
	hostile, err := parser.ParseFile(token.NewFileSet(), "hostile.go", "package manager; func f(s owner) { s.ApplyLifecycleProjection() }", 0)
	if err != nil || !inspect(hostile) {
		t.Fatalf("projection bypass escaped hostile census: %v", err)
	}
}

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

func TestAgentRegistrationCandidateIsBoundToObservedRow(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	record := lifecycleTestPersistedAgent(t)
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, probe, nil)
	builds := 0
	build := func() (Agent, error) {
		builds++
		if _, found := coordinator.stateByIdentity(record.Config.Identity); found {
			t.Fatal("candidate published before row adoption")
		}
		return reconfigureTestAgent{id: record.Config.ID}, nil
	}
	register := func(persist bool, candidate func() (Agent, error)) error {
		return coordinator.registerExecutionCandidate(ctx, record, persist, candidate, testManagerSubscriptionAdmission(t, record.Config), record.Topology, nil)
	}
	if err := register(false, build); err == nil || builds != 0 {
		t.Fatalf("missing row reached factory: builds=%d err=%v", builds, err)
	}
	if err := register(true, build); err != nil {
		t.Fatal(err)
	}
	if err := register(true, build); !errors.Is(err, ErrAgentAlreadyExists) || builds != 1 {
		t.Fatalf("existing row reached factory: builds=%d err=%v", builds, err)
	}
	coordinator.mu.Lock()
	delete(coordinator.cells, record.Config.Identity)
	coordinator.mu.Unlock()
	err := register(false, func() (Agent, error) {
		agent, err := build()
		probe.mu.Lock()
		view := probe.views[record.Config.Identity]
		view.Generation++
		view.LastTransitionID = uuid.NewString()
		probe.views[record.Config.Identity] = view
		probe.mu.Unlock()
		return agent, err
	})
	if err == nil || builds != 2 || len(coordinator.cells) != 0 {
		t.Fatalf("changed row published stale candidate: builds=%d err=%v cells=%d", builds, err, len(coordinator.cells))
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
	if err := coordinator.registerExecution(ctx, record, true, reconfigureTestAgent{id: record.Config.ID}, testManagerSubscriptionAdmission(t, record.Config)); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, coordinator, ctx, AgentRunModeStandard)
	_, old, done, err := replaceCoordinatorLoop(coordinator, ctx, record, "start", uuid.NewString(), nil, sessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := coordinator.acquireExecutionIdentity(ctx, record.Config.Identity, "before lost ack", true)
	if err != nil {
		t.Fatal(err)
	}
	admitted.Release()
	patch := record
	patch.Config.Tools = []string{"replacement-tool"}
	probe.failAfter = errors.New("lost native replacement acknowledgement")
	if _, _, _, err := replaceCoordinatorLoop(coordinator, ctx, record, "reconfigure", "", &patch, sessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("lost acknowledgment was reported as success")
	}
	committed, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if _, _, _, err := replaceCoordinatorLoop(coordinator, ctx, record, "reconfigure", "", &record, sessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("lost acknowledgment accepted the predecessor configuration as a no-op")
	}
	unchanged, _, _ := probe.LoadAgentLifecycleState(ctx, record.Config.Identity)
	if !sameAgentRowObservation(committed, unchanged) {
		t.Fatal("predecessor no-op changed the committed replacement")
	}
	lease, err := coordinator.acquireExecutionIdentity(ctx, record.Config.Identity, "lost-ack admission", true)
	if err == nil || lease != nil {
		if lease != nil {
			lease.Release()
		}
		t.Fatal("acknowledged native successor left predecessor admission open")
	}
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
