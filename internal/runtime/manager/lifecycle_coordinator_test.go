package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func releaseCoordinatorLoop(coordinator *agentLifecycleCoordinator, token runtimeeffects.LifecycleToken, done chan struct{}) error {
	return coordinator.releaseLoop(token, done)
}

func replaceCoordinatorLoop(
	coordinator *agentLifecycleCoordinator,
	ctx context.Context,
	rec PersistedAgent,
	trigger,
	operationID string,
	replacement *PersistedAgent,
	subordinate runtimesessions.LifecycleMutationPlan,
) (context.Context, runtimeeffects.LifecycleToken, chan struct{}, error) {
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return nil, runtimeeffects.LifecycleToken{}, nil, err
	}
	coordinator.executionPublishMu.Lock()
	defer coordinator.executionPublishMu.Unlock()
	cell, err := coordinator.lockIdentityOperation(identity)
	if err != nil {
		return nil, runtimeeffects.LifecycleToken{}, nil, err
	}
	defer cell.opMu.Unlock()
	return coordinator.replaceLoopLocked(ctx, identity.AgentID(), trigger, operationID, replacement, subordinate, nil, cell, runtimeeffects.LifecycleToken{})
}

func beginCoordinatorRun(t *testing.T, coordinator *agentLifecycleCoordinator, ctx context.Context, mode AgentRunMode) context.Context {
	t.Helper()
	runCtx, started, err := coordinator.beginRun(ctx, mode, newTestManagerWorkOwner(t))
	if err != nil {
		t.Fatalf("begin coordinator run: %v", err)
	}
	if !started {
		t.Fatal("coordinator run did not start")
	}
	coordinator.workMu.Lock()
	owner := coordinator.runOwner
	coordinator.workMu.Unlock()
	t.Cleanup(func() {
		coordinator.workMu.Lock()
		transitionExecutor := coordinator.transitionExecutor
		coordinator.transitionExecutor = nil
		coordinator.workRetiring = true
		coordinator.workMu.Unlock()
		coordinator.mu.Lock()
		coordinator.watcherExpected = false
		coordinator.mu.Unlock()
		if transitionExecutor != nil {
			if err := transitionExecutor.Done(); err != nil {
				t.Errorf("settle coordinator transition executor: %v", err)
			}
		}
		if owner == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.RetireAndWait(cleanupCtx); err != nil {
			t.Errorf("retire coordinator run owner: %v", err)
		}
	})
	return runCtx
}

type lifecyclePersistenceProbe struct {
	mu         sync.Mutex
	cell       lifecycleProbeCell
	exists     bool
	operations map[string]AgentLifecycleTransitionResult
	requests   []AgentLifecycleTransition
	failNext   error
	failAfter  error
}

type lifecycleProbeCell struct {
	Epoch      int64
	Generation uint64
	Phase      AgentLifecyclePhase
}

func newLifecyclePersistenceProbe() *lifecyclePersistenceProbe {
	return &lifecyclePersistenceProbe{operations: map[string]AgentLifecycleTransitionResult{}}
}

func lifecycleProbeProcessBinding() ProcessExecutionBinding {
	return ProcessExecutionBinding{
		ProcessAuthorityID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		ProcessOwnerID:     "manager-lifecycle-probe",
		ProcessBootID:      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		GenerationGrantID:  "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		BundleHash:         managerTestTopologyBundleHash,
		BundleSource:       "ephemeral",
		RuntimeInstanceID:  "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		RuntimeGeneration:  1,
	}
}

func (p *lifecyclePersistenceProbe) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	binding := lifecycleProbeProcessBinding()
	return binding, binding.Validate()
}

type lifecycleReintroductionBindingProbe struct {
	*lifecyclePersistenceProbe
	binding ProcessExecutionBinding
}

func (p lifecycleReintroductionBindingProbe) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return p.binding, p.binding.Validate()
}

func TestLifecycleReintroductionClassifiesExactExecutionAuthority(t *testing.T) {
	previous := lifecycleProbeProcessBinding()
	sameOwnerGrant := previous
	sameOwnerGrant.GenerationGrantID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	sameOwnerGrant.RuntimeGeneration++
	successorProcess := sameOwnerGrant
	successorProcess.ProcessAuthorityID = "11111111-1111-4111-8111-111111111111"
	successorProcess.ProcessOwnerID = "manager-lifecycle-successor"
	successorProcess.ProcessBootID = "22222222-2222-4222-8222-222222222222"
	successorProcess.GenerationGrantID = "33333333-3333-4333-8333-333333333333"
	successorProcess.RuntimeInstanceID = "44444444-4444-4444-8444-444444444444"

	for _, tc := range []struct {
		name   string
		target ProcessExecutionBinding
		want   string
	}{
		{name: "same grant is ordinary restart", target: previous, want: "restart"},
		{name: "same process new grant is source-set rebind", target: sameOwnerGrant, want: "source_set_rebind"},
		{name: "successor process is takeover", target: successorProcess, want: "process_takeover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := lifecycleReintroductionBindingProbe{lifecyclePersistenceProbe: newLifecyclePersistenceProbe(), binding: tc.target}
			got, target, err := lifecycleReintroductionAuthority(store, previous)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || !target.Equal(tc.target) {
				t.Fatalf("classification = %q %#v, want %q %#v", got, target, tc.want, tc.target)
			}
		})
	}
}

func TestLifecycleReintroductionRejectsPersistenceWithoutExecutionBinding(t *testing.T) {
	store := struct{ AgentLifecyclePersistence }{AgentLifecyclePersistence: newLifecyclePersistenceProbe()}
	if _, _, err := lifecycleReintroductionAuthority(store, lifecycleProbeProcessBinding()); err == nil ||
		!strings.Contains(err.Error(), "requires process execution binding") {
		t.Fatalf("missing binding error = %v", err)
	}
}

func TestLifecycleTerminalMutationClassifiesGrantRetirementAndProcessTakeover(t *testing.T) {
	previous := lifecycleProbeProcessBinding()
	sameOwnerGrant := previous
	sameOwnerGrant.GenerationGrantID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	sameOwnerGrant.RuntimeGeneration++
	successorProcess := sameOwnerGrant
	successorProcess.ProcessAuthorityID = "11111111-1111-4111-8111-111111111111"
	successorProcess.ProcessOwnerID = "manager-lifecycle-successor"
	successorProcess.ProcessBootID = "22222222-2222-4222-8222-222222222222"

	for _, tc := range []struct {
		name   string
		target ProcessExecutionBinding
		want   string
	}{
		{name: "same process retires old grant", target: sameOwnerGrant, want: "source_set_retire"},
		{name: "successor process takes over terminal transition", target: successorProcess, want: "process_takeover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := lifecycleReintroductionBindingProbe{lifecyclePersistenceProbe: newLifecyclePersistenceProbe(), binding: tc.target}
			got, _, err := lifecycleMutationExecutionAuthority(store, previous, "teardown", true)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("terminal classification = %q, want %q", got, tc.want)
			}
		})
	}
}

func (p *lifecyclePersistenceProbe) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if p.failNext != nil {
		err := p.failNext
		p.failNext = nil
		return AgentLifecycleTransitionResult{}, err
	}
	if result, ok := p.operations[req.OperationID]; ok {
		result.Replayed = true
		return result, nil
	}
	if p.exists {
		if p.cell.Epoch != req.ExpectedEpoch || p.cell.Generation != req.ExpectedGeneration || p.cell.Phase != req.ExpectedPhase {
			return AgentLifecycleTransitionResult{}, fmt.Errorf("probe lifecycle conflict")
		}
	} else if req.OperationKind != "spawn" {
		return AgentLifecycleTransitionResult{}, fmt.Errorf("probe lifecycle cell absent")
	}
	result := AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: p.cell.Epoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: p.cell.Generation, Generation: req.TargetGeneration,
		PreviousPhase: p.cell.Phase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode, Topology: req.Topology,
		ProcessBinding: lifecycleProbeProcessBinding(),
	}
	p.cell = lifecycleProbeCell{Epoch: req.TargetEpoch, Generation: req.TargetGeneration, Phase: req.TargetPhase}
	p.exists = true
	p.operations[req.OperationID] = result
	if p.failAfter != nil {
		err := p.failAfter
		p.failAfter = nil
		return AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func (p *lifecyclePersistenceProbe) requestsFor(kind string) []AgentLifecycleTransition {
	p.mu.Lock()
	defer p.mu.Unlock()
	requests := make([]AgentLifecycleTransition, 0, len(p.requests))
	for _, req := range p.requests {
		if req.OperationKind == kind {
			requests = append(requests, req)
		}
	}
	return requests
}

func TestLifecycleCoordinatorReplayDoesNotReplaceSuccessfulGeneration(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	operationID := uuid.NewString()
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", operationID, nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil || loopCtx == nil {
		t.Fatalf("first replacement ctx=%v token=%+v err=%v", loopCtx, token, err)
	}
	replayedCtx, replayedToken, replayedDone, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", operationID, nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("replay replacement: %v", err)
	}
	if replayedCtx != nil || replayedDone != nil || replayedToken != token {
		t.Fatalf("replay created another owner: ctx=%v token=%+v done=%v want token=%+v", replayedCtx, replayedToken, replayedDone, token)
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("replay cancelled the successful generation")
	default:
	}
	coordinator.cancelShutdownWork()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorReconfigureOperationIdentityTracksTransitionOccurrence(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	base := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	plan := runtimesessions.LifecycleMutationPlan{
		Action:            runtimesessions.LifecycleMutationRotateCurrentSet,
		TerminationReason: runtimesessions.TerminationReasonNormal,
		TerminationDetail: "agent_reconfigured",
		CheckpointSummary: "agent reconfigured",
	}
	recA := base
	recA.Config.Tools = []string{"tool-a"}
	recB := base
	recB.Config.Tools = []string{"tool-b"}
	for i, rec := range []*PersistedAgent{&recA, &recB, &recA} {
		if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", rec, plan); err != nil {
			t.Fatalf("reconfigure occurrence %d: %v", i+1, err)
		}
	}
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", &recA, plan); err != nil {
		t.Fatalf("same-current reconfigure: %v", err)
	}

	requests := probe.requestsFor("reconfigure")
	if len(requests) != 3 {
		t.Fatalf("reconfigure requests = %d, want 3 committed occurrences", len(requests))
	}
	seen := map[string]struct{}{}
	for i, req := range requests {
		if _, duplicate := seen[req.OperationID]; duplicate {
			t.Fatalf("occurrence %d reused operation_id %q", i+1, req.OperationID)
		}
		seen[req.OperationID] = struct{}{}
		if i > 0 && req.ExpectedGeneration != requests[i-1].TargetGeneration {
			t.Fatalf("occurrence %d expected generation = %d, want %d", i+1, req.ExpectedGeneration, requests[i-1].TargetGeneration)
		}
	}
	if requests[0].ConfigRevision != requests[2].ConfigRevision {
		t.Fatalf("A -> B -> A revisions differ: first=%q third=%q", requests[0].ConfigRevision, requests[2].ConfigRevision)
	}
	if requests[0].Subordinate.Action != requests[2].Subordinate.Action {
		t.Fatalf("A -> B -> A plans differ: first=%q third=%q", requests[0].Subordinate.Action, requests[2].Subordinate.Action)
	}
}

func TestLifecycleCoordinatorReconfigureOperationIdentityIsStableBeforeCommit(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	base := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	target := base
	target.Config.Tools = []string{"tool-a"}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected persistence failure")
	probe.mu.Unlock()
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("first reconfigure succeeded despite persistence failure")
	}
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err != nil {
		t.Fatalf("retry reconfigure: %v", err)
	}
	requests := probe.requestsFor("reconfigure")
	if len(requests) != 2 {
		t.Fatalf("reconfigure attempts = %d, want 2", len(requests))
	}
	if requests[0].OperationID != requests[1].OperationID {
		t.Fatalf("retry operation ids differ: first=%q retry=%q", requests[0].OperationID, requests[1].OperationID)
	}
}

func TestLifecycleCoordinatorReconfigureRetryAdoptsCommittedOccurrence(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	base := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), base, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	target := base
	target.Config.Tools = []string{"tool-a"}
	probe.mu.Lock()
	probe.failAfter = fmt.Errorf("injected response loss after commit")
	probe.mu.Unlock()
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("first reconfigure observed success despite injected response loss")
	}
	cell, ok := testLifecycleCell(t, coordinator, base.Config.ID, "")
	if !ok {
		t.Fatal("lifecycle cell not found")
	}
	beforeRetry := runtimeeffects.LifecycleToken{
		RuntimeEpoch: cell.epoch,
		Identity:     cell.identity,
		AgentID:      base.Config.ID,
		Generation:   cell.generation,
	}
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), base, "reconfigure", "", &target, runtimesessions.LifecycleMutationPlan{}); err != nil {
		t.Fatalf("retry reconfigure: %v", err)
	}
	cell, ok = testLifecycleCell(t, coordinator, base.Config.ID, "")
	if !ok {
		t.Fatal("lifecycle cell not found after retry")
	}
	afterRetry := cell.generation
	if afterRetry != beforeRetry.Generation+1 {
		t.Fatalf("retry generation = %d, want committed successor %d", afterRetry, beforeRetry.Generation+1)
	}
	requests := probe.requestsFor("reconfigure")
	if len(requests) != 2 || requests[0].OperationID != requests[1].OperationID {
		t.Fatalf("response-loss retry requests = %#v, want one stable operation identity", requests)
	}
}

func TestLifecycleCoordinatorPersistenceFailureLeavesPriorGenerationOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected persistence failure")
	probe.mu.Unlock()
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("restart succeeded despite persistence failure")
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("persistence failure cancelled the prior generation")
	default:
	}
	current, ok := coordinator.tokenIdentity(rec.Config.Identity)
	if !ok || current != token {
		t.Fatalf("current token = %+v ok=%v, want %+v", current, ok, token)
	}
	coordinator.cancelShutdownWork()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorSpawnPersistenceFailurePublishesNoCell(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	probe.failNext = fmt.Errorf("injected spawn persistence failure")
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err == nil {
		t.Fatal("register succeeded despite persistence failure")
	}
	_, exists := testLifecycleCell(t, coordinator, rec.Config.ID, "")
	if exists {
		t.Fatal("spawn persistence failure published a lifecycle cell")
	}
}

func TestLifecycleCoordinatorRecoveredGenerationZeroAdvancesFromDurableValue(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	epoch := runtimebus.CurrentRuntimeEpoch()
	probe.cell = lifecycleProbeCell{Epoch: epoch, Generation: 0, Phase: AgentLifecycleRegistered}
	probe.exists = true
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	rec.LifecycleEpoch = epoch
	rec.LifecycleGeneration = 0
	rec.LifecyclePhase = AgentLifecycleRegistered
	rec.LifecycleRunMode = AgentRunModeStopped
	rec.ProcessBinding = lifecycleProbeProcessBinding()
	if err := coordinator.registerExecution(testAuthorActivityContext(context.Background()), rec, false, reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
		t.Fatalf("register recovered agent: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start recovered generation zero: %v", err)
	}
	if token.Generation != 1 {
		t.Fatalf("recovered generation = %d, want 1 after transition from durable zero", token.Generation)
	}
	coordinator.cancelShutdownWork()
	<-loopCtx.Done()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release recovered loop: %v", err)
	}
}

func TestLifecycleCoordinatorInMemoryEffectContextCarriesCurrentToken(t *testing.T) {
	registry := runtimesessions.NewInMemoryRegistry(0)
	coordinator := newAgentLifecycleCoordinator(nil, registry, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.registerExecution(testAuthorActivityContext(context.Background()), rec, false, reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, managedExecutionTestContext(t, testAuthorActivityContext(context.Background())), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	lease, err := coordinator.acquireExecutionIdentity(testAuthorActivityContext(context.Background()), rec.Config.Identity, "test_effect_context", true)
	if err != nil {
		t.Fatalf("acquireExecution: %v", err)
	}
	got, ok := runtimeeffects.LifecycleTokenFromContext(lease.Context)
	if !ok || got != token {
		t.Fatalf("effect token = %+v ok=%v, want %+v", got, ok, token)
	}
	if admission, ok := managedexecution.FromContext(lease.Context); !ok || !admission.AuthorizesNormal() {
		t.Fatalf("effect admission = %+v ok=%v, want normal runtime admission", admission, ok)
	}
	lease.Release()
	coordinator.cancelShutdownWork()
	<-loopCtx.Done()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestLifecycleCoordinatorTeardownPersistenceFailureLeavesLoopOwned(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected teardown persistence failure")
	probe.mu.Unlock()
	if _, err := coordinator.terminateIdentityWithTopologyExpected(
		testAuthorActivityContext(context.Background()),
		rec.Config.Identity,
		"teardown",
		AgentLifecycleTerminated,
		nil,
		&rec.Config,
		false,
	); err == nil {
		t.Fatal("teardown succeeded despite persistence failure")
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("teardown persistence failure cancelled the current loop")
	default:
	}
	if current, ok := coordinator.tokenIdentity(rec.Config.Identity); !ok || current != token {
		t.Fatalf("current token = %+v ok=%v, want %+v", current, ok, token)
	}
	coordinator.cancelShutdownWork()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestLifecycleCoordinatorSelfRetirementCommitsAfterAcceptedLoopSettles(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, managedExecutionTestContext(t, testAuthorActivityContext(context.Background())), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	coordinator.mu.Lock()
	cell := coordinator.cells[token.Identity.Normalize()]
	cell.execution.routeToken = token
	stopAfterAccepted := cell.execution.stopAfterAccepted
	coordinator.mu.Unlock()

	if _, err := coordinator.terminateIdentityWithTopologyExpected(
		testAuthorActivityContext(context.Background()),
		rec.Config.Identity,
		"flow_instance_terminal",
		AgentLifecycleTerminated,
		nil,
		nil,
		true,
	); err != nil {
		t.Fatalf("defer self retirement: %v", err)
	}
	if got := len(probe.requestsFor("teardown")); got != 0 {
		t.Fatalf("terminal writes before accepted loop settlement = %d, want 0", got)
	}
	select {
	case <-loopCtx.Done():
		t.Fatal("self retirement cancelled accepted generation before loop settlement")
	default:
	}
	select {
	case <-stopAfterAccepted:
	default:
		t.Fatal("self retirement did not request loop stop after accepted work")
	}
	if current, ok := coordinator.tokenIdentity(rec.Config.Identity); !ok || current != token {
		t.Fatalf("current token before settlement = %+v ok=%v, want %+v", current, ok, token)
	}

	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release accepted loop: %v", err)
	}
	requests := probe.requestsFor("teardown")
	if len(requests) != 1 || requests[0].Trigger != "flow_instance_terminal" || requests[0].ExpectedGeneration != token.Generation {
		t.Fatalf("deferred terminal writes = %#v, want one exact flow terminalization", requests)
	}
	if got := len(probe.requestsFor("self_release")); got != 0 {
		t.Fatalf("self-release writes after deferred terminalization = %d, want 0", got)
	}
	coordinator.mu.Lock()
	cell = coordinator.cells[token.Identity.Normalize()]
	phase, generation := cell.phase, cell.generation
	coordinator.mu.Unlock()
	if phase != AgentLifecycleTerminated || generation != token.Generation+1 {
		t.Fatalf("final lifecycle = %s/%d, want terminated/%d", phase, generation, token.Generation+1)
	}
}

func TestLifecycleCoordinatorRestartVersusTeardownNeverResurrectsLoop(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	go func() {
		<-initialCtx.Done()
		_ = releaseCoordinatorLoop(coordinator, initialToken, initialDone)
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		loopCtx, token, done, restartErr := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
		if restartErr == nil && loopCtx != nil {
			go func() {
				<-loopCtx.Done()
				_ = releaseCoordinatorLoop(coordinator, token, done)
			}()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		_, _ = coordinator.terminateIdentityWithTopology(testAuthorActivityContext(context.Background()), rec.Config.Identity, "teardown", AgentLifecycleTerminated, nil)
	}()
	close(start)
	wg.Wait()

	if token, ok := coordinator.tokenIdentity(rec.Config.Identity); ok {
		t.Fatalf("restart-versus-teardown left live token %+v", token)
	}
	cell, _ := testLifecycleCell(t, coordinator, rec.Config.ID, "")
	coordinator.mu.Lock()
	var phase AgentLifecyclePhase
	var cancel context.CancelFunc
	var done chan struct{}
	if cell != nil {
		phase = cell.phase
		if cell.execution != nil {
			cancel, done = cell.execution.loopCancel, cell.execution.loopDone
		}
	}
	coordinator.mu.Unlock()
	if cell == nil || phase != AgentLifecycleTerminated || cancel != nil || done != nil {
		t.Fatalf("final lifecycle cell phase=%s cancel=%v done=%v, want terminated without loop owner", phase, cancel != nil, done != nil)
	}
}

func TestLifecycleCoordinatorSelfReleasePersistenceFailureFailsClosed(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	probe.mu.Lock()
	probe.failNext = fmt.Errorf("injected self-release persistence failure")
	probe.mu.Unlock()
	if err := releaseCoordinatorLoop(coordinator, token, done); err == nil {
		t.Fatal("self-release succeeded despite persistence failure")
	}
	if _, ok := coordinator.tokenIdentity(rec.Config.Identity); ok {
		t.Fatal("failed self-release remained available as a running generation")
	}
	if _, _, _, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("restart admitted over failed self-release")
	}
}

func TestLifecycleCoordinatorConcurrentReplacementsCommitAdjacentGenerations(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.register(testAuthorActivityContext(context.Background()), rec, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	initialCtx, initialToken, initialDone, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("initial start: %v", err)
	}
	go func() {
		<-initialCtx.Done()
		_ = releaseCoordinatorLoop(coordinator, initialToken, initialDone)
	}()

	const replacements = 8
	var wg sync.WaitGroup
	generations := make(chan uint64, replacements)
	errs := make(chan error, replacements)
	for i := 0; i < replacements; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				errs <- err
				return
			}
			generations <- token.Generation
			go func() {
				<-loopCtx.Done()
				_ = releaseCoordinatorLoop(coordinator, token, done)
			}()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent replacement: %v", err)
	}
	close(generations)
	got := make([]int, 0, replacements)
	for generation := range generations {
		got = append(got, int(generation))
	}
	sort.Ints(got)
	for i, generation := range got {
		want := int(initialToken.Generation) + i + 1
		if generation != want {
			t.Fatalf("generation[%d] = %d, want adjacent %d; all=%v", i, generation, want, got)
		}
	}
	transition := coordinator.requestShutdownTransition()
	if !coordinator.claimTransition(transition, runtimeLifecycleTransitionShutdown) {
		t.Fatal("claim shutdown transition")
	}
	coordinator.cancelShutdownWork()
	coordinator.completeShutdownTransition(transition, nil)
}

func TestLifecycleCoordinatorStartupAbortCompletesIssuedTransitions(t *testing.T) {
	for _, request := range []string{"shutdown", "reset"} {
		t.Run(request, func(t *testing.T) {
			coordinator := newAgentLifecycleCoordinator(nil, nil, nil, nil, nil)
			beginCoordinatorRun(t, coordinator, context.Background(), AgentRunModeStandard)
			var transitions []*runtimeLifecycleTransition
			switch request {
			case "shutdown":
				transitions = append(transitions, coordinator.requestShutdownTransition())
			case "reset":
				shutdown, reset := coordinator.requestResetTransition()
				transitions = append(transitions, shutdown, reset)
			}
			startErr := fmt.Errorf("injected shutdown watcher admission failure")
			if err := coordinator.abortRunStart(startErr); err != nil {
				t.Fatalf("abort run start: %v", err)
			}
			for _, transition := range transitions {
				if transition == nil {
					t.Fatal("startup race did not retain the issued transition")
				}
				select {
				case <-transition.done:
				case <-time.After(time.Second):
					t.Fatal("startup abort discarded an issued transition")
				}
				if !errors.Is(transition.result, startErr) {
					t.Fatalf("transition result = %v, want startup error", transition.result)
				}
			}
		})
	}
}

func lifecycleTestPersistedAgent(t testing.TB) PersistedAgent {
	t.Helper()
	return PersistedAgent{
		Config: managerTestAgentConfig(runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: "agent-lifecycle-test", Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global",
			Identity: runtimeagentidentitytest.RootRuntime(t, "agent-lifecycle-test", "lifecycle-test"),
		}),
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(), Topology: managerTestTopologyAdmission(t),
	}
}
