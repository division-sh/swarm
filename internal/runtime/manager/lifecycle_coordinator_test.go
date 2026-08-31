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

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
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

func registerCoordinatorLifecycleCell(t *testing.T, coordinator *agentLifecycleCoordinator, ctx context.Context, rec PersistedAgent, persist bool) error {
	t.Helper()
	return coordinator.registerExecution(ctx, rec, persist, nil, testManagerSubscriptionAdmission(t, rec.Config))
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), base, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), base, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), base, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err == nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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

func TestLifecycleCoordinatorDeliveryAdmissionFenceWins(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.registerExecution(
		testAuthorActivityContext(context.Background()), rec, true,
		reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config),
	); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	coordinator.mu.Lock()
	coordinator.cells[token.Identity.Normalize()].execution.routeToken = token
	coordinator.mu.Unlock()

	if _, err := coordinator.terminateIdentityWithTopologyExpected(
		testAuthorActivityContext(context.Background()), rec.Config.Identity, "flow_instance_terminal",
		AgentLifecycleTerminated, nil, nil, true,
	); err != nil {
		t.Fatalf("fence execution: %v", err)
	}
	if lease, err := coordinator.acquireDeliveryExecution(loopCtx, token); err == nil {
		lease.Release()
		t.Fatal("fenced execution admitted a delivery")
	}
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release fenced loop: %v", err)
	}
}

func TestLifecycleCoordinatorSourceSetTransitionBlocksDirectAndWaitsDelivery(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.registerExecution(
		testAuthorActivityContext(context.Background()), rec, true,
		reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config),
	); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(
		coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil,
		runtimesessions.LifecycleMutationPlan{},
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	coordinator.mu.Lock()
	coordinator.cells[token.Identity.Normalize()].execution.routeToken = token
	coordinator.mu.Unlock()

	admission := newSourceSetTransitionAdmissionProbe("source-set-successor")
	if err := coordinator.installSourceSetTransitionAdmission(admission, false); err != nil {
		t.Fatalf("install source-set transition admission: %v", err)
	}
	if lease, err := coordinator.acquireExecutionIdentity(loopCtx, token.Identity, "execute_directive", true); err == nil {
		lease.Release()
		t.Fatal("pending source-set transition admitted direct execution")
	} else if !strings.Contains(err.Error(), "source_set_transition_pending") {
		t.Fatalf("direct execution error=%v, want typed source-set transition conflict", err)
	}
	if _, _, _, err := replaceCoordinatorLoop(
		coordinator, testAuthorActivityContext(context.Background()), rec, "restart", uuid.NewString(), nil,
		runtimesessions.LifecycleMutationPlan{},
	); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
		admission.release()
		t.Fatalf("restart during source-set transition error=%v, want typed conflict", err)
	}
	if _, err := coordinator.terminateIdentityWithTopologyExpected(
		testAuthorActivityContext(context.Background()), rec.Config.Identity, "teardown",
		AgentLifecycleTerminated, nil, nil, true,
	); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
		admission.release()
		t.Fatalf("teardown during source-set transition error=%v, want typed conflict", err)
	}

	type deliveryResult struct {
		lease *agentExecutionLease
		err   error
	}
	result := make(chan deliveryResult, 1)
	go func() {
		lease, acquireErr := coordinator.acquireDeliveryExecution(loopCtx, token)
		result <- deliveryResult{lease: lease, err: acquireErr}
	}()
	select {
	case got := <-result:
		if got.lease != nil {
			got.lease.Release()
		}
		t.Fatalf("delivery admission returned while transition pending: %v", got.err)
	case <-time.After(100 * time.Millisecond):
	}
	admission.release()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("delivery admission after aggregate release: %v", got.err)
		}
		got.lease.Release()
	case <-time.After(time.Second):
		t.Fatal("delivery admission did not resume after aggregate release")
	}
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release loop: %v", err)
	}
}

func TestSourceSetTransitionKeepsRealEventBusDeliveryPendingUntilAggregateRelease(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	deliveryStore := newManagerDeliveryTestStore(t)
	persistence := &startupReplayTestStore{
		recoveryTestStore: recoveryTestStore{}, managerDeliveryTestStore: deliveryStore,
	}
	probe := lifecycletest.New(t)
	eventBus, err := newTestManagerEventBus(t)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	called := make(chan struct{}, 1)
	agent := shutdownTestAgent{
		id:            "source-set-waiting-agent",
		subscriptions: []events.EventType{"test.source_set_wait"},
		onEvent: func(context.Context, events.Event) ([]events.Event, error) {
			called <- struct{}{}
			return nil, nil
		},
	}
	manager := newTestAgentManagerWithOptions(t, eventBus, func(runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, AgentManagerOptions{DeliveryStore: deliveryStore, TestLifecycleProbe: probe.Raw()}, persistence)
	record := PersistedAgent{
		Topology: managerTestTopologyAdmission(t), ProcessBinding: lifecycleProbeProcessBinding(),
		Config: managerTestAgentConfig(runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: agent.ID(), Identity: managerAgentIdentity(agent.ID()),
			Subscriptions: []string{"test.source_set_wait"},
		}),
	}
	if err := manager.spawnAgentInternal(testAuthorActivityContext(context.Background()), record, false); err != nil {
		t.Fatalf("spawn waiting agent: %v", err)
	}
	if err := manager.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("run waiting manager: %v", err)
	}
	t.Cleanup(func() {
		if shutdownErr := manager.Shutdown(); shutdownErr != nil {
			t.Errorf("shutdown waiting manager: %v", shutdownErr)
		}
	})

	admission := newSourceSetTransitionAdmissionProbe("source-set-successor")
	if err := manager.lifecycle.installSourceSetTransitionAdmission(admission, false); err != nil {
		t.Fatalf("install source-set transition admission: %v", err)
	}
	if err := manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
		admission.release()
		t.Fatalf("shutdown during source-set transition error=%v, want typed conflict", err)
	}
	if err := manager.ResetRuntimeState(); err == nil || !strings.Contains(err.Error(), "source_set_transition_pending") {
		admission.release()
		t.Fatalf("reset during source-set transition error=%v, want typed conflict", err)
	}
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("source-set-waiting-event"), events.EventType("test.source_set_wait"), "test", "", nil, 0,
		managerIdentityTestRunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := eventBus.Publish(testAuthorActivityContext(context.Background()), evt); err != nil {
		t.Fatalf("publish waiting event: %v", err)
	}
	deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), managerAgentDeliveryRouteForRun(evt.RunID(), agent.ID()))
	if err != nil {
		admission.release()
		t.Fatal(err)
	}
	select {
	case <-called:
		admission.release()
		t.Fatal("pending aggregate transition invoked the agent")
	case <-time.After(150 * time.Millisecond):
	}
	if snapshot, snapshotErr := deliveryStore.Snapshot(context.Background(), deliveryID); !errors.Is(snapshotErr, runtimedelivery.ErrNotFound) {
		admission.release()
		t.Fatalf("waiting delivery = %#v err=%v, want no accepted obligation", snapshot, snapshotErr)
	}

	admission.release()
	probe.RequireAgentDelivered(evt.ID(), agent.ID())
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("released aggregate transition did not invoke the agent")
	}
	snapshot, err := deliveryStore.Snapshot(context.Background(), deliveryID)
	if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("released delivery = %#v err=%v, want delivered", snapshot, err)
	}
}

type sourceSetTransitionRouteTrackingBus struct {
	*runtimebus.EventBus
	mu      sync.Mutex
	removed []runtimeeffects.LifecycleToken
}

func (b *sourceSetTransitionRouteTrackingBus) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	b.mu.Lock()
	b.removed = append(b.removed, token)
	b.mu.Unlock()
	b.EventBus.RemoveAgentRoute(token)
}

func (b *sourceSetTransitionRouteTrackingBus) removedTokens() []runtimeeffects.LifecycleToken {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]runtimeeffects.LifecycleToken(nil), b.removed...)
}

func TestSourceSetTransitionRetainsDequeuedDeliveryAndRouteAcrossManagerCancellation(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	deliveryStore := newManagerDeliveryTestStore(t)
	persistence := &startupReplayTestStore{
		recoveryTestStore: recoveryTestStore{}, managerDeliveryTestStore: deliveryStore,
	}
	eventBus, err := newTestManagerEventBus(t)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	trackingBus := &sourceSetTransitionRouteTrackingBus{EventBus: eventBus}
	called := make(chan struct{}, 1)
	agent := shutdownTestAgent{
		id:            "source-set-cancelled-agent",
		subscriptions: []events.EventType{"test.source_set_cancelled"},
		onEvent: func(context.Context, events.Event) ([]events.Event, error) {
			called <- struct{}{}
			return nil, nil
		},
	}
	manager := newTestAgentManagerWithOptions(t, trackingBus, func(runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, AgentManagerOptions{DeliveryStore: deliveryStore}, persistence)
	record := PersistedAgent{
		Topology: managerTestTopologyAdmission(t), ProcessBinding: lifecycleProbeProcessBinding(),
		Config: managerTestAgentConfig(runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: agent.ID(), Identity: managerAgentIdentity(agent.ID()),
			Subscriptions: []string{"test.source_set_cancelled"},
		}),
	}
	if err := manager.spawnAgentInternal(testAuthorActivityContext(context.Background()), record, false); err != nil {
		t.Fatalf("spawn waiting agent: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(managedExecutionTestContext(t, testAuthorActivityContext(context.Background())))
	if err := manager.Run(runCtx); err != nil {
		cancelRun()
		t.Fatalf("run waiting manager: %v", err)
	}
	gate := newSourceSetTransitionAdmissionProbe("source-set-successor")
	t.Cleanup(func() {
		gate.release()
		cancelRun()
		_ = manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	if err := manager.lifecycle.installSourceSetTransitionAdmission(gate, false); err != nil {
		t.Fatalf("install source-set transition admission: %v", err)
	}
	baselineReads := gate.readCount()
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("source-set-cancelled-event"), events.EventType("test.source_set_cancelled"), "test", "", nil, 0,
		managerIdentityTestRunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := eventBus.Publish(testAuthorActivityContext(context.Background()), evt); err != nil {
		t.Fatalf("publish waiting event: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for gate.readCount() == baselineReads && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if gate.readCount() == baselineReads {
		t.Fatal("real EventBus delivery was not dequeued into transition admission")
	}

	identity, err := record.Config.ConcreteIdentity()
	if err != nil {
		t.Fatal(err)
	}
	manager.lifecycle.mu.Lock()
	cell := manager.lifecycle.cells[identity.Normalize()]
	token := cell.execution.routeToken
	done := cell.execution.loopDone
	manager.lifecycle.mu.Unlock()
	if !token.Valid() || done == nil {
		t.Fatalf("running generation omitted route/loop authority: token=%#v done=%t", token, done != nil)
	}

	cancelRun()
	waitForManagerShuttingDown(t, manager)
	time.Sleep(50 * time.Millisecond)
	if removed := trackingBus.removedTokens(); len(removed) != 0 {
		t.Fatalf("route retired before source-set transition settled: %#v", removed)
	}
	select {
	case <-done:
		t.Fatal("loop completion published before source-set transition settled")
	default:
	}
	select {
	case <-called:
		t.Fatal("dequeued delivery invoked agent while source-set transition was pending")
	default:
	}
	deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), managerAgentDeliveryRouteForRun(evt.RunID(), agent.ID()))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, snapshotErr := deliveryStore.Snapshot(context.Background(), deliveryID); !errors.Is(snapshotErr, runtimedelivery.ErrNotFound) {
		t.Fatalf("waiting delivery = %#v err=%v, want no accepted obligation", snapshot, snapshotErr)
	}

	gate.release()
	if lease, acquireErr := manager.lifecycle.acquireExecutionIdentity(
		testAuthorActivityContext(context.Background()), identity, "execute_directive", false,
	); acquireErr == nil {
		lease.Release()
		t.Fatal("cancelled generation admitted direct execution after source-set release")
	} else if !strings.Contains(acquireErr.Error(), "lifecycle_generation_not_running") {
		t.Fatalf("cancelled generation admission error=%v, want lifecycle_generation_not_running", acquireErr)
	}
	if err := manager.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
		t.Fatalf("join manager shutdown after source-set release: %v", err)
	}
	removed := trackingBus.removedTokens()
	if len(removed) != 1 || removed[0] != token {
		t.Fatalf("retired routes=%#v, want exact generation %#v once", removed, token)
	}
	select {
	case <-done:
	default:
		t.Fatal("loop completion remained open after source-set transition settled")
	}
	select {
	case <-called:
		t.Fatal("cancelled dequeued delivery invoked agent after source-set release")
	default:
	}
}

func TestLifecycleCoordinatorDeliveryAdmissionWinsBeforeDeferredFence(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := coordinator.registerExecution(
		testAuthorActivityContext(context.Background()), rec, true,
		reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config),
	); err != nil {
		t.Fatalf("register: %v", err)
	}
	beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
	loopCtx, token, done, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	coordinator.mu.Lock()
	coordinator.cells[token.Identity.Normalize()].execution.routeToken = token
	coordinator.mu.Unlock()

	lease, err := coordinator.acquireDeliveryExecution(loopCtx, token)
	if err != nil {
		t.Fatalf("admit delivery before fence: %v", err)
	}
	if _, err := coordinator.terminateIdentityWithTopologyExpected(
		testAuthorActivityContext(context.Background()), rec.Config.Identity, "flow_instance_terminal",
		AgentLifecycleTerminated, nil, nil, true,
	); err != nil {
		t.Fatalf("defer execution fence: %v", err)
	}
	select {
	case <-lease.Context.Done():
		t.Fatal("deferred fence cancelled already-admitted delivery work")
	default:
	}
	if second, err := coordinator.acquireDeliveryExecution(loopCtx, token); err == nil {
		second.Release()
		t.Fatal("deferred fence admitted a second delivery")
	}
	lease.Release()
	if err := releaseCoordinatorLoop(coordinator, token, done); err != nil {
		t.Fatalf("release accepted loop: %v", err)
	}
}

func TestLifecycleCoordinatorReplacementRejectsPredecessorDeliveryAdmission(t *testing.T) {
	tests := []struct {
		name    string
		trigger string
		mutate  func(*PersistedAgent)
	}{
		{name: "restart", trigger: "restart"},
		{name: "reconfigure", trigger: "reconfigure", mutate: func(rec *PersistedAgent) { rec.Config.Role = "replacement-worker" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := newLifecyclePersistenceProbe()
			coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
			rec := lifecycleTestPersistedAgent(t)
			if err := coordinator.registerExecution(
				testAuthorActivityContext(context.Background()), rec, true,
				reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config),
			); err != nil {
				t.Fatalf("register: %v", err)
			}
			beginCoordinatorRun(t, coordinator, testAuthorActivityContext(context.Background()), AgentRunModeStandard)
			oldCtx, oldToken, oldDone, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			coordinator.mu.Lock()
			coordinator.cells[oldToken.Identity.Normalize()].execution.routeToken = oldToken
			coordinator.mu.Unlock()
			go func() {
				<-oldCtx.Done()
				_ = releaseCoordinatorLoop(coordinator, oldToken, oldDone)
			}()

			replacement := rec
			var replacementRecord *PersistedAgent
			if test.mutate != nil {
				test.mutate(&replacement)
				replacementRecord = &replacement
			}
			newCtx, newToken, newDone, err := replaceCoordinatorLoop(coordinator, testAuthorActivityContext(context.Background()), rec, test.trigger, uuid.NewString(), replacementRecord, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatalf("replace execution: %v", err)
			}
			coordinator.mu.Lock()
			coordinator.cells[newToken.Identity.Normalize()].execution.routeToken = newToken
			coordinator.mu.Unlock()
			if lease, err := coordinator.acquireDeliveryExecution(context.Background(), oldToken); err == nil {
				lease.Release()
				t.Fatal("replacement admitted predecessor delivery token")
			}
			lease, err := coordinator.acquireDeliveryExecution(newCtx, newToken)
			if err != nil {
				t.Fatalf("replacement token delivery admission: %v", err)
			}
			lease.Release()

			coordinator.cancelShutdownWork()
			<-newCtx.Done()
			if err := releaseCoordinatorLoop(coordinator, newToken, newDone); err != nil {
				t.Fatalf("release replacement loop: %v", err)
			}
		})
	}
}

func TestLifecycleCoordinatorRestartVersusTeardownNeverResurrectsLoop(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	coordinator := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
	if err := registerCoordinatorLifecycleCell(t, coordinator, testAuthorActivityContext(context.Background()), rec, true); err != nil {
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
