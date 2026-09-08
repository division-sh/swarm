package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestPreparedTerminalCompletionSurvivesRetirementBeforeCommit(t *testing.T) {
	for _, scope := range []string{"runtime", "standing", "selected_fork"} {
		for _, abort := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/abort_%t", scope, abort), func(t *testing.T) {
				bus := &flowActivationTestBus{}
				instances := &flowActivationTestInstanceStore{}
				am := newFlowActivationManager(t, bus, instances, &flowActivationTestStore{})
				bundle := testFlowBundle(t, "")
				ctx := testAuthorActivityContext(context.Background())
				if err := activateFlowInstanceForTest(am, ctx, testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
					t.Fatal(err)
				}
				if err := am.lifecycle.waitForWork(ctx); err != nil {
					t.Fatal(err)
				}
				request := runtimepipeline.FlowInstanceDeactivationRequest{
					Instance: runtimeflowidentity.Stored(nil, "review", "review/inst-1", "inst-1", "ent-1", ""),
				}
				var companion interface {
					worklifetime.Occurrence
					RetireAndWait(context.Context) error
				}
				prepareCtx := flowActivationRunContext()
				root := am.workOwner.(*worklifetime.RuntimeOccurrence)
				switch scope {
				case "standing":
					owner, err := root.NewStanding(ctx, worklifetime.StandingIdentity{ServiceID: "terminal-test", RunID: flowActivationTestRunID, Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					companion = owner
				case "selected_fork":
					fixture, exists := managerTestWorkFixtures.Load(t)
					if !exists {
						t.Fatal("manager fixture has no process owner")
					}
					owner, err := fixture.(*managerTestWorkFixture).process.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{ExecutionID: "terminal-test", RunID: flowActivationTestRunID, Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					companion = owner
				}
				if companion != nil {
					prepareCtx = worklifetime.WithOccurrence(prepareCtx, companion)
					t.Cleanup(func() {
						if err := companion.RetireAndWait(ctx); err != nil {
							t.Error(err)
						}
					})
				}
				prepared, err := am.PrepareFlowInstanceDeactivation(prepareCtx, request)
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Abort()
				if !am.lifecycle.retireWorkAdmission() {
					t.Fatal("missing admitted Manager owner")
				}
				if companion != nil {
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if err := companion.RetireAndWait(cancelled); !errors.Is(err, context.Canceled) {
						t.Fatalf("terminal completion lost exact parent ownership: %v", err)
					}
				}
				if late, err := am.PrepareFlowInstanceDeactivation(flowActivationRunContext(), request); err == nil {
					_ = late.Abort()
					t.Fatal("retired Manager admitted a new terminal completion")
				}
				if abort {
					err = prepared.Abort()
				} else {
					err = prepared.Commit()
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := am.lifecycle.waitForWork(ctx); err != nil {
					t.Fatal(err)
				}
				if (len(instances.terminatedPaths) == 0) != abort {
					t.Fatalf("terminal evidence after abort=%t: %v", abort, instances.terminatedPaths)
				}
				if err := prepared.Commit(); err == nil {
					t.Fatal("consumed completion authorized a second mutation")
				}
			})
		}
	}
}

type terminalSetPersistenceProbe struct {
	AgentLifecyclePersistence
	mu     sync.Mutex
	agents map[runtimeagentidentity.Identity]*lifecyclePersistenceProbe
	before func(AgentLifecycleTransition) error
}

type terminalCommitErrorProbe struct {
	next interface {
		CommitFlowInstanceTermination(context.Context, runtimepipeline.FlowInstanceTerminationRequest) (runtimepipeline.FlowInstanceTermination, error)
	}
	after bool
	err   error
}

func (p terminalCommitErrorProbe) CommitFlowInstanceTermination(ctx context.Context, req runtimepipeline.FlowInstanceTerminationRequest) (runtimepipeline.FlowInstanceTermination, error) {
	if !p.after {
		return runtimepipeline.FlowInstanceTermination{}, p.err
	}
	committed, err := p.next.CommitFlowInstanceTermination(ctx, req)
	return committed, errors.Join(err, p.err)
}

func TestPreparedTerminalCompletionRetainsCommittedAuxiliaryError(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed_%t", after), func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			am := newFlowActivationManager(t, &flowActivationTestBus{}, instances, &flowActivationTestStore{})
			ctx := testAuthorActivityContext(context.Background())
			if err := activateFlowInstanceForTest(am, ctx, testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
				t.Fatal(err)
			}
			if err := am.lifecycle.waitForWork(ctx); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("terminal auxiliary failure")
			am.roles.FlowTermination = terminalCommitErrorProbe{next: am.roles.FlowTermination, after: after, err: injected}
			err := deactivateFlowInstanceForTest(am, flowActivationRunContext(), "review", "inst-1", "review/inst-1", "ent-1")
			if !errors.Is(err, injected) {
				t.Fatalf("caller lost mutation outcome: %v", err)
			}
			if err := am.lifecycle.waitForWork(ctx); errors.Is(err, injected) != after {
				t.Fatalf("committed=%t retirement result: %v", after, err)
			}
			_, present := testFlowActivationAgentConfig(t, am, "reviewer", "review/inst-1")
			if present == after || (len(instances.terminatedPaths) == 1) != after {
				t.Fatalf("committed=%t agent present=%t durable termination=%v", after, present, instances.terminatedPaths)
			}
			if err := am.Shutdown(); errors.Is(err, injected) != after {
				t.Fatalf("committed=%t shutdown evidence: %v", after, err)
			}
		})
	}
}

type terminalRetirementRouteProbe struct {
	AgentRouteBus
	remove func(runtimeeffects.LifecycleToken)
	fence  func(runtimeeffects.LifecycleToken)
}

func (p *terminalRetirementRouteProbe) FenceAgentRoute(token runtimeeffects.LifecycleToken) {
	if p.fence != nil {
		p.fence(token)
	}
}

func (p *terminalRetirementRouteProbe) RemoveAgentRoute(token runtimeeffects.LifecycleToken) {
	p.remove(token)
}

func TestTerminalCompletionRouteFailureStillJoinsEveryFinalizer(t *testing.T) {
	c := newAgentLifecycleCoordinator(newLifecyclePersistenceProbe(), nil, nil, nil, nil)
	ctx := testAuthorActivityContext(context.Background())
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
	set := &terminalFlowRetirement{}
	called := make([]chan struct{}, 2)
	settled := make([]chan struct{}, 2)
	for i := range called {
		called[i], settled[i] = make(chan struct{}), make(chan struct{})
		identity := runtimeagentidentitytest.RootRuntime(t, fmt.Sprintf("retire-%d", i), "terminal-test")
		done := make(chan struct{})
		close(done)
		execution := &agentExecutionProjection{
			token: lifecycleToken(identity, 1, 1), routeToken: lifecycleToken(identity, 1, 1),
			loopDone: done, loopSettled: settled[i],
		}
		cell := &agentLifecycleCell{identity: identity, execution: execution}
		set.retirements = append(set.retirements, retainAgentRetirement(cell, execution))
	}
	injected := errors.New("exact route retirement panicked")
	c.routes = &terminalRetirementRouteProbe{remove: func(token runtimeeffects.LifecycleToken) {
		for i, retirement := range set.retirements {
			if retirement.token == token {
				close(called[i])
				if i == 0 {
					panic(injected)
				}
				return
			}
		}
		t.Error("cleanup used an uncaptured generation")
	}}
	lease, err := am.beginWork(ctx, "test failed route completion")
	if err != nil {
		t.Fatal(err)
	}
	am.launchTerminalFlowCompletion(lease, set, nil)
	for i := range called {
		<-called[i]
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := c.waitForWork(cancelled); !errors.Is(err, context.Canceled) {
			t.Errorf("completion released ownership before finalizer %d: %v", i, err)
		}
		close(settled[i])
	}
	if err := c.waitForWork(ctx); err == nil || !strings.Contains(err.Error(), injected.Error()) {
		t.Fatalf("completion lost exact route failure: %v", err)
	}
	if set.retirements[0].cell.retirement == nil || set.retirements[1].cell.retirement != nil {
		t.Fatal("failed retirement was cleared or successful suffix remained unjoined")
	}
}

func (p *terminalSetPersistenceProbe) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	return lifecycleProbeProcessBinding(), nil
}

func (p *terminalSetPersistenceProbe) InspectRunExecutionOwnership(ctx context.Context, runID string) (RunExecutionOwnership, error) {
	return inspectRunExecutionOwnership(ctx, p.AgentLifecyclePersistence, runID)
}

func (p *terminalSetPersistenceProbe) CommitAgentLifecycleTransition(ctx context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	p.mu.Lock()
	member := p.agents[req.Identity]
	if member == nil {
		member = newLifecyclePersistenceProbe()
		p.agents[req.Identity] = member
	}
	before := p.before
	p.mu.Unlock()
	if before != nil {
		if err := before(req); err != nil {
			return AgentLifecycleTransitionResult{}, err
		}
	}
	return member.CommitAgentLifecycleTransition(ctx, req)
}

func TestTerminalFlowWholeSetDispositionSurvivesMiddleFailure(t *testing.T) {
	for _, mode := range []string{"success", "existing_retirement", "middle_error", "middle_panic", "middle_fence_panic"} {
		t.Run(mode, func(t *testing.T) {
			probe := &terminalSetPersistenceProbe{AgentLifecyclePersistence: newLifecyclePersistenceProbe(), agents: make(map[runtimeagentidentity.Identity]*lifecyclePersistenceProbe)}
			c := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
			ctx := testAuthorActivityContext(context.Background())
			records := make([]PersistedAgent, 3)
			plan := terminalFlowInstanceSideEffectPlan{}
			for i := range records {
				rec := lifecycleTestPersistedAgent(t)
				rec.Config.ID = fmt.Sprintf("terminal-agent-%d", i)
				rec.Config.Identity = runtimeagentidentitytest.RootRuntime(t, rec.Config.ID, "terminal-set-test")
				plan.RunID = rec.Config.Identity.RunID
				if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
					t.Fatal(err)
				}
				records[i] = rec
				plan.AgentIdentities = append(plan.AgentIdentities, rec.Config.Identity)
			}
			beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
			tokens := make([]runtimeeffects.LifecycleToken, len(records))
			dones := make([]chan struct{}, len(records))
			for i, rec := range records {
				_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
				if err != nil {
					t.Fatal(err)
				}
				tokens[i], dones[i] = token, done
			}
			wantAttempts := len(records)
			wantError := mode == "middle_error" || mode == "middle_panic" || mode == "middle_fence_panic"
			if mode == "existing_retirement" {
				if _, err := c.beginIdentityTermination(ctx, records[0].Config.Identity, "teardown", AgentLifecycleTerminated, nil, nil, terminalFenceAndSettle); err != nil {
					t.Fatal(err)
				}
				wantAttempts--
			}
			injected := errors.New("middle terminal persistence failure")
			fenced := make(map[runtimeagentidentity.Identity]bool)
			if mode == "middle_fence_panic" {
				c.routes = &terminalRetirementRouteProbe{remove: func(runtimeeffects.LifecycleToken) {}, fence: func(token runtimeeffects.LifecycleToken) {
					fenced[token.Identity] = true
					if token.Identity == records[1].Config.Identity {
						panic(injected)
					}
				}}
			}
			set, fenceErr := c.fenceTerminalFlow(plan)
			if (fenceErr != nil) != (mode == "middle_fence_panic") || set == nil {
				t.Fatalf("exact terminal fence evidence: set=%v err=%v", set, fenceErr)
			}
			if mode == "middle_fence_panic" && len(fenced) != len(records) {
				t.Fatal("failed route fence abandoned suffix")
			}
			attempts := 0
			probe.before = func(req AgentLifecycleTransition) error {
				if req.Trigger != "flow_instance_terminal" {
					return nil
				}
				attempts++
				// The lifecycle mutation calls the probe while holding c.mu.
				for _, member := range set.members {
					if !member.cell.execution.fenced || member.cell.terminalSet != set {
						t.Error("an affected member was unfenced at terminal disposition")
					}
				}
				if req.Identity == records[1].Config.Identity {
					switch mode {
					case "middle_error":
						return injected
					case "middle_panic":
						panic(injected)
					}
				}
				return nil
			}
			commitErr := errors.Join(fenceErr, c.commitTerminalFlow(ctx, set))
			if (commitErr != nil) != wantError || attempts != wantAttempts || len(set.retirements) != len(records) {
				t.Fatalf("whole-set disposition: attempts=%d retirements=%d error=%v", attempts, len(set.retirements), commitErr)
			}
			if commitErr != nil && !strings.Contains(commitErr.Error(), injected.Error()) {
				t.Fatalf("lost injected failure: %v", commitErr)
			}
			for i, token := range tokens {
				if err := releaseCoordinatorLoop(c, token, dones[i]); err != nil {
					t.Fatal(err)
				}
			}
			am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
			lease, err := am.beginWork(ctx, "test terminal completion")
			if err != nil {
				t.Fatal(err)
			}
			am.launchTerminalFlowCompletion(lease, set, commitErr)
			if err := c.waitForWork(ctx); (err != nil) != wantError || (err != nil && !strings.Contains(err.Error(), injected.Error())) {
				t.Fatalf("owned completion lost failure: %v", err)
			}
		})
	}
}

func TestPanicRetirementReturnsBeforeOwnLoopFinalizer(t *testing.T) {
	ProveTerminalPanicFinalization(t, newLifecyclePersistenceProbe(), nil, lifecycleTestPersistedAgent(t), true)
}

func TestTerminalPanicRejectsMissingAndPredecessorAuthority(t *testing.T) {
	for _, origin := range []string{"missing", "replaced"} {
		t.Run(origin, func(t *testing.T) {
			probe := newLifecyclePersistenceProbe()
			c := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
			rec := lifecycleTestPersistedAgent(t)
			ctx := testAuthorActivityContext(context.Background())
			if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
				t.Fatal(err)
			}
			beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
			_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatal(err)
			}
			panicCtx := ctx
			if origin == "replaced" {
				panicCtx = runtimeeffects.WithLifecycleToken(ctx, token)
				if err := releaseCoordinatorLoop(c, token, done); err != nil {
					t.Fatal(err)
				}
				_, successor, nextDone, err := replaceCoordinatorLoop(c, ctx, rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
				if err != nil {
					t.Fatal(err)
				}
				if successor == token {
					t.Fatal("replacement reused predecessor authority")
				}
				token, done = successor, nextDone
			}
			am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
			before := len(probe.requests)
			if err := am.requestPanicRetirement(panicCtx, rec.Config.Identity); err == nil {
				t.Fatal("panic without current exact authority was admitted")
			}
			c.mu.Lock()
			cell := c.cells[token.Identity.Normalize()]
			unchanged := cell.phase == AgentLifecycleRunning && cell.execution.token == token && !cell.execution.fenced && cell.retirement == nil
			c.mu.Unlock()
			if !unchanged || len(probe.requests) != before {
				t.Fatal("refused panic changed successor authority or persisted a transition")
			}
			if err := releaseCoordinatorLoop(c, token, done); err != nil {
				t.Fatal(err)
			}
			if err := c.waitForWork(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTerminalPanicPersistenceFailureRetainsRetirement(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	c := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	rec := lifecycleTestPersistedAgent(t)
	ctx := testAuthorActivityContext(context.Background())
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("panic terminal mutation failed")
	probe.failNext = injected
	am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
	if err := am.requestPanicRetirement(runtimeeffects.WithLifecycleToken(ctx, token), rec.Config.Identity); !errors.Is(err, injected) {
		t.Fatalf("panic persistence error = %v", err)
	}
	if err := releaseCoordinatorLoop(c, token, done); err != nil {
		t.Fatal(err)
	}
	if err := c.waitForWork(ctx); !errors.Is(err, injected) {
		t.Fatalf("joined panic completion lost commit failure: %v", err)
	}
	c.mu.Lock()
	cell := c.cells[token.Identity.Normalize()]
	retained := cell.retirement != nil && cell.retirement.execution.token == token && cell.execution.fenced
	c.mu.Unlock()
	if !retained {
		t.Fatal("successful loop join erased the failed exact terminal transition")
	}
	if _, _, _, err := replaceCoordinatorLoop(c, ctx, rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("failed panic retirement admitted a replacement")
	}
}

// These test-only entry points let the external package supply real selected
// stores without creating a production manager/store import cycle.
func TerminalPanicFixture(t *testing.T) PersistedAgent { return lifecycleTestPersistedAgent(t) }

func TerminalPanicFixtureContext() context.Context {
	return testAuthorActivityContext(context.Background())
}

func ProveTerminalPanicFinalization(t *testing.T, persistence AgentLifecyclePersistence, reader AgentLifecycleStateReader, rec PersistedAgent, persist bool) {
	t.Helper()
	c := newAgentLifecycleCoordinator(persistence, nil, nil, reader, nil)
	ctx := testAuthorActivityContext(context.Background())
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, persist); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
	// This is the active loop's production request path, before its finalizer.
	if err := am.requestPanicRetirement(runtimeeffects.WithLifecycleToken(ctx, token), rec.Config.Identity); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending := c.cells[token.Identity.Normalize()].retirement
	phase := c.cells[token.Identity.Normalize()].phase
	c.mu.Unlock()
	if pending == nil || phase != AgentLifecycleFailed {
		t.Fatalf("panic retirement lost exact pending completion: phase=%s pending=%v", phase, pending)
	}
	if err := releaseCoordinatorLoop(c, token, done); err != nil {
		t.Fatal(err)
	}
	if err := c.waitForWork(ctx); err != nil {
		t.Fatal(err)
	}
	if reader != nil {
		state, found, err := reader.LoadAgentLifecycleState(ctx, rec.Config.Identity)
		if err != nil || !found || state.Phase != AgentLifecycleFailed || state.Generation != token.Generation+1 {
			t.Fatalf("panic finalizer exact durable readback: state=%+v found=%t err=%v", state, found, err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cells[token.Identity.Normalize()].retirement != nil {
		t.Fatal("panic retirement did not join its finalizer")
	}
}

func ProveTerminalPanicMutationFailure(t *testing.T, persistence AgentLifecyclePersistence, reader AgentLifecycleStateReader, rec PersistedAgent, failure string, finalizerFails bool) {
	t.Helper()
	c := newAgentLifecycleCoordinator(persistence, nil, nil, reader, nil)
	ctx := testAuthorActivityContext(context.Background())
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, false); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
	if err := am.requestPanicRetirement(runtimeeffects.WithLifecycleToken(ctx, token), rec.Config.Identity); err == nil || !strings.Contains(err.Error(), failure) {
		t.Fatalf("panic mutation failure = %v", err)
	}
	finalizerErr := releaseCoordinatorLoop(c, token, done)
	if (finalizerErr != nil) != finalizerFails || (finalizerErr != nil && !strings.Contains(finalizerErr.Error(), failure)) {
		t.Fatalf("panic loop finalizer failure = %v, want failure=%t", finalizerErr, finalizerFails)
	}
	if err := c.waitForWork(ctx); err == nil || !strings.Contains(err.Error(), failure) {
		t.Fatalf("joined panic completion lost mutation error: %v", err)
	}
	state, found, err := reader.LoadAgentLifecycleState(ctx, rec.Config.Identity)
	wantPhase := AgentLifecycleRegistered
	if finalizerFails {
		wantPhase = AgentLifecycleRunning
	}
	if err != nil || !found || state.Phase != wantPhase || state.Generation != token.Generation {
		t.Fatalf("failed panic mutation did not roll back: state=%+v found=%t err=%v want=%s at %d", state, found, err, wantPhase, token.Generation)
	}
	c.mu.Lock()
	cell := c.cells[token.Identity.Normalize()]
	retained := cell.retirement != nil && cell.retirement.execution.token == token && cell.execution.fenced
	c.mu.Unlock()
	if !retained {
		t.Fatal("failed durable terminal evidence lost its exact process fence")
	}
	if _, _, _, err := replaceCoordinatorLoop(c, ctx, rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{}); err == nil {
		t.Fatal("failed terminal mutation admitted successor execution")
	}
}

type terminalReadinessRouteProbe struct {
	err           error
	panicOnRetire bool
}

func (p terminalReadinessRouteProbe) RetirePublishedFlowInstanceRoute(runtimeflowidentity.RunScopedFlowInstance) error {
	if p.panicOnRetire {
		panic(p.err)
	}
	return p.err
}

func TestTerminalReadinessRetirementReleasesAttemptBeforeJoin(t *testing.T) {
	for _, mode := range []string{"success", "route_error", "route_panic"} {
		t.Run(mode, func(t *testing.T) {
			c := newAgentLifecycleCoordinator(newLifecyclePersistenceProbe(), nil, nil, nil, nil)
			rec := lifecycleTestPersistedAgent(t)
			rec.Config.Identity = runtimeagentidentitytest.RuntimeForRun(t, rec.Config.Identity.RunID, rec.Config.ID, "lifecycle-test", "review", "inst-1", "review/inst-1")
			rec.Config.FlowPath = "review/inst-1"
			ctx := testAuthorActivityContext(context.Background())
			if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
				t.Fatal(err)
			}
			beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
			_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("readiness route retirement failure")
			route := terminalReadinessRouteProbe{}
			if mode != "success" {
				route.err = injected
				route.panicOnRetire = mode == "route_panic"
			}
			am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t), roles: PersistenceRoles{RouteRetirer: route}}
			lease, err := am.beginWork(ctx, "test readiness topology retirement")
			if err != nil {
				t.Fatal(err)
			}
			prepared := &preparedFlowTopologyRetirement{manager: am, lease: lease}
			defer prepared.abort()
			flow, err := runtimeflowidentity.NewRunScopedFlowInstance(rec.Config.Identity.RunID, runtimeflowidentity.RouteForInstancePath("review/inst-1"))
			if err != nil {
				t.Fatal(err)
			}
			// The finalizer is intentionally not started until the readiness
			// attempt returns. An inline join recreates its dependency cycle.
			err = prepared.retire(flow)
			if (err != nil) != (mode != "success") || (err != nil && !strings.Contains(err.Error(), injected.Error())) {
				t.Fatalf("readiness retirement result: %v", err)
			}
			if !prepared.transferred {
				t.Fatal("readiness retirement lost owned completion")
			}
			c.mu.Lock()
			cell := c.cells[token.Identity]
			pending := cell.retirement
			fenced := cell.execution.fenced
			c.mu.Unlock()
			if pending == nil || !fenced {
				t.Fatal("readiness returned without exact fenced retirement")
			}
			if err := releaseCoordinatorLoop(c, token, done); err != nil {
				t.Fatal(err)
			}
			if err := c.waitForWork(ctx); (err != nil) != (mode != "success") || (err != nil && !strings.Contains(err.Error(), injected.Error())) {
				t.Fatalf("owned readiness completion result: %v", err)
			}
		})
	}
}

func TestTerminalReadinessReplacementReleasesDependentCaller(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	am := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := am.lifecycle.waitForWork(ctx); err != nil {
		t.Fatal(err)
	}
	records, err := agents.LoadAgents(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("initial agents: %v %v", records, err)
	}
	rec := records[0]
	if _, started, err := am.lifecycle.beginRun(managedExecutionTestContext(t, ctx), AgentRunModeStandard, am.workOwner); err != nil || !started {
		t.Fatalf("begin readiness Manager run: started=%t err=%v", started, err)
	}
	am.lifecycle.mu.Lock()
	am.lifecycle.watcherExpected = false
	am.lifecycle.mu.Unlock()
	oldCtx, token, done, err := replaceCoordinatorLoop(am.lifecycle, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := am.lifecycle.acquireExecutionIdentity(ctx, rec.Config.Identity, "readiness dependent caller", true)
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Release()
	revisedSource := semanticview.Wrap(testFlowBundleWithTwoAgents(t, ""))
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	setFlowActivationManagerSemanticSource(am, revisedSource, fact)
	revisedCtx := runtimecorrelation.WithRunID(runtimecorrelation.WithSourceArtifactFact(ctx, fact), req.TriggerEvent.RunID())
	revisedCtx = worklifetime.WithOccurrence(revisedCtx, am.workOwner)
	// The caller cannot release accepted until reconciliation returns. The
	// owned attempt must retain replacement without waiting on this caller.
	if err := am.ReconcileDynamicFlowRuntimeReadinessPlansForRun(revisedCtx, time.Now().UTC()); !errors.Is(err, errDynamicFlowRuntimeReadinessRetiring) {
		t.Fatalf("dependent readiness caller did not return pending retirement: %v", err)
	}
	<-oldCtx.Done()
	am.dynamicFlowReadinessMu.Lock()
	attempt := am.dynamicFlowReadinessAttempts[dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: "review/inst-1"}]
	am.dynamicFlowReadinessMu.Unlock()
	if attempt == nil {
		t.Fatal("replacement discarded its keyed attempt before predecessor settlement")
	}
	if err := attempt.wait(ctx); !errors.Is(err, errDynamicFlowRuntimeReadinessRetiring) {
		t.Fatalf("coalesced caller retained the predecessor dependency: %v", err)
	}
	if err := releaseCoordinatorLoop(am.lifecycle, token, done); err != nil {
		t.Fatal(err)
	}
	accepted.Release()
	<-attempt.done
	if attempt.err != nil {
		t.Fatalf("owned readiness replacement: %v", attempt.err)
	}
	state, ok := am.lifecycle.stateByIdentity(rec.Config.Identity)
	if !ok || state.Generation <= token.Generation || state.Phase != AgentLifecycleRunning {
		t.Fatalf("replacement did not publish the joined successor: %+v", state)
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalFlowFencesWholeSetBeforeRetirementJoin(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	c := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	ctx := testAuthorActivityContext(context.Background())
	rec := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	plan := terminalFlowInstanceSideEffectPlan{RunID: rec.Config.Identity.RunID, AgentIdentities: []runtimeagentidentity.Identity{rec.Config.Identity}}
	set, err := c.fenceTerminalFlow(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.acquireDeliveryExecution(ctx, token); err == nil {
		t.Fatal("terminal set admitted another delivery")
	}
	if err := c.commitTerminalFlow(ctx, set); err != nil {
		t.Fatal(err)
	}
	if len(set.retirements) != 1 {
		t.Fatalf("retirements = %d", len(set.retirements))
	}
	if err := releaseCoordinatorLoop(c, token, done); err != nil {
		t.Fatal(err)
	}
	if err := c.joinAgentRetirement(set.retirements[0]); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalLoopRouteJoinReleasesSourceSetPublication(t *testing.T) {
	c := newAgentLifecycleCoordinator(newLifecyclePersistenceProbe(), nil, nil, nil, nil)
	ctx := testAuthorActivityContext(context.Background())
	rec := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	joined := false
	c.routes = &terminalRetirementRouteProbe{remove: func(actual runtimeeffects.LifecycleToken) {
		if actual != token {
			t.Error("route finalizer changed generation")
		}
		// A queued source-set writer must not be held off by the finalizer
		// while accepted route settlement is itself waiting for admission.
		if !c.sourceSetPublishMu.TryLock() {
			t.Error("route join retains source-set publication read ownership")
			return
		}
		c.sourceSetPublishMu.Unlock()
		joined = true
	}}
	if err := releaseCoordinatorLoop(c, token, done); err != nil {
		t.Fatal(err)
	}
	if !joined {
		t.Fatal("exact route never joined without source-set dependency")
	}
}

func TestTerminalRetirementReleasesOperationLockBeforeJoin(t *testing.T) {
	probe := newLifecyclePersistenceProbe()
	c := newAgentLifecycleCoordinator(probe, nil, nil, nil, nil)
	ctx := testAuthorActivityContext(context.Background())
	rec := lifecycleTestPersistedAgent(t)
	if err := registerCoordinatorLifecycleCell(t, c, ctx, rec, true); err != nil {
		t.Fatal(err)
	}
	beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
	_, token, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := c.beginIdentityTermination(ctx, rec.Config.Identity, "teardown", AgentLifecycleTerminated, nil, nil, terminalFenceAndSettle)
	if err != nil {
		t.Fatal(err)
	}
	cell := committed.retirement.cell
	if !cell.opMu.TryLock() {
		t.Fatal("terminal commit retained operation mutex across its pending join")
	}
	cell.opMu.Unlock()
	if _, err := c.lockIdentityOperation(rec.Config.Identity); err == nil {
		t.Fatal("retained retirement admitted competing mutation")
	}
	if err := releaseCoordinatorLoop(c, runtimeeffects.LifecycleToken(token), done); err != nil {
		t.Fatal(err)
	}
	if err := c.joinAgentRetirement(committed.retirement); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementRetainsTransitionWithoutFinalizerDependencies(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminal_%t", terminal), func(t *testing.T) {
			c := newAgentLifecycleCoordinator(newLifecyclePersistenceProbe(), nil, nil, nil, nil)
			rec := lifecycleTestPersistedAgent(t)
			ctx := testAuthorActivityContext(context.Background())
			if err := c.registerExecution(ctx, rec, true, &reconfigureTestAgent{id: rec.Config.ID}, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
				t.Fatal(err)
			}
			beginCoordinatorRun(t, c, managedExecutionTestContext(t, ctx), AgentRunModeStandard)
			oldCtx, old, done, err := replaceCoordinatorLoop(c, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			c.cells[old.Identity].execution.routeToken = old
			c.mu.Unlock()
			lease, err := c.acquireDeliveryExecution(ctx, old)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release()
			type replacementResult struct {
				token runtimeeffects.LifecycleToken
				done  chan struct{}
				err   error
			}
			replaced := make(chan replacementResult, 1)
			go func() {
				_, token, nextDone, err := replaceCoordinatorLoop(c, ctx, rec, "restart", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
				replaced <- replacementResult{token: token, done: nextDone, err: err}
			}()
			<-oldCtx.Done()
			// The predecessor has been fenced, but its accepted lease is still live.
			// These are precisely the locks its completion and source publication need.
			c.sourceSetPublishMu.Lock()
			c.executionPublishMu.Lock()
			c.mu.Lock()
			cell := c.cells[old.Identity]
			pending := cell.retirement
			c.mu.Unlock()
			cell.opMu.Lock()
			if pending == nil || pending.execution.token != old {
				t.Error("replacement did not retain the exact predecessor transition")
			}
			cell.opMu.Unlock()
			c.executionPublishMu.Unlock()
			c.sourceSetPublishMu.Unlock()
			var terminalSet *terminalFlowRetirement
			var terminalErr error
			if terminal {
				terminalSet, err = c.fenceTerminalFlow(terminalFlowInstanceSideEffectPlan{RunID: old.Identity.RunID, AgentIdentities: []runtimeagentidentity.Identity{old.Identity}})
				if err != nil {
					t.Fatal(err)
				}
				terminalErr = c.commitTerminalFlow(ctx, terminalSet)
				if terminalErr == nil || !strings.Contains(terminalErr.Error(), "pending predecessor transition") {
					t.Fatalf("terminal request lost the pending transition conflict: %v", terminalErr)
				}
			}
			if err := releaseCoordinatorLoop(c, old, done); err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-replaced:
				t.Fatalf("replacement published before accepted lease settlement: %+v", result)
			default:
			}
			lease.Release()
			result := <-replaced
			if terminal {
				if result.err == nil || result.token.Valid() || result.done != nil {
					t.Fatalf("terminal fence allowed successor publication: %+v", result)
				}
				am := &AgentManager{lifecycle: c, workOwner: newTestManagerWorkOwner(t)}
				completion, err := am.beginWork(ctx, "test pending terminal completion")
				if err != nil {
					t.Fatal(err)
				}
				am.launchTerminalFlowCompletion(completion, terminalSet, terminalErr)
				if err := c.waitForWork(ctx); !errors.Is(err, terminalErr) {
					t.Fatalf("owned terminal result lost transition conflict: %v", err)
				}
				return
			}
			if result.err != nil || result.token.Generation != old.Generation+1 {
				t.Fatalf("replacement completion: %+v", result)
			}
			if err := c.joinAgentRetirement(pending); err != nil {
				t.Fatal(err)
			}
			c.finishAgentRetirement(pending)
			c.mu.Lock()
			execution := c.cells[old.Identity].execution
			c.mu.Unlock()
			if execution == nil || execution.token != result.token {
				t.Fatal("late predecessor cleanup discarded successor execution")
			}
			if current, ok := c.tokenIdentity(old.Identity); !ok || current != result.token {
				t.Fatal("late predecessor completion changed successor authority")
			}
			c.cancelShutdownWork()
			if err := releaseCoordinatorLoop(c, result.token, result.done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
