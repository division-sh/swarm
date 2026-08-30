package bus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type retryReleaseInterceptor struct {
	eventID string
}

func (i retryReleaseInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if event.ID() != i.eventID {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
}

type retryReleaseSetInterceptor struct {
	eventIDs map[string]struct{}
}

func (i retryReleaseSetInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if _, retry := i.eventIDs[event.ID()]; !retry {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
}

type boundedRetryWindowInterceptor struct {
	retryEventID string
	blockEventID string
	blocked      chan<- struct{}
	release      <-chan struct{}
}

func (i boundedRetryWindowInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	switch event.ID() {
	case i.retryEventID:
		return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
	case i.blockEventID:
		close(i.blocked)
		<-i.release
	}
	return true, nil, runtimepipelineobligation.Continue(), nil
}

type observedRetryReleaseInterceptor struct {
	eventID string
	seen    chan<- struct{}
}

func (i observedRetryReleaseInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if event.ID() != i.eventID {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	select {
	case i.seen <- struct{}{}:
	default:
	}
	return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
}

type recordingPipelineRecoveryOwner struct {
	bus     *runtimebus.EventBus
	results []runtimepipelineobligation.SweepResult
}

type blockedRunDispatchGate map[string]bool

func (g blockedRunDispatchGate) QueueableRunDispatchBlocked(_ context.Context, runID string) (bool, error) {
	return g[runID], nil
}

type exactStandingRecoveryOwner struct {
	occurrence *worklifetime.StandingOccurrence
}

func (o exactStandingRecoveryOwner) BeginStandingRunRecovery(
	ctx context.Context,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) (*worklifetime.Lease, error) {
	identity := o.occurrence.Identity()
	if identity.RunID != runID ||
		identity.ServiceID != origin.ServiceID() ||
		identity.Generation != uint64(origin.Generation()) {
		return nil, errors.New("standing recovery requested the wrong occurrence")
	}
	return o.occurrence.Begin(ctx)
}

func (o *recordingPipelineRecoveryOwner) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	result, err := o.bus.SweepPipelineObligations(ctx, limit)
	o.results = append(o.results, result)
	return result, err
}

type recordingIngressRecoveryPublisher struct {
	bus              *runtimebus.EventBus
	results          []runtimepipelineobligation.SweepResult
	cancelAfterFirst context.CancelFunc
}

func (p *recordingIngressRecoveryPublisher) Publish(ctx context.Context, event events.Event) error {
	return p.bus.Publish(ctx, event)
}

func (p *recordingIngressRecoveryPublisher) PreflightRuntimeIngressQueue(ctx context.Context) error {
	return p.bus.PreflightRuntimeIngressQueue(ctx)
}

func (p *recordingIngressRecoveryPublisher) ReleaseRuntimeIngressQueue(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	result, err := p.bus.ReleaseRuntimeIngressQueue(ctx, limit)
	p.results = append(p.results, result)
	if len(p.results) == 1 && p.cancelAfterFirst != nil {
		p.cancelAfterFirst()
	}
	return result, err
}

type recordingRunRecoveryQueue struct {
	bus              *runtimebus.EventBus
	results          []runtimepipelineobligation.SweepResult
	errors           []error
	cancelAfterFirst context.CancelFunc
}

type terminalRunRecoveryQueue struct {
	result runtimepipelineobligation.SweepResult
	err    error
}

func (q terminalRunRecoveryQueue) PreflightRunQueue(context.Context, string) error { return nil }

func (q terminalRunRecoveryQueue) ReleaseRunQueue(
	context.Context,
	string,
	int,
) (runtimepipelineobligation.SweepResult, error) {
	return q.result, q.err
}

func (q *recordingRunRecoveryQueue) ReleaseRunQueue(ctx context.Context, runID string, limit int) (runtimepipelineobligation.SweepResult, error) {
	result, err := q.bus.ReleaseRunQueue(ctx, runID, limit)
	q.results = append(q.results, result)
	q.errors = append(q.errors, err)
	if len(q.results) == 1 && q.cancelAfterFirst != nil {
		q.cancelAfterFirst()
	}
	return result, err
}

func (q *recordingRunRecoveryQueue) PreflightRunQueue(ctx context.Context, runID string) error {
	return q.bus.PreflightRunQueue(ctx, runID)
}

func TestStandingPipelineRecoveryWaitsForOwnerInstallationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			serviceID := runtimeflowidentity.StandingServiceID(backend)
			origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, 1)
			if err != nil {
				t.Fatal(err)
			}
			fixture := newCompleteEventDispatchFixtureWithOrigin(t, backend, false, origin)
			deliveries := fixture.subscribe(t, fixture.event.Type())
			defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)

			blocked, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 10)
			if err != nil {
				t.Fatalf("sweep before standing owner installation: %v", err)
			}
			if blocked.Settled != 0 || !blocked.Blocked || !blocked.Exhausted {
				t.Fatalf("pre-registration sweep = %#v, want exhausted blocked scan with no settlement", blocked)
			}
			select {
			case delivery := <-deliveries:
				_ = delivery.Complete()
				t.Fatalf("standing recovery dispatched before owner installation: %s", delivery.Event().ID())
			case <-time.After(50 * time.Millisecond):
			}
			fixture.assertNoAgentDispatchMutation(t)

			process := worklifetime.NewProcess()
			runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
				RuntimeInstanceID: uuid.NewString(),
				BundleHash:        authorActivityTestSourceArtifactFact.BundleHash(),
			})
			if err != nil {
				t.Fatal(err)
			}
			standing, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
				ServiceID:  serviceID,
				RunID:      fixture.event.RunID(),
				Generation: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := standing.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire standing recovery occurrence: %v", err)
				}
				if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire standing recovery runtime: %v", err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join standing recovery process: %v", err)
				}
			})
			fixture.bus.SetStandingRunWorkOwner(exactStandingRecoveryOwner{occurrence: standing})

			recovered, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 10)
			if err != nil {
				t.Fatalf("sweep after standing owner installation: %v", err)
			}
			if recovered.Settled != 1 {
				t.Fatalf("post-registration sweep = %#v, want one settled recovery", recovered)
			}
			assertCompleteLocalDelivery(t, deliveries, fixture.event)
		})
	}
}

func TestPipelineRetryReleasePreservesReplayAcrossDispatchSurfacesOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/foreground", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			event := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Second))
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: event.ID()})

			if err := fixture.bus.Publish(fixture.ctx, event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			assertRetryReleaseReplayable(t, fixture, event.ID())
		})

		t.Run(backend+"/post_commit", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})

			if err := fixture.bus.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{{Event: fixture.event}}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
		})

		t.Run(backend+"/recovery_fairness", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Second))
			storetest.CommitSemanticEventWithRoutes(
				t,
				fixture.ctx,
				fixture.store,
				later,
				nil,
				runtimepipelineobligation.ScopeSubscribed,
			)
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})

			result, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 10)
			if err != nil {
				t.Fatalf("SweepPipelineObligations: %v", err)
			}
			if result.Settled != 1 {
				t.Fatalf("processed = %d, want later obligation only", result.Settled)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
				t.Fatalf("later event pipeline receipts = %d, want 1", got)
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
		})
	}
}

func TestPostgresRetryReleaseClaimSpansBoundedSweepWindow(t *testing.T) {
	fixture := newCompleteEventDispatchFixture(t, "postgres", false)
	later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Second))
	storetest.CommitSemanticEventWithRoutes(
		t,
		fixture.ctx,
		fixture.store,
		later,
		nil,
		runtimepipelineobligation.ScopeSubscribed,
	)

	laterBlocked := make(chan struct{})
	releaseLater := make(chan struct{})
	fixture.bus.SetInterceptors(boundedRetryWindowInterceptor{
		retryEventID: fixture.event.ID(),
		blockEventID: later.ID(),
		blocked:      laterBlocked,
		release:      releaseLater,
	})

	competingStore := storetest.AdmitPostgresRuntimeStore(t, fixture.db)
	competingBus, err := newScopedTestEventBus(competingStore)
	if err != nil {
		t.Fatalf("create competing event bus: %v", err)
	}
	competingRetrySeen := make(chan struct{}, 1)
	competingBus.SetInterceptors(observedRetryReleaseInterceptor{
		eventID: fixture.event.ID(),
		seen:    competingRetrySeen,
	})

	firstDone := make(chan error, 1)
	go func() {
		_, sweepErr := fixture.bus.SweepPipelineObligations(fixture.ctx, 2)
		firstDone <- sweepErr
	}()
	select {
	case <-laterBlocked:
	case err := <-firstDone:
		t.Fatalf("first sweep returned before later candidate blocked: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("first sweep did not reach the later candidate")
	}

	if _, err := competingBus.SweepPipelineObligations(fixture.ctx, 2); err != nil {
		t.Fatalf("competing sweep during bounded window: %v", err)
	}
	select {
	case <-competingRetrySeen:
		t.Fatal("competing sweep executed retry work before the bounded window returned")
	default:
	}
	if err := competingBus.WaitForOutboxSweeper(fixture.ctx); err != nil {
		t.Fatalf("abandon competing cursor: %v", err)
	}

	close(releaseLater)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("complete first bounded sweep: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first bounded sweep did not return")
	}

	if _, err := competingBus.SweepPipelineObligations(fixture.ctx, 2); err != nil {
		t.Fatalf("competing sweep after bounded window: %v", err)
	}
	select {
	case <-competingRetrySeen:
	default:
		t.Fatal("retry work was not reclaimable after the bounded window returned")
	}
}

func TestStartupRecoveryDrainsPastFullRetryReleasePageOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			retryEvents := []events.Event{fixture.event}
			for i := 1; i < 5; i++ {
				event := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Duration(i)*time.Microsecond))
				storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, event, nil, runtimepipelineobligation.ScopeSubscribed)
				retryEvents = append(retryEvents, event)
			}
			later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(5*time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, later, nil, runtimepipelineobligation.ScopeSubscribed)
			retryIDs := make(map[string]struct{}, len(retryEvents))
			for _, event := range retryEvents {
				retryIDs[event.ID()] = struct{}{}
			}
			fixture.bus.SetInterceptors(retryReleaseSetInterceptor{eventIDs: retryIDs})

			recovery := &recordingPipelineRecoveryOwner{bus: fixture.bus}
			if err := runtimepipeline.NewRecoveryManagerWithLimit(recovery, 2).Recover(fixture.ctx); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(recovery.results) < 4 {
				t.Fatalf("startup recovery passes = %d, want continuation across at least 4 bounded batches", len(recovery.results))
			}
			for i, result := range recovery.results {
				if result.Examined > 2 {
					t.Fatalf("startup recovery pass %d examined %d, want <= 2", i, result.Examined)
				}
			}
			last := recovery.results[len(recovery.results)-1]
			if !last.Exhausted || !last.Blocked {
				t.Fatalf("final startup recovery result = %#v, want exhausted with retained local blockage", last)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
				t.Fatalf("later event pipeline receipts = %d, want 1", got)
			}
			for _, event := range retryEvents {
				assertRetryReleaseReplayable(t, fixture, event.ID())
			}
		})
	}
}

func TestIngressResumeDrainsPartialBatchesUntilExplicitTerminationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, later := newControllerContinuationFixture(t, backend)
			ingressStore := fixture.store.(runtimeingress.Store)
			if _, changed, err := ingressStore.TransitionRuntimeIngressState(
				fixture.ctx,
				runtimeingress.StatusPaused,
				"continuation proof",
				"test",
				fixture.event.CreatedAt().Add(time.Second),
			); err != nil {
				t.Fatalf("pause ingress: %v", err)
			} else if !changed {
				t.Fatal("pause ingress did not change state")
			}
			publisher := &recordingIngressRecoveryPublisher{bus: fixture.bus}
			controller := runtimeingress.NewController(ingressStore, publisher, runtimeingress.Options{
				ExecutionPosture: executionposture.Live,
				ReleaseLimit:     2,
			})
			fixture.bus.SetRuntimeIngressDispatchGate(controller)
			t.Cleanup(func() {
				fixture.bus.SetRuntimeIngressDispatchGate(nil)
				runtimebus.ResumeRuntimeIngress()
			})

			result, err := controller.Resume(fixture.ctx, runtimeingress.TransitionRequest{
				Reason: "continue recovery proof",
				Now:    fixture.event.CreatedAt().Add(2 * time.Second),
			})
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			assertSweepContinuation(t, publisher.results, result.ReleasedCount)
			for _, event := range later {
				if got := retryReleasePipelineReceiptCount(t, fixture, event.ID()); got != 1 {
					t.Fatalf("later event %s pipeline receipts = %d, want 1", event.ID(), got)
				}
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
		})
	}
}

func TestRunControlContinueCommittedRecoveryOutcomeParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, later := newControllerContinuationFixture(t, backend)
			runStore := fixture.store.(runtimeruncontrol.Store)
			if _, err := runStore.PauseRunControl(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:        fixture.event.RunID(),
				Reason:       "continuation proof",
				ControlledBy: "test",
				Now:          fixture.event.CreatedAt().Add(time.Second),
			}); err != nil {
				t.Fatalf("PauseRunControl: %v", err)
			}
			queue := &recordingRunRecoveryQueue{bus: fixture.bus}
			controller := runtimeruncontrol.NewController(runStore, queue, runtimeruncontrol.Options{ReleaseLimit: 2})
			fixture.bus.SetRunDispatchGate(controller)
			t.Cleanup(func() { fixture.bus.SetRunDispatchGate(nil) })

			result, err := controller.Continue(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:  fixture.event.RunID(),
				Reason: "continue recovery proof",
				Now:    fixture.event.CreatedAt().Add(2 * time.Second),
			})
			if err != nil {
				t.Fatalf("Continue: %v", err)
			}
			assertRunControllerContinuation(t, queue.results, result.Recovery)
			for _, event := range later {
				if got := retryReleasePipelineReceiptCount(t, fixture, event.ID()); got != 1 {
					t.Fatalf("later event %s pipeline receipts = %d, want 1", event.ID(), got)
				}
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())

			if _, err := runStore.PauseRunControl(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:        fixture.event.RunID(),
				Reason:       "failed recovery proof",
				ControlledBy: "test",
				Now:          fixture.event.CreatedAt().Add(3 * time.Second),
			}); err != nil {
				t.Fatalf("PauseRunControl before failed recovery: %v", err)
			}
			recoveryErr := errors.New("injected immediate recovery failure")
			failedController := runtimeruncontrol.NewController(
				runStore,
				terminalRunRecoveryQueue{
					result: runtimepipelineobligation.SweepResult{Examined: 1},
					err:    recoveryErr,
				},
				runtimeruncontrol.Options{ReleaseLimit: 2},
			)
			failedResult, err := failedController.Continue(
				fixture.ctx,
				runtimeruncontrol.TransitionRequest{
					RunID:        fixture.event.RunID(),
					Reason:       "failed recovery proof",
					ControlledBy: "test",
					Now:          fixture.event.CreatedAt().Add(4 * time.Second),
				},
			)
			if err != nil {
				t.Fatalf("Continue after committed transition: %v", err)
			}
			if failedResult.Recovery.Disposition != runtimeruncontrol.RecoveryFailed ||
				!errors.Is(failedResult.Recovery.Err, recoveryErr) ||
				failedResult.Recovery.Sweep.Examined != 1 {
				t.Fatalf("failed post-commit recovery = %#v", failedResult.Recovery)
			}
			if blocked, err := runStore.RunDispatchBlocked(fixture.ctx, fixture.event.RunID()); err != nil || blocked {
				t.Fatalf("committed continue after recovery failure: blocked=%v err=%v", blocked, err)
			}

			if _, err := runStore.PauseRunControl(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:        fixture.event.RunID(),
				Reason:       "cancelled recovery proof",
				ControlledBy: "test",
				Now:          fixture.event.CreatedAt().Add(5 * time.Second),
			}); err != nil {
				t.Fatalf("PauseRunControl before cancelled recovery: %v", err)
			}
			cancelledController := runtimeruncontrol.NewController(
				runStore,
				terminalRunRecoveryQueue{err: context.Canceled},
				runtimeruncontrol.Options{ReleaseLimit: 2},
			)
			cancelledResult, err := cancelledController.Continue(
				fixture.ctx,
				runtimeruncontrol.TransitionRequest{
					RunID:        fixture.event.RunID(),
					Reason:       "cancelled recovery proof",
					ControlledBy: "test",
					Now:          fixture.event.CreatedAt().Add(6 * time.Second),
				},
			)
			if err != nil {
				t.Fatalf("Continue after cancelled immediate recovery: %v", err)
			}
			if cancelledResult.Recovery.Disposition != runtimeruncontrol.RecoveryCancelled ||
				!errors.Is(cancelledResult.Recovery.Err, context.Canceled) {
				t.Fatalf("cancelled post-commit recovery = %#v", cancelledResult.Recovery)
			}
			if blocked, err := runStore.RunDispatchBlocked(fixture.ctx, fixture.event.RunID()); err != nil || blocked {
				t.Fatalf("committed continue after recovery cancellation: blocked=%v err=%v", blocked, err)
			}
		})
	}
}

func TestRunContinueProcessesOnlyTargetRunDecisionRoutesOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			seedWork, err := fixture.store.PipelineObligations().ClaimEvent(
				fixture.ctx,
				fixture.event.ID(),
				runtimepipelineobligation.PurposeRecovery,
			)
			if err != nil {
				t.Fatalf("claim fixture seed obligation: %v", err)
			}
			if _, err := fixture.store.PipelineObligations().Settle(
				fixture.ctx,
				seedWork.Claim,
				runtimepipelineobligation.Acknowledged("fixture_seeded"),
			); err != nil {
				t.Fatalf("settle fixture seed obligation: %v", err)
			}
			runStore := fixture.store.(runtimeruncontrol.Store)
			pausedAt := fixture.event.CreatedAt().Add(time.Second)
			if _, err := runStore.PauseRunControl(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:        fixture.event.RunID(),
				Reason:       "decision-route continuation proof",
				ControlledBy: "test",
				Now:          pausedAt,
			}); err != nil {
				t.Fatalf("PauseRunControl: %v", err)
			}
			target := newRetryReleaseTestEvent(fixture, pausedAt.Add(time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(
				t,
				fixture.ctx,
				fixture.store,
				target,
				[]events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(fixture.agentID), AgentIdentity: fixture.identity}},
				runtimepipelineobligation.ScopeSubscribed,
			)
			fixture.insertDecisionObligationFor(t, target)
			deliveries := fixture.subscribe(t, target.Type())
			defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)

			foreignRunID := uuid.NewString()
			foreignAt := target.CreatedAt().Add(time.Microsecond)
			seedCompleteEventDispatchRun(t, fixture.ctx, fixture.db, backend, foreignRunID, foreignAt.Add(-time.Second))
			foreign := newRetryReleaseRunRoot(foreignRunID, foreignAt)
			storetest.CommitSemanticEventWithRoutes(
				t,
				fixture.ctx,
				fixture.store,
				foreign,
				nil,
				runtimepipelineobligation.ScopeSubscribed,
			)
			fixture.insertDecisionObligationFor(t, foreign)

			queue := &recordingRunRecoveryQueue{bus: fixture.bus}
			controller := runtimeruncontrol.NewController(runStore, queue, runtimeruncontrol.Options{ReleaseLimit: 2})
			fixture.bus.SetRunDispatchGate(controller)
			t.Cleanup(func() { fixture.bus.SetRunDispatchGate(nil) })

			result, err := controller.Continue(fixture.ctx, runtimeruncontrol.TransitionRequest{
				RunID:  target.RunID(),
				Reason: "continue pending decision route",
				Now:    target.CreatedAt().Add(time.Second),
			})
			if err != nil {
				t.Fatalf("Continue: %v", err)
			}
			if result.Recovery.Sweep.Settled != 1 {
				t.Fatalf("released pipeline obligations = %d with sweeps %#v and errors %v, want target decision route only", result.Recovery.Sweep.Settled, queue.results, queue.errors)
			}
			if got := fixture.decisionObligationStatus(t, target.ID()); got != "completed" {
				t.Fatalf("target decision route status = %q, want completed", got)
			}
			if got := fixture.decisionObligationStatus(t, foreign.ID()); got != "pending" {
				t.Fatalf("foreign decision route status = %q, want pending", got)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, target.ID()); got != 1 {
				t.Fatalf("target event pipeline receipts = %d, want 1", got)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, foreign.ID()); got != 0 {
				t.Fatalf("foreign event pipeline receipts = %d, want 0", got)
			}
			assertCompleteLocalDelivery(t, deliveries, target)
		})
	}
}

func TestPeriodicGlobalScanReentersDecisionRoutesUnderSustainedRecoveryBacklogOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			seedWork, err := fixture.store.PipelineObligations().ClaimEvent(
				fixture.ctx,
				fixture.event.ID(),
				runtimepipelineobligation.PurposeRecovery,
			)
			if err != nil {
				t.Fatalf("claim fixture seed obligation: %v", err)
			}
			if _, err := fixture.store.PipelineObligations().Settle(
				fixture.ctx,
				seedWork.Claim,
				runtimepipelineobligation.Acknowledged("fixture_seeded"),
			); err != nil {
				t.Fatalf("settle fixture seed obligation: %v", err)
			}

			base := fixture.event.CreatedAt().Add(time.Second)
			for i := 0; i < 3; i++ {
				event := newRetryReleaseTestEvent(fixture, base.Add(time.Duration(i*100)*time.Microsecond))
				storetest.CommitSemanticEventWithRoutes(
					t,
					fixture.ctx,
					fixture.store,
					event,
					nil,
					runtimepipelineobligation.ScopeSubscribed,
				)
			}
			first, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 1)
			if err != nil {
				t.Fatalf("first bounded sweep: %v", err)
			}
			if first.Examined != 1 || first.Exhausted {
				t.Fatalf("first bounded sweep = %#v, want retained ordinary-recovery cursor", first)
			}

			target := newRetryReleaseTestEvent(fixture, base.Add(10*time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(
				t,
				fixture.ctx,
				fixture.store,
				target,
				[]events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(fixture.agentID), AgentIdentity: fixture.identity}},
				runtimepipelineobligation.ScopeSubscribed,
			)
			fixture.insertDecisionObligationFor(t, target)
			deliveries := fixture.subscribe(t, target.Type())
			defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)

			completed := false
			// Each write sorts after the cursor but before the original phase tail.
			backdatedOffsets := [...]int{50, 75, 87, 93, 96, 98, 99}
			for pass := 0; pass < 7; pass++ {
				appended := newRetryReleaseTestEvent(fixture, base.Add(time.Duration(backdatedOffsets[pass])*time.Microsecond))
				storetest.CommitSemanticEventWithRoutes(
					t,
					fixture.ctx,
					fixture.store,
					appended,
					nil,
					runtimepipelineobligation.ScopeSubscribed,
				)
				if _, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 1); err != nil {
					t.Fatalf("bounded sweep %d: %v", pass+2, err)
				}
				if fixture.decisionObligationStatus(t, target.ID()) == "completed" {
					completed = true
					break
				}
			}
			if !completed {
				t.Fatal("newly due decision route starved behind sustained ordinary-recovery backlog")
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, target.ID()); got != 1 {
				t.Fatalf("target event pipeline receipts = %d, want 1", got)
			}
			assertCompleteLocalDelivery(t, deliveries, target)
		})
	}
}

func TestControllerCancellationBetweenBatchesAbandonsCursorOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"ingress_resume", "run_continue"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				fixture, later := newControllerContinuationFixture(t, backend)
				ctx, cancel := context.WithCancel(fixture.ctx)
				defer cancel()
				switch surface {
				case "ingress_resume":
					ingressStore := fixture.store.(runtimeingress.Store)
					if _, _, err := ingressStore.TransitionRuntimeIngressState(
						fixture.ctx,
						runtimeingress.StatusPaused,
						"cancellation proof",
						"test",
						fixture.event.CreatedAt().Add(time.Second),
					); err != nil {
						t.Fatalf("pause ingress: %v", err)
					}
					publisher := &recordingIngressRecoveryPublisher{bus: fixture.bus, cancelAfterFirst: cancel}
					controller := runtimeingress.NewController(ingressStore, publisher, runtimeingress.Options{
						ExecutionPosture: executionposture.Live,
						ReleaseLimit:     2,
					})
					fixture.bus.SetRuntimeIngressDispatchGate(controller)
					t.Cleanup(func() {
						fixture.bus.SetRuntimeIngressDispatchGate(nil)
						runtimebus.ResumeRuntimeIngress()
					})
					result, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
						Reason: "continue cancellation proof",
						Now:    fixture.event.CreatedAt().Add(2 * time.Second),
					})
					if err != nil {
						t.Fatalf("Resume: %v", err)
					}
					if result.ReleasedCount != 1 || len(publisher.results) != 2 {
						t.Fatalf("cancelled ingress continuation = released:%d results:%#v, want one settlement and explicit failed continuation", result.ReleasedCount, publisher.results)
					}
				case "run_continue":
					runStore := fixture.store.(runtimeruncontrol.Store)
					if _, err := runStore.PauseRunControl(fixture.ctx, runtimeruncontrol.TransitionRequest{
						RunID:        fixture.event.RunID(),
						Reason:       "cancellation proof",
						ControlledBy: "test",
						Now:          fixture.event.CreatedAt().Add(time.Second),
					}); err != nil {
						t.Fatalf("PauseRunControl: %v", err)
					}
					queue := &recordingRunRecoveryQueue{bus: fixture.bus, cancelAfterFirst: cancel}
					controller := runtimeruncontrol.NewController(runStore, queue, runtimeruncontrol.Options{ReleaseLimit: 2})
					fixture.bus.SetRunDispatchGate(controller)
					t.Cleanup(func() { fixture.bus.SetRunDispatchGate(nil) })
					result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{
						RunID:  fixture.event.RunID(),
						Reason: "continue cancellation proof",
						Now:    fixture.event.CreatedAt().Add(2 * time.Second),
					})
					if err != nil {
						t.Fatalf("Continue: %v", err)
					}
					if result.Recovery.Sweep.Settled != 1 ||
						result.Recovery.Disposition != runtimeruncontrol.RecoveryCancelled ||
						len(queue.results) != 2 {
						t.Fatalf("cancelled run continuation = recovery:%#v results:%#v, want one settlement and explicit cancelled continuation", result.Recovery, queue.results)
					}
				}

				reclaimed := drainControllerRecovery(t, fixture, surface)
				if reclaimed != 1 {
					t.Fatalf("fresh recovery settled %d abandoned later obligations, want 1", reclaimed)
				}
				assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
				for _, event := range later {
					if got := retryReleasePipelineReceiptCount(t, fixture, event.ID()); got != 1 {
						t.Fatalf("reclaimed event %s pipeline receipts = %d, want 1", event.ID(), got)
					}
				}
			})
		}
	}
}

func newControllerContinuationFixture(t *testing.T, backend string) (completeEventDispatchFixture, []events.Event) {
	t.Helper()
	fixture := newCompleteEventDispatchFixture(t, backend, false)
	later := []events.Event{
		newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Microsecond)),
		newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(2*time.Microsecond)),
	}
	for _, event := range later {
		storetest.CommitSemanticEventWithRoutes(
			t,
			fixture.ctx,
			fixture.store,
			event,
			nil,
			runtimepipelineobligation.ScopeSubscribed,
		)
	}
	fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})
	return fixture, later
}

func assertSweepContinuation(
	t *testing.T,
	results []runtimepipelineobligation.SweepResult,
	settled int,
) {
	t.Helper()
	if settled != 2 {
		t.Fatalf("controller settled = %d, want both later obligations", settled)
	}
	if len(results) < 2 {
		t.Fatalf("controller batches = %#v, want at least two", results)
	}
	first := results[0]
	if first.Settled != 1 || first.Examined != 2 || first.Exhausted || first.Blocked {
		t.Fatalf("first controller batch = %#v, want under-filled settlement without termination", first)
	}
	last := results[len(results)-1]
	if !last.Exhausted || !last.Blocked {
		t.Fatalf("final controller batch = %#v, want explicit exhaustion with retained local blockage", last)
	}
}

func assertRunControllerContinuation(
	t *testing.T,
	results []runtimepipelineobligation.SweepResult,
	recovery runtimeruncontrol.PostCommitRecovery,
) {
	t.Helper()
	assertSweepContinuation(t, results, recovery.Sweep.Settled)
	if recovery.Disposition != runtimeruncontrol.RecoveryBlocked ||
		!recovery.Sweep.Exhausted ||
		!recovery.Sweep.Blocked ||
		recovery.Err != nil {
		t.Fatalf("controller recovery = %#v, want explicit exhausted blockage", recovery)
	}
}

func drainControllerRecovery(t *testing.T, fixture completeEventDispatchFixture, surface string) int {
	t.Helper()
	total := 0
	for {
		var (
			result runtimepipelineobligation.SweepResult
			err    error
		)
		switch surface {
		case "ingress_resume":
			result, err = fixture.bus.ReleaseRuntimeIngressQueue(fixture.ctx, 2)
		case "run_continue":
			result, err = fixture.bus.ReleaseRunQueue(fixture.ctx, fixture.event.RunID(), 2)
		default:
			t.Fatalf("unknown controller recovery surface %q", surface)
		}
		if err != nil {
			t.Fatalf("fresh %s recovery: %v", surface, err)
		}
		total += result.Settled
		if result.Exhausted || result.Blocked {
			return total
		}
	}
}

func TestPipelineScanRunLocalBlockDoesNotStarveLaterRunOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, route := range []string{"ordinary", "decision"} {
			t.Run(backend+"/"+route, func(t *testing.T) {
				fixture := newCompleteEventDispatchFixture(t, backend, route == "decision")
				laterRunID := uuid.NewString()
				laterAt := fixture.event.CreatedAt().Add(time.Microsecond)
				seedCompleteEventDispatchRun(t, fixture.ctx, fixture.db, backend, laterRunID, laterAt.Add(-time.Second))
				later := newRetryReleaseRunRoot(laterRunID, laterAt)
				storetest.CommitSemanticEventWithRoutes(
					t,
					fixture.ctx,
					fixture.store,
					later,
					nil,
					runtimepipelineobligation.ScopeSubscribed,
				)
				if route == "decision" {
					fixture.insertDecisionObligationFor(t, later)
				}
				fixture.bus.SetRunDispatchGate(blockedRunDispatchGate{fixture.event.RunID(): true})

				result, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 10)
				if err != nil {
					t.Fatalf("SweepPipelineObligations: %v", err)
				}
				if result.Settled != 1 || !result.Exhausted || !result.Blocked {
					t.Fatalf("sweep result = %#v, want one later settlement plus exhausted local block", result)
				}
				if got := retryReleasePipelineReceiptCount(t, fixture, fixture.event.ID()); got != 0 {
					t.Fatalf("blocked predecessor receipts = %d, want 0", got)
				}
				if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
					t.Fatalf("later running-run receipts = %d, want 1", got)
				}
				if route == "decision" {
					if got := fixture.decisionObligationStatus(t, fixture.event.ID()); got != "pending" {
						t.Fatalf("blocked decision route status = %q, want pending", got)
					}
					if got := fixture.decisionObligationStatus(t, later.ID()); got != "completed" {
						t.Fatalf("later decision route status = %q, want completed", got)
					}
				}

			})
		}
	}
}

func newRetryReleaseTestEvent(fixture completeEventDispatchFixture, createdAt time.Time) events.Event {
	sourceRoute := events.RouteIdentity{
		FlowID:       "retry-source",
		FlowInstance: "retry-source/one",
		EntityID:     uuid.NewString(),
	}
	return eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
		uuid.NewString(),
		events.EventType("custom.replay.checked"),
		eventtest.Producer(events.EventProducerNode, "retry-release-node"),
		"retry-release-task",
		[]byte(`{"text":"retry release"}`),
		fixture.event.ChainDepth()+1,
		fixture.event.RunID(),
		fixture.event.ID(),
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute),
		createdAt.UTC().Truncate(time.Microsecond),
	), executionmode.Mock)
}

func newRetryReleaseRunRoot(runID string, createdAt time.Time) events.Event {
	entityID := uuid.NewString()
	return eventtest.ExistingRunRootIngress(
		uuid.NewString(),
		events.EventType("custom.replay.checked"),
		"api.v1",
		"",
		[]byte(`{"text":"later running run"}`),
		0,
		runID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		createdAt.UTC().Truncate(time.Microsecond),
	)
}

func assertRetryReleaseReplayable(t *testing.T, fixture completeEventDispatchFixture, eventID string) {
	t.Helper()
	if got := retryReleasePipelineReceiptCount(t, fixture, eventID); got != 0 {
		t.Fatalf("retry-release event pipeline receipts = %d, want 0", got)
	}
	work, err := fixture.store.PipelineObligations().ClaimEvent(
		fixture.ctx,
		eventID,
		runtimepipelineobligation.PurposeRecovery,
	)
	if err != nil {
		t.Fatalf("reclaim retry-release event: %v", err)
	}
	if err := fixture.store.PipelineObligations().Release(fixture.ctx, work.Claim); err != nil {
		t.Fatalf("release reclaimed retry-release event: %v", err)
	}
}

func retryReleasePipelineReceiptCount(t *testing.T, fixture completeEventDispatchFixture, eventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if fixture.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var count int
	if err := fixture.db.QueryRowContext(fixture.ctx, query, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts: %v", err)
	}
	return count
}

var _ runtimebus.EventInterceptor = retryReleaseInterceptor{}
var _ runtimebus.EventInterceptor = retryReleaseSetInterceptor{}
