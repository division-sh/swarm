package bus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type terminalReleasePipelineOwner struct {
	runtimepipelineobligation.Store

	mu           sync.Mutex
	issuer       *runtimepipelineobligation.ClaimIssuer
	current      map[string]runtimepipelineobligation.Claim
	releaseCalls map[string]int
	releaseError map[string]func(int) error
}

func newTerminalReleasePipelineOwner() *terminalReleasePipelineOwner {
	return &terminalReleasePipelineOwner{
		issuer:       runtimepipelineobligation.NewClaimIssuer(),
		current:      map[string]runtimepipelineobligation.Claim{},
		releaseCalls: map[string]int{},
		releaseError: map[string]func(int) error{},
	}
}

func (o *terminalReleasePipelineOwner) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return o.claim(eventID, runtimepipelineobligation.PurposePublication)
}

func (o *terminalReleasePipelineOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := o.claim(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return runtimepipelineobligation.ClaimedWork{Claim: claim}, nil
}

func (o *terminalReleasePipelineOwner) claim(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	eventID = strings.TrimSpace(eventID)
	if _, ok := o.current[eventID]; ok {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	claim, err := o.issuer.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	o.current[eventID] = claim
	return claim, nil
}

func (o *terminalReleasePipelineOwner) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	current, ok := o.current[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, currentErr := o.issuer.Token(current)
	claimToken, claimErr := o.issuer.Token(claim)
	if currentErr != nil || claimErr != nil || currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	delete(o.current, claim.EventID())
	o.releaseCalls[claim.EventID()]++
	if failure := o.releaseError[claim.EventID()]; failure != nil {
		return failure(o.releaseCalls[claim.EventID()])
	}
	return nil
}

type terminalReleasePausedGate struct{}

func (terminalReleasePausedGate) QueueableIngressPaused(context.Context) (bool, error) {
	return true, nil
}

type signalingPipelineSettlementOwner struct {
	runtimepipelineobligation.Store
	err       error
	committed bool
	settled   int
}

func (o *signalingPipelineSettlementOwner) Settle(
	_ context.Context,
	_ runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) (runtimepipelineobligation.SettlementOutcome, error) {
	if o.err != nil {
		if o.committed {
			return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), o.err
		}
		return runtimepipelineobligation.SettlementOutcome{}, o.err
	}
	o.settled++
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
}

func TestSuccessfulPipelineSettlementSignalsDeliveryContinuationsAfterCommit(t *testing.T) {
	issuer := runtimepipelineobligation.NewClaimIssuer()
	claim, err := issuer.Issue(uuid.NewString(), runtimepipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatal(err)
	}
	pipeline := &signalingPipelineSettlementOwner{}
	continuations := &controlledTestDeliveryOwner{}
	bus := &EventBus{
		pipelineObligations:   pipeline,
		deliveryContinuations: continuations,
	}
	if err := bus.settlePipelineObligation(
		context.Background(),
		claim,
		runtimepipelineobligation.Acknowledged("pipeline_persisted"),
	); err != nil {
		t.Fatalf("settle pipeline obligation: %v", err)
	}
	continuations.mu.Lock()
	signals := continuations.signals
	continuations.mu.Unlock()
	if pipeline.settled != 1 || signals != 1 {
		t.Fatalf("settled/signals = %d/%d, want one durable settlement then one wake", pipeline.settled, signals)
	}

	pipeline.err = errors.New("injected settlement failure")
	if err := bus.settlePipelineObligation(
		context.Background(),
		claim,
		runtimepipelineobligation.Acknowledged("pipeline_persisted"),
	); err == nil {
		t.Fatal("failed pipeline settlement succeeded")
	}
	continuations.mu.Lock()
	signals = continuations.signals
	continuations.mu.Unlock()
	if signals != 1 {
		t.Fatalf("failed pipeline settlement signaled delivery continuation: %d", signals)
	}

	pipeline.committed = true
	if err := bus.settlePipelineObligation(
		context.Background(),
		claim,
		runtimepipelineobligation.Acknowledged("pipeline_persisted"),
	); !errors.Is(err, pipeline.err) {
		t.Fatalf("committed settlement cleanup error = %v, want %v", err, pipeline.err)
	}
	continuations.mu.Lock()
	signals = continuations.signals
	continuations.mu.Unlock()
	if signals != 2 {
		t.Fatalf("committed settlement cleanup failure signals = %d, want 2", signals)
	}
}

func TestPipelinePublicationReleaseIsTerminalAndImmediatelyReclaimable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure func(int) error
	}{
		{
			name: "fail once",
			failure: func(attempt int) error {
				if attempt == 1 {
					return errors.New("fail-once publication cleanup")
				}
				return nil
			},
		},
		{
			name: "persistent failure",
			failure: func(int) error {
				return errors.New("persistent publication cleanup failure")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newTerminalReleasePipelineOwner()
			eventID := uuid.NewString()
			owner.releaseError[eventID] = tc.failure
			bus := &EventBus{
				pipelineObligations: owner,
				bundleSourceFact:    sourceMutationFact(t, "9"),
			}
			claim, err := bus.claimPipelinePublication(context.Background(), eventID)
			if err != nil {
				t.Fatalf("claim publication: %v", err)
			}
			if err := claim.Release(context.Background()); err == nil {
				t.Fatal("terminal release hid cleanup evidence")
			}
			if err := claim.Release(context.Background()); err != nil {
				t.Fatalf("repeated wrapper release = %v, want local terminal no-op", err)
			}
			if got := owner.releaseCalls[eventID]; got != 1 {
				t.Fatalf("store release calls = %d, want one terminal attempt", got)
			}

			reclaimed, err := bus.claimPipelinePublication(context.Background(), eventID)
			if err != nil {
				t.Fatalf("reclaim after terminal cleanup failure: %v", err)
			}
			delete(owner.releaseError, eventID)
			if err := reclaimed.Release(context.Background()); err != nil {
				t.Fatalf("release reclaimed publication: %v", err)
			}
		})
	}
}

func TestPipelineOneShotTerminalSinksPropagateOrRecordCleanupEvidence(t *testing.T) {
	for _, sink := range []string{"selected_fork_abandon", "rollback_cleanup", "direct_recovery"} {
		for _, failureMode := range []string{"fail_once", "persistent_failure"} {
			t.Run(sink+"/"+failureMode, func(t *testing.T) {
				owner := newTerminalReleasePipelineOwner()
				eventID := uuid.NewString()
				owner.releaseError[eventID] = func(attempt int) error {
					if failureMode == "fail_once" && attempt > 1 {
						return nil
					}
					return errors.New(failureMode + " one-shot cleanup failure")
				}
				bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{PipelineObligations: owner})
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}

				switch sink {
				case "selected_fork_abandon":
					claim, err := bus.claimPipelinePublication(context.Background(), eventID)
					if err != nil {
						t.Fatalf("claim selected-fork publication: %v", err)
					}
					err = bus.AbandonPreparedPublish(context.Background(), PreparedPublish{
						publicationClaim: claim,
						receiver:         receiverDispatchProjection{occurrence: bus.workOwner},
					})
					if err == nil || !strings.Contains(err.Error(), failureMode+" one-shot cleanup failure") {
						t.Fatalf("abandon cleanup error = %v, want propagated evidence", err)
					}
				case "rollback_cleanup":
					claim, err := bus.claimPipelinePublication(context.Background(), eventID)
					if err != nil {
						t.Fatalf("claim rollback publication: %v", err)
					}
					claim.releaseAndLog(context.Background())
				case "direct_recovery":
					event := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
					bus.SetRuntimeIngressDispatchGate(terminalReleasePausedGate{})
					err := (engineDispatcher{bus: bus}).dispatchAndRecord(
						context.Background(),
						runtimeengine.EmitIntent{Event: event},
						nil,
					)
					if err == nil || !strings.Contains(err.Error(), failureMode+" one-shot cleanup failure") {
						t.Fatalf("direct recovery cleanup error = %v with %d release call(s), want propagated evidence", err, owner.releaseCalls[eventID])
					}
				}

				if got := owner.releaseCalls[eventID]; got != 1 {
					t.Fatalf("%s release calls = %d, want one terminal attempt", sink, got)
				}
				reclaimed, err := bus.claimPipelinePublication(context.Background(), eventID)
				if err != nil {
					t.Fatalf("reclaim after %s cleanup failure: %v", sink, err)
				}
				delete(owner.releaseError, eventID)
				if err := reclaimed.Release(context.Background()); err != nil {
					t.Fatalf("release reclaimed %s publication: %v", sink, err)
				}
			})
		}
	}
}

func TestPreparedDispatchAdmissionFailureTerminallyConsumesPublicationClaim(t *testing.T) {
	dispatches := []struct {
		name string
		run  func(*EventBus, context.Context, PreparedPublish) error
	}{
		{
			name: "sync",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublish(ctx, prepared)
			},
		},
		{
			name: "async",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublishAsync(ctx, prepared)
			},
		},
		{
			name: "wait",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublishAndWait(ctx, prepared)
			},
		},
	}
	for _, lifecycle := range []string{"fenced", "retired"} {
		for _, dispatch := range dispatches {
			t.Run(lifecycle+"/"+dispatch.name, func(t *testing.T) {
				process := worklifetime.NewProcess()
				runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
					RuntimeInstanceID: "prepared-dispatch-" + lifecycle + "-" + dispatch.name,
					BundleHash:        "prepared-dispatch-bundle",
				})
				if err != nil {
					t.Fatalf("create runtime occurrence: %v", err)
				}
				t.Cleanup(func() {
					if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
						t.Errorf("retire runtime occurrence: %v", err)
					}
					process.Retire()
					if _, err := process.Join(context.Background()); err != nil {
						t.Errorf("join process occurrence: %v", err)
					}
				})

				owner := newTerminalReleasePipelineOwner()
				bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{
					PipelineObligations: owner,
					WorkOwner:           runtimeOwner,
				})
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}
				eventID := uuid.NewString()
				claim, err := bus.claimPipelinePublication(context.Background(), eventID)
				if err != nil {
					t.Fatalf("claim publication: %v", err)
				}
				cleanupFailure := lifecycle + " " + dispatch.name + " cleanup failure"
				owner.releaseError[eventID] = func(int) error { return errors.New(cleanupFailure) }
				prepared := PreparedPublish{
					Event: eventtest.RuntimeControl(
						eventID,
						events.EventType("test.prepared.dispatch"),
						"test",
						"",
						[]byte(`{}`),
						0,
						uuid.NewString(),
						"",
						events.EventEnvelope{},
						time.Now().UTC(),
					),
					publicationClaim: claim,
					receiver:         receiverDispatchProjection{occurrence: runtimeOwner},
				}
				if lifecycle == "fenced" {
					if err := runtimeOwner.Fence(); err != nil {
						t.Fatalf("fence runtime occurrence: %v", err)
					}
				} else {
					runtimeOwner.Retire()
				}

				err = dispatch.run(bus, context.Background(), prepared)
				if err == nil || !strings.Contains(err.Error(), cleanupFailure) {
					t.Fatalf("dispatch error = %v, want admission plus cleanup evidence", err)
				}
				if got := owner.releaseCalls[eventID]; got != 1 {
					t.Fatalf("terminal release calls = %d, want 1", got)
				}
				delete(owner.releaseError, eventID)
				reclaimed, err := bus.claimPipelinePublication(context.Background(), eventID)
				if err != nil {
					t.Fatalf("immediate durable reclaim: %v", err)
				}
				if err := reclaimed.Release(context.Background()); err != nil {
					t.Fatalf("release reclaimed publication: %v", err)
				}
			})
		}
	}
}

func TestInvalidPreparedDispatchTerminallyConsumesAttachedPublicationClaim(t *testing.T) {
	for _, dispatch := range []struct {
		name string
		run  func(*EventBus, context.Context, PreparedPublish) error
	}{
		{
			name: "sync",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublish(ctx, prepared)
			},
		},
		{
			name: "async",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublishAsync(ctx, prepared)
			},
		},
		{
			name: "wait",
			run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
				return bus.DispatchPreparedPublishAndWait(ctx, prepared)
			},
		},
	} {
		t.Run(dispatch.name, func(t *testing.T) {
			process := worklifetime.NewProcess()
			runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
				RuntimeInstanceID: "invalid-prepared-dispatch-" + dispatch.name,
				BundleHash:        "invalid-prepared-dispatch-bundle",
			})
			if err != nil {
				t.Fatalf("create runtime occurrence: %v", err)
			}
			t.Cleanup(func() {
				if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire runtime occurrence: %v", err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join process occurrence: %v", err)
				}
			})

			owner := newTerminalReleasePipelineOwner()
			bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{
				PipelineObligations: owner,
				WorkOwner:           runtimeOwner,
			})
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			eventID := uuid.NewString()
			claim, err := bus.claimPipelinePublication(context.Background(), eventID)
			if err != nil {
				t.Fatalf("claim publication: %v", err)
			}
			cleanupFailure := "invalid prepared " + dispatch.name + " cleanup failure"
			owner.releaseError[eventID] = func(int) error { return errors.New(cleanupFailure) }

			err = dispatch.run(bus, context.Background(), PreparedPublish{
				publicationClaim: claim,
				receiver:         receiverDispatchProjection{occurrence: runtimeOwner},
			})
			if err == nil ||
				!strings.Contains(err.Error(), "prepared event is required") ||
				!strings.Contains(err.Error(), cleanupFailure) {
				t.Fatalf("invalid prepared dispatch error = %v, want validation plus cleanup evidence", err)
			}
			if got := owner.releaseCalls[eventID]; got != 1 {
				t.Fatalf("terminal release calls = %d, want 1", got)
			}
			delete(owner.releaseError, eventID)
			reclaimed, err := bus.claimPipelinePublication(context.Background(), eventID)
			if err != nil {
				t.Fatalf("immediate durable reclaim: %v", err)
			}
			if err := reclaimed.Release(context.Background()); err != nil {
				t.Fatalf("release reclaimed publication: %v", err)
			}
		})
	}
}

func TestEventBusResetPreservesPendingOperationUntilPriorRetirementSucceeds(t *testing.T) {
	store := newExactHandoffProofStore(t, false)
	owner := newTerminalReleasePipelineOwner()
	bus, err := newScopedTestEventBus(store, EventBusOptions{PipelineObligations: owner})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	continuations := &controlledTestDeliveryOwner{returnFailures: 1}
	if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatalf("install fail-once delivery continuation owner: %v", err)
	}
	token := testAgentLifecycleToken(t, "agent-a", "", 7, 1)
	bus.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work")))
	eventID, runID := uuid.NewString(), token.Identity.RunID
	event := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now())
	store.seed(t, eventID, runID, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(token.AgentID), AgentIdentity: token.Identity})
	if err := deliverToTestAgent(context.Background(), bus, event, token.Identity); err != nil {
		t.Fatalf("queue buffered delivery: %v", err)
	}
	claim, err := bus.claimPipelinePublication(context.Background(), eventID)
	if err != nil {
		t.Fatalf("claim pending publication: %v", err)
	}
	bus.stageCommittedOutboxOperation(
		runtimeengine.EmitIntent{Event: event},
		EventAppendInserted,
		claim,
		nil,
	)

	if err := bus.ResetInMemoryState(); err == nil {
		t.Fatal("first reset unexpectedly hid route handoff failure")
	}
	if got := owner.releaseCalls[eventID]; got != 0 {
		t.Fatalf("pending release calls after prior retirement failure = %d, want 0", got)
	}
	bus.mu.RLock()
	pending := len(bus.pendingOutboxByID[eventID])
	bus.mu.RUnlock()
	if pending != 1 {
		t.Fatalf("pending operations after prior retirement failure = %d, want exact operation retained", pending)
	}

	if err := bus.ResetInMemoryState(); err != nil {
		t.Fatalf("retry reset after handoff proof recovery: %v", err)
	}
	if got := owner.releaseCalls[eventID]; got != 1 {
		t.Fatalf("pending release calls after successful prior retirement = %d, want 1", got)
	}
	bus.mu.RLock()
	pending = len(bus.pendingOutboxByID[eventID])
	bus.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("pending operations after terminal release = %d, want 0", pending)
	}
}

func TestRecoveredPublicationRejectsForeignSourceBeforePendingRelease(t *testing.T) {
	owner := newTerminalReleasePipelineOwner()
	bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{PipelineObligations: owner})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eventID := uuid.NewString()
	event := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
	claim, err := bus.claimPipelinePublication(context.Background(), eventID)
	if err != nil {
		t.Fatalf("claim pending publication: %v", err)
	}
	bus.stageCommittedOutboxOperation(
		runtimeengine.EmitIntent{Event: event},
		EventAppendInserted,
		claim,
		nil,
	)
	foreign, err := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("construct foreign source fact: %v", err)
	}
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), foreign)

	if _, err := bus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{
		Event: event,
		Scope: runtimepipelineobligation.ScopeSubscribed,
	}, nil); err == nil || !strings.Contains(err.Error(), "bundle source fact conflicts") {
		t.Fatalf("RecoverPersistedPipeline error = %v, want bundle source conflict", err)
	}
	if got := owner.releaseCalls[eventID]; got != 0 {
		t.Fatalf("pending release calls = %d, want 0", got)
	}
	bus.mu.RLock()
	pending := len(bus.pendingOutboxByID[eventID])
	bus.mu.RUnlock()
	if pending != 1 {
		t.Fatalf("pending operations = %d, want exact operation retained", pending)
	}

	if err := bus.clearPendingOutboxOperation(context.Background(), eventID); err != nil {
		t.Fatalf("clear pending outbox operation: %v", err)
	}
	if got := owner.releaseCalls[eventID]; got != 1 {
		t.Fatalf("cleanup release calls = %d, want 1", got)
	}
}

func TestEventBusResetAttemptsEveryTerminalPendingReleaseAndAggregatesEvidence(t *testing.T) {
	owner := newTerminalReleasePipelineOwner()
	bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{PipelineObligations: owner})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	eventIDs := []string{uuid.NewString(), uuid.NewString()}
	owner.releaseError[eventIDs[0]] = func(attempt int) error {
		if attempt == 1 {
			return errors.New("fail-once reset cleanup")
		}
		return nil
	}
	owner.releaseError[eventIDs[1]] = func(int) error {
		return errors.New("persistent reset cleanup failure")
	}
	for _, eventID := range eventIDs {
		event := eventtest.RuntimeControl(eventID, events.EventType("test.work"), "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now())
		claim, err := bus.claimPipelinePublication(context.Background(), eventID)
		if err != nil {
			t.Fatalf("claim pending publication %s: %v", eventID, err)
		}
		bus.stageCommittedOutboxOperation(
			runtimeengine.EmitIntent{Event: event},
			EventAppendInserted,
			claim,
			nil,
		)
	}

	resetErr := bus.ResetInMemoryState()
	if resetErr == nil ||
		!strings.Contains(resetErr.Error(), "fail-once reset cleanup") ||
		!strings.Contains(resetErr.Error(), "persistent reset cleanup failure") {
		t.Fatalf("reset cleanup error = %v, want all terminal evidence", resetErr)
	}
	for _, eventID := range eventIDs {
		if got := owner.releaseCalls[eventID]; got != 1 {
			t.Fatalf("release calls for %s = %d, want one terminal attempt", eventID, got)
		}
		bus.mu.RLock()
		pending := len(bus.pendingOutboxByID[eventID])
		bus.mu.RUnlock()
		if pending != 0 {
			t.Fatalf("pending operations for %s = %d, want cleared terminal owner", eventID, pending)
		}
		reclaimed, err := bus.claimPipelinePublication(context.Background(), eventID)
		if err != nil {
			t.Fatalf("reclaim %s after reset cleanup failure: %v", eventID, err)
		}
		delete(owner.releaseError, eventID)
		if err := reclaimed.Release(context.Background()); err != nil {
			t.Fatalf("release reclaimed publication %s: %v", eventID, err)
		}
	}
}
