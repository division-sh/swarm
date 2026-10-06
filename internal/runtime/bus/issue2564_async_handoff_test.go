package bus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type issue2564HandoffPipelineOwner struct {
	*terminalReleasePipelineOwner
	settlements int
	settleErr   error
	committed   bool
	settled     chan struct{}
}

func (o *issue2564HandoffPipelineOwner) Settle(ctx context.Context, claim pipelineobligation.Claim, disposition pipelineobligation.Disposition) (pipelineobligation.SettlementOutcome, error) {
	if o.settleErr != nil && !o.committed {
		return pipelineobligation.SettlementOutcome{}, o.settleErr
	}
	if !disposition.Successful() {
		return pipelineobligation.SettlementOutcome{}, errors.New("expected successful handoff disposition")
	}
	if err := o.Release(ctx, claim); err != nil {
		return pipelineobligation.SettlementOutcome{}, err
	}
	o.settlements++
	close(o.settled)
	return pipelineobligation.CommittedSettlement(true), o.settleErr
}

type issue2564HandoffContinuation struct {
	permissiveTestDeliveryOwner
	mu       sync.Mutex
	proofs   []runtimedelivery.DurableHandoffProof
	signals  int
	signaled chan struct{}
}

func (*issue2564HandoffContinuation) OwnsPersistedRecovery() bool { return true }
func (o *issue2564HandoffContinuation) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	if err := o.permissiveTestDeliveryOwner.AcceptCommitted(proofs); err != nil {
		return err
	}
	o.mu.Lock()
	o.proofs = append(o.proofs, proofs...)
	o.mu.Unlock()
	return nil
}
func (o *issue2564HandoffContinuation) Signal() {
	o.mu.Lock()
	o.signals++
	o.mu.Unlock()
	close(o.signaled)
}

type issue2564HandoffRouteInterceptor struct{ entered, release chan struct{} }

func (i issue2564HandoffRouteInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	return true, nil, pipelineobligation.Continue(), nil
}
func (i issue2564HandoffRouteInterceptor) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	close(i.entered)
	select {
	case <-ctx.Done():
		return false, nil, pipelineobligation.Continue(), ctx.Err()
	case <-i.release:
		return false, nil, pipelineobligation.Continue(), consumeReceiverProjectionTestCarrier(ctx, delivery.Event(), route)
	}
}

func TestIssue2564AsyncHandoffPreservesForegroundCompletion(t *testing.T) {
	for _, name := range []string{"synchronous", "local_completion", "direct", "unacknowledged"} {
		t.Run(name, func(t *testing.T) {
			bus, prepared, owner, _ := issue2564PreparedHandoff(t)
			interceptor := issue2564HandoffRouteInterceptor{entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(interceptor.release) }) })
			bus.SetInterceptors(interceptor)
			switch name {
			case "local_completion":
				prepared.receiver.completion = newLocalDeliveryCompletionGroup()
			case "direct":
				prepared.direct = true
			case "unacknowledged":
				prepared.durableHandoffReady = false
			}
			done := make(chan error, 1)
			go func() {
				if name == "synchronous" {
					done <- bus.DispatchPreparedPublish(context.Background(), prepared)
				} else {
					done <- bus.dispatchPreparedPublishAsyncBody(context.Background(), prepared)
				}
			}()
			select {
			case <-interceptor.entered:
			case err := <-done:
				t.Fatalf("completion path was incorrectly transferred: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("completion path never entered its node")
			}
			owner.mu.Lock()
			retained := len(owner.current)
			owner.mu.Unlock()
			if retained != 1 {
				t.Fatalf("completion path prematurely released claim: %d", retained)
			}
			release.Do(func() { close(interceptor.release) })
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("completed node did not release publication")
			}
		})
	}
}

func issue2564PreparedHandoff(t *testing.T) (*EventBus, PreparedPublish, *issue2564HandoffPipelineOwner, *issue2564HandoffContinuation) {
	t.Helper()
	owner := &issue2564HandoffPipelineOwner{terminalReleasePipelineOwner: newTerminalReleasePipelineOwner(), settled: make(chan struct{})}
	bus, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{PipelineObligations: owner})
	if err != nil {
		t.Fatal(err)
	}
	continuations := &issue2564HandoffContinuation{signaled: make(chan struct{})}
	if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatal(err)
	}
	event := receiverProjectionEvent(uuid.NewString())
	event = eventtest.RunCreatingRootIngress(uuid.NewString(), event.Type(), "", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	plan := nodeOnlyDeliveryPlan(t, event, "handoff-node")
	claim, err := bus.claimPipelinePublication(context.Background(), event.ID())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := bus.receiverProjection(context.Background(), event.DeliveryContext())
	if err != nil {
		t.Fatal(err)
	}
	authority, err := bus.DeliveryAuthority()
	if err != nil {
		t.Fatal(err)
	}
	route := plan.DeliveryRoutes()[0]
	id, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := runtimedelivery.AdmitDurableHandoffProof(id, event.ID(), events.EncodeDeliveryRouteIdentity(identity), authority)
	if err != nil {
		t.Fatal(err)
	}
	prepared := PreparedPublish{Event: event, plan: plan, publicationClaim: claim, receiver: projection}
	consequences, err := bus.finalizeCommittedPublicationConsequences(context.Background(), prepared, CommittedPublication{
		AppendOutcome: EventAppendInserted, DeliveryHandoffs: []runtimedelivery.DurableHandoffProof{proof}, Acknowledged: true,
	}, false)
	if err != nil || !consequences.ready {
		t.Fatalf("finalize prerequisites: ready=%t err=%v", consequences.ready, err)
	}
	t.Cleanup(func() { _ = claim.Release(context.Background()) })
	return bus, consequences.prepared, owner, continuations
}

func TestIssue2564AsyncNodeHandoffRetiresPublicationBeforeExecution(t *testing.T) {
	bus, prepared, owner, continuations := issue2564PreparedHandoff(t)
	interceptor := issue2564HandoffRouteInterceptor{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(interceptor.release) })
	bus.SetInterceptors(interceptor)
	if err := bus.DispatchPreparedPublishAsync(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	select {
	case <-continuations.signaled:
	case <-interceptor.entered:
		t.Fatal("already-acknowledged publication retained its session into node execution")
	case <-time.After(3 * time.Second):
		t.Fatal("publication did not hand off")
	}
	owner.mu.Lock()
	retained := len(owner.current)
	owner.mu.Unlock()
	continuations.mu.Lock()
	proofs, signals := len(continuations.proofs), continuations.signals
	continuations.mu.Unlock()
	if retained != 0 || owner.settlements != 1 || proofs != 1 || signals != 1 {
		t.Fatalf("retained=%d settlements=%d proofs=%d signals=%d", retained, owner.settlements, proofs, signals)
	}
}

func TestIssue2564NodeHandoffRequiresExactCoverage(t *testing.T) {
	for _, name := range []string{"exact", "no_routes", "missing", "wrong_event", "wrong_route", "foreign_generation", "duplicate", "event_wide", "ephemeral_owner", "agent_recipient", "uncovered_persisted_recipient", "runtime_coordination"} {
		t.Run(name, func(t *testing.T) {
			bus, prepared, _, _ := issue2564PreparedHandoff(t)
			proofs := prepared.committedHandoffs
			switch name {
			case "no_routes":
				prepared.plan = newRoutePlan(prepared.Event)
			case "missing":
				proofs = nil
			case "wrong_event", "wrong_route", "foreign_generation":
				eventID, deliveryID := prepared.Event.ID(), proofs[0].DeliveryID()
				authority := proofs[0].Authority()
				if name == "wrong_event" {
					eventID = uuid.NewString()
				} else if name == "wrong_route" {
					deliveryID = uuid.NewString()
				} else {
					var err error
					authority, err = runtimedelivery.NewNormalExecutionAuthority(bus.sourceArtifactFact, uuid.NewString(), 2)
					if err != nil {
						t.Fatal(err)
					}
				}
				proof, err := runtimedelivery.AdmitDurableHandoffProof(deliveryID, eventID, "other", authority)
				if err != nil {
					t.Fatal(err)
				}
				proofs = []runtimedelivery.DurableHandoffProof{proof}
			case "duplicate":
				second := nodeOnlyDeliveryPlan(t, prepared.Event, "second-node").DeliveryRoutes()[0]
				prepared.plan.AddDeliveryIntents(RoutePlanDeliveryIntent{
					Recipient: second.Recipient, TargetBlueprint: second.Target.Route(), TargetOwnership: second.Target,
					Producer: routeIntentProducerInternalTargetCarrier, Persist: true,
				})
				proofs = append(proofs, proofs[0])
			case "event_wide":
				bus.SetInterceptors(continuationEventPassthroughInterceptor{})
			case "ephemeral_owner":
				if err := bus.SetDeliveryContinuationOwner(permissiveTestDeliveryOwner{}); err != nil {
					t.Fatal(err)
				}
			case "agent_recipient":
				prepared.plan.AddLiveRecipients(RoutePlanLiveRecipient{
					Recipient: events.MustAgentDeliveryRecipient("agent"), AgentIdentity: agentidentitytest.RootDeclaredForRun(t, prepared.Event.RunID(), "agent", "role"),
					Producer: routeIntentProducerAgentPolicy,
				})
			case "uncovered_persisted_recipient":
				prepared.plan.AddLiveRecipients(RoutePlanLiveRecipient{
					Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "uncovered")),
					Producer:  routeIntentProducerInternalTargetCarrier, PersistAsDelivery: true,
				})
			case "runtime_coordination":
				prepared.Event = eventtest.RuntimeControl(prepared.Event.ID(), prepared.Event.Type(), "test", "", []byte(`{}`), 0, prepared.Event.RunID(), "", events.EventEnvelope{}, time.Now().UTC())
			}
			if got := bus.canTransferNodeDeliveries(prepared.Event, prepared.plan, proofs); got != (name == "exact") {
				t.Fatalf("transfer eligibility = %t", got)
			}
		})
	}
}

func TestIssue2564AsyncNodeHandoffSettlementFailureRetainsRecovery(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "uncommitted"
		if committed {
			name = "acknowledged_cleanup_failure"
		}
		t.Run(name, func(t *testing.T) {
			bus, prepared, owner, continuations := issue2564PreparedHandoff(t)
			failure := errors.New("injected settlement failure")
			owner.settleErr, owner.committed = failure, committed
			if err := bus.dispatchPreparedPublishAsyncBody(context.Background(), prepared); !errors.Is(err, failure) {
				t.Fatalf("settlement error = %v", err)
			}
			continuations.mu.Lock()
			signals := continuations.signals
			continuations.mu.Unlock()
			want := 0
			if committed {
				want = 1
			}
			owner.mu.Lock()
			retained := len(owner.current)
			owner.mu.Unlock()
			if signals != want || owner.settlements != want || retained != 0 {
				t.Fatalf("signals=%d settlements=%d retained=%d, want %d/%d/0", signals, owner.settlements, retained, want, want)
			}
		})
	}
}
