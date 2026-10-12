package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func preparedAgentTransition(t *testing.T) (*EventBus, PreparedPublish, CommittedPublication, *issue2564HandoffPipelineOwner, *issue2564HandoffContinuation) {
	t.Helper()
	bus, prepared, owner, coordinator := issue2564PreparedHandoff(t)
	identity := agentidentitytest.RootDeclaredForRun(t, prepared.Event.RunID(), "agent", "role")
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent"), AgentIdentity: identity,
		Target: prepared.plan.DeliveryRoutes()[0].Target}
	prepared.Event = eventtest.RunCreatingRootIngress(prepared.Event.ID(), prepared.Event.Type(), "operator", "", prepared.Event.Payload(), 0,
		prepared.Event.RunID(), "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, route.Target.Route()), prepared.Event.CreatedAt())
	prepared.plan.AddLiveRecipients(RoutePlanLiveRecipient{Recipient: route.Recipient, AgentIdentity: identity, PersistAsDelivery: true, Producer: routeIntentProducerAgentPolicy})
	prepared.plan.AddDeliveryIntents(RoutePlanDeliveryIntent{Recipient: route.Recipient, AgentIdentity: identity, TargetOwnership: route.Target, TargetBlueprint: route.Target.Route(), Persist: true, Producer: routeIntentProducerAgentPolicy})
	id, err := deliverylifecycle.DeliveryID(prepared.Event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	routeID, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := deliverylifecycle.AdmitDurableHandoffProof(id, prepared.Event.ID(), events.EncodeDeliveryRouteIdentity(routeID), bus.deliveryAuthority)
	if err != nil {
		t.Fatal(err)
	}
	committed := CommittedPublication{AppendOutcome: EventAppendInserted, Acknowledged: true,
		DeliveryHandoffs: append(prepared.committedHandoffs, proof)}
	bus.SetCommittedAgentReadinessFinalizer(CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
		return ErrCommittedAgentRouteTransition
	}))
	return bus, prepared, committed, owner, coordinator
}

func TestCommittedAgentTransitionTransfersExactHandoff(t *testing.T) {
	bus, prepared, committed, owner, coordinator := preparedAgentTransition(t)
	prepared, dispatchable, err := bus.applyCommittedPublication(context.Background(), prepared, committed)
	if err != nil || !dispatchable {
		t.Fatalf("positively owned transition lost committed consequence: dispatchable=%t err=%v", dispatchable, err)
	}
	coordinator.mu.Lock()
	transferred := len(coordinator.proofs)
	coordinator.mu.Unlock()
	if transferred != len(committed.DeliveryHandoffs) {
		t.Fatalf("exact committed handoff not transferred: got=%d want=%d", transferred, len(committed.DeliveryHandoffs))
	}
	if err := bus.DispatchPreparedPublish(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if owner.settlements != 1 || coordinator.dispatches != 0 {
		t.Fatalf("handoff performed direct delivery or skipped settlement: settlements=%d direct=%d", owner.settlements, coordinator.dispatches)
	}
}

func TestCommittedAgentTransitionPreservesPrerequisiteRefusals(t *testing.T) {
	for _, name := range []string{"no_handoff", "unrelated_handoff", "unacknowledged", "duplicate", "mixed_error", "cancellation", "joined_cancellation", "no_coordinator", "selected_authority", "ephemeral_recipient", "event_wide", "target_failure", "queued", "internal_failure", "activation_failure", "refused_transfer", "invalid_commit"} {
		t.Run(name, func(t *testing.T) {
			bus, prepared, committed, owner, coordinator := preparedAgentTransition(t)
			ctx := context.Background()
			switch name {
			case "no_handoff":
				committed.DeliveryHandoffs = nil
			case "unrelated_handoff":
				committed.DeliveryHandoffs = committed.DeliveryHandoffs[:1]
			case "unacknowledged":
				committed.Acknowledged = false
			case "duplicate":
				committed.AppendOutcome = EventAppendExactDuplicate
			case "mixed_error":
				bus.SetCommittedAgentReadinessFinalizer(CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
					return errors.Join(ErrCommittedAgentRouteTransition, errors.New("independent readiness failure"))
				}))
			case "joined_cancellation":
				bus.SetCommittedAgentReadinessFinalizer(CommittedAgentReadinessFinalizerFunc(func(context.Context, events.Event, []events.DeliveryRoute) error {
					return errors.Join(ErrCommittedAgentRouteTransition, context.Canceled)
				}))
			case "cancellation":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "no_coordinator":
				bus.deliveryContinuations = nil
			case "selected_authority":
				var err error
				bus.deliveryAuthority, err = deliverylifecycle.NewSelectedExecutionAuthority(bus.sourceArtifactFact, uuid.NewString(), prepared.Event.RunID(), 1)
				if err != nil {
					t.Fatal(err)
				}
			case "ephemeral_recipient":
				prepared.plan.AddLiveRecipients(RoutePlanLiveRecipient{InternalID: "independent-ephemeral"})
			case "event_wide":
				bus.SetInterceptors(continuationEventPassthroughInterceptor{})
			case "target_failure":
				prepared.targetFailure = true
			case "queued":
				prepared.dispatchQueued = true
			case "internal_failure":
				prepared.plan.LiveRecipients[0].PersistAsDelivery = false
				bus.pendingInternalByID = nil
			case "activation_failure":
				committed.Activations = []CommittedFlowInstanceActivation{agentTransitionActivation(t, bus, prepared.Event.RunID())}
				bus.flowActivationFinalizer = pipeline.CommittedFlowInstanceActivationFinalizerFunc(func(context.Context, pipeline.CommittedFlowInstanceActivation) error {
					return errors.New("independent activation failure")
				})
			case "refused_transfer":
				bus.deliveryContinuations = refusingAgentHandoff{issue2564HandoffContinuation: coordinator}
			case "invalid_commit":
				committed.DeliveryHandoffs[0] = deliverylifecycle.DurableHandoffProof{}
			}
			result, err := bus.finalizeCommittedPublicationConsequences(ctx, prepared, committed, true)
			dispatchable := result.dispatchable()
			if err == nil || dispatchable || owner.settlements != 0 || len(coordinator.proofs) != 0 {
				t.Fatalf("ineligible transition swallowed: ready=%t err=%v settlements=%d transferred=%d", dispatchable, err, owner.settlements, len(coordinator.proofs))
			}
		})
	}
}

type refusingAgentHandoff struct{ *issue2564HandoffContinuation }

func (refusingAgentHandoff) AcceptCommitted([]deliverylifecycle.DurableHandoffProof) error {
	return errors.New("coordinator retired or generation differs")
}

func agentTransitionActivation(t *testing.T, bus *EventBus, runID string) CommittedFlowInstanceActivation {
	t.Helper()
	source, _ := topologyOperationFixture(t)
	identity := ConstructedFlowInstanceIdentityFixture(source, "workers", "alpha", runID)
	activation := CommittedFlowInstanceActivation{Acknowledged: true, ReadinessAttemptOrdinal: 1, Plan: pipeline.FlowInstanceActivationPlan{
		Identity: identity, Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: bus.sourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: "live"},
		OccurredAt: time.Now().UTC(), Instance: pipeline.WorkflowInstance{InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityType: "test_entity"},
	}}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation
}

func TestCommittedAgentTransitionConsumersPreserveHandoff(t *testing.T) {
	for _, consumer := range []string{"immediate", "inbound", "engine_outbox", "fan_out", "committed_continuation"} {
		t.Run(consumer, func(t *testing.T) {
			bus, prepared, committed, owner, coordinator := preparedAgentTransition(t)
			ctx := context.Background()
			var err error
			switch consumer {
			case "immediate":
				prepared, _, err = bus.applyCommittedPublication(ctx, prepared, committed)
				if err == nil {
					err = bus.DispatchPreparedPublish(ctx, prepared)
				}
			case "inbound":
				var plans []PreparedPublish
				plans, err = bus.ApplyInboundDeliveryCommit(ctx, InboundDeliveryPlan{prepared: []PreparedPublish{prepared}}, []CommittedPublication{committed})
				if err == nil {
					err = bus.DispatchPreparedPublish(ctx, plans[0])
				}
			default:
				prepared.admitted, err = events.AdmitForPublish(prepared.Event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
				ledger, err := events.NewConnectEvaluationLedger(nil)
				if err != nil {
					t.Fatal(err)
				}
				prepared.settlement, err = events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
				if err != nil {
					t.Fatal(err)
				}
				intent := engine.EmitIntent{Event: prepared.Event}
				plan := EnginePublicationPlan{prepared: prepared, intent: intent, admittedSource: prepared.admitted, command: PublicationCommand{Commit: prepared.CommitRequest()}}
				proof, err := NewCommittedEnginePublication(plan, committed)
				if err != nil {
					t.Fatal(err)
				}
				if err := bus.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{proof}); err != nil {
					t.Fatal(err)
				}
				switch consumer {
				case "engine_outbox":
					err = bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{intent})
				case "committed_continuation":
					err = (engineDispatcher{bus: bus}).DispatchCommittedPublication(ctx, proof)
				case "fan_out":
					operation, found, takeErr := bus.takeCommittedOutboxOperation(proof)
					if takeErr != nil || !found {
						t.Fatalf("lost typed fan-out operation: found=%t err=%v", found, takeErr)
					}
					err = (engineDispatcher{bus: bus}).dispatchFanOutOperation(ctx, operation, prepared.plan, &fanOutPublicationSettlement{}, false)
				}
			}
			if err != nil || owner.settlements != 1 || coordinator.dispatches != 0 {
				t.Fatalf("consumer lost owned deferred routes: error=%v settlements=%d direct=%d", err, owner.settlements, coordinator.dispatches)
			}
		})
	}
}
