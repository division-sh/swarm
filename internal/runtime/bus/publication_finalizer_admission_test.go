package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestCommittedFinalizersScopeIngressAdmissionWithoutLosingRuntimeAuthority(t *testing.T) {
	for _, boundary := range []string{"flow_activation", "agent_readiness"} {
		t.Run(boundary, func(t *testing.T) {
			runID, eventID := uuid.NewString(), uuid.NewString()
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: testAgentRouteIdentityForRun(t, runID, "agent-a", "")}
			store := newExactHandoffProofStore(t, false)
			store.seed(t, eventID, runID, route)
			claim := store.claim(t, eventID, runID, route)
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
			defer cancel()
			ctx = deliverylifecycle.WithClaim(ctx, claim)
			ctx = correlation.WithRunID(ctx, runID)
			ctx = correlation.WithSourceArtifactFact(ctx, authorActivityTestSourceArtifactFact)
			ctx = WithCurrentRuntimeEpoch(ctx)
			public := publicInputAdmission{endpointID: "input", flowID: "child", pinName: "thing.created", eventType: "thing.created"}
			api := apiEventPublicationAdmission{kind: apiEventPublicationEndpointOrdinaryFlow, flowID: "child", flowPath: "child", eventType: "thing.created"}
			ctx = withPublicInputAdmission(withAPIEventPublicationAdmission(ctx, api), public)
			root := eventtest.RunCreatingRootIngress(eventID, "thing.created", "operator-api", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := public.validateEvent(root); err != nil {
				t.Fatal(err)
			}
			if err := api.validateEvent(root); err != nil {
				t.Fatal(err)
			}
			var followup context.Context
			eb := &EventBus{
				flowActivationFinalizer: pipeline.CommittedFlowInstanceActivationFinalizerFunc(func(next context.Context, _ pipeline.CommittedFlowInstanceActivation) error {
					followup = next
					return nil
				}),
				agentReadinessFinalizer: CommittedAgentReadinessFinalizerFunc(func(next context.Context, _ events.Event, _ []events.DeliveryRoute) error {
					followup = next
					return nil
				}),
			}
			var err error
			if boundary == "flow_activation" {
				err = eb.finalizeCommittedFlowInstanceActivations(ctx, []CommittedFlowInstanceActivation{{}})
			} else {
				err = eb.finalizeCommittedAgentReadiness(ctx, root, []events.DeliveryRoute{route})
			}
			if err != nil || followup == nil {
				t.Fatalf("finalizer handoff missing: %v", err)
			}
			if _, ok := publicInputAdmissionFromContext(followup); ok {
				t.Fatal("followup inherited public-input grant")
			}
			if _, ok := apiEventPublicationAdmissionFromContext(followup); ok {
				t.Fatal("followup inherited API publication grant")
			}
			if got, ok := deliverylifecycle.ClaimFromContext(followup); !ok || !got.Same(claim) {
				t.Fatal("followup lost exact delivery claim")
			}
			if got, ok := correlation.SourceArtifactFactFromContext(followup); !ok || !got.Matches(authorActivityTestSourceArtifactFact) {
				t.Fatal("followup lost exact source authority")
			}
			if correlation.RunIDFromContext(followup) != runID {
				t.Fatal("followup lost run authority")
			}
			beforeEpoch, _ := RuntimeEpochFromContext(ctx)
			if after, ok := RuntimeEpochFromContext(followup); !ok || beforeEpoch != after {
				t.Fatal("followup lost runtime epoch")
			}
			before, _ := ctx.Deadline()
			if after, ok := followup.Deadline(); !ok || !after.Equal(before) || followup.Done() != ctx.Done() {
				t.Fatal("followup changed deadline or cancellation channel")
			}
			cancel()
			if followup.Err() != context.Canceled {
				t.Fatal("followup escaped cancellation")
			}
			child := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "thing.created", eventtest.Producer(events.EventProducerNode, "child"), "", []byte(`{}`), 0,
				events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: executionmode.Live}, events.EventEnvelope{}, eventtest.StaticFlowRoutingSource("child", "child", eventtest.UUID("finalizer-child")), time.Now().UTC())
			retainedPublic, publicOK := publicInputAdmissionFromContext(ctx)
			retainedAPI, apiOK := apiEventPublicationAdmissionFromContext(ctx)
			if !publicOK || !apiOK || retainedPublic.validateEvent(child) == nil || retainedAPI.validateEvent(child) == nil {
				t.Fatal("original ingress grants were weakened to authorize descendants")
			}
		})
	}
}
