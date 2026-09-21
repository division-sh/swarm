package engine

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"testing"
	"time"
)

func TestDeclarativeEmitPreservesScopedProducerSourceRoute(t *testing.T) {
	cases := []struct {
		name            string
		eventType       string
		stateFlowPath   string
		inboundFlowPath string
		producerRoute   events.RouteIdentity
		targetRoute     events.RouteIdentity
		wantFlowPath    string
	}{
		{
			name:          "success uses state flow path",
			eventType:     "work.completed",
			stateFlowPath: "worker/inst-1",
			wantFlowPath:  "worker/inst-1",
		},
		{
			name:          "failure uses state flow path",
			eventType:     "work.rejected",
			stateFlowPath: "worker/inst-1",
			wantFlowPath:  "worker/inst-1",
		},
		{
			name:            "success uses admitted producer route over stale inbound flow instance",
			eventType:       "work.completed",
			inboundFlowPath: "upstream/component-a",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker",
				EntityID:     "ent-worker",
			},
			wantFlowPath: "worker",
		},
		{
			name:            "failure ignores delivery target in favor of exact producer route",
			eventType:       "work.rejected",
			inboundFlowPath: "upstream/component-a",
			producerRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker",
				EntityID:     "ent-worker",
			},
			targetRoute: events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: "worker",
				EntityID:     "target-ent",
			},
			wantFlowPath: "worker",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entityID := "ent-worker"
			parentEnvelope := events.EventEnvelope{EntityID: "upstream-ent", FlowInstance: tc.inboundFlowPath}
			if tc.stateFlowPath != "" || !tc.producerRoute.Empty() || !tc.targetRoute.Empty() {
				parentEnvelope = events.EnvelopeForSourceRoute(parentEnvelope, events.RouteIdentity{
					FlowID:       "upstream",
					FlowInstance: "upstream/inst-0",
					EntityID:     "upstream-ent",
				})

			}
			if !tc.targetRoute.Empty() {
				parentEnvelope = events.EnvelopeForTargetRoute(parentEnvelope, tc.targetRoute)
			} else if tc.inboundFlowPath != "" {
				parentEnvelope = events.EnvelopeForFlowInstance(parentEnvelope, tc.inboundFlowPath)
			}
			parent := eventtest.RunCreatingRootIngress(
				"evt-parent",
				"work.requested",
				"workflow-runtime",
				"",
				json.RawMessage(`{"request_id":"req-1"}`),
				4,
				"run-1",
				"",
				parentEnvelope,
				time.Unix(1_700_000_000, 0).UTC())
			stateMetadata := map[string]any{}
			if tc.stateFlowPath != "" {
				stateMetadata["flow_path"] = tc.stateFlowPath
			}
			execCtx := ExecutionContext{
				Request: ExecutionRequest{
					ExecutionFlowID: identity.NormalizeFlowID("worker"),
					EntityID:        identity.NormalizeEntityID(entityID),
					Node:            testFlowExecutableNode(t, "worker", "worker-node"),
					Event:           parent,
					ChainDepth:      4,
					State: StateSnapshot{
						EntityID:     identity.NormalizeEntityID(entityID),
						StateCarrier: NewStateCarrier(stateMetadata, nil, nil),
					},
				},
			}

			mode := "static"
			if tc.wantFlowPath != "worker" {
				mode = "template"
			}
			producerRoute := tc.producerRoute.Normalized()
			if producerRoute.Empty() {
				producerRoute = events.RouteIdentity{FlowID: "worker", FlowInstance: tc.wantFlowPath, EntityID: entityID}
			}
			execCtx.Request.ProducerSource = publicationRoutingSource(t, mode, producerRoute)
			executor, err := NewExecutor(RuntimeDependencies{
				Source:    sourceWithFixtureStages(stubSource(), "worker", "ready", "ready"),
				StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{},
				Locker: stubLocker{}, Dispatcher: stubDispatcher{},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			execCtx.Request.Handler.Emit = runtimecontracts.EmitSpec{Event: tc.eventType}
			result, err := executor.ExecuteSemanticFixture(context.Background(), execCtx.Request)
			if err != nil || len(result.EmitIntents) != 1 {
				t.Fatalf("declarative emit: intents=%#v err=%v", result.EmitIntents, err)
			}
			intent := result.EmitIntents[0]
			emitted := intent.Event
			wantEventType := tc.wantFlowPath + "/" + tc.eventType
			if got := string(emitted.Type()); got != wantEventType {
				t.Fatalf("event type = %q, want %q", got, wantEventType)
			}
			if got := emitted.EntityID(); got != entityID {
				t.Fatalf("entity_id = %q, want %q", got, entityID)
			}
			if got := emitted.FlowInstance(); got != tc.wantFlowPath {
				t.Fatalf("flow_instance = %q, want %q", got, tc.wantFlowPath)
			}
			wantSource := events.RouteIdentity{
				FlowID:       "worker",
				FlowInstance: tc.wantFlowPath,
				EntityID:     entityID,
			}.Normalized()
			if got := emitted.SourceRoute(); got != wantSource {
				t.Fatalf("source route = %#v, want %#v", got, wantSource)
			}
			if got := emitted.TargetRoute(); !got.Empty() {
				t.Fatalf("target route = %#v, want empty result-event target", got)
			}
			if got := emitted.ParentEventID(); got != parent.ID() {
				t.Fatalf("parent_event_id = %q, want %q", got, parent.ID())
			}
			if got := emitted.RunID(); got != parent.RunID() {
				t.Fatalf("run_id = %q, want %q", got, parent.RunID())
			}
			if got := emitted.ChainDepth(); got != 5 {
				t.Fatalf("chain_depth = %d, want 5", got)
			}
			if got := intent.ParentEventID; got != parent.ID() {
				t.Fatalf("intent parent_event_id = %q, want %q", got, parent.ID())
			}
		})
	}
}

func publicationRoutingSource(t testing.TB, mode string, route events.RouteIdentity) events.RoutingSource {
	t.Helper()
	var (
		source events.RoutingSource
		err    error
	)
	if mode == "template" {
		source, err = events.NewConcreteTemplateInstanceRoutingSource(route)
	} else {
		source, err = events.NewStaticFlowRoutingSource(route)
	}
	if err != nil {
		t.Fatalf("construct %s publication routing source: %v", mode, err)
	}
	return source
}
