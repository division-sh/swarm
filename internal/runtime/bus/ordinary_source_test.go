package bus

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestOrdinaryProviderSourceUsesExactSelectedOwner(t *testing.T) {
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, map[string]string{
		"schema.yaml":       "name: root\n",
		"alpha/schema.yaml": "name: alpha\nmode: singleton\n",
		"beta/schema.yaml":  "name: beta\nmode: singleton\n",
	}))
	runID, entityID := eventtest.UUID("provider-run"), eventtest.UUID("provider-entity")
	for _, flow := range []string{semanticview.RootExecutionFlowID(source), "alpha"} {
		t.Run(flow, func(t *testing.T) {
			root := flow == semanticview.RootExecutionFlowID(source)
			instance := "alpha"
			wantKeys := []string{"alpha/inbound.telegram.text_message"}
			localAgent := agentidentitytest.DeclaredForRun(t, runID, "local", "alpha", "alpha", "alpha", "alpha")
			if root {
				instance = runID
				wantKeys = []string{"inbound.telegram.text_message", "./inbound.telegram.text_message"}
				localAgent = agentidentitytest.RootDeclaredForRun(t, runID, "local", ".")
			}
			routing, err := events.NewExternalIngressRoutingSource(flow, entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
			if err != nil {
				t.Fatal(err)
			}
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID("event"), "inbound.telegram.text_message", "gateway", "", []byte("{}"), 0, runID, events.EventEnvelope{}, routing, time.Now())
			projection := selectedRunTargetOwnerProjection{source: source, descriptors: []ActiveTargetDescriptor{
				{FlowInstance: instance, EntityID: entityID},
				{FlowInstance: "beta", EntityID: entityID},
			}}
			local, err := projection.ordinarySource(evt)
			if err != nil {
				t.Fatal(err)
			}
			if local.route.FlowInstance != instance || local.route.FlowID != flow || local.route.EntityID != entityID {
				t.Fatalf("source=%#v", local)
			}
			if evt.RoutingSource() != routing || evt.FlowInstance() != "" || evt.HasTargetRoute() {
				t.Fatalf("source projection rewrote the gateway carrier: %#v", evt)
			}
			if got := local.eventKeys(evt); !reflect.DeepEqual(got, wantKeys) {
				t.Fatalf("keys=%v want=%v", got, wantKeys)
			}
			foreign := agentidentitytest.DeclaredForRun(t, runID, "foreign", "beta", "beta", "beta", "beta")
			localPlan, err := localAgent.Plan()
			if err != nil {
				t.Fatal(err)
			}
			foreignPlan, err := foreign.Plan()
			if err != nil {
				t.Fatal(err)
			}
			if !local.includesSubscriber(Subscriber{Recipient: events.MustAgentDeliveryRecipient(localAgent.AgentID()), AgentPlan: localPlan}) || local.includesSubscriber(Subscriber{Recipient: events.MustAgentDeliveryRecipient(foreign.AgentID()), AgentPlan: foreignPlan}) {
				t.Fatal("typed agent plans escaped exact source scope")
			}
			if !local.includesCandidate(deliveryRecipientCandidate{ID: localAgent.AgentID(), AgentIdentity: localAgent, PersistAsDelivery: true}) || local.includesCandidate(deliveryRecipientCandidate{ID: foreign.AgentID(), AgentIdentity: foreign, PersistAsDelivery: true}) {
				t.Fatal("direct agent subscriptions escaped exact source scope")
			}

			runtimeLocal := agentidentitytest.RuntimeForRun(t, runID, "runtime-local", "alpha", "alpha", "alpha", "alpha")
			if root {
				runtimeLocal = agentidentitytest.RootRuntimeForRun(t, runID, "runtime-local", ".")
			}
			runtimeForeign := agentidentitytest.RuntimeForRun(t, runID, "runtime-foreign", "beta", "beta", "beta", "beta")
			if !local.includesCandidate(deliveryRecipientCandidate{ID: runtimeLocal.AgentID(), AgentIdentity: runtimeLocal, PersistAsDelivery: true}) || local.includesCandidate(deliveryRecipientCandidate{ID: runtimeForeign.AgentID(), AgentIdentity: runtimeForeign, PersistAsDelivery: true}) {
				t.Fatal("runtime-created subscriptions did not preserve exact source ownership")
			}

			candidate := deliveryRecipientCandidate{ID: localAgent.AgentID(), AgentIdentity: localAgent, PersistAsDelivery: true}
			for _, descriptorEntity := range []string{entityID, eventtest.UUID("wrong-agent-entity")} {
				manifest, err := filterDeliveryRecipientCandidates(source, evt, []deliveryRecipientCandidate{candidate},
					map[agentidentity.Identity]ActiveAgentDescriptor{localAgent: {Identity: localAgent, EntityID: descriptorEntity}},
					projection.descriptors, projection, local)
				if err != nil {
					t.Fatal(err)
				}
				if descriptorEntity != entityID {
					if len(manifest.DeliveryRoutes) != 0 {
						t.Fatalf("wrong-entity agent acquired provider delivery: %#v", manifest.DeliveryRoutes)
					}
					continue
				}
				if len(manifest.DeliveryRoutes) != 1 || manifest.DeliveryRoutes[0].Target.Route().FlowInstance != instance || manifest.DeliveryRoutes[0].Target.Route().EntityID != entityID {
					t.Fatalf("target-free provider agent lost its exact execution owner: %#v", manifest.DeliveryRoutes)
				}
			}
			if evt.RoutingSource() != routing || evt.HasTargetRoute() {
				t.Fatal("agent filtering rewrote the admitted source or invented an event target")
			}
			projection.descriptors = []ActiveTargetDescriptor{{FlowInstance: "beta", EntityID: entityID}}
			if _, err := projection.ordinarySource(evt); err == nil {
				t.Fatal("foreign same-entity descriptor supplied the source instance")
			}
			projection.descriptors = []ActiveTargetDescriptor{{FlowInstance: instance, EntityID: eventtest.UUID("other-entity")}}
			if _, err := projection.ordinarySource(evt); err == nil {
				t.Fatal("wrong entity supplied the source instance")
			}
			projection.descriptors = []ActiveTargetDescriptor{{FlowInstance: instance, EntityID: entityID, Materializing: true}}
			if _, err := projection.ordinarySource(evt); err == nil {
				t.Fatal("uncommitted activation supplied provider source authority")
			}
		})
	}
}
