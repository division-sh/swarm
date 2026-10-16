package runtime

import (
	"fmt"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func s1TriggerConsumerSource(t *testing.T) semanticview.Source {
	t.Helper()
	members := s1EffectiveTriggerMembers()
	const event = "inbound.team-bot.text_message"
	for _, prefix := range []string{"", "sibling/"} {
		members[prefix+"schema.yaml"] = []byte(fmt.Sprintf("stages: []\npins:\n  inputs: [%s]\n  outputs: [%s]\n", event, event))
		members[prefix+"nodes.yaml"] = []byte(fmt.Sprintf("listener:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s: {guard: {check: true}}\n", event, event))
	}
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())})
	if err != nil {
		t.Fatal(err)
	}
	return projection.Source()
}

func s1InvalidTriggerConsumerFlows() []string {
	return []string{"", " ", " . ", " sibling ", "\tsibling\n", "./", "/sibling", "sibling/", "child/../sibling", "foreign", "team-bot"}
}

func TestS1TriggerEventProofPreservesRawFlowCoordinates(t *testing.T) {
	source := s1TriggerConsumerSource(t)
	const event = "inbound.team-bot.text_message"
	for _, flow := range []string{".", "sibling"} {
		proof := semanticview.ResolveFlowEventProof(source, flow, event)
		if !proof.HasSchema || proof.FlowID != flow || !proof.CrossesDeclaredOutputBoundary(source) {
			t.Fatalf("exact %s lost its scoped proof/output evidence: %+v", flow, proof)
		}
		node := identitytest.FlowNode(t, flow, "listener")
		if proof := semanticview.ResolveExecutableNodeEventProof(source, node, event); !proof.HasSchema {
			t.Fatalf("typed %s node lost its schema proof", flow)
		}
	}
	for _, flow := range append(s1InvalidTriggerConsumerFlows(), "child", "child/deep") {
		t.Run(fmt.Sprintf("%q", flow), func(t *testing.T) {
			proof := semanticview.ResolveFlowEventProof(source, flow, event)
			if proof.HasSchema || proof.FlowID != flow {
				t.Errorf("raw flow acquired or changed schema proof: %+v", proof)
			}
			if proof.CrossesDeclaredOutputBoundary(source) {
				t.Error("refused proof acquired an output boundary")
			}
			if _, found := semanticview.FlowScopeByID(source, flow); found && flow != "child" && flow != "child/deep" {
				t.Error("scope wrapper normalized a refused selector")
			}
		})
	}
	if !semanticview.ResolveEventSchema(source, "", "inbound.team_bot.text_message").HasCompiled {
		t.Fatal("target-free imported schema inspection was lost")
	}
}

func TestS1TriggerSubscriptionConsumersRequireExactSourceScope(t *testing.T) {
	source := s1TriggerConsumerSource(t)
	const event = "inbound.team-bot.text_message"
	for _, kind := range []semanticview.AuthoredSubscriptionConsumerKind{semanticview.AuthoredSubscriptionConsumerNode, semanticview.AuthoredSubscriptionConsumerAgent, semanticview.AuthoredSubscriptionConsumerTimer} {
		t.Run(string(kind), func(t *testing.T) {
			for _, flow := range append([]string{".", "sibling", "child", "child/deep"}, s1InvalidTriggerConsumerFlows()...) {
				want := flow == "." || flow == "sibling"
				admission := semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{ConsumerKind: kind, ConsumerID: "listener", FlowID: flow, Authored: event})
				if admission.Admitted() != want {
					t.Errorf("%q admission=%t want=%t: %s", flow, admission.Admitted(), want, admission.Message())
				}
				if !want && len(admission.RoutePatterns()) != 0 {
					t.Errorf("%q refusal retained executable route patterns", flow)
				}
			}
		})
	}
	for _, flow := range append([]string{".", "sibling", "child", "child/deep"}, s1InvalidTriggerConsumerFlows()...) {
		want := flow == "." || flow == "sibling"
		if admitted := semanticview.ClassifyTimerSubscription(source, "timer-listener", flow, event); admitted.Admitted() != want {
			t.Errorf("timer wrapper flow %q admission=%t want=%t", flow, admitted.Admitted(), want)
		}
		agent, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{AgentID: "listener", FlowID: flow, Subscriptions: []string{event}})
		if (err == nil) != want || agent.ValidForAgent("listener") != want {
			t.Errorf("agent wrapper flow %q accepted=%t want=%t: %v", flow, agent.ValidForAgent("listener"), want, err)
		}
	}
	for _, flow := range s1InvalidTriggerConsumerFlows() {
		for _, event := range []string{event, "inbound.team-bot.*"} {
			admission := semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{
				ConsumerKind: semanticview.AuthoredSubscriptionConsumerNode, ConsumerID: "listener", FlowID: flow, Authored: event,
				LocalEvents: map[string]struct{}{event: {}}, InputEvents: []string{event},
			})
			if admission.Admitted() {
				t.Errorf("caller maps/pattern bypassed raw source scope %q", flow)
			}
		}
		if agent, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{AgentID: "listener", FlowID: flow}); err == nil || agent.ValidForAgent("listener") {
			t.Errorf("empty-subscription agent admission accepted noncanonical flow %q", flow)
		}
	}
	for _, flow := range []string{".", "sibling"} {
		node := identitytest.FlowNode(t, flow, "listener")
		if !semanticview.ClassifyExecutableNodeSubscription(source, node, event).Admitted() {
			t.Fatalf("exact %s node wrapper refused its handler", flow)
		}
	}
}

func TestS1TriggerProofConsumersCannotNormalizeFlowSelectors(t *testing.T) {
	source := s1TriggerConsumerSource(t)
	const event = "inbound.team-bot.text_message"
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, flow := range []string{".", "sibling"} {
		if len(census.MatchingConsumers(flow, event)) == 0 || !pinrouting.PinDeclaredOutput(source, flow, event) || !census.ResolveDeclaredInputEndpoint(flow, event).Matched() {
			t.Fatalf("exact %s lost declared consumer/output", flow)
		}
		producer := semanticview.AuthoredEventEndpoint{FlowID: flow, Event: semanticview.ResolveFlowEventProof(source, flow, event)}
		if len(census.ResolveTypedPubSubConsumerMatches(producer)) == 0 {
			t.Fatalf("exact %s lost its same-flow relation", flow)
		}
		canonical := semanticview.ResolveFlowEventProof(source, flow, event).Canonical
		if _, err := runtimebus.NewOrdinaryFlowAPIEventPublicationEndpoint(source, flow, canonical); err != nil {
			t.Fatalf("exact %s endpoint lost its schema: %v", flow, err)
		}
	}
	for _, flow := range s1InvalidTriggerConsumerFlows() {
		if matches := census.MatchingConsumers(flow, event); len(matches) != 0 {
			t.Errorf("endpoint census normalized %q into %d consumers", flow, len(matches))
		}
		if pinrouting.PinDeclaredOutput(source, flow, event) {
			t.Errorf("output declaration normalized %q", flow)
		}
		if census.ResolveDeclaredInputEndpoint(flow, event).Matched() {
			t.Errorf("declared input lookup normalized %q", flow)
		}
		producer := semanticview.AuthoredEventEndpoint{FlowID: flow, Event: semanticview.ResolveFlowEventProof(source, ".", event)}
		if len(census.ResolveTypedPubSubConsumerMatches(producer)) != 0 {
			t.Errorf("typed relation normalized producer flow %q", flow)
		}
		if _, err := runtimebus.NewOrdinaryFlowAPIEventPublicationEndpoint(source, flow, event); err == nil {
			t.Errorf("public endpoint normalized %q", flow)
		}
	}
}
