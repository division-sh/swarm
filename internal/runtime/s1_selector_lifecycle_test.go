package runtime

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func s1SelectorAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	members := s1EffectiveTriggerMembers()
	for _, prefix := range []string{"", "sibling/"} {
		members[prefix+"agents.yaml"] = []byte("worker:\n  role: worker\n  intent: prompts/worker.md\n  model: regular\n  subscriptions: [platform.join_complete]\n")
		members[prefix+"prompts/worker.md"] = []byte("Handle the admitted work.\n")
	}
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())})
	if err != nil {
		t.Fatal(err)
	}
	return projection.Source()
}

func TestS1AgentDeclarationProofsPreserveRawSelectors(t *testing.T) {
	source := s1SelectorAgentSource(t)
	for _, flow := range []string{".", "sibling"} {
		declarations := semanticview.AgentDeclarationsForOwner(source, flow)
		if len(declarations) != 1 {
			t.Fatalf("exact %s has %d declarations", flow, len(declarations))
		}
		declaration := declarations[0]
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			t.Fatal(err)
		}
		actor := runtimeactors.AgentConfig{ID: plan.AgentID, FlowID: flow}
		if owner, found := semanticview.AgentDeclarationOwner(source, flow, declaration.LocalID); !found || owner != declaration.OwnerURI {
			t.Fatalf("exact %s lost declaration ownership", flow)
		}
		if _, _, found := semanticview.ResolveAgentRegistryEntry(source, actor); !found {
			t.Fatalf("exact %s lost registry declaration", flow)
		}
		if proof := semanticview.ResolveAgentMemoryProof(source, semanticview.AgentMemoryLocator{AgentID: actor.ID, FlowPath: flow}); proof.OwningFlowID != flow {
			t.Fatalf("exact %s lost memory proof: %+v", flow, proof)
		}
		for _, raw := range []string{" " + flow + " ", "\t" + flow + "\n", "/" + flow + "/"} {
			actor.FlowID = raw
			if _, _, found := semanticview.ResolveAgentRegistryEntry(source, actor); found {
				t.Errorf("registry repaired %q into an exact declaration", raw)
			}
			if _, found := semanticview.AgentDeclarationOwner(source, raw, declaration.LocalID); found {
				t.Errorf("owner query repaired %q", raw)
			}
			if len(semanticview.AgentDeclarationsForOwner(source, raw)) != 0 {
				t.Errorf("declaration census repaired %q", raw)
			}
			if proof := semanticview.ResolveAgentMemoryProof(source, semanticview.AgentMemoryLocator{AgentID: actor.ID, FlowPath: raw}); proof.OwningFlowID != "" {
				t.Errorf("memory proof repaired %q into %q", raw, proof.OwningFlowID)
			}
		}
	}
	if _, found := semanticview.ResolveAgentDeclaration(source, runtimeactors.AgentConfig{ID: "worker", FlowID: " "}); found {
		t.Fatal("whitespace became a target-free agent query")
	}
}

func TestS1AgentExecutionScopeRefusesPaddedActorFlow(t *testing.T) {
	source := s1SelectorAgentSource(t)
	declarations := semanticview.AgentDeclarationsForOwner(source, ".")
	if len(declarations) != 1 {
		t.Fatal("fixture lost its exact root declaration")
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
	if err != nil {
		t.Fatal(err)
	}
	actor := runtimeactors.AgentConfig{ID: plan.AgentID, FlowID: ".", Identity: agentidentitytest.RootDeclared(t, plan.AgentID, plan.OwnerURI)}
	if _, err := semanticview.ResolveAgentExecutionSemanticScope(source, actor, nil); err != nil {
		t.Fatalf("canonical root lost semantic scope: %v", err)
	}
	for _, raw := range []string{" . ", "\t.\n", "", " ", "sibling"} {
		actor.FlowID = raw
		if _, err := semanticview.ResolveAgentExecutionSemanticScope(source, actor, nil); err == nil {
			t.Errorf("actor flow %q acquired root execution scope", raw)
		}
	}
}

func TestS1NodeAndInputProducerProofsPreserveRawSelectors(t *testing.T) {
	source := s1TriggerConsumerSource(t)
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	_, resolver := pinrouting.CompileConnectGraphWithInputProducerResolver(source)
	const event = "inbound.team-bot.text_message"
	for _, flow := range []string{".", "sibling"} {
		if _, err := semanticview.ResolveExecutableNodeDeclaration(source, flow, "listener"); err != nil {
			t.Fatalf("exact %s lost node declaration: %v", flow, err)
		}
		positive := semanticview.ResolveNonConnectFlowInputProducer(source, flow, event)
		if positive.FlowID != flow {
			t.Fatalf("exact %s input query changed coordinate", flow)
		}
		for _, raw := range []string{" " + flow + " ", "\t" + flow + "\n"} {
			if node, err := semanticview.ResolveExecutableNodeDeclaration(source, raw, "listener"); err == nil || node.Valid() {
				t.Errorf("node lookup repaired %q", raw)
			}
			result := census.ResolveDeclaredInputEndpoint(raw, event)
			if result.FlowID != raw || result.Matched() {
				t.Errorf("refused input result rewrote %q into %+v", raw, result)
			}
			proof := semanticview.ResolveNonConnectFlowInputProducer(source, raw, event)
			if proof.FlowID != raw {
				t.Errorf("input producer proof repaired %q into %q", raw, proof.FlowID)
			}
			if proof.HasEvidence() {
				t.Errorf("invalid input producer scope %q acquired evidence: %+v", raw, proof)
			}
			options := runtimecontracts.FlowInputProducerResolutionOptions{AllowNonInputEvent: true}
			if !semanticview.ResolveNonConnectFlowInputProducerWithOptions(source, flow, "platform.runtime_log", options).HasEvidence() {
				t.Fatal("canonical platform proof was lost")
			}
			for _, proof := range []runtimecontracts.FlowInputProducerResolution{
				semanticview.ResolveNonConnectFlowInputProducerWithOptions(source, raw, "platform.runtime_log", options),
				census.ResolveNonConnectFlowInputProducer(raw, "platform.runtime_log", options),
				pinrouting.ResolveFlowInputProducerWithOptions(source, raw, "platform.runtime_log", options),
				resolver.ResolveWithOptions(raw, "platform.runtime_log", options),
			} {
				if proof.FlowID != raw || proof.HasEvidence() {
					t.Errorf("platform/census producer proof repaired %q: %+v", raw, proof)
				}
			}
			if proof := resolver.ResolveWithOptions(flow, "platform.runtime_log", options); proof.FlowID != flow || !proof.HasEvidence() {
				t.Fatalf("refused query poisoned canonical producer cache: %+v", proof)
			}
		}
	}
	root, err := semanticview.AdmitRootExecutionCoordinate(source, "current-run")
	if err != nil || !root.Matches(".", "current-run") {
		t.Fatalf("canonical root coordinate failed: %v", err)
	}
	for _, raw := range []string{" . ", "\t.\n", "", " "} {
		if root.Matches(raw, "current-run") {
			t.Errorf("root coordinate repaired %q", raw)
		}
	}
}
