package authority

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestLifecycleProducerDoesNotGrantActorAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		root func(*testing.T) string
	}{
		{"colliding_node_and_role", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticActorCollision)
		}},
		{"loop", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			provider := NewSourceProvider(source)
			if len(provider.ProducerRoles()) != 0 {
				t.Fatalf("lifecycle acquired actor role: %#v", provider.ProducerRoles())
			}
			for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
				if endpoint.Kind != semanticview.EventEndpointGateOutcome && endpoint.Kind != semanticview.EventEndpointLoopEscape {
					continue
				}
				for _, label := range []string{endpoint.ID, endpoint.Site, endpoint.StageID, endpoint.DecisionID, endpoint.Verdict, endpoint.LoopID, "review_decision", "review-decision", "controller", "collector"} {
					if events := provider.ProducerEventsForRole(label); len(events) != 0 {
						t.Fatalf("label %q acquired emits: %q", label, events)
					}
				}
			}
		})
	}
}
