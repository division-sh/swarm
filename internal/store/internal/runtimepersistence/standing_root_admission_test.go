package runtimepersistence

import (
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type rootedStandingStore interface {
	rootedRunStore
	runtimepipeline.StandingServicePersistence
}

func TestS1StandingSameContentRootRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedStandingStore)
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			candidate := runtimepipeline.StandingServiceCandidate{
				BindingEnabled: true, ServiceID: runtimeflowidentity.StandingServiceID("service"), FlowPath: "service",
				Source: sourceartifactfixture.FactFor(sourceA.Artifact()), RootAdmission: sourceA.Admission(),
			}
			created, err := selected.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			candidate.RootAdmission = sourceB.Admission()
			revised, err := selected.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if revised.Transition != "revised" || revised.RunID != created.RunID || revised.Generation != created.Generation {
				t.Errorf("same-content root revision was ignored or created another generation: before=%+v after=%+v", created, revised)
			}
			requireRunRoot(t, ctx, selected, created.RunID, sourceB)
		})
	}
}
