package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func selectedPreparationProcessForTest(t testing.TB, store any) startupownership.ProcessCapability {
	t.Helper()
	owner, ok := store.(startupownership.Store)
	if !ok {
		t.Fatal("selected preparation fixture requires process ownership store")
	}
	capability, err := owner.AcquireProcessCapability(context.Background(), testStartupAcquireRequest("selected-preparation-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return capability
}

func selectedMaterializationProcessForTest(t testing.TB, work *storeTestWorkFixture, selected any) startupownership.ProcessCapability {
	t.Helper()
	work.capabilitiesMu.Lock()
	defer work.capabilitiesMu.Unlock()
	if capability := work.capabilities[selected]; capability != nil {
		if err := capability.ProveCurrent(context.Background()); err != nil {
			t.Fatal(err)
		}
		return capability
	}
	capability := selectedPreparationProcessForTest(t, selected)
	if work.capabilities == nil {
		work.capabilities = make(map[any]startupownership.ProcessCapability)
	}
	work.capabilities[selected] = capability
	return capability
}

// A predecessor already admitted by the native actor fixture retains its own
// process through selected preparation; acquiring another would self-deadlock.
func bindPreparedAgentProcessToSelectedMaterialization(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) {
	t.Helper()
	owner := agentfixture.Lifecycle(t, selected).(interface {
		ProcessExecutionBinding() (manager.ProcessExecutionBinding, error)
	})
	binding, err := owner.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	admission, found := managedexecution.FromContext(ctx)
	if !found || !admission.AuthorizesNormal() || admission.ExecutionAuthorityID != binding.GenerationGrantID || admission.Generation != binding.RuntimeGeneration || admission.BundleHash != binding.BundleHash {
		t.Fatal("selected preparation requires its exact admitted predecessor generation")
	}
	capability, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	if err := capability.ProveCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	work.capabilitiesMu.Lock()
	defer work.capabilitiesMu.Unlock()
	if current := work.capabilities[selected]; current != nil && current != capability {
		t.Fatal("selected preparation cannot replace another retained process capability")
	}
	if work.capabilities == nil {
		work.capabilities = make(map[any]startupownership.ProcessCapability)
	}
	work.capabilities[selected] = capability
}

func selectedPreparationForTest(t testing.TB, capability startupownership.ProcessCapability, sourceRun, forkRun, eventID string, declarations agenttopology.SelectedDeclarationPlan) runfork.SelectedForkPreparationBinding {
	t.Helper()
	evidence, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	binding := runfork.SelectedForkPreparationBinding{
		ForkRunID: forkRun,
		SelectedForkPreparation: runfork.SelectedForkPreparation{
			DeclarationPlanFingerprint: declarations.Revision,
			PreparationID:              uuid.NewString(), ProcessGeneration: evidence.AuthorityGeneration,
			SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: eventID}, ForkEventID: eventID,
			Coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: evidence.AuthorityID, ProcessOwnerID: evidence.OwnerID, ProcessBootID: evidence.BootID,
				BundleHash: declarations.BundleHash, SourceFingerprint: strings.Repeat("a", 64), AdmittedPlanFingerprint: strings.Repeat("b", 64),
				ConfigurationFingerprint: strings.Repeat("c", 64), CatalogFingerprint: strings.Repeat("d", 64),
			}, Actors: []runfork.SelectedForkPreparedActor{},
		},
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	return binding
}
