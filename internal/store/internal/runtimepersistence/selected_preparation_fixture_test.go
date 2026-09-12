package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
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
			SourceRunID: sourceRun, ForkEventID: eventID,
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
