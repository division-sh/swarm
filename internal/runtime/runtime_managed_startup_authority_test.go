package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type managedStartupAuthorityPersistence interface {
	runtimemanager.ManagerPersistence
	runtimeeffects.Store
	managedcapabilities.Persistence
}

type managedStartupAuthorityStoreStub struct {
	managedStartupAuthorityPersistence
}

func TestManagedProviderPreflightAuthorityCarriesLiveExecutionIdentity(t *testing.T) {
	startupAuthority := runtimestartupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(),
		ProcessOwnerID: "release-e2e-runtime-owner", ProcessBootID: uuid.NewString(),
		BundleHash:        runtimeTestBundleHash,
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID, RuntimeGeneration: 7,
		SourceSetRevision: "source-set-v7", StateVersion: 3,
		State: runtimestartupownership.GrantProbeSettled,
	}
	store := &managedStartupAuthorityStoreStub{}
	rt := &Runtime{ExecutionPosture: executionposture.Live, effectsStore: store, managedCapabilitiesStore: store}
	preflight, err := rt.managedProviderPreflightAuthority(startupAuthority)
	if err != nil {
		t.Fatalf("build managed provider preflight authority: %v", err)
	}
	if preflight.ExecutionKind != managedcapabilities.ExecutionNormalAgent {
		t.Fatalf("execution kind = %q, want %q", preflight.ExecutionKind, managedcapabilities.ExecutionNormalAgent)
	}
	if preflight.ExecutionAuthorityID != startupAuthority.GrantID {
		t.Fatalf("execution authority id = %q, want %q", preflight.ExecutionAuthorityID, startupAuthority.GrantID)
	}
	if preflight.StartupOwnerID != startupAuthority.ProcessOwnerID || preflight.StartupGeneration != startupAuthority.RuntimeGeneration {
		t.Fatalf("startup owner/generation = %q/%d, want %q/%d", preflight.StartupOwnerID, preflight.StartupGeneration, startupAuthority.ProcessOwnerID, startupAuthority.RuntimeGeneration)
	}

	probeID := uuid.NewString()
	const actorID = "release-e2e-agent"
	effectAuthority, err := preflight.EffectAuthority(probeID, actorID)
	if err != nil {
		t.Fatalf("build startup probe effect authority: %v", err)
	}
	if !effectAuthority.Valid() {
		t.Fatalf("startup probe effect authority is invalid: %#v", effectAuthority)
	}
	if effectAuthority.ExecutionMode != runtimeeffects.ExecutionModeLive {
		t.Fatalf("execution mode = %q, want %q", effectAuthority.ExecutionMode, runtimeeffects.ExecutionModeLive)
	}
	if effectAuthority.Kind != runtimeeffects.AuthorityStartupProbe ||
		effectAuthority.ID != probeID ||
		effectAuthority.ExecutionOwner != startupAuthority.ProcessOwnerID ||
		effectAuthority.FenceGeneration != startupAuthority.RuntimeGeneration {
		t.Fatalf("startup probe envelope = %#v", effectAuthority)
	}
	if got := effectAuthority.StartupProbe; got.ProbeID != probeID ||
		got.StartupAuthorityID != startupAuthority.GrantID ||
		got.StartupStateVersion != startupAuthority.StateVersion ||
		got.ActorID != actorID ||
		got.ExecutionKind != string(managedcapabilities.ExecutionNormalAgent) ||
		got.ExecutionAuthorityID != startupAuthority.GrantID {
		t.Fatalf("startup probe identity = %#v", got)
	}
}
