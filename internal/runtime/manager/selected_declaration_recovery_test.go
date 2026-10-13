package manager

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

func selectedDeclarationRecoveryFixture(t *testing.T) (AgentLifecycleState, PersistedAgent) {
	t.Helper()
	name, err := agentidentity.DeclaredName("selected-root", "root")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(uuid.NewString(), name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	desired := PersistedAgent{Config: actors.AgentConfig{ID: identity.AgentID(), Identity: identity}}
	revision, err := lifecycleConfigRevision(desired)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := identity.Plan()
	declarations, err := agenttopology.NewSelectedDeclarationPlan(managerTestTopologyBundleHash, []agenttopology.DesiredAgent{{
		Identity: plan, Source: agenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash}, ConfigRevision: revision,
	}})
	if err != nil {
		t.Fatal(err)
	}
	desired.Topology, err = agenttopology.SelectedDeclarationAdmission(identity.RunID, declarations)
	if err != nil {
		t.Fatal(err)
	}
	previous := AgentLifecycleState{Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 7, Generation: 3,
		Phase: AgentLifecycleRunning, ConfigRevision: revision, RunMode: AgentRunModeAuthoritativeDeliveryOnly, Topology: desired.Topology,
		ProcessBinding: startupProcessBinding(managerTestTopologyBundleHash, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), 1)}
	return previous, desired
}

func TestSelectedDeclarationRecoveryRetainsNativeMetadataForAuthoritativeStart(t *testing.T) {
	previous, desired := selectedDeclarationRecoveryFixture(t)
	rebound, err := selectedDeclarationRecoveryRecord(previous.Identity, previous, desired)
	if err != nil || rebound.LifecycleEpoch != previous.RuntimeEpoch || rebound.LifecycleGeneration != previous.Generation ||
		rebound.LifecyclePhase != previous.Phase || rebound.LifecycleRunMode != previous.RunMode || !rebound.ProcessBinding.Equal(previous.ProcessBinding) {
		t.Fatalf("selected recovery lost the predecessor fence before authoritative start: %+v error=%v", rebound, err)
	}
	if revision, err := lifecycleConfigRevision(rebound); err != nil || revision != previous.ConfigRevision || !rebound.Topology.Equal(previous.Topology) {
		t.Fatalf("selected recovery rewrote retained configuration: revision=%s error=%v", revision, err)
	}
}

func TestSelectedDeclarationRecoveryRefusesChangedEvidence(t *testing.T) {
	for _, name := range []string{"identity", "config", "topology", "zero_epoch", "zero_generation", "exhausted_generation", "failed", "missing_binding"} {
		t.Run(name, func(t *testing.T) {
			previous, desired := selectedDeclarationRecoveryFixture(t)
			identity := previous.Identity
			switch name {
			case "identity":
				previous.Identity.RunID = uuid.NewString()
			case "config":
				previous.ConfigRevision = "different"
			case "topology":
				copy := *previous.Topology.Authority.Selected
				copy.RunID = uuid.NewString()
				previous.Topology.Authority.Selected = &copy
			case "zero_epoch":
				previous.RuntimeEpoch = 0
			case "zero_generation":
				previous.Generation = 0
			case "exhausted_generation":
				previous.Generation = ^uint64(0)
			case "failed":
				previous.Phase = AgentLifecycleFailed
			case "missing_binding":
				previous.ProcessBinding = ProcessExecutionBinding{}
			}
			if rebound, err := selectedDeclarationRecoveryRecord(identity, previous, desired); err == nil || rebound.Config.ID != "" {
				t.Fatalf("%s admitted contradictory native metadata: %+v error=%v", name, rebound, err)
			}
		})
	}
}
