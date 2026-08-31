package manager

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const (
	managerTestTopologyBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	managerIdentityTestRunID      = "00000000-0000-0000-0000-000000000001"
)

func TestRequireAdmittedAgentIdentityRejectsRunlessAndMismatchedIdentity(t *testing.T) {
	if err := requireAdmittedAgentIdentity(
		runtimecorrelation.WithRunID(context.Background(), managerIdentityTestRunID),
		runtimeactors.AgentConfig{ID: "runless-agent"},
	); err == nil || !strings.Contains(err.Error(), "complete live identity") {
		t.Fatalf("runless materialization error = %v, want complete live identity rejection", err)
	}

	config := managerRootAgentConfig("exact-agent")
	if err := requireAdmittedAgentIdentity(
		runtimecorrelation.WithRunID(context.Background(), "00000000-0000-0000-0000-000000000002"),
		config,
	); err == nil || !strings.Contains(err.Error(), "conflicts with admitted run_id") {
		t.Fatalf("mismatched materialization error = %v, want run conflict", err)
	}
	if err := requireAdmittedAgentIdentity(
		runtimecorrelation.WithRunID(context.Background(), managerIdentityTestRunID),
		config,
	); err != nil {
		t.Fatalf("exact materialization identity: %v", err)
	}
}

func managerTestTopologyAdmission(t testing.TB) runtimeagenttopology.Admission {
	t.Helper()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatalf("construct manager test topology plan: %v", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatalf("construct manager test topology admission: %v", err)
	}
	return admission
}

func managerTestStaticAgentRecord(am *AgentManager, cfg runtimeactors.AgentConfig) (PersistedAgent, error) {
	var err error
	cfg, err = managerTestBindRuntimeCreatedIdentity(cfg, managerIdentityTestRunID, "manager.test.static_agent")
	if err != nil {
		return PersistedAgent{}, err
	}
	if err := am.resolveAgentModel(&cfg); err != nil {
		return PersistedAgent{}, err
	}
	if err := bindCanonicalAgentPrompt(am.semanticSource, &cfg); err != nil {
		return PersistedAgent{}, err
	}
	rec := PersistedAgent{Config: cfg, Status: "active", HiredBy: "manager-test", StartedAt: time.Now().UTC()}
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return PersistedAgent{}, err
	}
	identityPlan, err := identity.Plan()
	if err != nil {
		return PersistedAgent{}, err
	}
	revision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return PersistedAgent{}, err
	}
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	plan, err := runtimeagenttopology.NewSourceSetPlan(
		[]runtimeagenttopology.SourceCoordinate{coordinate},
		[]runtimeagenttopology.DesiredAgent{{Identity: identityPlan, Source: coordinate, ConfigRevision: revision}},
	)
	if err != nil {
		return PersistedAgent{}, err
	}
	rec.Topology, err = runtimeagenttopology.StaticAdmission(
		plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged,
	)
	return rec, err
}

func spawnManagerTestAgent(am *AgentManager, cfg runtimeactors.AgentConfig) error {
	rec, err := managerTestStaticAgentRecord(am, cfg)
	if err != nil {
		return err
	}
	return am.spawnAgentInternal(context.Background(), rec, true)
}

func installManagerTestStaticTopology(
	t testing.TB,
	am *AgentManager,
	store AgentLifecyclePersistence,
	configs ...runtimeactors.AgentConfig,
) ([]PersistedAgent, runtimeagenttopology.Admission, runtimeagenttopology.SourceSetPlan) {
	t.Helper()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: managerTestTopologyBundleHash, BundleSource: "ephemeral"}
	records := make([]PersistedAgent, 0, len(configs))
	desired := make([]runtimeagenttopology.DesiredAgent, 0, len(configs))
	for _, authored := range configs {
		cfg, err := managerTestBindRuntimeCreatedIdentity(authored, managerIdentityTestRunID, "manager.test.static_topology")
		if err != nil {
			t.Fatalf("bind manager test identity: %v", err)
		}
		if err := am.resolveAgentModel(&cfg); err != nil {
			t.Fatalf("resolve manager test agent: %v", err)
		}
		if err := bindCanonicalAgentPrompt(am.semanticSource, &cfg); err != nil {
			t.Fatalf("bind manager test prompt: %v", err)
		}
		rec := PersistedAgent{Config: cfg, Status: "active", HiredBy: "manager-test", StartedAt: time.Now().UTC()}
		identity, err := cfg.ConcreteIdentity()
		if err != nil {
			t.Fatalf("resolve manager test identity: %v", err)
		}
		identityPlan, err := identity.Plan()
		if err != nil {
			t.Fatalf("resolve manager test identity plan: %v", err)
		}
		revision, err := lifecycleConfigRevision(rec)
		if err != nil {
			t.Fatalf("resolve manager test config revision: %v", err)
		}
		records = append(records, rec)
		desired = append(desired, runtimeagenttopology.DesiredAgent{Identity: identityPlan, Source: coordinate, ConfigRevision: revision})
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct manager test complete source set: %v", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, coordinate.BundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatalf("construct manager test static admission: %v", err)
	}
	if err := am.InstallStartupTopology(store, admission, plan); err != nil {
		t.Fatalf("install manager test static topology: %v", err)
	}
	am.mu.Lock()
	am.startupAgentsHydrated = false
	am.mu.Unlock()
	for i := range records {
		records[i].Topology = admission
	}
	return records, admission, plan
}

func registerManagerTestEphemeralAgent(ctx context.Context, am *AgentManager, rec PersistedAgent) error {
	var err error
	rec.Topology, err = runtimeagenttopology.NewEphemeralAdmission("11111111-1111-4111-8111-111111111111", "runtime_shard")
	if err != nil {
		return err
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	return am.MaterializeAdmittedAgentForExecution(runtimecorrelation.WithRunID(ctx, identity.RunID), rec)
}

func managerTestBindRuntimeCreatedIdentity(cfg runtimeactors.AgentConfig, runID, owner string) (runtimeactors.AgentConfig, error) {
	cfg.NormalizeRuntimeDescriptor()
	if !cfg.Identity.IsZero() {
		if _, err := cfg.ConcreteIdentity(); err != nil {
			return runtimeactors.AgentConfig{}, err
		}
		return cfg, nil
	}
	name, err := runtimeagentidentity.RuntimeName(cfg.ID, owner)
	if err != nil {
		return runtimeactors.AgentConfig{}, err
	}
	route := runtimeagentidentity.RootRoute()
	if flowPath := cfg.CanonicalFlowPath(); flowPath != "" {
		route, err = runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
		if err != nil {
			return runtimeactors.AgentConfig{}, err
		}
	}
	cfg.Identity, err = runtimeagentidentity.New(runID, name, route)
	if err != nil {
		return runtimeactors.AgentConfig{}, err
	}
	return cfg, nil
}

func managerRootAgentConfig(agentID string, subscriptions ...string) runtimeactors.AgentConfig {
	return managerTestAgentConfig(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      managerAgentIdentity(agentID),
		Subscriptions: append([]string(nil), subscriptions...),
	})
}

func managerTestResolvedIntent(agentID string) runtimeagentintent.Resolved {
	resolved, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(agentID)+".intent",
		"Perform the manager test agent's assigned work.",
	)
	if err != nil {
		panic(err)
	}
	return resolved
}

func managerTestAgentConfig(cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	if cfg.Intent.Empty() {
		cfg.Intent = managerTestResolvedIntent(cfg.ID)
	}
	if cfg.Prompt.Empty() {
		prompt, err := runtimeagentintent.IntentOnlyPrompt(cfg.Intent)
		if err != nil {
			panic(err)
		}
		cfg.Prompt = prompt
	}
	return cfg
}

func managerTestAgentEntry(agentID string, entry runtimecontracts.AgentRegistryEntry) runtimecontracts.AgentRegistryEntry {
	if entry.ResolvedIntent.Empty() {
		entry.ResolvedIntent = managerTestResolvedIntent(agentID)
	}
	return entry
}

func managerScopedRuntimeAgentIdentity(agentID, owner, scopeKey, instanceID, instancePath string) runtimeagentidentity.Identity {
	name, err := runtimeagentidentity.RuntimeName(agentID, owner)
	if err != nil {
		panic(err)
	}
	route, err := runtimeagentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		panic(err)
	}
	identity, err := runtimeagentidentity.New(managerIdentityTestRunID, name, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func managerRuntimeAgentIdentityForFlowPath(agentID, instancePath string) runtimeagentidentity.Identity {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	instanceID := path.Base(instancePath)
	scopeKey := strings.TrimSuffix(instancePath, "/"+instanceID)
	return managerScopedRuntimeAgentIdentity(
		agentID,
		"manager.test/"+strings.TrimSpace(agentID),
		scopeKey,
		instanceID,
		instancePath,
	)
}

func testAgentIdentity(t testing.TB, am *AgentManager, agentID, flowInstance string) runtimeagentidentity.Identity {
	return testAgentIdentityForRun(t, am, managerIdentityTestRunID, agentID, flowInstance)
}

func testAgentIdentityForRun(t testing.TB, am *AgentManager, runID, agentID, flowInstance string) runtimeagentidentity.Identity {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(runID, agentID, flowInstance, false)
	if err != nil {
		t.Fatalf("resolve test agent identity %q@%q: %v", agentID, flowInstance, err)
	}
	return identity
}

func testAgentConfig(t testing.TB, am *AgentManager, agentID, flowInstance string) (runtimeactors.AgentConfig, bool) {
	return testAgentConfigForRun(t, am, managerIdentityTestRunID, agentID, flowInstance)
}

func testAgentConfigForRun(t testing.TB, am *AgentManager, runID, agentID, flowInstance string) (runtimeactors.AgentConfig, bool) {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(runID, agentID, flowInstance, false)
	if err != nil {
		return runtimeactors.AgentConfig{}, false
	}
	return am.getAgentConfigIdentity(identity)
}

func testExecutionSnapshot(t testing.TB, am *AgentManager, agentID, flowInstance string) (agentExecutionSnapshot, bool) {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(managerIdentityTestRunID, agentID, flowInstance, false)
	if err != nil {
		return agentExecutionSnapshot{}, false
	}
	return am.lifecycle.executionSnapshotByIdentity(identity)
}

func testLifecycleCell(t testing.TB, coordinator *agentLifecycleCoordinator, agentID, flowInstance string) (*agentLifecycleCell, bool) {
	t.Helper()
	identity, _, err := coordinator.resolveAgentTargetLockedForTest(agentID, flowInstance, true)
	if err != nil {
		return nil, false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	cell := coordinator.cells[identity]
	return cell, cell != nil
}

func (c *agentLifecycleCoordinator) resolveAgentTargetLockedForTest(
	agentID,
	flowInstance string,
	includeTerminated bool,
) (runtimeagentidentity.Identity, *agentLifecycleCell, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resolveAgentTargetLocked(managerIdentityTestRunID, agentID, flowInstance, includeTerminated)
}
