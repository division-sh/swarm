package manager_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestReconfigureAgent_MemoryEnabledConfigChangeRotatesExactIdentity(t *testing.T) {
	proveNativeReconfigureMemoryBothStores(t, true, manager.ProveReconfigureMemoryEnabledConfigChange)
}

func TestReconfigureAgent_ExplicitFalseTerminatesReusableMemory(t *testing.T) {
	proveNativeReconfigureMemoryBothStores(t, true, manager.ProveReconfigureExplicitFalse)
}

func TestReconfigureAgent_ExplicitTrueStartsFreshAndOmissionRetains(t *testing.T) {
	proveNativeReconfigureMemoryBothStores(t, false, manager.ProveReconfigureExplicitTrue)
}

func proveNativeReconfigureMemoryBothStores(t *testing.T, enabled bool, prove func(*testing.T, manager.AgentLifecyclePersistence, manager.ReconfigureMemoryNativeTestStore, manager.PersistedAgent, func(agentmemory.Identity) []sessions.Record, manager.ReconfigureMemoryNativeAdmission)) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				storetest.AgentFixtureStore
				manager.ReconfigureMemoryNativeTestStore
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t)
			}
			record := manager.ReconfigureMemoryNativeFixture(t, enabled)
			storetest.RequireStaticAgentFixture(t, manager.TerminalPanicFixtureContext(), selected, record)
			view, found, err := selected.LoadAgentLifecycleState(context.Background(), record.Config.Identity)
			if err != nil || !found {
				t.Fatalf("native memory fixture row: found=%v err=%v", found, err)
			}
			record.Topology = view.Topology
			prove(t, storetest.AgentLifecycleFixture(t, selected), selected, record, func(identity agentmemory.Identity) []sessions.Record {
				return readNativeReconfigureSessionHistory(t, selected, identity)
			}, func(candidate actors.AgentConfig) (manager.AgentLifecyclePersistence, agenttopology.Admission) {
				return admitNativeReconfigureCandidate(t, selected, candidate)
			})
		})
	}
}

func admitNativeReconfigureCandidate(t *testing.T, selected storetest.AgentFixtureStore, candidate actors.AgentConfig) (manager.AgentLifecyclePersistence, agenttopology.Admission) {
	t.Helper()
	ctx := manager.TerminalPanicFixtureContext()
	process, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := process.CurrentSourceSet(ctx)
	if err != nil || !found {
		t.Fatalf("current native source plan: found=%v err=%v", found, err)
	}
	identity, err := candidate.Identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := manager.AgentConfigPlanRevision(candidate, identity)
	if err != nil {
		t.Fatal(err)
	}
	agents := append([]agenttopology.DesiredAgent(nil), current.Agents...)
	var coordinate agenttopology.SourceCoordinate
	for i := range agents {
		if agents[i].Identity == identity {
			agents[i].ConfigRevision = revision
			coordinate = agents[i].Source
		}
	}
	if err := coordinate.Validate(); err != nil {
		t.Fatalf("native candidate has no admitted source: %v", err)
	}
	plan, err := agenttopology.NewSourceSetPlan(current.Sources, agents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	topology, err := agenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	return grant, topology
}

func readNativeReconfigureSessionHistory(t *testing.T, selected any, identity agentmemory.Identity) []sessions.Record {
	t.Helper()
	evidence, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(evidence["agent_sessions"])
	if err != nil {
		t.Fatal(err)
	}
	var history []sessions.Record
	for _, row := range rows {
		text := func(column string) string {
			t.Helper()
			cell, found := row[column]
			if !found {
				t.Fatalf("session evidence omits %s", column)
			}
			value, err := cell.Text()
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
		actual, err := agentidentity.FromStorageFields(agentidentity.StorageFields{
			RunID: text("run_id"), AgentID: text("agent_id"), NameOwner: text("agent_name_owner"), NameSource: text("agent_name_source"),
			RoutePresence: text("agent_route_presence"), FlowScopeKey: text("flow_scope_key"), FlowInstanceID: text("flow_instance_id"), FlowInstancePath: text("flow_instance"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if actual != agentidentity.Identity(identity) {
			continue
		}
		history = append(history, sessions.Record{SessionID: text("session_id"), Identity: identity,
			Status: text("status"), TerminationReason: text("termination_reason"), TerminationDetail: text("termination_detail"), SuccessorSessionID: text("successor_session_id")})
	}
	return history
}
