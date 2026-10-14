package manager

import (
	"context"
	"reflect"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestPublicAgentTargetIgnoresRetainedUnpublishedSibling(t *testing.T) {
	for _, method := range []string{"config", "directive"} {
		t.Run(method, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			probe := &terminalSetPersistenceProbe{AgentLifecyclePersistence: newLifecyclePersistenceProbe(), agents: make(map[runtimeagentidentity.Identity]*lifecyclePersistenceProbe)}
			bus := &directiveTestBus{}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{LifecycleStore: probe}, &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{
				RunID: managerIdentityTestRunID, Mode: runtimeagentcontrol.RunResolutionSpecified,
			}})
			const agentID = "public-target-worker"
			identities := make([]runtimeagentidentity.Identity, 2)
			agents := make([]*chatTestAgent, 2)
			records := make([]PersistedAgent, 2)
			for i, instance := range []string{"hidden", "visible"} {
				rec := lifecycleTestPersistedAgent(t)
				rec.Config.ID = agentID
				rec.Config.Identity = runtimeagentidentitytest.RuntimeForRun(t, rec.Config.Identity.RunID, agentID, "public-target-test", "review", instance, "review/"+instance)
				rec.Config.FlowPath = rec.Config.Identity.FlowInstance()
				agents[i] = &chatTestAgent{id: agentID}
				if err := am.lifecycle.registerExecution(ctx, rec, true, agents[i], testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
					t.Fatal(err)
				}
				identities[i], records[i] = rec.Config.Identity, rec
			}
			runCtx, started, err := am.lifecycle.beginRun(managedExecutionTestContext(t, ctx), AgentRunModeStandard, am.workOwner)
			if err != nil || !started {
				t.Fatalf("begin owned Manager run: started=%t err=%v", started, err)
			}
			if err := am.startShutdownWatcher(runCtx, "public target test shutdown watcher"); err != nil {
				t.Fatal(err)
			}
			tokens := make([]runtimeeffects.LifecycleToken, len(records))
			for i, rec := range records {
				_, token, done, err := replaceCoordinatorLoop(am.lifecycle, ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
				if err != nil {
					t.Fatal(err)
				}
				tokens[i] = token
				t.Cleanup(func() {
					if err := releaseCoordinatorLoop(am.lifecycle, token, done); err != nil {
						t.Error(err)
					}
				})
			}
			// Fencing removes publication, not the retained cleanup identity.
			am.lifecycle.mu.Lock()
			am.lifecycle.cells[identities[0]].execution.fenced = true
			am.lifecycle.mu.Unlock()
			before, found, err := probe.LoadAgentLifecycleState(ctx, identities[0])
			if err != nil || !found || before.Phase != AgentLifecycleRunning {
				t.Fatalf("retained row: state=%+v found=%t err=%v", before, found, err)
			}
			retained, found := am.lifecycle.executionSnapshotByIdentity(identities[0])
			if !found || retained.Token != tokens[0] {
				t.Fatalf("fenced execution was not retained: snapshot=%+v found=%t", retained, found)
			}
			if _, found := am.lifecycle.publishedExecutionSnapshotByIdentity(identities[0]); found {
				t.Fatal("fenced execution remains publishable")
			}
			if configs := am.ListAgentConfigs(); len(configs) != 1 || configs[0].Identity != identities[1] {
				t.Fatalf("public config census=%+v, want visible sibling", configs)
			}
			if method == "config" {
				cfg, err := am.ResolveAgentConfig(identities[1].RunID, agentID, "")
				if err != nil || cfg.Identity != identities[1] {
					t.Errorf("broad config did not elect sole published sibling: identity=%+v err=%v", cfg.Identity, err)
				}
			} else {
				result, err := am.SendDirective(ctx, runtimeagentcontrol.SendDirectiveRequest{
					RunID: identities[1].RunID, AgentID: agentID, Directive: "use published sibling",
					Source: runtimeagentcontrol.DirectiveSourceV1RPC, ActorTokenID: "public-target-operator",
				})
				if err != nil || result.FlowInstance != identities[1].FlowInstance() || agents[0].calls != 0 || agents[1].calls != 1 {
					t.Errorf("broad directive did not elect sole published sibling: result=%+v hidden_calls=%d visible_calls=%d err=%v", result, agents[0].calls, agents[1].calls, err)
				}
			}
			after, found, err := probe.LoadAgentLifecycleState(ctx, identities[0])
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("public selection mutated retained row: before=%+v after=%+v found=%t err=%v", before, after, found, err)
			}
			stillRetained, found := am.lifecycle.executionSnapshotByIdentity(identities[0])
			if !found || !reflect.DeepEqual(retained, stillRetained) {
				t.Fatal("public selection changed retained execution evidence")
			}
			if identities := am.lifecycle.executionIdentities(); len(identities) != 2 {
				t.Fatalf("public selection removed retained cleanup membership: %+v", identities)
			}
		})
	}
}
