package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type publicAgentTargetFixture struct {
	ctx        context.Context
	manager    *AgentManager
	probe      *terminalSetPersistenceProbe
	operations *publicTargetDirectiveStore
	identities []runtimeagentidentity.Identity
	agents     []*chatTestAgent
	tokens     []runtimeeffects.LifecycleToken
}

type publicTargetDirectiveStore struct {
	runtimeagentcontrol.DirectiveOperationStore
	reservations int
}

func (s *publicTargetDirectiveStore) ReserveDirectiveOperation(ctx context.Context, req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperationReservation, error) {
	s.reservations++
	return s.DirectiveOperationStore.ReserveDirectiveOperation(ctx, req)
}

func newPublicAgentTargetFixture(t *testing.T, instances ...string) *publicAgentTargetFixture {
	t.Helper()
	f := &publicAgentTargetFixture{
		ctx:   testAuthorActivityContext(context.Background()),
		probe: &terminalSetPersistenceProbe{AgentLifecyclePersistence: newLifecyclePersistenceProbe(), agents: make(map[runtimeagentidentity.Identity]*lifecyclePersistenceProbe)},
	}
	bus := &directiveTestBus{store: &directiveEventStore{}}
	f.operations = &publicTargetDirectiveStore{DirectiveOperationStore: bus.store}
	f.manager = newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{
		LifecycleStore: f.probe, PersistenceRoles: PersistenceRoles{DirectiveOperations: f.operations},
	}, &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{Mode: runtimeagentcontrol.RunResolutionSpecified}})
	records := make([]PersistedAgent, len(instances))
	for i, instance := range instances {
		rec := lifecycleTestPersistedAgent(t)
		rec.Config.ID = "public-target-worker"
		rec.Config.Identity = runtimeagentidentitytest.RuntimeForRun(t, rec.Config.Identity.RunID, rec.Config.ID, "public-target-test", "review", instance, "review/"+instance)
		rec.Config.FlowPath = rec.Config.Identity.FlowInstance()
		agent := &chatTestAgent{id: rec.Config.ID}
		if err := f.manager.lifecycle.registerExecution(f.ctx, rec, true, agent, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
			t.Fatal(err)
		}
		f.identities = append(f.identities, rec.Config.Identity)
		f.agents = append(f.agents, agent)
		records[i] = rec
	}
	runCtx, started, err := f.manager.lifecycle.beginRun(managedExecutionTestContext(t, f.ctx), AgentRunModeStandard, f.manager.workOwner)
	if err != nil || !started {
		t.Fatalf("begin owned Manager run: started=%t err=%v", started, err)
	}
	if err := f.manager.startShutdownWatcher(runCtx, "public target test shutdown watcher"); err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		_, token, done, err := replaceCoordinatorLoop(f.manager.lifecycle, f.ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens = append(f.tokens, token)
		t.Cleanup(func() {
			if err := releaseCoordinatorLoop(f.manager.lifecycle, token, done); err != nil {
				t.Error(err)
			}
		})
	}
	return f
}

func (f *publicAgentTargetFixture) fence(index int) {
	f.manager.lifecycle.mu.Lock()
	f.manager.lifecycle.cells[f.identities[index]].execution.fenced = true
	f.manager.lifecycle.mu.Unlock()
}

func TestPublicAgentTargetIgnoresRetainedUnpublishedSibling(t *testing.T) {
	for _, method := range []string{"config", "directive"} {
		t.Run(method, func(t *testing.T) {
			f := newPublicAgentTargetFixture(t, "hidden", "visible")
			am, probe, ctx := f.manager, f.probe, f.ctx
			identities, agents, tokens := f.identities, f.agents, f.tokens
			agentID := identities[0].AgentID()
			// Fencing removes publication, not the retained cleanup identity.
			f.fence(0)
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

func TestPublicAgentTargetKeyedDirectiveReplayAfterFence(t *testing.T) {
	for _, sibling := range []bool{false, true} {
		name, instances := "no_public_candidate", []string{"hidden"}
		if sibling {
			name, instances = "sole_public_sibling", append(instances, "visible")
		}
		t.Run(name, func(t *testing.T) {
			f := newPublicAgentTargetFixture(t, instances...)
			identity := f.identities[0]
			req := runtimeagentcontrol.SendDirectiveRequest{
				RunID: identity.RunID, AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(),
				Directive: "accepted before fence", Source: runtimeagentcontrol.DirectiveSourceV1RPC,
				ActorTokenID: "public-target-operator", IdempotencyKey: "accepted-directive",
			}
			first, err := f.manager.SendDirective(f.ctx, req)
			if err != nil || first.FlowInstance != identity.FlowInstance() || f.agents[0].calls != 1 || f.operations.reservations != 1 {
				t.Fatalf("initial exact directive: result=%+v calls=%d reservations=%d err=%v", first, f.agents[0].calls, f.operations.reservations, err)
			}
			before, found, err := f.operations.LoadDirectiveOperation(f.ctx, first.OperationID)
			if err != nil || !found || before.State != runtimeagentcontrol.DirectiveOperationSucceeded {
				t.Fatalf("accepted operation: operation=%+v found=%t err=%v", before, found, err)
			}
			f.fence(0)
			if configs := f.manager.ListAgentConfigs(); len(configs) != len(instances)-1 {
				t.Fatalf("public candidates after fence: %+v", configs)
			}
			replayed, err := f.manager.SendDirective(f.ctx, req)
			if err != nil || !reflect.DeepEqual(first, replayed) {
				t.Fatalf("fenced accepted directive lost its exact historical result: first=%+v replay=%+v err=%v", first, replayed, err)
			}
			changed := req
			changed.Directive = "different command on the same key"
			_, err = f.manager.SendDirective(f.ctx, changed)
			var conflict *runtimeagentcontrol.DirectiveIdempotencyConflictError
			if !errors.As(err, &conflict) || conflict.OperationID != first.OperationID {
				t.Fatalf("changed hash did not retain exact operation conflict: %v", err)
			}
			after, found, err := f.operations.LoadDirectiveOperation(f.ctx, first.OperationID)
			if err != nil || !found || !reflect.DeepEqual(before, after) || f.operations.reservations != 1 || f.agents[0].calls != 1 {
				t.Fatalf("replay/conflict mutated operation or repeated execution: before=%+v after=%+v calls=%d reservations=%d err=%v", before, after, f.agents[0].calls, f.operations.reservations, err)
			}
			if sibling && f.agents[1].calls != 0 {
				t.Fatal("historical directive replay was redirected to the public sibling")
			}
		})
	}
}
