package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
	reservations  int
	beforeReserve func()
}

func (s *publicTargetDirectiveStore) ReserveDirectiveOperation(ctx context.Context, req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperationReservation, error) {
	s.reservations++
	if s.beforeReserve != nil {
		s.beforeReserve()
	}
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

func TestPublicAgentTargetExcludedPostures(t *testing.T) {
	for _, posture := range []struct {
		name   string
		change func(*agentLifecycleCell)
	}{
		{"fenced", func(c *agentLifecycleCell) { c.execution.fenced = true }},
		{"retirement", func(c *agentLifecycleCell) { c.retirement = &agentRetirement{} }},
		{"terminal_set", func(c *agentLifecycleCell) { c.terminalSet = &terminalFlowRetirement{} }},
		{"settlement_error", func(c *agentLifecycleCell) { c.execution.settlementErr = errors.New("retained settlement failure") }},
		{"missing_execution", func(c *agentLifecycleCell) { c.execution = nil }},
		{"missing_actor", func(c *agentLifecycleCell) { c.execution.agent = nil }},
		{"token_mismatch", func(c *agentLifecycleCell) { c.execution.token.Generation++ }},
	} {
		t.Run(posture.name, func(t *testing.T) {
			f := newPublicAgentTargetFixture(t, "hidden", "visible")
			c := f.manager.lifecycle
			c.mu.Lock()
			cell := c.cells[f.identities[0]]
			execution, saved := cell.execution, *cell.execution
			posture.change(cell)
			c.mu.Unlock()
			// Restore visibility faults before the normal owned loop cleanup.
			t.Cleanup(func() {
				c.mu.Lock()
				*execution = saved
				cell.execution, cell.retirement, cell.terminalSet = execution, nil, nil
				c.mu.Unlock()
			})
			identity := f.identities[1]
			cfg, err := f.manager.ResolveAgentConfig(identity.RunID, identity.AgentID(), "")
			if err != nil || cfg.Identity != identity {
				t.Fatalf("public config did not exclude %s: cfg=%+v err=%v", posture.name, cfg, err)
			}
			result, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{
				RunID: identity.RunID, AgentID: identity.AgentID(), Directive: "public candidate only",
			})
			if err != nil || result.FlowInstance != identity.FlowInstance() || f.agents[0].calls != 0 || f.agents[1].calls != 1 {
				t.Fatalf("public directive did not exclude %s: result=%+v err=%v", posture.name, result, err)
			}
			retained, err := c.resolveAgentTarget(identity.RunID, identity.AgentID(), f.identities[0].FlowInstance(), false)
			if err != nil || retained != f.identities[0] {
				t.Fatalf("public filter changed retained exact selection: identity=%+v err=%v", retained, err)
			}
			if _, err := c.resolveAgentTarget(identity.RunID, identity.AgentID(), "", false); err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("retained broad selection lost its two candidates: %v", err)
			}
		})
	}
}

func TestPublicAgentTargetSelectorControls(t *testing.T) {
	t.Run("multiple_publishable", func(t *testing.T) {
		f := newPublicAgentTargetFixture(t, "hidden", "visible")
		identity := f.identities[0]
		var previous string
		for range 3 {
			_, err := f.manager.ResolveAgentConfig(identity.RunID, identity.AgentID(), "")
			if err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("two published candidates did not refuse ambiguity: %v", err)
			}
			if previous != "" && previous != err.Error() {
				t.Fatalf("ambiguity changed candidate ordering: first=%s current=%s", previous, err)
			}
			previous = err.Error()
		}
		_, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{RunID: identity.RunID, AgentID: identity.AgentID(), Directive: "ambiguous"})
		if err == nil || !strings.Contains(err.Error(), "ambiguous") || f.operations.reservations != 0 || f.agents[0].calls != 0 || f.agents[1].calls != 0 {
			t.Fatalf("ambiguous directive persisted or executed: err=%v reservations=%d", err, f.operations.reservations)
		}
	})
	t.Run("exact_and_foreign_run", func(t *testing.T) {
		f := newPublicAgentTargetFixture(t, "hidden", "visible")
		f.fence(0)
		identity := f.identities[1]
		cfg, err := f.manager.ResolveAgentConfig(identity.RunID, identity.AgentID(), identity.FlowInstance())
		if err != nil || cfg.Identity != identity {
			t.Fatalf("exact published selector changed: cfg=%+v err=%v", cfg, err)
		}
		for _, request := range []struct{ runID, instance string }{
			{identity.RunID, f.identities[0].FlowInstance()},
			{uuid.NewString(), identity.FlowInstance()},
		} {
			if _, err := f.manager.ResolveAgentConfig(request.runID, identity.AgentID(), request.instance); !errors.Is(err, ErrAgentNotFound) {
				t.Fatalf("fenced/foreign config selector admitted: %v", err)
			}
			_, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{RunID: request.runID, AgentID: identity.AgentID(), FlowInstance: request.instance, Directive: "refuse"})
			if !errors.Is(err, ErrAgentNotFound) || f.operations.reservations != 0 {
				t.Fatalf("fenced/foreign directive selector admitted: err=%v reservations=%d", err, f.operations.reservations)
			}
		}
	})
	t.Run("no_publishable_candidate", func(t *testing.T) {
		f := newPublicAgentTargetFixture(t, "hidden", "visible")
		f.fence(0)
		f.fence(1)
		identity := f.identities[0]
		if _, err := f.manager.ResolveAgentConfig(identity.RunID, identity.AgentID(), ""); !errors.Is(err, ErrAgentNotFound) {
			t.Fatalf("empty public election changed: %v", err)
		}
		_, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{RunID: identity.RunID, AgentID: identity.AgentID(), Directive: "refuse"})
		if !errors.Is(err, ErrAgentNotFound) || f.operations.reservations != 0 {
			t.Fatalf("empty public directive election admitted: err=%v reservations=%d", err, f.operations.reservations)
		}
	})
}

func TestPublicAgentTargetNewDirectiveNeverUsesFencedSibling(t *testing.T) {
	f := newPublicAgentTargetFixture(t, "hidden", "visible")
	f.fence(0)
	identity := f.identities[1]
	for _, key := range []string{"", "new-key"} {
		result, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{
			RunID: identity.RunID, AgentID: identity.AgentID(), Directive: "new public command", IdempotencyKey: key,
		})
		if err != nil || result.FlowInstance != identity.FlowInstance() {
			t.Fatalf("new key/no-key did not elect B: result=%+v err=%v", result, err)
		}
	}
	if f.agents[0].calls != 0 || f.agents[1].calls != 2 || f.operations.reservations != 2 {
		t.Fatalf("new commands used a retained predecessor: A=%d B=%d reservations=%d", f.agents[0].calls, f.agents[1].calls, f.operations.reservations)
	}
}

func TestPublicAgentTargetConcurrentFenceNeverReroutesDirective(t *testing.T) {
	f := newPublicAgentTargetFixture(t, "hidden", "visible")
	f.fence(0)
	selected := f.identities[1]
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	f.operations.beforeReserve = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{
			RunID: selected.RunID, AgentID: selected.AgentID(), Directive: "keep exact election", IdempotencyKey: "fence-after-election",
		})
		done <- err
	}()
	t.Cleanup(func() { resume() })
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("directive did not reach elected reservation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("elected reservation barrier timed out")
	}
	f.manager.lifecycle.mu.Lock()
	f.manager.lifecycle.cells[selected].execution.fenced = true
	f.manager.lifecycle.cells[f.identities[0]].execution.fenced = false
	f.manager.lifecycle.mu.Unlock()
	resume()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fenced elected directive did not return")
	}
	var failure *runtimefailures.Error
	if !errors.As(err, &failure) || failure.Failure.Detail.Code != "lifecycle_generation_not_running" {
		t.Fatalf("elected generation fence was not rechecked: %v", err)
	}
	op, found, err := f.operations.LoadDirectiveOperationByKey(f.ctx, runtimeagentcontrol.DirectiveOperationMethod, "internal:"+runtimeagentcontrol.DirectiveSourceInternalRuntime, "fence-after-election")
	if err != nil || !found || op.AgentIdentity != selected || op.State != runtimeagentcontrol.DirectiveOperationPrepared || f.agents[0].calls != 0 || f.agents[1].calls != 0 {
		t.Fatalf("fence silently rerouted or executed: operation=%+v A=%d B=%d err=%v", op, f.agents[0].calls, f.agents[1].calls, err)
	}
}

func TestPublicAgentTargetForeignRunCandidateIsSeparate(t *testing.T) {
	f := newPublicAgentTargetFixture(t, "hidden", "visible")
	rec := lifecycleTestPersistedAgent(t)
	rec.Config.ID = f.identities[0].AgentID()
	rec.Config.Identity = runtimeagentidentitytest.RuntimeForRun(t, uuid.NewString(), rec.Config.ID, "public-target-test", "review", "visible", "review/visible")
	rec.Config.FlowPath = rec.Config.Identity.FlowInstance()
	foreign := &chatTestAgent{id: rec.Config.ID}
	if err := f.manager.lifecycle.registerExecution(f.ctx, rec, true, foreign, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
		t.Fatal(err)
	}
	_, token, done, err := replaceCoordinatorLoop(f.manager.lifecycle, f.ctx, rec, "start", uuid.NewString(), nil, runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := releaseCoordinatorLoop(f.manager.lifecycle, token, done); err != nil {
			t.Error(err)
		}
	})
	f.fence(0)
	for _, identity := range []runtimeagentidentity.Identity{f.identities[1], rec.Config.Identity} {
		cfg, err := f.manager.ResolveAgentConfig(identity.RunID, identity.AgentID(), "")
		if err != nil || cfg.Identity != identity {
			t.Fatalf("foreign run entered candidate election: cfg=%+v want=%+v err=%v", cfg.Identity, identity, err)
		}
		result, err := f.manager.SendDirective(f.ctx, runtimeagentcontrol.SendDirectiveRequest{
			RunID: identity.RunID, AgentID: identity.AgentID(), Directive: "own run only",
		})
		if err != nil || result.RunID != identity.RunID || result.FlowInstance != identity.FlowInstance() {
			t.Fatalf("foreign run changed directive target: result=%+v want=%+v err=%v", result, identity, err)
		}
	}
	if f.agents[0].calls != 0 || f.agents[1].calls != 1 || foreign.calls != 1 {
		t.Fatalf("cross-run selection executed a foreign candidate: hidden=%d visible=%d foreign=%d", f.agents[0].calls, f.agents[1].calls, foreign.calls)
	}
}

func TestPublicAgentTargetRetainedTerminatedSelectionUnchanged(t *testing.T) {
	f := newPublicAgentTargetFixture(t, "hidden")
	c, identity := f.manager.lifecycle, f.identities[0]
	c.mu.Lock()
	cell := c.cells[identity]
	phase := cell.view.Phase
	cell.view.Phase = AgentLifecycleTerminated
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		cell.view.Phase = phase
		c.mu.Unlock()
	})
	if _, err := c.resolveAgentTarget(identity.RunID, identity.AgentID(), identity.FlowInstance(), false); !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("retained selector changed terminated exclusion: %v", err)
	}
	if got, err := c.resolveAgentTarget(identity.RunID, identity.AgentID(), identity.FlowInstance(), true); err != nil || got != identity {
		t.Fatalf("include-terminated inspection changed: got=%+v err=%v", got, err)
	}
}

func TestPublicAgentTargetReplayRequestValidation(t *testing.T) {
	f := newPublicAgentTargetFixture(t, "hidden")
	identity := f.identities[0]
	req := runtimeagentcontrol.SendDirectiveRequest{
		RunID: identity.RunID, AgentID: identity.AgentID(), FlowInstance: identity.FlowInstance(),
		Directive: "accepted", ActorTokenID: "operator-a", IdempotencyKey: "same-key",
	}
	first, err := f.manager.SendDirective(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	before, found, err := f.operations.LoadDirectiveOperation(f.ctx, first.OperationID)
	if err != nil || !found {
		t.Fatalf("initial operation: found=%t err=%v", found, err)
	}
	f.fence(0)
	for _, mutate := range []func(*runtimeagentcontrol.SendDirectiveRequest){
		func(r *runtimeagentcontrol.SendDirectiveRequest) { r.RunID = "" },
		func(r *runtimeagentcontrol.SendDirectiveRequest) { r.RunID = uuid.NewString() },
		func(r *runtimeagentcontrol.SendDirectiveRequest) { r.AgentID = "other-agent" },
		func(r *runtimeagentcontrol.SendDirectiveRequest) { r.FlowInstance = "review/other" },
		func(r *runtimeagentcontrol.SendDirectiveRequest) { r.ActorTokenID = "operator-b" },
	} {
		changed := req
		changed.RequestHash = before.RequestHash
		mutate(&changed)
		if _, err := f.manager.SendDirective(f.ctx, changed); err == nil {
			t.Fatalf("historical lookup bypassed request/run/actor validation: %+v", changed)
		}
	}
	after, found, err := f.operations.LoadDirectiveOperation(f.ctx, first.OperationID)
	if err != nil || !found || !reflect.DeepEqual(before, after) || f.operations.reservations != 1 || f.agents[0].calls != 1 {
		t.Fatalf("hostile replay changed historical operation: operation=%+v reservations=%d calls=%d err=%v", after, f.operations.reservations, f.agents[0].calls, err)
	}
}
