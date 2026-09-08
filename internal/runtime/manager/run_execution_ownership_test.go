package manager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Pure loop tests publish through an ephemeral bus without any selected-store
// rows. They can verify an existing ephemeral route, never materialize an agent.
func (am *AgentManager) finalizeEphemeralTestReadiness(ctx context.Context, event events.Event, routes []events.DeliveryRoute) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, route := range routes {
		if !route.Recipient.IsAgent() {
			continue
		}
		if route.AgentIdentity.RunID != event.RunID() {
			return errors.New("ephemeral test route belongs to a different run")
		}
		readiness, err := am.lifecycle.committedRouteReadinessByIdentity(route.AgentIdentity)
		if err != nil {
			return err
		}
		if readiness.State.Topology.Lifetime != agenttopology.LifetimeEphemeral {
			return errors.New("ephemeral test finalizer cannot admit durable topology")
		}
	}
	return nil
}

type runExecutionOwnerProbe struct {
	AgentLifecyclePersistence
	mu          sync.Mutex
	disposition RunExecutionOwnership
	err         error
}

func (p *runExecutionOwnerProbe) InspectRunExecutionOwnership(context.Context, string) (RunExecutionOwnership, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.disposition, p.err
}

func (p *runExecutionOwnerProbe) set(disposition RunExecutionOwnership, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disposition, p.err = disposition, err
}

func TestLifecycleRegistrationRequiresRunOwnershipForSpawnAndAdoption(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(map[bool]string{false: "adoption", true: "spawn"}[persist], func(t *testing.T) {
			store := newLifecyclePersistenceProbe()
			owner := &runExecutionOwnerProbe{AgentLifecyclePersistence: store, disposition: RunExecutionForeign}
			coordinator := newAgentLifecycleCoordinator(owner, nil, nil, nil, nil)
			record := lifecycleTestPersistedAgent(t)
			ctx := testAuthorActivityContext(context.Background())
			register := func() error {
				return coordinator.registerExecution(ctx, record, persist, recoveryTestAgent{id: record.Config.ID}, testManagerSubscriptionAdmission(t, record.Config))
			}
			err := register()
			if !errors.Is(err, ErrRunExecutionNotOwned) {
				t.Fatalf("registration refusal: %v", err)
			}
			identity, err := record.Config.ConcreteIdentity()
			if err != nil {
				t.Fatal(err)
			}
			if _, found := coordinator.executionSnapshotByIdentity(identity); found || len(store.requestsFor("spawn")) != 0 {
				t.Fatal("foreign registration constructed execution or changed persistence")
			}
			owner.set(RunExecutionOwned, nil)
			if err := register(); err != nil {
				t.Fatalf("owned registration: %v", err)
			}
			if _, found := coordinator.executionSnapshotByIdentity(identity); !found {
				t.Fatal("owned registration omitted execution projection")
			}
		})
	}
}

func TestDynamicReadinessRunOwnershipBeforeEnqueueAndAfterAdmission(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, failure := range []string{"foreign", "fenced", "invalid"} {
			t.Run(map[bool]string{false: "before-enqueue", true: "already-queued"}[queued]+"/"+failure, func(t *testing.T) {
				instances := &flowActivationTestInstanceStore{}
				bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
				am := newFlowActivationManager(t, bus, instances)
				bundle := testFlowBundle(t, "")
				setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
				req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
				ctx := testAuthorActivityContext(context.Background())
				if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
					t.Fatal(err)
				}
				before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found {
					t.Fatalf("load readiness: %v %v", found, err)
				}
				owner := &runExecutionOwnerProbe{AgentLifecyclePersistence: am.lifecycle.persistence(), disposition: RunExecutionOwned}
				am.lifecycle.replacePersistence(owner)
				fenced := errors.New("exact execution fence changed")
				revoke := func() {
					switch failure {
					case "foreign":
						owner.set(RunExecutionForeign, nil)
					case "fenced":
						owner.set(0, fenced)
					case "invalid":
						owner.set(0, nil)
					}
				}
				admitted := make(chan struct{})
				release := make(chan struct{})
				if queued {
					am.testAfterDynamicFlowReadinessAdmission = func() {
						close(admitted)
						<-release
					}
				} else {
					revoke()
					am.testAfterDynamicFlowReadinessAdmission = func() { t.Error("foreign work was enqueued") }
				}
				done := make(chan error, 1)
				go func() {
					done <- am.reconcileDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.InstancePath)
				}()
				if queued {
					<-admitted
					revoke()
					close(release)
				}
				err = <-done
				if err == nil || (failure == "foreign" && !errors.Is(err, ErrRunExecutionNotOwned)) || (failure == "fenced" && !errors.Is(err, fenced)) {
					t.Fatalf("ownership refusal=%v", err)
				}
				after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found || !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected callback changed durable readiness: %v %v", found, err)
				}
				if !bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
					t.Fatal("rejected callback retired another owner's published route")
				}
				if len(instances.terminatedPaths) != 0 || len(instances.retiredTimerEntries) != 0 {
					t.Fatal("rejected callback performed terminal cleanup")
				}
				owner.set(RunExecutionOwned, nil)
				am.testAfterDynamicFlowReadinessAdmission = nil
				if err := am.reconcileDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.InstancePath); err != nil {
					t.Fatalf("owned control: %v", err)
				}
			})
		}
	}
}
