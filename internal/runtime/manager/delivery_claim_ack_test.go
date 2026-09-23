package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type faultedManagerClaimStore struct {
	runtimedelivery.Store
	acknowledged bool
	fault        error
	injected     atomic.Bool
}

func (s *faultedManagerClaimStore) managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority {
	return s.Store.(interface {
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	}).managerTestDeliveryAuthority()
}

func (s *faultedManagerClaimStore) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimResult, error) {
	result, err := s.Store.ClaimDelivery(ctx, authority, event, route)
	if err != nil || result.Disposition != runtimedelivery.ClaimAcquired || !s.injected.CompareAndSwap(false, true) {
		return result, err
	}
	result.Acknowledged = s.acknowledged
	return result, s.fault
}

type managerClaimLogBus struct {
	*runtimebus.EventBus
	claimErrors chan runtimepipeline.RuntimeLogEntry
}

func (b *managerClaimLogBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	if entry.Action == "delivery_claim_failed" {
		select {
		case b.claimErrors <- entry:
		default:
		}
	}
	return b.EventBus.LogRuntime(ctx, entry)
}

func TestManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			runtimebus.ResumeRuntimeIngress()
			t.Cleanup(runtimebus.ResumeRuntimeIngress)

			base := newManagerDeliveryTestStore(t)
			claimErr := runtimefailures.New(runtimefailures.ClassInternalFailure, "claim_cleanup_failed", "manager-test", "claim_delivery", nil)
			faulted := &faultedManagerClaimStore{Store: base, acknowledged: acknowledged, fault: claimErr}
			persistence := &startupReplayTestStore{
				recoveryTestStore:        recoveryTestStore{},
				managerDeliveryTestStore: base,
			}
			probe := lifecycletest.New(t)
			eventBus, err := newTestManagerEventBus(t)
			if err != nil {
				t.Fatalf("new event bus: %v", err)
			}
			bus := &managerClaimLogBus{EventBus: eventBus, claimErrors: make(chan runtimepipeline.RuntimeLogEntry, 1)}
			var calls atomic.Int32
			agent := shutdownTestAgent{
				id: "claim-fault-agent", subscriptions: []events.EventType{"test.intervention"},
				onEvent: func(context.Context, events.Event) ([]events.Event, error) {
					calls.Add(1)
					return nil, nil
				},
			}
			manager := newTestAgentManagerWithOptions(t, bus, func(runtimeactors.AgentConfig) (Agent, error) { return agent, nil }, AgentManagerOptions{
				DeliveryStore: faulted, TestLifecycleProbe: probe.Raw(),
			}, persistence)
			if err := manager.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{
				Topology: managerTestTopologyAdmission(t), ProcessBinding: lifecycleProbeProcessBinding(),
				Config: managerTestAgentConfig(runtimeactors.AgentConfig{
					ExecutionMode: "live", ID: agent.ID(), Identity: managerAgentIdentity(agent.ID()),
					Subscriptions: []string{"test.intervention"},
				}),
			}, false); err != nil {
				t.Fatalf("spawn agent: %v", err)
			}
			if err := manager.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			event := eventtest.RunCreatingRootIngress(
				eventtest.UUID("manager-claim-fault"), events.EventType("test.intervention"),
				"test", "", nil, 0, managerIdentityTestRunID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			if err := eventBus.Publish(testAuthorActivityContext(context.Background()), event); err != nil {
				t.Fatalf("publish event: %v", err)
			}
			select {
			case entry := <-bus.claimErrors:
				if entry.Failure == nil || entry.Failure.Detail.Code != "claim_cleanup_failed" {
					t.Fatalf("claim error log lost failure: %#v", entry)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("claim error was not logged")
			}
			if acknowledged {
				probe.RequireAgentDelivered(event.ID(), agent.ID())
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("shutdown manager: %v", err)
			}
			if got := calls.Load(); (got == 1) != acknowledged {
				t.Fatalf("handler calls = %d, acknowledged=%t", got, acknowledged)
			}
			deliveryID, err := runtimedelivery.DeliveryID(event.ID(), managerAgentDeliveryRouteForRun(event.RunID(), agent.ID()))
			if err != nil {
				t.Fatalf("delivery identity: %v", err)
			}
			snapshot, err := base.Snapshot(context.Background(), deliveryID)
			if err != nil {
				t.Fatalf("delivery snapshot: %v", err)
			}
			if acknowledged && snapshot.Status != runtimedelivery.StatusDelivered {
				t.Fatalf("acknowledged claim status = %q, want delivered", snapshot.Status)
			}
			if !acknowledged && snapshot.Status != runtimedelivery.StatusInProgress {
				t.Fatalf("unacknowledged claim status = %q, want in progress", snapshot.Status)
			}
		})
	}
}
