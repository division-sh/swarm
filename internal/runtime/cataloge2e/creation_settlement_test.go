package cataloge2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

type catalogCreationSettlementObserver func(context.Context, lifecycleprobe.Signal)

func (observe catalogCreationSettlementObserver) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	observe(ctx, signal)
}

func TestCatalogConfiguredCreationCompletesInEitherHandlerOrder(t *testing.T) {
	requireCatalogCreationHandlerOrders(t, "test-create-flow-instance-config", "worker-flow/ti-5c59b413ad4dd4f8d1321e7a", "flow.spawned", "worker.ready", "awaiting_ready", "awaiting_spawned")
}

func TestCatalogDuplicateCreationCompletesInEitherHandlerOrder(t *testing.T) {
	requireCatalogCreationHandlerOrders(t, "test-create-flow-instance-duplicate", "worker-flow/ti-bc9c6acffc914a7ed5a2793b", "flow.spawned", "worker.ready", "awaiting_ready", "awaiting_spawned")
}

func TestCatalogAgentCreationCompletesInEitherHandlerOrder(t *testing.T) {
	requireCatalogCreationHandlerOrders(t, "test-create-flow-instance", "worker-flow/ti-878653cc40fdc8ad8e9c2d85", "flow.spawned", "worker.observed", "awaiting_observed", "awaiting_spawned")
}

func TestCatalogAutoEmitCreationCompletesInEitherHandlerOrder(t *testing.T) {
	requireCatalogCreationHandlerOrders(t, "test-auto-emit-on-create", "worker/ti-7f54a49b1f8b8765cb7c40ef", "worker.requested", "auto.processed", "requested_started", "awaiting_requested")
}

func TestCatalogDynamicCreationSettlesInEitherHandlerOrder(t *testing.T) {
	requireCatalogCreationHandlerOrders(t, "test-dynamic-flow-instance", "worker/ti-7561254fcace846571c87052", "worker.requested", "work.setup", "idle", "idle")
}

func requireCatalogCreationHandlerOrders(t *testing.T, fixtureName, workerPath, creating, completing, afterCreating, afterCompleting string) {
	t.Helper()
	claim := "catalog.runtime.flow_lifecycle"
	if fixtureName == "test-dynamic-flow-instance" {
		claim = "catalog.runtime.flow_composition"
	}
	fixture := catalogRuntimeFixture(t, claim, fixtureName)
	for _, backend := range []catalogRuntimeBackend{catalogBackendPostgres, catalogBackendSQLite} {
		for _, first := range []string{creating, completing} {
			t.Run(string(backend)+"/"+first+"_first", func(t *testing.T) {
				transcript := buildCatalogExecutionTranscript(t, fixture)
				h := newRuntimeHarnessFromTranscript(t, fixture.Root, backend, transcript.runtimeStart, transcript)
				defer h.shutdown()
				h.seedEntityFields(transcript.expected)
				waitingState, second := afterCreating, completing
				if first == completing {
					waitingState, second = afterCompleting, creating
				}
				firstCommitted := make(chan struct{})
				var once sync.Once
				observedState := make(chan string, 1)
				h.rt.Pipeline.SetTestLifecycleProbe(catalogCreationSettlementObserver(func(_ context.Context, signal lifecycleprobe.Signal) {
					if signal.Kind == lifecycleprobe.HandlerCompleted && strings.HasSuffix(signal.EventType, first) {
						once.Do(func() { close(firstCommitted) })
					}
				}))
				// Hold the other handler before target preparation, not inside a
				// state transaction. The fixture itself supplies the handshake.
				h.rt.Pipeline.SetTestWorkflowNodeHandlerStartHook(func(ctx context.Context, _ string, event events.Event) error {
					if !strings.HasSuffix(string(event.Type()), second) {
						return nil
					}
					ctx, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
					defer cancel()
					select {
					case <-firstCommitted:
						child, found, err := catalogFlowInstanceForCausalFlow(h.db, h.workflow, nil, nil, workerPath, false)
						if err != nil || !found {
							return fmt.Errorf("read first handler state: found=%t err=%v", found, err)
						}
						t.Logf("%s completed before %s target preparation; receiver state=%s", first, second, child.CurrentState)
						select {
						case observedState <- child.CurrentState:
							return nil
						default:
							return fmt.Errorf("unexpected repeated %s handler attempt", second)
						}
					case <-ctx.Done():
						return fmt.Errorf("wait for %s fixture handshake: %w", first, ctx.Err())
					}
				})
				for _, group := range transcript.groups {
					for _, step := range group.steps {
						if err := h.publishRuntimeEventResultForStep(step, catalogRuntimePublishTimeout, true); err != nil {
							t.Fatal(err)
						}
					}
					h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
				}
				h.waitForExpectedEmittedEvents(transcript.expected, catalogRuntimePublishTimeout)
				h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
				assertCatalogRuntimeOutcome(t, h, transcript.expected)
				assertCatalogReplayFixtureOutcome(t, fixture, h, transcript)
				select {
				case state := <-observedState:
					if state != waitingState {
						t.Fatalf("first handler committed state=%q, want %q", state, waitingState)
					}
				default:
					t.Fatal("second handler never observed the first handler's committed state")
				}
				transcript.requireUnchanged(t)
			})
		}
	}
}
