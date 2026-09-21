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
	fixture := catalogRuntimeFixture(t, "catalog.runtime.flow_lifecycle", "test-create-flow-instance-config")
	for _, backend := range []catalogRuntimeBackend{catalogBackendPostgres, catalogBackendSQLite} {
		for _, first := range []string{"flow.spawned", "worker.ready"} {
			t.Run(string(backend)+"/"+first+"_first", func(t *testing.T) {
				transcript := buildCatalogExecutionTranscript(t, fixture)
				h := newRuntimeHarnessFromTranscript(t, fixture.Root, backend, transcript.runtimeStart, transcript)
				defer h.shutdown()
				h.seedEntityFields(transcript.expected)
				waitingState, second := "awaiting_ready", "worker.ready"
				if first == "worker.ready" {
					waitingState, second = "awaiting_spawned", "flow.spawned"
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
						child, found, err := catalogFlowInstanceForCausalFlow(h.db, h.workflow, nil, nil, "worker-flow/ti-5c59b413ad4dd4f8d1321e7a", false)
						if err != nil || !found {
							return fmt.Errorf("read first handler state: found=%t err=%v", found, err)
						}
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
				}
				h.waitForExpectedEmittedEvents(transcript.expected, catalogRuntimePublishTimeout)
				h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
				assertCatalogRuntimeOutcome(t, h, transcript.expected)
				assertCatalogReplayFixtureOutcome(t, fixture, h)
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
