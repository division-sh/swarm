package serveapp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type clockBootBarrier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	clocks  []lifecycleprobe.Signal
}

func (b *clockBootBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *clockBootBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.EventPersisted {
		return
	}
	if signal.EventType == "poll.tick" || signal.EventType == "clock/poll.tick" {
		b.mu.Lock()
		b.clocks = append(b.clocks, signal)
		b.mu.Unlock()
	}
	if signal.EventType != "platform.boot" {
		return
	}
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}

func TestServedClockStartupAndFiniteHostFrontierBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, nested := range []bool{false, true} {
			for _, finite := range []bool{false, true} {
				name := backend + map[bool]string{false: "/root", true: "/child"}[nested] + map[bool]string{false: "/serve", true: "/finite"}[finite]
				t.Run(name, func(t *testing.T) {
					opts, _ := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, nested))
					opts.LocalRun = finite
					barrier := &clockBootBarrier{entered: make(chan struct{}, 1), release: make(chan struct{})}
					opts.TestLifecycleProbe = barrier
					var countClocks func() (int, error)
					captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
						countClocks = func() (int, error) {
							return storetest.CountInstanceClockActivations(t.Context(), persistence.deps.EventStore)
						}
					})
					process := startServeRuntimeTestProcess(t, *opts)
					// Unblock before the process cleanup attempts to join boot.
					t.Cleanup(barrier.unblock)
					select {
					case <-barrier.entered:
					case <-time.After(10 * time.Second):
						t.Fatalf("real startup did not reach held boot frontier:\n%s", process.outputString())
					}
					if count, err := countClocks(); err != nil || count != 0 {
						t.Fatalf("startup admitted clocks before releasing its frontier: count=%d err=%v", count, err)
					}
					// Hold longer than two declaration intervals. The already-running
					// runtime must not publish before deployment admission.
					time.Sleep(550 * time.Millisecond)
					barrier.mu.Lock()
					premature := append([]lifecycleprobe.Signal(nil), barrier.clocks...)
					barrier.mu.Unlock()
					if len(premature) != 0 {
						t.Fatalf("clock escaped the startup frontier: %+v", premature)
					}
					barrier.unblock()
					process.waitForReadyLine()
					want := 1
					if finite {
						want = 0
					}
					if count, err := countClocks(); err != nil || count != want {
						t.Fatalf("entrypoint binding count=%d want=%d err=%v", count, want, err)
					}
					if !finite {
						endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
						var page struct {
							Runs []operatorread.RunHeader `json:"runs"`
						}
						requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 100}, &page)
						generations := 0
						for _, run := range page.Runs {
							if string(run.Origin.Kind()) == "standing_generation" {
								generations++
							}
						}
						if generations != 1 {
							t.Fatalf("deployment frontier did not publish one generation: %+v", page)
						}
					}
					if code := process.stop(); code != 0 {
						t.Fatalf("frontier shutdown code=%d\n%s", code, process.outputString())
					}
				})
			}
		}
	}
}
