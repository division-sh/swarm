package serveapp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564H2AdmissionObservationGate struct {
	held     chan lifecycleprobe.Signal
	release  chan struct{}
	observed chan lifecycleprobe.Signal
	count    atomic.Int32
}

func (g *issue2564H2AdmissionObservationGate) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != "hub.bump" {
		return
	}
	g.held <- signal
	select {
	case <-g.release:
	case <-ctx.Done():
		return
	}
	g.count.Add(1)
	g.observed <- signal
}

func TestIssue2564H2AcknowledgmentPrecedesAdmissionObservationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			gate := &issue2564H2AdmissionObservationGate{
				held: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{}), observed: make(chan lifecycleprobe.Signal, 1),
			}
			unblock := sync.OnceFunc(func() { close(gate.release) })
			defer unblock()
			opts, start := issue2564ServeHarness(t, backend, issue2564H2Source(t), false)
			opts.TestLifecycleProbe = gate
			var owner *worklifetime.Process
			opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { owner = rt.Options.ProcessWorkOwner }
			process, served := start()
			t.Cleanup(func() {
				unblock()
				if code := process.stop(); code != 0 {
					t.Errorf("H2 observation control stop=%d", code)
				}
			})
			reader, ok := served.selected.(storetest.Issue2564WorkloadReader)
			if !ok || owner == nil {
				t.Fatalf("H2 exact observation/process owners missing: %T", served.selected)
			}
			rt := issue2564H2Fixture{Endpoint: served.Endpoint, Backend: backend, BundleHash: served.BundleHash, selected: reader}
			ctx, cancel := context.WithTimeout(t.Context(), servedProofPollDeadline)
			defer cancel()
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h05"}, "idempotency_key": "observation-start",
			})
			rt.waitDeliveries(t, seed.RunID)
			// Delivery settlement alone does not join the seed's publication log.
			// Establish zero before attributing one exact slot to the held bump.
			tick := time.NewTicker(25 * time.Millisecond)
			defer tick.Stop()
			for owner.PublicCompletionCount() != 0 {
				select {
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal("seed publication did not release its reservation")
				}
			}
			bump := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "hub.bump", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"hub_id": "h05", "n": 1}, "idempotency_key": "observation-bump",
			})
			select {
			case signal := <-gate.held:
				if signal.EventID != bump.EventID || bump.RunID != seed.RunID || gate.count.Load() != 0 || owner.PublicCompletionCount() != 1 {
					t.Fatalf("early ACK lost exact unobserved responsibility: signal=%+v result=%+v observations=%d slots=%d", signal, bump, gate.count.Load(), owner.PublicCompletionCount())
				}
			case <-ctx.Done():
				t.Fatal("held H2 post-ACK observation not reached")
			}
			unblock()
			select {
			case signal := <-gate.observed:
				if signal.EventID != bump.EventID || gate.count.Load() != 1 {
					t.Fatalf("H2 eventual observation is not exact: %+v count=%d", signal, gate.count.Load())
				}
			case <-ctx.Done():
				t.Fatal("accepted H2 observation did not finish")
			}
			rt.waitDeliveries(t, seed.RunID)
			process.stop()
			if gate.count.Load() != 1 || owner.PublicCompletionCount() != 0 {
				t.Fatalf("H2 observer duplicated or leaked its reservation: observations=%d slots=%d", gate.count.Load(), owner.PublicCompletionCount())
			}
		})
	}
}
