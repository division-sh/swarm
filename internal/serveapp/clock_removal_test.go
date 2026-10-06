package serveapp

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedClockDeclarationRemovalRetiresLastBindingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyClockDeployment(t, false)
			opts, start := clockDeploymentHarness(t, backend, root)
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			first, served := start()
			rt := servedTestProcessRuntime(t, first)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("clock removal precondition=%+v err=%v", statuses, err)
			}
			initial := readServedClockHeader(t, served.Endpoint, statuses[0].RunID).ClockSchedules[0]
			if code := first.stop(); code != 0 {
				t.Fatalf("clock source-edit shutdown=%d", code)
			}
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: clock-export\nstages: []\n")
			if err := os.Remove(filepath.Join(root, "events.yaml")); err != nil {
				t.Fatal(err)
			}
			barrier := &clockBootBarrier{entered: make(chan struct{}, 1), release: make(chan struct{})}
			barrier.unblock()
			opts.TestLifecycleProbe = barrier
			var countClocks func() (int, error)
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
				countClocks = func() (int, error) { return storetest.CountInstanceClockActivations(t.Context(), p.deps.EventStore) }
			})
			second, current := start()
			removed := readServedClockHeader(t, current.Endpoint, initial.RunID).ClockSchedules[0]
			if removed.ActivationID != initial.ActivationID || !removed.InitialDueAt.Equal(initial.InitialDueAt) || removed.Status != genericschedule.StatusCancelled || removed.NextDueAt != nil || removed.RetainsRun || removed.CancelCause != "standing_declaration_removed" {
				t.Fatalf("removed binding erased/rearmed historical clock: before=%+v after=%+v", initial, removed)
			}
			time.Sleep(550 * time.Millisecond)
			barrier.mu.Lock()
			published := append([]lifecycleprobe.Signal(nil), barrier.clocks...)
			barrier.mu.Unlock()
			if len(published) != 0 {
				t.Fatalf("removed declaration still emitted: %+v", published)
			}
			if count, err := countClocks(); err != nil || count != 1 {
				t.Fatalf("removal minted/erased an activation: count=%d err=%v", count, err)
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("removed clock shutdown=%d\n%s", code, second.outputString())
			}
		})
	}
}
