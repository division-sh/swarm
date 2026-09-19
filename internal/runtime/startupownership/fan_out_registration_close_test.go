package startupownership

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// Race admission with cancellation/removal repeatedly under both capacity
// policies. A returned permit is still owed even if its context is canceled.
func TestFanOutRegistrationCloseRacesTurnAdmission(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			probe, plan := testRetainedSession(t)
			capacity := SQLiteFanOutCapacity()
			if backend == "postgres" {
				var err error
				capacity, err = PostgreSQLFanOutCapacity(6, 1)
				if err != nil {
					t.Fatal(err)
				}
			}
			process, err := newProcessCapability(&fanOutCapacitySession{retainedSessionProbe: probe, capacity: capacity}, time.Hour, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer process.Release(ctx)
			work := worklifetime.NewProcess()
			defer work.Retire()
			grant, err := process.IssueGenerationGrant(ctx, GrantRequest{BundleHash: startupBundleHashA, RuntimeInstanceID: probe.authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := grant.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			occurrence, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: startupBundleHashA})
			if err != nil {
				t.Fatal(err)
			}
			defer retireFanOutTestRuntime(t, occurrence)
			for iteration := 0; iteration < 32; iteration++ {
				r, err := RegisterFanOutServing(ctx, grant, occurrence, nil)
				if err != nil {
					t.Fatal(err)
				}
				start := make(chan struct{})
				type admission struct {
					permit *FanOutTurnPermit
					found  bool
					err    error
				}
				admitted := make(chan admission, 1)
				go func() {
					<-start
					permit, found, err := r.BeginTurn(ctx)
					admitted <- admission{permit, found, err}
				}()
				closed := make(chan struct{})
				go func() { <-start; r.Close(); close(closed) }()
				close(start)
				var result admission
				select {
				case result = <-admitted:
				case <-time.After(time.Second):
					t.Fatal("turn admission deadlocked against Close")
				}
				if result.permit != nil {
					select {
					case <-closed:
						t.Error("Close escaped an admitted permit before Done")
					default:
					}
					result.permit.Done()
					result.permit.Done()
					if !result.found || result.err != nil {
						t.Errorf("ambiguous turn admission: %+v", result)
					}
				} else if result.found || result.err == nil {
					t.Errorf("lost permit or invented capacity exhaustion: %+v", result)
				}
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Fatal("Close did not join after exact permit Done")
				}
				owner := r.grant.owner
				owner.mu.Lock()
				used := owner.fanOutCapacity.used
				registrations := len(owner.fanOutCapacity.registrations)
				owner.mu.Unlock()
				if used != 0 || registrations != 0 || occurrence.ActiveCount() != 0 {
					t.Fatalf("retirement leaked ownership: used=%d registrations=%d active=%d", used, registrations, occurrence.ActiveCount())
				}
			}
		})
	}
}
