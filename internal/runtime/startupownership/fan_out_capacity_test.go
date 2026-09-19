package startupownership

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type fanOutCapacitySession struct {
	*retainedSessionProbe
	capacity FanOutCapacity
}

func (s *fanOutCapacitySession) FanOutServingCapacity() (FanOutCapacity, error) {
	return s.capacity, nil
}

func TestFanOutCapacityUsesUsablePoolWithoutClamping(t *testing.T) {
	postgres, err := PostgreSQLFanOutCapacity(7, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		capacity  FanOutCapacity
		requested *int
		want      int
	}{
		{"sqlite default", SQLiteFanOutCapacity(), nil, 1},
		{"sqlite explicit", SQLiteFanOutCapacity(), fanOutWorkers(1), 1},
		{"sqlite two", SQLiteFanOutCapacity(), fanOutWorkers(2), 0},
		{"postgres default with two reservations", postgres, nil, 4},
		{"postgres override", postgres, fanOutWorkers(2), 2},
		{"postgres consumes headroom", postgres, fanOutWorkers(5), 0},
		{"postgres zero", postgres, fanOutWorkers(0), 0},
		{"postgres negative", postgres, fanOutWorkers(-1), 0},
		{"sqlite zero", SQLiteFanOutCapacity(), fanOutWorkers(0), 0},
		{"missing evidence", FanOutCapacity{}, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.capacity.resolve(tc.requested)
			if got != tc.want || (err != nil) != (tc.want == 0) {
				t.Fatalf("workers=%d err=%v, want %d", got, err, tc.want)
			}
		})
	}
	for _, counts := range [][2]int{{-1, 0}, {2, 2}, {3, -1}} {
		if _, err := PostgreSQLFanOutCapacity(counts[0], counts[1]); err == nil {
			t.Fatalf("accepted unprovable usable capacity: %v", counts)
		}
	}
	unlimited, err := PostgreSQLFanOutCapacity(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if workers, err := unlimited.resolve(nil); err != nil || workers != 4 {
		t.Fatalf("unlimited connection pool must still have bounded workers: %d %v", workers, err)
	}
}

func fanOutWorkers(n int) *int { return &n }

func TestFanOutCapacityIsSharedAcrossExactRuntimeGrants(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx := context.Background()
			probe, _ := testRetainedSession(t)
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: startupBundleHashA}, {BundleHash: startupBundleHashB}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			probe.plan = plan
			capacity := SQLiteFanOutCapacity()
			if workers == 4 {
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
			defer func() {
				work.Retire()
				deadline, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				if _, err := work.Join(deadline); err != nil {
					t.Error(err)
				}
			}()
			var registrations []*FanOutServingRegistration
			var grants []LiveGenerationGrant
			for _, bundle := range []string{startupBundleHashA, startupBundleHashB} {
				grant, err := process.IssueGenerationGrant(ctx, GrantRequest{BundleHash: bundle, RuntimeInstanceID: probe.authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := grant.AdmitExecution(ctx); err != nil {
					t.Fatal(err)
				}
				occurrence, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: bundle})
				if err != nil {
					t.Fatal(err)
				}
				defer retireFanOutTestRuntime(t, occurrence)
				registration, err := RegisterFanOutServing(ctx, grant, occurrence, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer registration.Close()
				registrations = append(registrations, registration)
				grants = append(grants, grant)
				if _, err := RegisterFanOutServing(ctx, grant, occurrence, nil); err == nil {
					t.Fatal("duplicate registration accepted")
				}
			}
			var permits []*FanOutTurnPermit
			for index := 0; index < workers; index++ {
				permit, found, err := registrations[index%2].BeginTurn(ctx)
				if err != nil || !found {
					t.Fatalf("capacity %d: found=%v err=%v", index, found, err)
				}
				permits = append(permits, permit)
				defer permit.Done()
			}
			for _, registration := range registrations {
				if _, found, err := registration.BeginTurn(ctx); err != nil || found {
					t.Fatalf("budget multiplied per source: found=%v err=%v", found, err)
				}
			}
			permits[0].Done()
			permits[0].Done()
			replacement, found, err := registrations[1].BeginTurn(ctx)
			if err != nil || !found {
				t.Fatalf("released permit not reusable across source: %v", err)
			}
			defer replacement.Done()
			if _, found, err := registrations[0].BeginTurn(ctx); err != nil || found {
				t.Fatalf("double Done created capacity: found=%v err=%v", found, err)
			}
			if err := grants[1].Retire(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-replacement.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("retirement did not cancel exact generation turn")
			}
			if _, _, err := registrations[1].BeginTurn(ctx); err == nil {
				t.Fatal("retired registration admitted new work")
			}
			// Retirement cancels work; it must not manufacture free capacity before
			// the admitted work has completed its disposition and released its permit.
			if _, found, err := registrations[0].BeginTurn(ctx); err != nil || found {
				t.Fatalf("retirement prematurely recycled permit: found=%v err=%v", found, err)
			}
			replacement.Done()
			last, found, err := registrations[0].BeginTurn(ctx)
			if err != nil || !found {
				t.Fatalf("healthy source could not use retired source's settled permit: %v", err)
			}
			last.Done()
		})
	}
}

func TestFanOutRegistrationRequiresExactOccurrenceAndAdmittedSource(t *testing.T) {
	ctx := context.Background()
	probe, plan := testRetainedSession(t)
	process, err := newProcessCapability(&fanOutCapacitySession{retainedSessionProbe: probe, capacity: SQLiteFanOutCapacity()}, time.Hour, time.Second)
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
	occurrence, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: startupBundleHashA})
	if err != nil {
		t.Fatal(err)
	}
	defer retireFanOutTestRuntime(t, occurrence)
	if _, err := RegisterFanOutServing(ctx, grant, occurrence, nil); err == nil {
		t.Fatal("prepared grant admitted execution")
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterFanOutServing(ctx, grant, occurrence, nil); err == nil {
		t.Fatal("probe-settled grant admitted execution")
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	wrong, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: startupBundleHashB})
	if err != nil {
		t.Fatal(err)
	}
	defer retireFanOutTestRuntime(t, wrong)
	if _, err := RegisterFanOutServing(ctx, grant, wrong, nil); err == nil {
		t.Fatal("foreign source occurrence admitted")
	}
	type runtimeContextKey struct{}
	runtimeCtx := context.WithValue(ctx, runtimeContextKey{}, "registered-source")
	registration, err := RegisterFanOutServing(runtimeCtx, grant, occurrence, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Close()
	foreignCtx, cancelCaller := context.WithCancel(context.WithValue(ctx, runtimeContextKey{}, "foreign-selector-source"))
	permit, available, err := registration.BeginTurn(foreignCtx)
	if err != nil || !available {
		t.Fatalf("admit context-preservation turn: %v", err)
	}
	if got := permit.Context().Value(runtimeContextKey{}); got != "registered-source" {
		t.Fatalf("shared selector replaced runtime context: %v", got)
	}
	cancelCaller()
	select {
	case <-permit.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("selector cancellation did not reach finite turn")
	}
	permit.Done()
	probe.mu.Lock()
	probe.plan.Revision = "superseding-source-set"
	probe.mu.Unlock()
	if _, _, err := registration.BeginTurn(ctx); err == nil {
		t.Fatal("stale source generation received a permit")
	}
	if occurrence.ActiveCount() != 1 {
		t.Fatalf("failed turn leaked work: active=%d", occurrence.ActiveCount())
	}
}

func retireFanOutTestRuntime(t *testing.T, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := occurrence.RetireAndWait(ctx); err != nil {
		t.Error(err)
	}
}
