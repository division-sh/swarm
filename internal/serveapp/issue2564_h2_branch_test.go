package serveapp

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2564H2BranchGate struct {
	next    atomic.Int32
	held    [2]chan lifecycleprobe.Signal
	release [2]chan struct{}
	unblock [2]func()
}

func newIssue2564H2BranchGate() *issue2564H2BranchGate {
	g := &issue2564H2BranchGate{}
	for i := range g.held {
		g.held[i], g.release[i] = make(chan lifecycleprobe.Signal, 1), make(chan struct{})
		g.unblock[i] = sync.OnceFunc(func() { close(g.release[i]) })
	}
	return g
}

func (g *issue2564H2BranchGate) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != "platform.stage_timer" {
		return
	}
	i := g.next.Add(1) - 1
	if i >= int32(len(g.held)) {
		return
	}
	// This single-hub proof holds only its first two committed occurrences,
	// before dispatch and outside the transaction/entity lock. Keep the real
	// callback context: neither the authored interval nor its deadline changes.
	g.held[i] <- signal
	select {
	case <-g.release[i]:
	case <-ctx.Done():
	}
}

func (g *issue2564H2BranchGate) wait(t *testing.T, i int) lifecycleprobe.Signal {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	select {
	case signal := <-g.held[i]:
		return signal
	case <-ctx.Done():
		t.Fatalf("H2 exact branch occurrence %d not reached: %v", i, ctx.Err())
		return lifecycleprobe.Signal{}
	}
}

func TestIssue2564H2SynchronizedBranchesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2564H2Source(t)
			keys := issue2564H2DeclarationKeys(t, root)
			gate := newIssue2564H2BranchGate()
			opts, start := issue2564ServeHarness(t, backend, root, false)
			opts.TestLifecycleProbe = gate
			process, served := start()
			t.Cleanup(func() {
				for _, unblock := range gate.unblock {
					unblock()
				}
				if code := process.stop(); code != 0 {
					t.Errorf("H2 synchronized serve stop=%d", code)
				}
			})
			reader, ok := served.selected.(storetest.Issue2564WorkloadReader)
			if !ok {
				t.Fatalf("H2 native observation owner missing: %T", served.selected)
			}
			rt := issue2564H2Fixture{Endpoint: served.Endpoint, Backend: backend, BundleHash: served.BundleHash, selected: reader, cuts: map[issue2564H2CutCoordinate]issue2564H2CutWitness{}}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h05"}, "idempotency_key": "branch-start"})
			accepted := map[string]string{}
			var previous string
			for i, stage := range []string{"s1", "s2"} {
				signal := gate.wait(t, i)
				before, err := issue2564H2Read(t.Context(), rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				hub := before.Hubs["h05"]
				event := before.Events[signal.EventID]
				timer := before.Timers[event.Occurrence.Activation.ActivationID]
				if len(before.Hubs) != 1 || hub.Stage != stage || event.ID == "" || event.Outcome != "" || timer.Status != "fired" || timer.Fired.IsZero() || timer.Entity != hub.Entity || timer.Instance != hub.Instance || timer.Ref.DeclarationKey != keys[stage] {
					t.Fatalf("H2 branch lacks exact committed entry/held occurrence: stage=%s hub=%+v event=%+v timer=%+v", stage, hub, event, timer)
				}
				if i == 1 && (before.Events[previous].Outcome != "success" || len(hub.History) != 1 || hub.History[0].TriggerEventID != previous || hub.History[0].From != "s1" || hub.History[0].To != "s2") {
					t.Fatalf("H2 second branch did not follow the first exact occurrence: %+v", hub)
				}
				bump := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.bump", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": "h05", "n": i}, "idempotency_key": "branch-" + stage})
				accepted[bump.EventID] = "h05"
				rt.waitDeliveries(t, seed.RunID)
				after, err := issue2564H2Read(t.Context(), rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				hub = after.Hubs["h05"]
				if hub.Stage != stage || hub.Count != int64(i+1) || hub.C1 != 1 || hub.C2 != int64(i) || after.Events[signal.EventID].Outcome != "" {
					t.Fatalf("H2 synchronized branch effect missing or escaped held stage: %+v", hub)
				}
				rows, err := storetest.ObserveH2CounterMutations(t.Context(), rt.selected, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if err := issue2564H2ValidateCounters(rows, accepted, after, int64(i+1)); err != nil {
					t.Fatal(err)
				}
				issue2564H2CommittedStageEvidence(t, rt, seed.RunID, accepted)
				t.Logf("H2 synchronized branch=%s event=%s held_occurrence=%s count=%d c1=%d c2=%d", stage, bump.EventID, signal.EventID, hub.Count, hub.C1, hub.C2)
				previous = signal.EventID
				gate.unblock[i]()
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.close", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": "h05"}, "idempotency_key": "branch-close"})
			rt.waitDeliveries(t, seed.RunID)
			closed, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil || closed.Hubs["h05"].Stage != "closed" {
				t.Fatalf("H2 explicit close changed branch evidence: %+v err=%v", closed, err)
			}
			if err := issue2564H2BranchCoverage(closed.Hubs["h05"]); err != nil {
				t.Fatal(err)
			}
			for _, timer := range closed.Timers {
				if timer.Status == "active" {
					t.Fatal("H2 explicit close retained active timer")
				}
			}
			issue2564H2Deliveries(t, rt, seed.RunID, accepted, 4)
		})
	}
}

func TestIssue2564H2CounterOracleSingleStageAndHostileEvidence(t *testing.T) {
	accepted := map[string]string{}
	var rows []storetest.H2CounterMutationsEvidence
	for n := 0; n < 150; n++ {
		event := fmt.Sprintf("bump-%d", n)
		accepted[event] = "h05"
		for _, path := range []string{"count", "c2"} {
			row := storetest.H2CounterMutationsEvidence{Entity: "entity", Path: path, Cause: event}
			row.Before.String, row.Before.Valid = fmt.Sprint(n), true
			row.After.String, row.After.Valid = fmt.Sprint(n+1), true
			rows = append(rows, row)
		}
	}
	hub := issue2564H2Hub{ID: "h05", Entity: "entity", Count: 150, C2: 150}
	snapshot := issue2564H2Snapshot{Hubs: map[string]issue2564H2Hub{"h05": hub}}
	if err := issue2564H2ValidateCounters(rows, accepted, snapshot, 150); err != nil {
		t.Fatal("valid single-stage trace rejected:", err)
	}
	for _, name := range []string{"missing_stage_effect", "duplicate_stage_effect", "missing_count", "foreign_receiver", "disconnected_history", "wrong_total", "absent_branch_coverage"} {
		t.Run(name, func(t *testing.T) {
			probe, changed := slices.Clone(rows), hub
			switch name {
			case "missing_stage_effect":
				probe = slices.Delete(probe, 1, 2)
			case "duplicate_stage_effect":
				probe = append(probe, probe[1])
			case "missing_count":
				probe = slices.Delete(probe, 0, 1)
			case "foreign_receiver":
				probe[0].Entity = "foreign"
			case "disconnected_history":
				probe[0].Before.String, probe[0].After.String = "150", "151"
			case "wrong_total":
				changed.C1 = 1
			case "absent_branch_coverage":
				if issue2564H2BranchCoverage(changed) == nil {
					t.Fatal("single-stage stress evidence substituted for synchronized coverage")
				}
				return
			}
			if issue2564H2ValidateCounters(probe, accepted, issue2564H2Snapshot{Hubs: map[string]issue2564H2Hub{"h05": changed}}, 150) == nil {
				t.Fatal("invalid exact counter evidence accepted")
			}
		})
	}
	if err := issue2564H2BranchCoverage(issue2564H2Hub{Count: 2, C1: 1, C2: 1}); err != nil {
		t.Fatal(err)
	}
}

func issue2564H2BranchCoverage(hub issue2564H2Hub) error {
	if hub.Count != 2 || hub.C1 != 1 || hub.C2 != 1 {
		return fmt.Errorf("H2 synchronized proof lacks one real effect from each branch: %+v", hub)
	}
	return nil
}
