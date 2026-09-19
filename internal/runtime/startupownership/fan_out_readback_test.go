package startupownership

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestFanOutRuntimeReadbackUsesExecutionOwnerAndFullHandoffTiming(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	f := newServingFixture(t, 1, release, servingProbeControls{commitBeforeRelease: true})
	key := f.append(t, 0, uuid.NewString())
	f.registrations[0].Wake()
	f.waitStarted(t, time.Second)
	select {
	case <-f.committed:
	case <-time.After(time.Second):
		t.Fatal("store commit acknowledgement was not reached")
	}
	grant, err := f.grants[0].Evidence()
	if err != nil {
		t.Fatal(err)
	}
	otherKey := key
	otherKey.TriggeringDeliveryID = uuid.NewString()
	f.store.mu.Lock()
	f.store.executionRows = map[fanoutobligation.IntentKey]FanOutExecutionObservation{
		key:      {Key: key, GrantID: grant.GrantID, Eligible: false, Reason: "run_paused"},
		otherKey: {Key: otherKey, GrantID: grant.GrantID, Eligible: false, Reason: "retry_wait"},
	}
	f.store.mu.Unlock()
	page := fanoutobligation.ListPage{RunID: key.RunID, Intents: []fanoutobligation.IntentReadback{
		{Key: key, BundleHash: grant.BundleHash, DurableState: "eligible", Runtime: fanoutobligation.UnavailableRuntimeReadback()},
		{Key: otherKey, BundleHash: grant.BundleHash, DurableState: "eligible", Runtime: fanoutobligation.UnavailableRuntimeReadback()},
	}}
	readback, err := ObserveFanOutRuntimePage(context.Background(), f.process, page)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range readback.Intents {
		if row.Runtime.Availability != "available" || row.Runtime.Eligible == nil || *row.Runtime.Eligible || row.Runtime.ActiveWorkers == nil || *row.Runtime.ActiveWorkers != 1 || row.Runtime.LastCommitMS != nil {
			t.Fatalf("execution eligibility or unfinished timing was fabricated from durable state: %+v", row.Runtime)
		}
		if row.Runtime.ObservedAt == nil || row.Runtime.Validate() != nil {
			t.Fatalf("invalid separately observed runtime facts: %+v", row.Runtime)
		}
	}
	// The store commit has returned, but its required handoff is still held.
	// A reported duration cannot stop at the SQL commit acknowledgement.
	time.Sleep(30 * time.Millisecond)
	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(time.Second)
	for {
		readback, err = ObserveFanOutRuntimePage(context.Background(), f.process, page)
		if err != nil {
			t.Fatal(err)
		}
		if readback.Intents[0].Runtime.LastCommitMS != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed commit/handoff timing unavailable")
		}
		time.Sleep(time.Millisecond)
	}
	if *readback.Intents[0].Runtime.LastCommitMS < 25 || readback.Intents[1].Runtime.LastCommitMS != nil {
		t.Fatalf("handoff time lost or another intent borrowed latency: %+v", readback.Intents)
	}
	if page.Intents[0].Runtime.Availability != "unavailable" {
		t.Fatal("runtime projection mutated its caller's durable page")
	}
	hostile := page
	hostile.Intents = append([]fanoutobligation.IntentReadback{}, page.Intents...)
	hostile.Intents[0].BundleHash = startupBundleHashB
	if _, err := ObserveFanOutRuntimePage(context.Background(), f.process, hostile); err == nil {
		t.Fatal("foreign bundle borrowed a live intent's execution metrics")
	}
	if err := f.process.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	readback, err = ObserveFanOutRuntimePage(context.Background(), f.process, page)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range readback.Intents {
		if row.Runtime.Availability != "retired" || row.Runtime.LastCommitMS != nil || row.Runtime.Eligible != nil || row.Runtime.ObservedAt != nil {
			t.Fatalf("retired occurrence leaked available metrics: %+v", row.Runtime)
		}
	}
}
