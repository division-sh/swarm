package runtimepersistence

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Hold the real owner's acknowledged result before the collector marks its
// local proxies released. This is not an uncertain database commit.
type b10AcknowledgedGroupPause struct {
	pipelineobligation.PublicationGroup
	committed chan pipelineobligation.PublicationGroupOutcome
	resume    chan struct{}
}

func (g *b10AcknowledgedGroupPause) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	out, err := g.PublicationGroup.Settle(ctx, members)
	g.committed <- out
	<-g.resume
	return out, err
}

func TestB10GroupCommittedCollectorOwnsCallbacksAcrossResetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newB12CollectorFixture(t, backend)
			probe := &b12DeferredInterceptor{calls: map[string]int{}}
			f.bus.SetInterceptors(probe)
			group := &b10AcknowledgedGroupPause{PublicationGroup: f.group, committed: make(chan pipelineobligation.PublicationGroupOutcome, 1), resume: make(chan struct{})}
			var once sync.Once
			resume := func() { once.Do(func() { close(group.resume) }) }
			t.Cleanup(resume)
			dispatched := make(chan error, 1)
			go func() { dispatched <- f.bus.DispatchFanOutPublications(f.ctx, group, f.committed) }()
			var outcome pipelineobligation.PublicationGroupOutcome
			select {
			case outcome = <-group.committed:
			case <-f.ctx.Done():
				t.Fatal("dispatch did not reach acknowledged group settlement")
			}
			if len(outcome.Results) != 2 {
				t.Fatalf("real settlement did not acknowledge both members: %+v", outcome)
			}
			for _, result := range outcome.Results {
				if !result.Outcome.Committed() || !result.Outcome.DeliveryHandoffCommitted() {
					t.Fatalf("missing acknowledged handoff: %+v", result)
				}
			}
			before := readP16PreservationSnapshot(t, f.db, "runs", "events", "event_deliveries", "fan_out_intents", "fan_out_outcomes", "decision_card_route_obligations")
			// The collector took these exact callbacks before dispatch. Reset
			// therefore must not reacquire or release their spent store claims.
			if err := f.bus.ResetInMemoryState(); err != nil {
				t.Errorf("reset released collector-owned committed callbacks: %v", err)
			}
			resume()
			if err := receiveGroupProof(t, dispatched); err != nil {
				t.Fatalf("reset erased acknowledged dispatch: %v", err)
			}
			for _, event := range f.events {
				if probe.calls[event.ID()] != 1 {
					t.Fatalf("event %s executed %d times", event.ID(), probe.calls[event.ID()])
				}
			}
			after := readP16PreservationSnapshot(t, f.db, "runs", "events", "event_deliveries", "fan_out_intents", "fan_out_outcomes", "decision_card_route_obligations")
			if !reflect.DeepEqual(before, after) {
				t.Fatal("reset or acknowledged handoff mutated the committed domain/history")
			}
		})
	}
}
