package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Only the gate decision is controlled. Publication, per-event claims,
// predecessor settlement, continuation, and singleton recovery are real owners.
func TestB07GroupQueuedAndBlockedMembersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"ingress_queued", "run_queued", "ingress_paused_error", "run_blocked_error"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				f, _ := newCollectorFixtureWithCardinality(t, backend, 3)
				gate := &b07SecondMemberGate{}
				if kind == "ingress_paused_error" {
					gate.err = bus.ErrRuntimeIngressPaused
				}
				if kind == "run_blocked_error" {
					gate.err = bus.ErrRunDispatchBlocked
				}
				if kind == "run_queued" || kind == "run_blocked_error" {
					f.bus.SetRunDispatchGate(gate)
				} else {
					f.bus.SetRuntimeIngressDispatchGate(gate)
				}
				interceptor := &b07CountDispatch{calls: make(map[string]int)}
				f.bus.SetInterceptors(interceptor)
				err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed)
				if !errors.Is(err, gate.err) {
					t.Fatalf("nonterminal gate cause changed: got=%v want=%v", err, gate.err)
				}
				third := 1
				if gate.err != nil {
					third = 0
				}
				for i, want := range []int{1, 0, third} {
					if got := interceptor.calls[f.events[i].ID()]; got != want {
						t.Fatalf("member%d dispatched=%d want=%d", i, got, want)
					}
					requireB07ReceiptCount(t, f, i, want)
				}
				// The healthy prefix has a durable receipt even when the middle
				// member cannot dispatch. The queued member has no receipt at all.
				f.bus.SetRunDispatchGate(nil)
				f.bus.SetRuntimeIngressDispatchGate(nil)
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				owner := f.raw.(pipelineObligationParityStore).PipelineObligations()
				for i, event := range f.events {
					if i == 0 || (i == 2 && third == 1) {
						if _, err := owner.ClaimEvent(f.ctx, event.ID(), pipelineobligation.PurposeRecovery); !errors.Is(err, pipelineobligation.ErrIneligible) {
							t.Fatalf("acknowledged member%d became replayable: %v", i, err)
						}
						continue
					}
					purpose := pipelineobligation.PurposeRecovery
					if i == 1 {
						purpose = pipelineobligation.PurposeDecisionRoute
					}
					work, err := owner.ClaimEvent(f.ctx, event.ID(), purpose)
					if err != nil {
						t.Fatalf("unacknowledged member%d recovery admission: %v", i, err)
					}
					outcome, err := f.bus.RecoverPersistedPipeline(f.ctx, work, nil)
					if err != nil || !outcome.ContinueDispatch() {
						t.Fatalf("member%d real recovery dispatch=%+v err=%v", i, outcome, err)
					}
					ack, err := owner.Settle(f.ctx, work.Claim, pipelineobligation.Acknowledged("pipeline_persisted"))
					if err != nil || !ack.Committed() {
						t.Fatalf("member%d singleton recovery settlement=%+v err=%v", i, ack, err)
					}
				}
				for i, event := range f.events {
					if interceptor.calls[event.ID()] != 1 {
						t.Fatalf("member%d did not execute exactly once: %v", i, interceptor.calls)
					}
					requireB07ReceiptCount(t, f, i, 1)
				}
			})
		}
	}
}

type b07SecondMemberGate struct {
	calls int
	err   error
}

func (g *b07SecondMemberGate) QueueableIngressPaused(context.Context) (bool, error) {
	g.calls++
	if g.calls == 2 {
		return g.err == nil, g.err
	}
	return false, nil
}

func (g *b07SecondMemberGate) QueueableRunDispatchBlocked(ctx context.Context, _ string) (bool, error) {
	return g.QueueableIngressPaused(ctx)
}

type b07CountDispatch struct{ calls map[string]int }

func (p *b07CountDispatch) Intercept(_ context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	p.calls[event.ID()]++
	return true, nil, pipelineobligation.Continue(), nil
}

func requireB07ReceiptCount(t *testing.T, f *fanOutGroupHistoryFixture, member, want int) {
	t.Helper()
	var count int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, f.events[member].ID()).Scan(&count); err != nil || count != want {
		t.Fatalf("member%d platform receipts=%d want=%d err=%v", member, count, want, err)
	}
}
