package runtimepersistence

import (
	"context"
	"errors"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"

	"github.com/division-sh/swarm/internal/runtime/bus"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestB22GroupMixedCandidateWakeAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			const count = 16
			f, _ := newCollectorFixtureWithCardinality(t, backend, count)
			if bootstrap, err := f.bus.ReleaseRunQueue(f.ctx, f.runID, count+2); err != nil || bootstrap.Settled != 1 || !bootstrap.Exhausted {
				t.Fatalf("fixture trigger recovery=%+v err=%v", bootstrap, err)
			}
			interceptor := &b22MixedOutcomes{ordinals: map[string]int{}, visits: map[string]int{}}
			for i, event := range f.events {
				interceptor.ordinals[event.ID()] = i
			}
			f.bus.SetInterceptors(interceptor)
			f.bus.SetRuntimeIngressDispatchGate(&b22PrefixGate{stopAt: count/2 + 1})
			if err := f.bus.DispatchFanOutPublications(f.ctx, f.group, f.committed); !errors.Is(err, bus.ErrRuntimeIngressPaused) {
				t.Fatalf("retained incomplete suffix boundary: %v", err)
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			f.bus.SetRuntimeIngressDispatchGate(nil)
			if result, err := f.bus.ReleaseRunQueue(f.ctx, f.runID, count); err != nil || result.Settled != count/2 {
				t.Fatalf("exact canonical suffix recovery=%+v err=%v", result, err)
			}
			if err := f.bus.WaitForQuiescence(f.ctx); err != nil {
				t.Fatal(err)
			}
			if result, err := f.bus.ReleaseRunQueue(f.ctx, f.runID, count); err != nil || result.Settled != 0 || !result.Exhausted {
				t.Fatalf("duplicate recovery=%+v err=%v", result, err)
			}
			for i, event := range f.events {
				if interceptor.visits[event.ID()] != 1 {
					t.Fatalf("member%d visits=%d", i, interceptor.visits[event.ID()])
				}
				want := "success"
				if i%2 == 0 {
					want = "dead_letter"
				}
				var got string
				if err := f.db.QueryRowContext(f.ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, event.ID()).Scan(&got); err != nil || got != want {
					t.Fatalf("exact member%d receipt=%s want=%s err=%v", i, got, want, err)
				}
			}
			store := f.raw.(runLifecycleCandidateParityStore)
			snapshot, err := store.LoadRunLifecycleSnapshot(f.ctx, f.runID)
			if err != nil || snapshot.Status != string(runlifecycle.StateRunning) || snapshot.EndedAt != nil {
				t.Fatalf("terminal failures were misreported as successful completion=%+v err=%v", snapshot, err)
			}
			failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassComputeFailure, "b22_member_rejected", "b22", "dispatch", nil))
			terminal, mutation, err := store.MarkTerminalRun(f.ctx, runlifecycle.TerminalRequest{RunID: f.runID, State: runlifecycle.StateFailed, Failure: &failure, EndedAt: time.Now().UTC()})
			if err != nil || mutation != runlifecycle.MutationApplied || terminal.State != runlifecycle.StateFailed || terminal.EndedAt == nil {
				t.Fatalf("exact failure terminality=%+v mutation=%s err=%v", terminal, mutation, err)
			}
		})
	}
}

type b22PrefixGate struct{ calls, stopAt int }

func (g *b22PrefixGate) QueueableIngressPaused(context.Context) (bool, error) {
	g.calls++
	if g.calls == g.stopAt {
		return false, bus.ErrRuntimeIngressPaused
	}
	return false, nil
}

type b22MixedOutcomes struct{ ordinals, visits map[string]int }

func (p *b22MixedOutcomes) Intercept(_ context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	ordinal, exists := p.ordinals[event.ID()]
	if !exists {
		return false, nil, pipelineobligation.Continue(), errors.New("unexpected candidate-proof event")
	}
	p.visits[event.ID()]++
	if ordinal%2 == 0 {
		failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassComputeFailure, "b22_member_rejected", "b22", "dispatch", nil))
		return false, nil, pipelineobligation.DeadLetterExecution("b22_member_rejected", &failure), nil
	}
	return true, nil, pipelineobligation.Continue(), nil
}
