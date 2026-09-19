package runtimepersistence

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type b09SingletonDispatchGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (g *b09SingletonDispatchGate) Intercept(ctx context.Context, _ events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	g.calls.Add(1)
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
		return true, nil, pipelineobligation.Continue(), nil
	case <-ctx.Done():
		return false, nil, pipelineobligation.Continue(), ctx.Err()
	}
}

func TestB09IndependentSingletonPublicationSurfacesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"sync", "async", "outbox"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				// Reuse admitted source/store setup, not the fixture's group.
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				gate := &b09SingletonDispatchGate{started: make(chan struct{}), release: make(chan struct{})}
				f.bus.SetInterceptors(gate)
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(gate.release) }) }
				t.Cleanup(release)
				collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(restore)
				intent := engine.EmitIntent{Event: f.events[0]}
				if surface == "outbox" {
					plans, err := f.bus.PrepareEnginePublications(f.ctx, []engine.EmitIntent{intent})
					if err != nil || len(plans) != 1 {
						t.Fatalf("ordinary outbox preparation: %d plans err=%v", len(plans), err)
					}
					plan := plans[0].(bus.EnginePublicationPlan)
					committed, err := f.raw.(publicationRevisionProofStore).CommitPublication(f.ctx, plan.PublicationCommand())
					if err != nil {
						t.Fatal(err)
					}
					evidence, err := bus.NewCommittedEnginePublication(plan, committed)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.bus.FinalizeEnginePublications(f.ctx, []engine.CommittedDurablePublication{evidence}); err != nil {
						t.Fatal(err)
					}
				}
				published, joined := make(chan error, 1), make(chan struct{})
				go func() {
					defer close(joined)
					var err error
					switch surface {
					case "sync":
						err = f.bus.Publish(f.ctx, f.events[0])
					case "async":
						err = f.bus.PublishAcknowledged(f.ctx, f.events[0])
					case "outbox":
						err = f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, []engine.EmitIntent{intent})
					}
					published <- err
				}()
				t.Cleanup(func() {
					release()
					select {
					case <-joined:
					case <-time.After(5 * time.Second):
						t.Error("singleton publication did not join")
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := f.bus.WaitForQuiescence(ctx); err != nil {
						t.Error(err)
					}
				})
				select {
				case <-gate.started:
				case <-f.ctx.Done():
					t.Fatal("singleton did not reach real postcommit dispatch")
				}
				if surface == "async" {
					if err := receiveGroupProof(t, published); err != nil {
						t.Fatal(err)
					}
				}
				var events, receipts int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, f.events[0].ID()).Scan(&events); err != nil || events != 1 {
					t.Fatalf("dispatch preceded publication: count=%d err=%v", events, err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, f.events[0].ID()).Scan(&receipts); err != nil || receipts != 0 {
					t.Fatalf("held dispatch already acknowledged: count=%d err=%v", receipts, err)
				}
				if _, err := f.store().ClaimEvent(f.ctx, f.events[0].ID(), pipelineobligation.PurposeRecovery); !errors.Is(err, pipelineobligation.ErrBusy) {
					t.Fatalf("singleton lost exact publication exclusion: %v", err)
				}
				release()
				if surface != "async" {
					if err := receiveGroupProof(t, published); err != nil {
						t.Fatal(err)
					}
				}
				if err := f.bus.WaitForQuiescence(f.ctx); err != nil {
					t.Fatal(err)
				}
				mixedAssertReceipt(t, f, f.events[0].ID(), "success", "pipeline_persisted")
				counts := collector.Snapshot()
				if gate.calls.Load() != 1 || counts.ByOperation[transactiontest.PipelineSettlement].WriteCommits != 1 || counts.Active != 0 {
					t.Fatalf("ordinary singleton dispatch/settlement changed: calls=%d counts=%+v", gate.calls.Load(), counts)
				}
				if _, err := f.store().ClaimEvent(f.ctx, f.events[0].ID(), pipelineobligation.PurposeRecovery); !errors.Is(err, pipelineobligation.ErrIneligible) {
					t.Fatalf("settled singleton remained replayable: %v", err)
				}
				before := f.snapshot(t)
				if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, []engine.EmitIntent{intent}); err != nil {
					t.Fatalf("duplicate dispatch: %v", err)
				}
				f.unchanged(t, before)
				if gate.calls.Load() != 1 {
					t.Fatal("duplicate singleton dispatch reentered interceptor")
				}
			})
		}
	}
}
