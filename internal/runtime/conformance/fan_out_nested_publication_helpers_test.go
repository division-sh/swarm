package conformance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type nestedTurnContextKey struct{}

type nestedPublicationEntry struct {
	turn       *nestedServingTurn
	committed  bool
	sealed     bool
	finalized  bool
	dispatched bool
}

type nestedPublicationLifetime struct {
	mu                                                                sync.Mutex
	live                                                              map[string]nestedPublicationEntry
	peak, acquired, released, returned                                int
	err                                                               error
	groupPrepared, groupSeals, groupFinalized, groupDispatched        int
	batchCalls, batchResults, batchErrors, batchRowErrors             int
	batchErrorPlans, batchEmptyResults                                int
	prepareDuration, sealDuration, finalizeDuration, dispatchDuration time.Duration
}

func (p *nestedPublicationLifetime) failLocked(format string, args ...any) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
	}
}

func (p *nestedPublicationLifetime) prepared(turn *nestedServingTurn, plans []engine.DurablePublicationPlan) {
	if turn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.live == nil {
		p.live = make(map[string]nestedPublicationEntry)
	}
	for _, plan := range plans {
		id := plan.DurablePublicationEventID()
		if _, exists := p.live[id]; exists {
			p.failLocked("publication %s acquired twice without disposal", id)
			continue
		}
		// Preserve the first over-bound observation without growing a diagnostic
		// map with the workload. This proof explicitly configures capacity one.
		if len(p.live) >= fanoutobligation.InitialChunkSize+1 {
			p.failLocked("nested retained publication bound exceeded")
			continue
		}
		p.live[id] = nestedPublicationEntry{turn: turn}
		p.acquired++
		p.peak = max(p.peak, len(p.live))
	}
}

func (p *nestedPublicationLifetime) committed(publications []engine.CommittedDurablePublication) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, publication := range publications {
		id := publication.CommittedDurablePublicationEventID()
		entry, ok := p.live[id]
		if !ok || !entry.sealed || entry.committed {
			p.failLocked("publication %s committed without exactly one live plan", id)
			continue
		}
		entry.committed = true
		p.live[id] = entry
	}
}

func (p *nestedPublicationLifetime) finish(turn *nestedServingTurn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, entry := range p.live {
		if entry.turn != turn {
			continue
		}
		if !entry.committed || !entry.finalized || !entry.dispatched {
			// Do not manufacture a release on uncertainty or failed handoff.
			p.failLocked("publication %s still owned at caller return: committed=%v finalized=%v dispatched=%v", id, entry.committed, entry.finalized, entry.dispatched)
			continue
		}
		delete(p.live, id)
		p.returned++
	}
}

// Keep every concrete bus capability through embedding. Plans/carriers are
// passed through unchanged; the selected writer still receives its real types.
type nestedPublicationBus struct {
	*fanInBarrierDiagnosticBus
	lifetime *nestedPublicationLifetime
}

var _ pipeline.FanOutPublicationPlanner = (*nestedPublicationBus)(nil)

func (b *nestedPublicationBus) PrepareFanOutPublications(ctx context.Context, group pipelineobligation.PublicationGroup, requests []pipeline.FanOutPublicationRequest) ([]pipeline.FanOutPublicationPreparation, error) {
	started := time.Now()
	results, err := b.fanInBarrierDiagnosticBus.PrepareFanOutPublications(ctx, group, requests)
	elapsed := time.Since(started)
	turn, _ := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn)
	if turn == nil {
		return results, err
	}
	// A partial acquisition can return owned plans alongside either kind of
	// error. Observe those plans before the caller performs its real cleanup.
	for _, result := range results {
		if result.Publication != nil {
			b.lifetime.prepared(turn, []engine.DurablePublicationPlan{result.Publication})
		}
	}
	p := b.lifetime
	p.mu.Lock()
	p.prepareDuration += elapsed
	p.batchCalls++
	p.batchResults += len(results)
	if err != nil {
		p.batchErrors++
	}
	for _, result := range results {
		if result.Err != nil {
			p.batchRowErrors++
		}
		if result.Publication != nil {
			p.groupPrepared++
			if result.Err != nil {
				p.batchErrorPlans++
			}
		} else if result.Err == nil {
			p.batchEmptyResults++
		}
	}
	p.mu.Unlock()
	return results, err
}

func (b *nestedPublicationBus) PrepareFanOutPublication(ctx context.Context, group pipelineobligation.PublicationGroup, ordinal int, intent engine.EmitIntent) (engine.DurablePublicationPlan, error) {
	started := time.Now()
	plan, err := b.fanInBarrierDiagnosticBus.PrepareFanOutPublication(ctx, group, ordinal, intent)
	elapsed := time.Since(started)
	turn, _ := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn)
	if plan != nil {
		b.lifetime.prepared(turn, []engine.DurablePublicationPlan{plan})
	}
	if turn != nil {
		p := b.lifetime
		p.mu.Lock()
		p.prepareDuration += elapsed
		if plan != nil {
			p.groupPrepared++
		}
		p.mu.Unlock()
	}
	return plan, err
}

func (b *nestedPublicationBus) SealFanOutPublications(ctx context.Context, group pipelineobligation.PublicationGroup, end int, plans []engine.DurablePublicationPlan) error {
	started := time.Now()
	err := b.fanInBarrierDiagnosticBus.SealFanOutPublications(ctx, group, end, plans)
	elapsed := time.Since(started)
	if _, ok := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn); !ok {
		return err
	}
	p := b.lifetime
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealDuration += elapsed
	if err != nil {
		return err
	}
	p.groupSeals++
	for _, plan := range plans {
		id := plan.DurablePublicationEventID()
		entry, ok := p.live[id]
		if !ok || entry.committed {
			p.failLocked("publication %s sealed without its live uncommitted plan", id)
			continue
		}
		entry.sealed = true
		p.live[id] = entry
	}
	return nil
}

func (b *nestedPublicationBus) DispatchFanOutPublications(ctx context.Context, group pipelineobligation.PublicationGroup, publications []engine.CommittedDurablePublication) error {
	started := time.Now()
	err := b.fanInBarrierDiagnosticBus.DispatchFanOutPublications(ctx, group, publications)
	elapsed := time.Since(started)
	if _, ok := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn); !ok {
		return err
	}
	p := b.lifetime
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatchDuration += elapsed
	if err != nil {
		return err
	}
	for _, publication := range publications {
		id := publication.CommittedDurablePublicationEventID()
		entry, ok := p.live[id]
		if !ok || !entry.committed || !entry.finalized || entry.dispatched {
			p.failLocked("publication %s group-dispatched outside exact finalized ownership", id)
			continue
		}
		entry.dispatched = true
		p.live[id] = entry
		p.groupDispatched++
	}
	return nil
}

func (b *nestedPublicationBus) PrepareEnginePublications(ctx context.Context, intents []engine.EmitIntent) ([]engine.DurablePublicationPlan, error) {
	plans, err := b.fanInBarrierDiagnosticBus.PrepareEnginePublications(ctx, intents)
	turn, _ := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn)
	b.lifetime.prepared(turn, plans)
	return plans, err
}

func (b *nestedPublicationBus) ReleaseEnginePublications(ctx context.Context, plans []engine.DurablePublicationPlan) error {
	err := b.fanInBarrierDiagnosticBus.ReleaseEnginePublications(ctx, plans)
	if err != nil {
		return err
	}
	p := b.lifetime
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, plan := range plans {
		id := plan.DurablePublicationEventID()
		if entry, ok := p.live[id]; ok {
			if entry.committed {
				p.failLocked("committed publication %s sent through precommit release", id)
			}
			delete(p.live, id)
			p.released++
		}
	}
	return nil
}

func (b *nestedPublicationBus) FinalizeEnginePublications(ctx context.Context, publications []engine.CommittedDurablePublication) error {
	err := b.fanInBarrierDiagnosticBus.FinalizeEnginePublications(ctx, publications)
	if err != nil {
		return err
	}
	b.lifetime.finalized(publications, false)
	return nil
}

func (b *nestedPublicationBus) FinalizeFanOutPublications(ctx context.Context, group pipelineobligation.PublicationGroup, publications []engine.CommittedDurablePublication) error {
	started := time.Now()
	err := b.fanInBarrierDiagnosticBus.FinalizeFanOutPublications(ctx, group, publications)
	elapsed := time.Since(started)
	if _, ok := ctx.Value(nestedTurnContextKey{}).(*nestedServingTurn); !ok {
		return err
	}
	p := b.lifetime
	p.mu.Lock()
	p.finalizeDuration += elapsed
	p.mu.Unlock()
	if err != nil {
		return err
	}
	// Only the actual group-validating finalizer may authorize this observation;
	// do not call the singleton finalizer first or synthesize activation success.
	p.finalized(publications, true)
	return nil
}

func (p *nestedPublicationLifetime) finalized(publications []engine.CommittedDurablePublication, grouped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, publication := range publications {
		id := publication.CommittedDurablePublicationEventID()
		if entry, ok := p.live[id]; ok {
			if !entry.committed || entry.finalized {
				p.failLocked("publication %s finalized outside exact committed ownership", id)
				continue
			}
			entry.finalized = true
			p.live[id] = entry
			if grouped {
				p.groupFinalized++
			}
		} else if grouped {
			p.failLocked("publication %s group-finalized without its live plan", id)
		}
	}
}

func (b *nestedPublicationBus) EngineDispatcher() engine.PostCommitDispatcher {
	return nestedPublicationDispatcher{PostCommitDispatcher: b.fanInBarrierDiagnosticBus.EngineDispatcher(), lifetime: b.lifetime}
}

type nestedPublicationDispatcher struct {
	engine.PostCommitDispatcher
	lifetime *nestedPublicationLifetime
}

func (d nestedPublicationDispatcher) DispatchPostCommit(ctx context.Context, intents []engine.EmitIntent) error {
	err := d.PostCommitDispatcher.DispatchPostCommit(ctx, intents)
	if err != nil {
		return err
	}
	p := d.lifetime
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, intent := range intents {
		id := intent.Event.ID()
		if entry, ok := p.live[id]; ok {
			if !entry.finalized || entry.dispatched {
				p.failLocked("publication %s dispatched outside exact finalized ownership", id)
			}
			entry.dispatched = true
			p.live[id] = entry
		}
	}
	return nil
}
