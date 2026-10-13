package startupownership

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// FanOutCapacity is a selected-backend observation, not operator authority to
// change its pool. Dedicated sessions cannot be counted as worker headroom.
type FanOutCapacity struct {
	postgres       bool
	valid          bool
	maxConnections int
	reservations   int
}

func PostgreSQLFanOutCapacity(maxConnections, reservations int) (FanOutCapacity, error) {
	if maxConnections < 0 || reservations < 0 || (maxConnections > 0 && reservations >= maxConnections) {
		return FanOutCapacity{}, errors.New("fan-out serving requires valid PostgreSQL connection capacity and dedicated reservations")
	}
	return FanOutCapacity{postgres: true, valid: true, maxConnections: maxConnections, reservations: reservations}, nil
}

func SQLiteFanOutCapacity() FanOutCapacity { return FanOutCapacity{valid: true} }

func (c FanOutCapacity) resolve(requested *int) (int, error) {
	if !c.valid {
		return 0, errors.New("fan-out serving requires selected-backend capacity evidence")
	}
	workers := 1
	if c.postgres {
		workers = 4
	}
	if requested != nil {
		workers = *requested
	}
	if workers <= 0 {
		return 0, errors.New("runtime.fan_out_workers must be positive")
	}
	if !c.postgres && workers != 1 {
		return 0, errors.New("runtime.fan_out_workers must be exactly 1 for SQLite")
	}
	if c.postgres && c.maxConnections > 0 && workers >= c.maxConnections-c.reservations {
		return 0, fmt.Errorf("runtime.fan_out_workers=%d leaves no control/read connection: pool=%d dedicated=%d; configure a larger pool or fewer workers", workers, c.maxConnections, c.reservations)
	}
	return workers, nil
}

type fanOutCapacityReader interface {
	FanOutServingCapacity() (FanOutCapacity, error)
}

// This budget belongs to the retained process, not to one runtime, generation,
// coordinator, pool handle or source bundle. It does not decide queue fairness.
type fanOutCapacityState struct {
	limit         int
	used          int
	workProcess   *worklifetime.Process
	registrations map[string]*FanOutServingRegistration
	service       *fanOutServingService
}

type FanOutServingRegistration struct {
	grant             *generationGrant
	occurrence        worklifetime.Occurrence
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	closeOnce         sync.Once
	turns             sync.WaitGroup // Add is fenced by registration removal under the process mutex.
	turnsClosing      bool           // protected by the process mutex
	executor          FanOutExecutor // protected by the process mutex
	testScanObserver  func(FanOutCandidate, bool, error)
	testChunkObserver func(context.Context, pipeline.FanOutChunkCommand)
	lastCommit        *fanOutCommitTiming
}

// RegisterFanOutServing consumes only process-minted authority and its exact
// ordinary runtime or selected execution occurrence. Both share one budget.
func RegisterFanOutServing(ctx context.Context, grant GenerationGrant, occurrence worklifetime.Occurrence, requestedWorkers *int) (*FanOutServingRegistration, error) {
	g, err := fanOutGenerationGrant(grant)
	if err != nil {
		return nil, err
	}
	p := g.owner
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return nil, err
	}
	evidence, err := g.Evidence()
	if err != nil {
		return nil, err
	}
	if evidence.State != GrantAdmitted || !fanOutOccurrenceMatches(occurrence, evidence) {
		return nil, errors.New("fan-out registration differs from the admitted runtime generation")
	}
	if err := g.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
		return nil, err
	}
	reader, ok := p.session.(fanOutCapacityReader)
	if !ok {
		return nil, errors.New("retained process session does not provide fan-out capacity evidence")
	}
	capacity, err := reader.FanOutServingCapacity()
	if err != nil {
		return nil, err
	}
	// Selected work joins the process's existing configured capacity; it does
	// not reinterpret a loaded runtime's worker options or acquire another pool.
	if evidence.SelectedFork != nil && requestedWorkers == nil {
		p.mu.Lock()
		if p.fanOutCapacity != nil {
			limit := p.fanOutCapacity.limit
			requestedWorkers = &limit
		}
		p.mu.Unlock()
	}
	limit, err := capacity.resolve(requestedWorkers)
	if err != nil {
		return nil, err
	}
	standing, err := occurrence.BeginStanding(ctx)
	if err != nil {
		return nil, err
	}
	registrationCtx, cancel := context.WithCancel(standing.Context())
	workProcess, processPresent := worklifetime.ProcessFromContext(standing.Context())
	if !processPresent {
		cancel()
		standing.Done()
		return nil, errors.New("fan-out runtime occurrence has no process work owner")
	}
	r := &FanOutServingRegistration{grant: g, occurrence: occurrence, ctx: registrationCtx, cancel: cancel, done: make(chan struct{})}
	r.testChunkObserver, _ = ctx.Value(fanOutChunkObserverContextKey{}).(func(context.Context, pipeline.FanOutChunkCommand))
	p.mu.Lock()
	if p.fanOutCapacity == nil {
		p.fanOutCapacity = &fanOutCapacityState{limit: limit, workProcess: workProcess, registrations: make(map[string]*FanOutServingRegistration)}
	}
	budget := p.fanOutCapacity
	previous := budget.registrations[evidence.GrantID]
	if budget.limit != limit || budget.workProcess != workProcess || (previous != nil && !previous.closedAndJoinedLocked()) || p.fanOutClosing || p.requireLive() != nil {
		p.mu.Unlock()
		cancel()
		standing.Done()
		return nil, errors.New("fan-out registration is duplicate, retired or conflicts with the process worker limit")
	}
	budget.registrations[evidence.GrantID] = r
	p.mu.Unlock()
	go func() {
		defer close(r.done)
		defer standing.Done()
		select {
		case <-g.Done():
		case <-p.Done():
		case <-registrationCtx.Done():
		}
		cancel()
		p.mu.Lock()
		r.turnsClosing = true
		// An acknowledged closing registration remains in the complete census
		// until its grant retires. It retains no standing lease after this join.
		if budget.registrations[evidence.GrantID] == r && (r.executor == nil || p.requireLive() != nil || grantRetired(g)) {
			delete(budget.registrations, evidence.GrantID)
		}
		idleService := p.retireIdleFanOutServiceLocked()
		p.mu.Unlock()
		r.Wake()
		// Cancellation precedes the locked removal, so no BeginTurn can Add
		// after this point. Join only this registration's admitted finite work.
		r.turns.Wait()
		if idleService != nil {
			<-idleService.done
			p.mu.Lock()
			if budget.service == idleService {
				budget.service = nil
			}
			p.mu.Unlock()
		}
	}()
	return r, nil
}

// The process owns the shared observer, but its last registered occurrence
// must join it. Durable closing-grant census entries are not live observers.
func (p *processCapability) retireIdleFanOutServiceLocked() *fanOutServingService {
	if p.fanOutCapacity == nil || p.fanOutCapacity.service == nil {
		return nil
	}
	for _, r := range p.fanOutCapacity.registrations {
		if !r.turnsClosing && r.ctx.Err() == nil {
			return nil
		}
	}
	service := p.fanOutCapacity.service
	service.cancel()
	return service
}

func fanOutGenerationGrant(grant GenerationGrant) (*generationGrant, error) {
	var g *generationGrant
	switch grant := grant.(type) {
	case *liveGenerationGrant:
		if grant != nil {
			g = grant.generationGrant
		}
	case *generationGrant:
		g = grant
	}
	if g == nil || g.owner == nil {
		return nil, errors.New("fan-out serving requires a process-minted generation grant")
	}
	return g, nil
}

func fanOutOccurrenceMatches(occurrence worklifetime.Occurrence, evidence GrantEvidence) bool {
	if evidence.SelectedFork != nil {
		selected, ok := occurrence.(*worklifetime.SelectedForkOccurrence)
		return ok && selected != nil && selected.Identity() == (worklifetime.SelectedForkIdentity{
			ExecutionID: evidence.SelectedFork.ExecutionID, RunID: evidence.SelectedFork.ForkRunID,
			Generation: evidence.SelectedFork.ExecutionGeneration,
		})
	}
	runtime, ok := occurrence.(*worklifetime.RuntimeOccurrence)
	return ok && runtime != nil && runtime.Identity().RuntimeInstanceID == evidence.RuntimeInstanceID && runtime.Identity().BundleHash == evidence.BundleHash
}

func grantRetired(g *generationGrant) bool {
	select {
	case <-g.Done():
		return true
	default:
		return false
	}
}

func (r *FanOutServingRegistration) closedAndJoinedLocked() bool {
	if !r.turnsClosing {
		return false
	}
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// Close cancels admission and joins this registration's finite turns, including
// mandatory post-commit handoff. It does not join unrelated runtime carriers.
func (r *FanOutServingRegistration) Close() {
	if r == nil || r.cancel == nil || r.done == nil {
		return
	}
	r.closeOnce.Do(r.cancel)
	<-r.done
}

// BeginTurn is nonblocking on capacity. Its lease covers the finite turn,
// including post-commit handoff. It is not a substitute for the store's current
// generation, run and claim checks inside mutation admission.
func (r *FanOutServingRegistration) BeginTurn(ctx context.Context) (*FanOutTurnPermit, bool, error) {
	if r == nil || r.grant == nil || r.ctx.Err() != nil {
		return nil, false, errors.New("fan-out serving registration is retired")
	}
	if err := r.grant.ProveCurrent(ctx); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	// Execution values come from the exact registered runtime, never from the
	// shared selector or another source's request context. The selector supplies
	// cancellation only, while the runtime parent already owns its own revocation.
	lease, err := r.occurrence.Begin(r.ctx)
	if err != nil {
		return nil, false, err
	}
	p := r.grant.owner
	p.mu.Lock()
	budget := p.fanOutCapacity
	retired := false
	select {
	case <-r.grant.Done():
		retired = true
	default:
	}
	if retired || r.ctx.Err() != nil || r.turnsClosing || p.fanOutClosing || p.requireLive() != nil {
		p.mu.Unlock()
		lease.Done()
		return nil, false, errors.New("fan-out registration retired during turn admission")
	}
	if budget.used == budget.limit {
		p.mu.Unlock()
		lease.Done()
		return nil, false, nil
	}
	budget.used++
	r.turns.Add(1)
	p.mu.Unlock()
	turnCtx, cancel := context.WithCancel(lease.Context())
	bridgeDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(bridgeDone)
		cancel()
	})
	return &FanOutTurnPermit{registration: r, lease: lease, ctx: turnCtx, cancel: cancel, stop: stop, bridgeDone: bridgeDone}, true, nil
}

type FanOutTurnPermit struct {
	registration *FanOutServingRegistration
	lease        *worklifetime.Lease
	ctx          context.Context
	cancel       context.CancelFunc
	stop         func() bool
	bridgeDone   <-chan struct{}
	once         sync.Once
}

func (p *FanOutTurnPermit) Context() context.Context { return p.ctx }

func (p *FanOutTurnPermit) Done() {
	if p == nil || p.registration == nil || p.lease == nil {
		return
	}
	p.once.Do(func() {
		if !p.stop() {
			<-p.bridgeDone
		}
		p.cancel()
		owner := p.registration.grant.owner
		owner.mu.Lock()
		owner.fanOutCapacity.used--
		owner.mu.Unlock()
		p.lease.Done()
		p.registration.turns.Done()
	})
}
