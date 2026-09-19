package startupownership

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

const fanOutRecoveryInterval = time.Second

// One selector belongs to the retained process. It observes durable fairness
// across all registered sources; executors neither poll nor own a local queue.
type fanOutServingService struct {
	owner  *processCapability
	store  FanOutServingStore
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	turns  sync.WaitGroup
	// Protected by owner.mu. Reservations exclude only the exact intent, not
	// every intent in its run. Durable global ordering owns fairness.
	intents      map[fanoutobligation.IntentKey]struct{}
	claimEntered map[fanoutobligation.IntentKey]bool
	opportunity  *fanOutOpportunity
}

func StartFanOutServing(ctx context.Context, grant LiveGenerationGrant, occurrence *worklifetime.RuntimeOccurrence, workers *int, executor FanOutExecutor) (*FanOutServingRegistration, error) {
	if executor == nil {
		return nil, errors.New("fan-out serving requires an exact runtime executor")
	}
	live, ok := grant.(*liveGenerationGrant)
	if !ok || live == nil || live.owner == nil {
		return nil, errors.New("fan-out serving requires a retained live generation")
	}
	provider, ok := live.owner.session.(fanOutServingStoreProvider)
	if !ok {
		return nil, errors.New("selected process session does not supply the fan-out serving owner")
	}
	store, err := provider.FanOutServingStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("selected process session returned no fan-out serving owner")
	}
	r, err := RegisterFanOutServing(ctx, grant, occurrence, workers)
	if err != nil {
		return nil, err
	}
	p := live.owner
	p.mu.Lock()
	budget := p.fanOutCapacity
	evidence, evidenceErr := r.grant.Evidence()
	if evidenceErr != nil || p.fanOutClosing || r.ctx.Err() != nil || r.turnsClosing || budget.registrations[evidence.GrantID] != r {
		p.mu.Unlock()
		r.Close()
		return nil, errors.New("process fan-out serving is closing")
	}
	if budget.service == nil {
		// This standing observer outlives individual source registrations, but
		// not retained process possession or the process work owner.
		standing, beginErr := budget.workProcess.BeginStanding(context.Background())
		if beginErr != nil {
			p.mu.Unlock()
			r.Close()
			return nil, beginErr
		}
		serviceCtx, cancel := context.WithCancel(standing.Context())
		s := &fanOutServingService{owner: p, store: store, ctx: serviceCtx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}), intents: make(map[fanoutobligation.IntentKey]struct{}), claimEntered: make(map[fanoutobligation.IntentKey]bool)}
		budget.service = s
		go s.run(standing)
	}
	r.executor = executor
	p.mu.Unlock()
	r.Wake()
	return r, nil
}

// Wake is a latency hint. Recovery independently queries durable eligibility;
// coalescing or losing any number of hints cannot discard queued work.
func (r *FanOutServingRegistration) Wake() {
	if r == nil || r.grant == nil || r.grant.owner == nil {
		return
	}
	p := r.grant.owner
	p.mu.Lock()
	var service *fanOutServingService
	if p.fanOutCapacity != nil {
		service = p.fanOutCapacity.service
	}
	p.mu.Unlock()
	if service != nil {
		service.signal()
	}
}

// SetTestScanObserver observes the real shared selector after its store read.
// It changes neither the recovery interval nor candidate/claim authority.
func (r *FanOutServingRegistration) SetTestScanObserver(observer func(FanOutCandidate, bool, error)) {
	r.grant.owner.mu.Lock()
	r.testScanObserver = observer
	r.grant.owner.mu.Unlock()
}

func (s *fanOutServingService) reportTestScan(registrations map[string]*FanOutServingRegistration, candidate FanOutCandidate, found bool, err error) {
	s.owner.mu.Lock()
	var observers []func(FanOutCandidate, bool, error)
	for id, r := range registrations {
		if r.testScanObserver != nil && (!found || id == candidate.GrantID) {
			observers = append(observers, r.testScanObserver)
		}
	}
	s.owner.mu.Unlock()
	for _, observer := range observers {
		observer(candidate, found, err)
	}
}

func (s *fanOutServingService) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *fanOutServingService) run(standing *worklifetime.Lease) {
	defer close(s.done)
	defer standing.Done()
	defer s.cancel()
	detectorDone := make(chan struct{})
	go func() {
		defer close(detectorDone)
		s.detectMissedOpportunities()
	}()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-s.owner.Done():
			s.cancel()
		case <-s.ctx.Done():
		}
	}()
	defer func() {
		s.cancel()
		<-watchDone
		<-detectorDone
		s.turns.Wait()
	}()
	ticker := time.NewTicker(fanOutRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
		s.refill()
	}
}

func (s *fanOutServingService) snapshot(observeOpportunity bool) ([]FanOutRegistrationObservation, map[string]*FanOutServingRegistration, []fanoutobligation.IntentKey) {
	p := s.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	budget := p.fanOutCapacity
	used := budget.used
	if observeOpportunity {
		// A reservation without actual claim entry does not discharge the D3
		// bound. Already-entered claims and their handoffs consume real capacity.
		used -= len(s.intents) - len(s.claimEntered)
	}
	if p.requireLive() != nil || p.fanOutClosing || used >= budget.limit {
		return nil, nil, nil
	}
	grants, registrations := p.fanOutRegistrationsLocked()
	excluded := make([]fanoutobligation.IntentKey, 0, len(s.intents))
	for key := range s.intents {
		if observeOpportunity && !s.claimEntered[key] {
			continue
		}
		excluded = append(excluded, key)
	}
	return grants, registrations, excluded
}

func (p *processCapability) fanOutRegistrationsLocked() ([]FanOutRegistrationObservation, map[string]*FanOutServingRegistration) {
	if p.fanOutCapacity == nil || p.fanOutClosing || p.requireLive() != nil {
		return nil, nil
	}
	grants := make([]FanOutRegistrationObservation, 0, len(p.fanOutCapacity.registrations))
	registrations := make(map[string]*FanOutServingRegistration, len(p.fanOutCapacity.registrations))
	for id, r := range p.fanOutCapacity.registrations {
		if r.executor == nil {
			continue
		}
		// Evidence locks the grant, not the process. Store admission separately
		// checks that this exact observation is still authoritative.
		evidence, err := r.grant.Evidence()
		if err != nil || evidence.State != GrantAdmitted {
			continue
		}
		grants = append(grants, FanOutRegistrationObservation{Grant: evidence, Closing: r.turnsClosing || r.ctx.Err() != nil})
		registrations[id] = r
	}
	sort.Slice(grants, func(i, j int) bool { return grants[i].Grant.GrantID < grants[j].Grant.GrantID })
	return grants, registrations
}

func (p *processCapability) stopFanOutServing(ctx context.Context) error {
	p.mu.Lock()
	p.fanOutClosing = true
	var service *fanOutServingService
	if p.fanOutCapacity != nil {
		service = p.fanOutCapacity.service
	}
	if service != nil {
		service.cancel()
	}
	p.mu.Unlock()
	if service == nil {
		return nil
	}
	select {
	case <-service.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("join fan-out serving before process release: %w", ctx.Err())
	}
}

func (s *fanOutServingService) refill() {
	for s.ctx.Err() == nil {
		grants, registrations, excluded := s.snapshot(false)
		if len(grants) == 0 {
			return
		}
		var reporter *FanOutServingRegistration
		for _, observation := range grants {
			if !observation.Closing {
				reporter = registrations[observation.Grant.GrantID]
				break
			}
		}
		if reporter == nil {
			return
		}
		candidate, found, err := s.store.NextFanOutCandidate(s.ctx, grants, excluded)
		s.reportTestScan(registrations, candidate, found, err)
		if err != nil {
			reporter.executor.ReportFanOutServingError(s.ctx, err)
			return
		}
		if !found {
			return
		}
		r := registrations[candidate.GrantID]
		if r == nil {
			reporter.executor.ReportFanOutServingError(s.ctx, errors.New("fan-out selector returned an unregistered generation"))
			return
		}
		if err := candidate.Validate(); err != nil {
			r.executor.ReportFanOutServingError(s.ctx, fmt.Errorf("fan-out selector returned invalid candidate: %w", err))
			return
		}
		evidence, err := r.grant.Evidence()
		if err != nil {
			return
		}
		owner, err := s.store.BindFanOutGrant(evidence)
		if err != nil {
			r.executor.ReportFanOutServingError(s.ctx, err)
			return
		}
		if owner == nil {
			r.executor.ReportFanOutServingError(s.ctx, errors.New("fan-out binding returned no granted store owner"))
			return
		}
		permit, available, err := r.BeginTurn(s.ctx)
		if err != nil {
			r.executor.ReportFanOutServingError(s.ctx, err)
			return
		}
		if !available {
			return
		}
		s.owner.mu.Lock()
		_, alreadyServing := s.intents[candidate.Key]
		if !alreadyServing {
			s.intents[candidate.Key] = struct{}{}
		}
		s.owner.mu.Unlock()
		if alreadyServing {
			permit.Done()
			r.executor.ReportFanOutServingError(s.ctx, errors.New("fan-out selector returned an excluded intent"))
			return
		}
		s.turns.Add(1)
		go func() {
			defer s.turns.Done()
			observedOwner := &fanOutObservedClaimOwner{FanOutObligationOwner: owner, service: s, candidate: candidate}
			turnCtx := correlation.WithRunID(permit.Context(), candidate.Key.RunID)
			result, err := r.executor.ServeFanOutCandidate(turnCtx, observedOwner, candidate.Key)
			completedAt := time.Now()
			if err != nil {
				r.executor.ReportFanOutServingError(turnCtx, err)
			}
			s.owner.mu.Lock()
			if observedOwner.committed && !observedOwner.commitStarted.IsZero() {
				r.lastCommit = &fanOutCommitTiming{key: candidate.Key, duration: completedAt.Sub(observedOwner.commitStarted)}
			}
			delete(s.intents, candidate.Key)
			delete(s.claimEntered, candidate.Key)
			s.owner.mu.Unlock()
			permit.Done()
			if result.Refill {
				s.signal()
			}
		}()
	}
}
