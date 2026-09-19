package startupownership

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

const fanOutOpportunityObservationInterval = 250 * time.Millisecond

type fanOutOpportunity struct {
	candidate FanOutCandidate
	since     time.Time // monotonic local observation, never a persisted timestamp
	reported  bool
}

func sameFanOutOpportunity(a, b FanOutCandidate) bool {
	return a.GrantID == b.GrantID && a.Key == b.Key
}

// The wrapper records actual store-call entry, not worker callback scheduling.
// It adds no source authority and cannot change the selected candidate.
type fanOutObservedClaimOwner struct {
	pipeline.FanOutObligationOwner
	service       *fanOutServingService
	candidate     FanOutCandidate
	commitStarted time.Time
	committed     bool
}

func (o *fanOutObservedClaimOwner) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	if o.commitStarted.IsZero() {
		o.commitStarted = time.Now()
	}
	result, err := o.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	if err == nil {
		o.committed = true
	}
	return result, err
}

func (o *fanOutObservedClaimOwner) ClaimFanOutIntent(ctx context.Context, request pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if request.Candidate == nil || *request.Candidate != o.candidate.Key {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, errors.New("fan-out claim attempt differs from observed candidate")
	}
	o.service.owner.mu.Lock()
	o.service.claimEntered[o.candidate.Key] = true
	if opportunity := o.service.opportunity; opportunity != nil && sameFanOutOpportunity(opportunity.candidate, o.candidate) {
		o.service.opportunity = nil
	}
	o.service.owner.mu.Unlock()
	return o.FanOutObligationOwner.ClaimFanOutIntent(ctx, request)
}

// This observer never claims or wakes work. Query failure is unknown state, not
// proof of eligible work, and ends the currently observed opportunity episode.
func (s *fanOutServingService) detectMissedOpportunities() {
	ticker := time.NewTicker(fanOutOpportunityObservationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		grants, registrations, excluded := s.snapshot(true)
		if len(grants) == 0 {
			s.observeOpportunity(FanOutCandidate{}, false, time.Now())
			continue
		}
		lease, err := s.owner.fanOutCapacity.workProcess.Begin(s.ctx)
		if err != nil {
			return
		}
		candidate, found, err := s.store.NextFanOutCandidate(lease.Context(), grants, excluded)
		lease.Done()
		if err != nil || !found {
			s.observeOpportunity(FanOutCandidate{}, false, time.Now())
			continue
		}
		r := registrations[candidate.GrantID]
		if r == nil || r.ctx.Err() != nil || candidate.Validate() != nil {
			s.observeOpportunity(FanOutCandidate{}, false, time.Now())
			continue
		}
		if !s.observeOpportunity(candidate, true, time.Now()) {
			continue
		}
		if err := r.grant.ProveCurrent(r.ctx); err != nil {
			s.observeOpportunity(FanOutCandidate{}, false, time.Now())
			continue
		}
		reportLease, err := r.occurrence.Begin(r.ctx)
		if err != nil {
			continue
		}
		s.owner.mu.Lock()
		stillPending := s.opportunity != nil && sameFanOutOpportunity(s.opportunity.candidate, candidate) && !s.claimEntered[candidate.Key]
		s.owner.mu.Unlock()
		if !stillPending {
			reportLease.Done()
			continue
		}
		r.executor.ReportFanOutServingError(correlation.WithRunID(reportLease.Context(), candidate.Key.RunID), failures.New(failures.ClassInternalFailure, "fan_out_claim_opportunity_missed", "runtime.fan_out", "observe_opportunity", map[string]any{
			"grant_id": candidate.GrantID, "run_id": candidate.Key.RunID,
			"triggering_delivery_id": candidate.Key.TriggeringDeliveryID,
			"flow_path":              candidate.Key.ElementRef.FlowPath, "declaration_family": candidate.Key.ElementRef.Family,
			"semantic_path": candidate.Key.ElementRef.SemanticPath, "claim_attempt_bound_ms": 1000,
		}))
		reportLease.Done()
	}
}

func (s *fanOutServingService) observeOpportunity(candidate FanOutCandidate, eligible bool, now time.Time) bool {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	capacity := s.owner.fanOutCapacity
	capacityAvailable := capacity != nil && capacity.used-len(s.intents)+len(s.claimEntered) < capacity.limit
	if !eligible || !capacityAvailable || s.claimEntered[candidate.Key] {
		s.opportunity = nil
		return false
	}
	if s.opportunity == nil || !sameFanOutOpportunity(s.opportunity.candidate, candidate) {
		s.opportunity = &fanOutOpportunity{candidate: candidate, since: now}
		return false
	}
	if s.opportunity.reported || now.Sub(s.opportunity.since) <= time.Second {
		return false
	}
	s.opportunity.reported = true
	return true
}
