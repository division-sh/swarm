package startupownership

import (
	"context"
	"errors"
	"math"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type agentRecoveryCapacityAttempt struct {
	policy  *agentRecoveryProfilePolicy
	write   *AgentRecoveryWriteObservation
	inner   *AgentRecoveryWriteObservation
	epoch   uint64
	decided bool
}

// DecideAgentRecoveryCapacityClaim consumes one decision inside the original
// fenced first claim. Its boolean is a local wait, not an admission permit.
// The native owner retains all eligibility, profile, mutation and ACK duties.
func DecideAgentRecoveryCapacityClaim(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute, profile deliverylifecycle.AgentClaimProfile) (bool, error) {
	intent, err := capacityClaimIntent(ctx, authority, event, route, profile)
	if err != nil {
		return false, err
	}
	observed, capacity, p := intent.observation, intent.capacity, intent.process
	o := observed.writes
	if o == nil {
		return false, errAgentRecoverySeedChanged
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := errors.Join(ctx.Err(), intent.life.Err()); err != nil {
		return false, err
	}
	if !intent.capacitySeedCurrentLocked() || !capacity.ownsWriteLocked(observed) {
		return false, errAgentRecoverySeedChanged
	}
	count := observed.seed.inProgressByProfile[profile.ProfileID]
	if count < 0 {
		return false, errAgentRecoverySeedChanged
	}
	limit := capacity.policy.limits[profile.ProfileID]
	capacity.decided = true
	return limit.Enabled && count >= limit.Limit, nil
}

func capacityClaimIntent(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute, profile deliverylifecycle.AgentClaimProfile) (*agentRecoveryClaimIntent, error) {
	if ctx == nil {
		return nil, errors.New("agent recovery capacity decision requires its bound native invocation")
	}
	entry, ok := ctx.Value(agentRecoveryClaimContextKey{}).(agentRecoveryClaimEntry)
	if !ok || entry.intent == nil || entry.intent.capacity == nil || entry.intent.process == nil || entry.intent.life == nil {
		return nil, errors.New("agent recovery capacity decision has no original capacity invocation")
	}
	intent := entry.intent
	identity, err := ordinaryAgentClaimIdentity(authority, event, route)
	if err != nil || !intent.authority.Equal(authority) || intent.eventID != event.ID() || intent.runID != event.RunID() ||
		intent.routeIdentity != identity || !profile.MatchesFirstAdmission(event, route, authority) {
		return nil, errors.Join(errors.New("agent recovery capacity profile differs from the exact native invocation"), err)
	}
	return intent, nil
}

// Caller holds the original observer -> process locks. These checks perform no
// native proof, operation-lock acquisition or SQL while the locks are held.
func (intent *agentRecoveryClaimIntent) capacitySeedCurrentLocked() bool {
	observed, capacity, p := intent.observation, intent.capacity, intent.process
	o, seed := observed.writes, observed.seed
	return !o.invalid && o.process == p && p.agentRecoveryWrites == o && p.terminal.Cause == "" &&
		p.agentRecoverySeed == nil && seed != nil && seed.process == observed.authority && seed.sourceHead == observed.sourceHead &&
		seed.sourceHead != "" && seed.inProgressByProfile != nil &&
		capacity.policy != nil && p.agentRecoveryPolicy == capacity.policy && capacity.policy.limits != nil
}

func (capacity *agentRecoveryCapacityAttempt) ownsWriteLocked(observed agentRecoverySeedCheck) bool {
	o := observed.writes
	if !capacity.outerIdentityCurrentLocked(observed) {
		return false
	}
	switch observed.authority.Backend {
	case processbinding.PostgresLeaseBackend:
		return capacity.inner == nil && o.epoch == capacity.epoch && o.active == 1
	case processbinding.SQLiteLeaseBackend:
		return capacity.epoch != math.MaxUint64 && capacity.inner != nil && capacity.inner != capacity.write &&
			capacity.inner.observer == o && capacity.inner.capacity == capacity && !capacity.inner.ended.Load() &&
			o.epoch == capacity.epoch+1 && o.active == 2
	default:
		return false
	}
}

func (capacity *agentRecoveryCapacityAttempt) outerIdentityCurrentLocked(observed agentRecoverySeedCheck) bool {
	return capacity.write != nil && capacity.write.observer == observed.writes && capacity.write.capacity == capacity && !capacity.write.ended.Load() &&
		observed.epoch != math.MaxUint64 && capacity.epoch == observed.epoch+1 && !capacity.decided
}
