package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type agentRecoveryCapacityClaimSession interface {
	TryClaimAgentRecoveryDelivery(context.Context, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error)
}

// AgentRecoveryCapacityClaimResult separates a process-local wait from the
// original native lifecycle result. Neither case authorizes provider launch.
type AgentRecoveryCapacityClaimResult struct {
	claim        deliverylifecycle.ClaimResult
	waiting      bool
	native       bool
	acknowledged bool
}

func (r AgentRecoveryCapacityClaimResult) Waiting() bool { return r.waiting }
func (r AgentRecoveryCapacityClaimResult) NativeClaim() (deliverylifecycle.ClaimResult, bool) {
	return r.claim, r.native
}
func (r AgentRecoveryCapacityClaimResult) Acknowledged() bool { return r.acknowledged }

// TryClaimAgentRecoveryDelivery consumes one complete private seed at the
// original fenced first-claim owner. It never retries or restores that seed.
func TryClaimAgentRecoveryDelivery(ctx context.Context, capability ProcessCapability, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (AgentRecoveryCapacityClaimResult, error) {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil {
		return AgentRecoveryCapacityClaimResult{}, errors.New("capacity claim requires context and its original process")
	}
	identity, err := ordinaryAgentClaimIdentity(authority, event, route)
	if err != nil {
		return AgentRecoveryCapacityClaimResult{}, err
	}
	if ctx.Value(agentRecoveryClaimContextKey{}) != nil {
		return AgentRecoveryCapacityClaimResult{}, errors.New("capacity claim cannot nest a native entry scope")
	}
	observed, err := p.captureAgentRecoverySeedCheck(ctx)
	if err != nil {
		return AgentRecoveryCapacityClaimResult{}, err
	}
	p.mu.Lock()
	policy := p.agentRecoveryPolicy
	p.mu.Unlock()
	if policy == nil {
		return AgentRecoveryCapacityClaimResult{}, errors.New("capacity claim requires its complete bound profile policy")
	}
	owner, ok := p.session.(agentRecoveryCapacityClaimSession)
	if !ok {
		return AgentRecoveryCapacityClaimResult{}, errors.New("original process session has no capacity claim owner")
	}
	probe := p.agentRecoverySeedProbe.Load()
	if probe != nil && probe.AfterCheckCapture != nil {
		probe.AfterCheckCapture()
	}
	if err := p.ProveCurrent(ctx); err != nil {
		return AgentRecoveryCapacityClaimResult{}, err
	}
	if probe != nil && probe.AfterCheckProof != nil {
		probe.AfterCheckProof()
	}
	life, end := context.WithCancel(context.WithoutCancel(ctx))
	defer end()
	intent := &agentRecoveryClaimIntent{process: p, observation: observed, life: life,
		authority: authority, eventID: event.ID(), runID: event.RunID(), routeIdentity: identity,
		capacity: &agentRecoveryCapacityAttempt{policy: policy}}
	claim, waiting, err := owner.TryClaimAgentRecoveryDelivery(context.WithValue(ctx, agentRecoveryClaimContextKey{}, intent), authority, event, route)
	result := AgentRecoveryCapacityClaimResult{acknowledged: claim.Acknowledged}
	if waiting {
		// Only the joined native ACK with no independent error publishes wait.
		result.waiting = claim.Acknowledged && err == nil
	} else {
		result.claim, result.native = claim, claim.Disposition != ""
	}
	return result, err
}

// BindAgentRecoveryCapacityClaimIntent is the fixed native role's entry. A
// plain claim cannot accidentally opt into capacity, or vice versa.
func BindAgentRecoveryCapacityClaimIntent(ctx context.Context, writes *AgentRecoveryWriteObserver, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (context.Context, error) {
	if ctx == nil {
		return nil, errors.New("capacity claim has no original invocation")
	}
	intent, ok := ctx.Value(agentRecoveryClaimContextKey{}).(*agentRecoveryClaimIntent)
	if !ok || intent == nil || intent.capacity == nil {
		return nil, errors.New("capacity claim requires its private capacity invocation")
	}
	return BindAgentRecoveryClaimIntent(ctx, writes, authority, event, route)
}
