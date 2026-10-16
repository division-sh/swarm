package startupownership

import (
	"context"
	"errors"
	"math"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type agentRecoveryClaimSession interface {
	ClaimAgentRecoveryDelivery(context.Context, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimResult, error)
}

type agentRecoveryClaimIntent struct {
	process       *processCapability
	observation   agentRecoverySeedCheck
	life          context.Context
	authority     deliverylifecycle.ExecutionAuthority
	eventID       string
	runID         string
	routeIdentity string
}

type agentRecoveryClaimEntry struct{ intent *agentRecoveryClaimIntent }
type agentRecoveryClaimContextKey struct{}

// ClaimAgentRecoveryDelivery joins one private observation to its original
// native claim entry. It does not make a capacity decision or authorize launch.
func ClaimAgentRecoveryDelivery(ctx context.Context, capability ProcessCapability, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	p, ok := capability.(*processCapability)
	if !ok || p == nil || ctx == nil {
		return deliverylifecycle.ClaimResult{}, errors.New("agent recovery claim requires context and its original process")
	}
	identity, err := ordinaryAgentClaimIdentity(authority, event, route)
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	if ctx.Value(agentRecoveryClaimContextKey{}) != nil {
		return deliverylifecycle.ClaimResult{}, errors.New("agent recovery claim cannot nest or replay a native entry scope")
	}
	observed, err := p.captureAgentRecoverySeedCheck(ctx)
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	owner, ok := p.session.(agentRecoveryClaimSession)
	if !ok {
		return deliverylifecycle.ClaimResult{}, errors.New("original process session has no agent recovery claim owner")
	}
	probe := p.agentRecoverySeedProbe.Load()
	if probe != nil && probe.AfterCheckCapture != nil {
		probe.AfterCheckCapture()
	}
	if err := p.ProveCurrent(ctx); err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	if probe != nil && probe.AfterCheckProof != nil {
		probe.AfterCheckProof()
	}
	life, end := context.WithCancel(context.WithoutCancel(ctx))
	defer end()
	intent := &agentRecoveryClaimIntent{process: p, observation: observed, life: life,
		authority: authority, eventID: event.ID(), runID: event.RunID(), routeIdentity: identity}
	return owner.ClaimAgentRecoveryDelivery(context.WithValue(ctx, agentRecoveryClaimContextKey{}, intent), authority, event, route)
}

func ordinaryAgentClaimIdentity(authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (string, error) {
	if authority.Validate() != nil || authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime ||
		!route.Recipient.IsAgent() || event.ID() == "" || event.RunID() == "" || route.AgentIdentity.RunID != event.RunID() {
		return "", errors.New("agent recovery claim requires its closed ordinary agent invocation")
	}
	identity, err := route.Identity()
	if err != nil {
		return "", err
	}
	return events.EncodeDeliveryRouteIdentity(identity), nil
}

// BindAgentRecoveryClaimIntent is used only by the original session's fixed
// claim forwarder. Durable row eligibility remains inside the native claim tx.
func BindAgentRecoveryClaimIntent(ctx context.Context, writes *AgentRecoveryWriteObserver, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (context.Context, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errors.Join(errors.New("agent recovery claim has no live invocation"), callerContextError(ctx))
	}
	intent, ok := ctx.Value(agentRecoveryClaimContextKey{}).(*agentRecoveryClaimIntent)
	identity, err := ordinaryAgentClaimIdentity(authority, event, route)
	if err != nil || !ok || intent == nil || intent.life == nil || intent.life.Err() != nil || writes == nil ||
		intent.observation.writes != writes || !intent.authority.Equal(authority) ||
		intent.eventID != event.ID() || intent.runID != event.RunID() || intent.routeIdentity != identity {
		return nil, errors.Join(errors.New("agent recovery claim differs from its original typed invocation"), err)
	}
	return context.WithValue(ctx, agentRecoveryClaimContextKey{}, agentRecoveryClaimEntry{intent}), nil
}

// BindAgentRecoveryClaimIfPresent leaves ordinary callers unchanged. Only the
// original process operation can mint the private intent consumed here.
func BindAgentRecoveryClaimIfPresent(ctx context.Context, writes *AgentRecoveryWriteObserver, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (context.Context, error) {
	if ctx == nil || ctx.Value(agentRecoveryClaimContextKey{}) == nil {
		return ctx, nil
	}
	return BindAgentRecoveryClaimIntent(ctx, writes, authority, event, route)
}

// ObserveWriteStartContext is the original native writer's single start hook.
// Plain writes retain their old behavior. A checked claim validates and revokes
// its same private observation atomically, before pool/session/SQL admission.
func (o *AgentRecoveryWriteObserver) ObserveWriteStartContext(ctx context.Context) (*AgentRecoveryWriteObservation, error) {
	if ctx == nil || ctx.Value(agentRecoveryClaimContextKey{}) == nil {
		return o.ObserveWriteStart(), nil
	}
	entry, ok := ctx.Value(agentRecoveryClaimContextKey{}).(agentRecoveryClaimEntry)
	if !ok || entry.intent == nil || entry.intent.life == nil || o == nil || entry.intent.process == nil || entry.intent.observation.writes != o {
		return nil, errors.New("native write requires its exact bound agent recovery claim intent")
	}
	intent, p := entry.intent, entry.intent.process
	o.mu.Lock()
	defer o.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := errors.Join(ctx.Err(), intent.life.Err()); err != nil {
		return nil, err
	}
	current, err := o.readSeedCheckLocked(ctx, p, intent.observation.authority)
	if err != nil {
		return nil, err
	}
	if current != intent.observation || o.epoch == math.MaxUint64 || o.active == math.MaxUint64 {
		return nil, errAgentRecoverySeedChanged
	}
	return o.startWriteLocked(), nil
}
