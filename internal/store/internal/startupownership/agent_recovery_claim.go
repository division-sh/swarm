package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type agentRecoveryClaimOwner interface {
	ClaimDelivery(context.Context, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimResult, error)
	TryClaimAgentRecoveryDelivery(context.Context, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error)
}

func BindPostgresAgentRecoveryClaims(owner *StartupPostgresOwner, claims *delivery.DeliveryPostgresOwner) error {
	if owner == nil || claims == nil {
		return errors.New("agent recovery claim requires its original PostgreSQL assembly")
	}
	return owner.bindAgentRecoveryClaims(claims)
}

func BindSQLiteAgentRecoveryClaims(owner *StartupSQLiteOwner, claims *delivery.DeliverySQLiteOwner) error {
	if owner == nil || claims == nil {
		return errors.New("agent recovery claim requires its original SQLite assembly")
	}
	return owner.bindAgentRecoveryClaims(claims)
}

func (p *processOperations) bindAgentRecoveryClaims(claims agentRecoveryClaimOwner) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if claims == nil || p.recoveryClaims != nil || p.current != nil {
		return errors.New("agent recovery claim owner must be bound once before process acquisition")
	}
	p.recoveryClaims = claims
	return nil
}

func (p *processOperations) claimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	p.mu.Lock()
	claims := p.recoveryClaims
	p.mu.Unlock()
	if claims == nil {
		return deliverylifecycle.ClaimResult{}, errors.New("original session has no assembled agent recovery claim owner")
	}
	return claims.ClaimDelivery(ctx, authority, event, route)
}

func (s *postgresSession) ClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	return s.owner.claimAgentRecoveryDelivery(ctx, authority, event, route)
}

func (s *sqliteSession) ClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	return s.owner.claimAgentRecoveryDelivery(ctx, authority, event, route)
}

func (p *processOperations) tryClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error) {
	p.mu.Lock()
	claims := p.recoveryClaims
	p.mu.Unlock()
	if claims == nil {
		return deliverylifecycle.ClaimResult{}, false, errors.New("original session has no assembled capacity claim owner")
	}
	return claims.TryClaimAgentRecoveryDelivery(ctx, authority, event, route)
}

func (s *postgresSession) TryClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error) {
	return s.owner.tryClaimAgentRecoveryDelivery(ctx, authority, event, route)
}

func (s *sqliteSession) TryClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error) {
	return s.owner.tryClaimAgentRecoveryDelivery(ctx, authority, event, route)
}
