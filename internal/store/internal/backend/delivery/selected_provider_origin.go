package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func validateSelectedProviderOrigin(ctx context.Context, tx *sql.Tx, adapter *Adapter, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	if authority.Validate() != nil || authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || actor.Validate() != nil || actor.RunID != authority.ForkRunID() {
		return fmt.Errorf("selected provider origin requires exact fork execution and actor evidence")
	}
	record, _, err := adapter.requireCurrentClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if !record.Authority.Equal(authority) || record.Route.AgentIdentity != actor.Normalize() || claim.SubscriberClass() != deliverylifecycle.SubscriberAgent || claim.SubscriberID() != actor.AgentID() {
		return fmt.Errorf("selected provider claim contradicts its exact execution/actor ownership")
	}
	return nil
}

func (s *DeliveryPostgresOwner) ValidateSelectedProviderOriginTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	return validateSelectedProviderOrigin(ctx, tx, postgresDeliveryAdapter, claim, actor, authority)
}

func (s *DeliverySQLiteOwner) ValidateSelectedProviderOriginTx(ctx context.Context, tx *sql.Tx, claim deliverylifecycle.Claim, actor agentidentity.Identity, authority deliverylifecycle.ExecutionAuthority) error {
	return validateSelectedProviderOrigin(ctx, tx, sqliteDeliveryAdapter, claim, actor, authority)
}
