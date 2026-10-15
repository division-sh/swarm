package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

type AgentClaimProfileFault = delivery.AgentClaimProfileFault

const (
	AgentClaimProfileMissing = delivery.AgentClaimProfileMissing
	AgentClaimProfileCorrupt = delivery.AgentClaimProfileCorrupt
	AgentClaimProfileForeign = delivery.AgentClaimProfileForeign
)

func ApplyAgentClaimProfileFaultForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim, expected deliverylifecycle.AgentClaimProfile, fault AgentClaimProfileFault, foreign deliverylifecycle.Claim) error {
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return delivery.ApplyAgentClaimProfileFaultTx(ctx, tx, delivery.Dialect(dialect), claim, expected, fault, foreign)
	})
}
