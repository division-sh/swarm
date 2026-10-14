package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func ReadFirstAgentClaimProfileForTest(ctx context.Context, selected any, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	var profile deliverylifecycle.AgentClaimProfile
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	var dialect delivery.Dialect
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return profile, fmt.Errorf("agent profile evidence requires its original postgres owner")
		}
		read, dialect = owner.backend.RunReadTransaction, delivery.DialectPostgres
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return profile, fmt.Errorf("agent profile evidence requires its original sqlite owner")
		}
		read, dialect = owner.backend.RunReadTransaction, delivery.DialectSQLite
	default:
		return profile, fmt.Errorf("agent profile evidence requires the original selected owner, got %T", selected)
	}
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		profile, err = delivery.ReadFirstAgentClaimProfileTx(ctx, tx, dialect, expected)
		return err
	})
	return profile, err
}
