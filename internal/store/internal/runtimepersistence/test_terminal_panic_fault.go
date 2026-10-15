package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func InstallTerminalPanicMutationFailureForTest(ctx context.Context, selected any, agentID string, failFinalizer bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	if agentID == "" || strings.TrimSpace(agentID) != agentID {
		return fmt.Errorf("terminal panic fault requires the exact admitted agent ID")
	}
	_, postgres := selected.(*PostgresStore)
	return runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return agentpersistence.InstallTerminalPanicMutationFailureTx(ctx, tx, postgres, agentID, failFinalizer)
		})
	})
}
