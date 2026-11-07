package effectpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
)

func requireRegisteredConfirmation(ctx context.Context, tx *sql.Tx, a effects.Authority, postgres bool) error {
	current, err := channelonboarding.RegisteredConfirmationCurrent(ctx, tx, a, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(a, "registered_binding_not_current")
	}
	return nil
}
