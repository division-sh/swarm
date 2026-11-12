package effectpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/effects"
)

func requireRegisteredConfirmation(ctx context.Context, nativeChecks nativeChannelChecks, tx *sql.Tx, a effects.Authority, postgres bool) error {
	current, err := nativeChecks.RegisteredConfirmationCurrent(ctx, tx, a, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(a, "registered_binding_not_current")
	}
	return nil
}
