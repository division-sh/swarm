package channelonboarding

import (
	"context"
	"database/sql"
	"errors"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	startupowner "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/google/uuid"
)

func (s *PostgresOwner) RecoverUnlaunchedChannelLogout(ctx context.Context, id string, revision int64, now time.Time) (domain.TeardownOperation, error) {
	return recoverUnlaunchedLogout(ctx, postgresRunner{s}, id, revision, now)
}

func (s *SQLiteOwner) RecoverUnlaunchedChannelLogout(ctx context.Context, id string, revision int64, now time.Time) (domain.TeardownOperation, error) {
	return recoverUnlaunchedLogout(ctx, sqliteRunner{s}, id, revision, now)
}

func recoverUnlaunchedLogout(ctx context.Context, r runner, id string, revision int64, now time.Time) (domain.TeardownOperation, error) {
	if err := r.require(); err != nil {
		return domain.TeardownOperation{}, err
	}
	if ctx == nil || uuid.Validate(id) != nil || revision < 1 || now.IsZero() {
		return domain.TeardownOperation{}, domain.ErrInvalidRequest
	}
	var out domain.TeardownOperation
	err := r.mutate(ctx, "recover never-authorized channel logout", func(txctx context.Context, tx *sql.Tx) error {
		if err := generationauthority.FenceMutation(txctx, tx, r.dialect() == dialectSQLite); err != nil {
			return err
		}
		op, found, err := loadTeardown(txctx, tx, r.dialect(), id, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Kind != domain.TeardownLogout || op.Logout == nil || op.Logout.Validate() != nil {
			return domain.ErrConflict
		}
		out = op
		if op.Phase.Terminal() {
			if revision != op.Revision && revision != op.Revision-1 {
				return domain.ErrRevisionConflict
			}
			return nil
		}
		if op.Revision != revision || op.Phase != domain.TeardownAuthorityRetired {
			return domain.ErrRevisionConflict
		}
		neverAuthorized, err := logoutOriginalWithdrawnWithoutAuthorizationTx(txctx, tx, r.dialect(), op)
		if err != nil || !neverAuthorized {
			return err
		}
		phase, code, message, err := logoutEffectResult(effects.Settlement{State: effects.StateTerminalFailure})
		if err != nil {
			return err
		}
		if err := settleLogoutResultTx(txctx, tx, r.dialect(), op, phase, code, message, now); err != nil {
			return err
		}
		out, _, err = loadTeardown(txctx, tx, r.dialect(), id, false)
		return err
	})
	return out, err
}

func logoutOriginalWithdrawnWithoutAuthorizationTx(ctx context.Context, tx *sql.Tx, d dialect, op domain.TeardownOperation) (bool, error) {
	withdrawn, err := startupowner.RuntimeAuthorityWithdrawnTx(ctx, tx, op.Logout.ProcessStamp, d == dialectSQLite)
	if err != nil || !withdrawn {
		return false, err
	}
	identity, err := effects.ChannelLogoutOperationID(op.TeardownID)
	if err != nil {
		return false, err
	}
	var present int
	err = tx.QueryRowContext(ctx, d.bind(`SELECT 1 FROM runtime_external_effect_operations WHERE operation_id=?`), identity).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return false, err
}
