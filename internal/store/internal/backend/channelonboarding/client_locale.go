package channelonboarding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

func (s *PostgresOwner) SetChannelClientLocale(ctx context.Context, req domain.SetClientLocaleRequest) (domain.Operation, error) {
	return setClientLocale(ctx, postgresRunner{s}, req)
}

func (s *SQLiteOwner) SetChannelClientLocale(ctx context.Context, req domain.SetClientLocaleRequest) (domain.Operation, error) {
	return setClientLocale(ctx, sqliteRunner{s}, req)
}

func requireCurrentClientLocaleOperationTx(txctx context.Context, tx *sql.Tx, d dialect, op domain.Operation) error {
	if op.Phase == domain.PhaseFailed || op.Phase == domain.PhaseRetired {
		return domain.ErrConflict
	}
	if op.Phase == domain.PhaseSucceeded {
		activation, found, err := loadActivationBySlot(txctx, tx, d, op.SlotKey, true)
		if err != nil {
			return err
		}
		if !found || activation.OperationID != op.OperationID || activation.PrincipalID != op.PrincipalID {
			return domain.ErrConflict
		}
	}
	return nil
}

func setClientLocale(ctx context.Context, r runner, req domain.SetClientLocaleRequest) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	if req.OperationID == "" || req.PrincipalID == "" || req.ExpectedRevision < 1 || req.Language == "" || req.Now.IsZero() {
		return domain.Operation{}, fmt.Errorf("%w: exact principal, operation, locale revision, language and time are required", domain.ErrInvalidRequest)
	}
	var op domain.Operation
	var changed render.ReconcileDemand
	acknowledged, err := r.mutateOutcome(ctx, "set channel client locale", func(txctx context.Context, tx *sql.Tx) error {
		changed = 0
		var found bool
		var err error
		op, found, err = loadOperation(txctx, tx, r.dialect(), req.OperationID, true)
		if err != nil {
			return err
		}
		if !found || op.PrincipalID != req.PrincipalID {
			return domain.ErrNotFound
		}
		if err := requireCurrentClientLocaleOperationTx(txctx, tx, r.dialect(), op); err != nil {
			return err
		}
		if op.ClientLanguage == req.Language && (op.ClientLocaleRevision == req.ExpectedRevision || op.ClientLocaleRevision == req.ExpectedRevision+1) {
			return nil
		}
		if op.ClientLocaleRevision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		result, err := tx.ExecContext(txctx, r.dialect().bind(`UPDATE channel_onboarding_operations
			SET client_language=?, client_locale_revision=client_locale_revision+1, updated_at=?
			WHERE operation_id=? AND client_locale_revision=?`), req.Language, req.Now, op.OperationID, req.ExpectedRevision)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return domain.ErrRevisionConflict
		}
		op.ClientLanguage, op.ClientLocaleRevision, op.UpdatedAt = req.Language, op.ClientLocaleRevision+1, req.Now
		changed = render.ReconcileOrdinary | render.ReconcileNative
		return nil
	})
	return op, errors.Join(err, r.publishChannelChange(acknowledged, changed))
}
