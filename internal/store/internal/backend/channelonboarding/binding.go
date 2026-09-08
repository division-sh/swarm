package channelonboarding

import (
	"context"
	"database/sql"
	"errors"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

func (s *PostgresOwner) ReconcileChannelOnboardingBinding(ctx context.Context, req domain.ReconcileBindingRequest) (domain.Operation, error) {
	return reconcileBinding(ctx, postgresRunner{s}, req)
}

func (s *SQLiteOwner) ReconcileChannelOnboardingBinding(ctx context.Context, req domain.ReconcileBindingRequest) (domain.Operation, error) {
	return reconcileBinding(ctx, sqliteRunner{s}, req)
}

func reconcileBinding(ctx context.Context, r runner, req domain.ReconcileBindingRequest) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	if req.OperationID == "" || req.ExpectedRevision < 1 || req.ExpectedBindingRevision < 1 || req.Now.IsZero() {
		return domain.Operation{}, domain.ErrInvalidRequest
	}
	var out domain.Operation
	err := r.mutate(ctx, "reconcile channel onboarding binding", func(ctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(ctx, tx, r.dialect(), req.OperationID, true)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.Revision != req.ExpectedRevision {
			return domain.ErrRevisionConflict
		}
		if op.Phase != domain.PhaseAwaitingOperatorConfirmation && op.Phase != domain.PhasePublishingActivation {
			return domain.ErrConflict
		}
		if op.IdentityOperationID == "" && (op.BindingRevision == 0 || op.Verb != domain.VerbReconnect) {
			return domain.ErrRevisionConflict
		}
		if err := identityowner.RequireOnboardingBinding(ctx, tx, r.dialect() == dialectPostgres, identityowner.OnboardingBindingRequest{
			ParentID: op.OperationID, PrincipalID: op.PrincipalID, Interface: op.Interface,
			ChildID: op.IdentityOperationID, BindingRevision: req.ExpectedBindingRevision,
			ParentBindingRevision: op.BindingRevision,
		}); err != nil {
			if errors.Is(err, operatorchannel.ErrRevisionConflict) {
				return errors.Join(domain.ErrRevisionConflict, err)
			}
			return err
		}
		if op.BindingRevision == req.ExpectedBindingRevision && !req.ResetCredentials {
			out = op
			return nil
		}
		op.BindingRevision = req.ExpectedBindingRevision
		if req.ResetCredentials {
			op.Phase = domain.PhasePreparing
			op.CredentialAdmissions = nil
			op.IdentityOperationID = ""
		}
		op.Revision++
		op.UpdatedAt = canonicalTime(req.Now)
		if err := updateOperation(ctx, tx, r.dialect(), op); err != nil {
			return err
		}
		out = op
		return nil
	})
	return out, err
}
