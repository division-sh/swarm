package channelonboarding

import (
	"context"
	"database/sql"
	"errors"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

func (s *PostgresOwner) ResetChannelOnboardingPendingIdentity(ctx context.Context, req domain.PendingResetRequest) (domain.Operation, error) {
	return resetPendingIdentity(ctx, postgresRunner{s}, req)
}

func (s *SQLiteOwner) ResetChannelOnboardingPendingIdentity(ctx context.Context, req domain.PendingResetRequest) (domain.Operation, error) {
	return resetPendingIdentity(ctx, sqliteRunner{s}, req)
}

func resetPendingIdentity(ctx context.Context, r runner, req domain.PendingResetRequest) (domain.Operation, error) {
	if err := r.require(); err != nil {
		return domain.Operation{}, err
	}
	if req.OperationID == "" || req.ExpectedRevision < 1 || req.Now.IsZero() {
		return domain.Operation{}, domain.ErrInvalidRequest
	}
	var out domain.Operation
	err := r.mutate(ctx, "reset pending channel identity", func(ctx context.Context, tx *sql.Tx) error {
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
		if op.Phase != domain.PhaseAwaitingExternalIdentity && op.Phase != domain.PhaseAwaitingOperatorConfirmation {
			return domain.ErrConflict
		}
		evidence := make([]credentials.ValueEvidence, len(op.CredentialAdmissions))
		for i, admission := range op.CredentialAdmissions {
			evidence[i] = credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}
		}
		if err := identityowner.RequireStaleOnboardingChild(ctx, tx, r.dialect() == dialectPostgres, identityowner.StaleOnboardingChildRequest{
			ParentID: op.OperationID, PrincipalID: op.PrincipalID, Interface: op.Interface,
			ChildID: op.IdentityOperationID, RetainedBindingRevision: op.BindingRevision, AdmittedCredentials: evidence,
		}); err != nil {
			if errors.Is(err, operatorchannel.ErrRevisionConflict) {
				return errors.Join(domain.ErrRevisionConflict, err)
			}
			return err
		}
		if req.Commit {
			op.Phase = domain.PhasePreparing
			op.CredentialAdmissions = nil
			op.IdentityOperationID = ""
			// A stale pending child did not consume an inherited reconnect obligation.
			op.Revision++
			op.UpdatedAt = canonicalTime(req.Now)
			if err := updateOperation(ctx, tx, r.dialect(), op); err != nil {
				return err
			}
		}
		out = op
		return nil
	})
	return out, err
}
