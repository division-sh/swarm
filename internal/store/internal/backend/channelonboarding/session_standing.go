package channelonboarding

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

func (s *PostgresOwner) SessionStandingBindingCurrent(ctx context.Context, expected domain.Operation) (bool, error) {
	return sessionStandingBindingCurrent(ctx, postgresRunner{s}, expected)
}

func (s *SQLiteOwner) SessionStandingBindingCurrent(ctx context.Context, expected domain.Operation) (bool, error) {
	return sessionStandingBindingCurrent(ctx, sqliteRunner{s}, expected)
}

// Standing retention consumes confirmed responsibility, not current socket
// health. This read grants no native input or output execution admission.
func sessionStandingBindingCurrent(ctx context.Context, r runner, expected domain.Operation) (bool, error) {
	if err := r.require(); err != nil {
		return false, err
	}
	if expected.Posture != domain.ActivationSessionConnection || expected.OperationID == "" ||
		expected.Revision < 1 || expected.BindingRevision < 1 || expected.ValidateSessionAccount() != nil {
		return false, nil
	}
	var current bool
	err := r.mutate(ctx, "observe session standing binding", func(ctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(ctx, tx, r.dialect(), expected.OperationID, true)
		if err != nil || !found {
			return err
		}
		if !sessionStandingOperationMatches(expected, op) {
			return nil
		}
		if err := channeldelivery.LockPrincipalTx(ctx, tx, op.PrincipalID, r.dialect() == dialectPostgres); err != nil {
			return err
		}
		binding, found, err := identityowner.LockBindingTx(ctx, tx, r.dialect() == dialectPostgres, op.Interface)
		if err != nil || !found {
			return err
		}
		if binding.Status != operatorchannel.BindingCurrent || binding.Revision != op.BindingRevision ||
			binding.PrincipalID != op.PrincipalID || binding.ProviderAuthority.Kind != operatorchannel.ProviderAuthoritySession ||
			binding.ProviderAuthority.Session != op.SessionAccount {
			return nil
		}
		if err := identityowner.RequireOnboardingBinding(ctx, tx, r.dialect() == dialectPostgres, identityowner.OnboardingBindingRequest{
			ParentID: op.OperationID, PrincipalID: op.PrincipalID, Interface: op.Interface,
			ChildID: op.IdentityOperationID, BindingRevision: op.BindingRevision, ParentBindingRevision: op.BindingRevision,
		}); err != nil {
			if errors.Is(err, operatorchannel.ErrRevisionConflict) {
				return nil
			}
			return err
		}
		current, err = sessionStandingActivationCurrent(ctx, tx, r.dialect(), op, binding)
		return err
	})
	if err != nil {
		return false, err
	}
	return current, nil
}

func sessionStandingActivationCurrent(ctx context.Context, tx *sql.Tx, d dialect, op domain.Operation, binding operatorchannel.Binding) (bool, error) {
	if !op.Phase.RequiresExecutableTarget(op.Posture) || op.Phase == domain.PhasePublishingActivation {
		return true, nil
	}
	activation, found, err := loadActivationBySlot(ctx, tx, d, op.SlotKey, true)
	if err != nil || !found {
		return false, err
	}
	responsibility := domain.AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: activation.OperationRevision,
		ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
		Provider: op.Provider, Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
	return responsibility.MatchesBusinessBinding(op, activation, binding, op.BindingRevision), nil
}

func sessionStandingOperationMatches(expected, op domain.Operation) bool {
	if op.Phase == domain.PhaseFailed || op.Phase == domain.PhaseRetired ||
		op.Phase != domain.PhaseAwaitingOperatorConfirmation && !op.Phase.RequiresExecutableTarget(op.Posture) {
		return false
	}
	return op.Posture == domain.ActivationSessionConnection && op.ValidateSessionAccount() == nil && op.SlotKey == expected.SlotKey &&
		op.Revision >= expected.Revision && op.PrincipalID == expected.PrincipalID && op.Provider == expected.Provider &&
		op.Interface.Normalized() == expected.Interface.Normalized() && op.Coordinate.MatchesDurableIdentity(expected.Coordinate) &&
		op.TargetSelector == expected.TargetSelector && op.Ceremony == expected.Ceremony && op.BindingRevision == expected.BindingRevision &&
		op.IdentityOperationID == expected.IdentityOperationID && op.SessionConnectionID == expected.SessionConnectionID &&
		op.SessionAccount == expected.SessionAccount && slices.Equal(op.CredentialAdmissions, expected.CredentialAdmissions)
}
