package channelonboarding

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

type DeploymentSessionOwner interface {
	VerifyRegistration(context.Context, Operation) (bool, error)
	LogoutRegistration(context.Context, Operation, Operation) error
	CloseRegistration(context.Context, Operation) error
}

func (s *Service) LogoutDeployment(ctx context.Context, id string, revision int64) (Result, error) {
	if s == nil || !s.deployment {
		return Result{}, ErrInvalidRequest
	}
	unlock, err := s.lockDrive(ctx, id)
	if err != nil {
		return Result{}, err
	}
	defer unlock()
	op, err := s.store.GetChannelOnboarding(ctx, id)
	if err != nil || !op.DeploymentRegistration || op.Revision != revision {
		return Result{}, errors.Join(ErrRevisionConflict, err)
	}
	candidate, err := s.currentCandidate(op)
	if err != nil {
		return Result{}, err
	}
	if op.Phase == PhaseRetired {
		if !op.LogoutCompleted {
			return Result{}, fmt.Errorf("%w: logout outcome is unconfirmed; no unlink was repeated", ErrConflict)
		}
		return s.result(ctx, op, &candidate)
	}
	principal, err := s.identities.Principal()
	if err != nil || principal.ID != op.PrincipalID {
		return Result{}, errors.Join(ErrConflict, err)
	}
	session, ok := s.sessions.(DeploymentSessionOwner)
	if !ok {
		return Result{}, ErrInvalidRequest
	}
	if op.Posture == ActivationSessionConnection {
		if err := s.sessions.ResumeSession(ctx, op, candidate); err != nil {
			return Result{}, err
		}
		if op.SessionAccount.Validate() == nil {
			verified, err := session.VerifyRegistration(ctx, op)
			if err != nil || !verified {
				return Result{}, errors.Join(ErrConflict, err)
			}
		} else {
			paired, err := s.sessions.ReadSessionPairing(ctx, op, principal)
			if err != nil || !paired.Paired || !paired.Connected || paired.AccountPresentation != op.AccountReference {
				return Result{}, errors.Join(ErrConflict, err)
			}
		}
	}
	if err := s.retireDeploymentIdentity(ctx, op); err != nil {
		return Result{}, err
	}
	retired, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: id, ExpectedRevision: revision,
		Phase: PhaseRetired, FailureCode: "account_logout_pending", Now: s.now().UTC()})
	if err != nil {
		return Result{}, err
	}
	if err := session.LogoutRegistration(ctx, op, retired); err != nil {
		return Result{}, err
	}
	if err := s.credentials.ReleaseOperation(ctx, op); err != nil {
		return Result{}, err
	}
	retired, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: id, ExpectedRevision: retired.Revision,
		Phase: PhaseRetired, CompleteDeploymentLogout: true, Now: s.now().UTC()})
	if err != nil {
		return Result{}, err
	}
	return s.result(ctx, retired, &candidate)
}

func (s *Service) retireDeploymentIdentity(ctx context.Context, op Operation) error {
	binding, err := s.identities.RetainedCurrentBinding(ctx, op.Interface)
	if err != nil && !errors.Is(err, operatorchannel.ErrNotFound) && !errors.Is(err, operatorchannel.ErrBindingUnavailable) {
		return err
	}
	if err == nil && binding.OperationID == op.IdentityOperationID && binding.Status == operatorchannel.BindingCurrent {
		identities, ok := s.identities.(DestructiveIdentityLifecycle)
		if !ok {
			return ErrInvalidRequest
		}
		key, hash := operatorchannel.RequestIdentity("deployment.channel.logout", op.PrincipalID, op.OperationID, op.RequestHash)
		if _, _, err := identities.Unbind(ctx, op.Interface.Selector, binding.Revision, key, hash, s.now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ReverifyDeployment(ctx context.Context, id string) (Result, error) {
	if s == nil || !s.deployment {
		return Result{}, ErrInvalidRequest
	}
	unlock, err := s.lockDrive(ctx, id)
	if err != nil {
		return Result{}, err
	}
	defer unlock()
	op, err := s.store.GetChannelOnboarding(ctx, id)
	if err != nil || !op.DeploymentRegistration || !op.AccountConfirmed || op.Phase != PhaseSucceeded {
		return Result{}, errors.Join(ErrInvalidRequest, err)
	}
	candidate, err := s.currentCandidate(op)
	if err != nil {
		return Result{}, err
	}
	if err := s.sessions.ResumeSession(ctx, op, candidate); err != nil && !errors.Is(err, ErrSessionPending) {
		return Result{}, err
	}
	session, ok := s.sessions.(DeploymentSessionOwner)
	if !ok {
		return Result{}, ErrInvalidRequest
	}
	verified, err := session.VerifyRegistration(ctx, op)
	if err != nil {
		return Result{}, err
	}
	if !verified {
		if err := s.retireDeploymentIdentity(ctx, op); err != nil {
			return Result{}, err
		}
		retired, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: id, ExpectedRevision: op.Revision,
			Phase: PhaseRetired, FailureCode: "native_account_revoked", Now: s.now().UTC()})
		if err != nil {
			return Result{}, err
		}
		if err := session.CloseRegistration(ctx, op); err != nil {
			return Result{}, err
		}
		return s.result(ctx, retired, &candidate)
	}
	if s.operatorAccount != nil && s.operatorAccount.Provider == op.Provider && s.operatorAccount.Account == op.AccountReference && op.IdentityOperationID == "" {
		op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision,
			BeginOperatorEnrollment: true, Phase: PhaseAwaitingExternalIdentity, Now: s.now().UTC()})
		if err != nil {
			return Result{}, err
		}
		return s.driveLocked(ctx, op, candidate, "", false)
	}
	return s.result(ctx, op, &candidate)
}

// DeploymentAccounts never consults flow activations or another scope's table.
func (s *Service) DeploymentAccounts(ctx context.Context, provider string) ([]Result, error) {
	if s == nil || !s.deployment {
		return nil, ErrInvalidRequest
	}
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	accounts := make([]Result, 0, len(operations))
	for _, op := range operations {
		if op.Provider != provider || !op.AccountConfirmed || op.Phase == PhaseRetired {
			continue
		}
		candidate, err := s.currentCandidate(op)
		if err != nil {
			return nil, err
		}
		view, err := s.result(ctx, op, &candidate)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, view)
	}
	return accounts, nil
}

// Only operation persistence changes scope. Runtime activation/teardown ports
// remain the original owner and are never traversed by deployment completion.
type deploymentStore struct {
	Store
	operations OperationStore
}

func (s deploymentStore) ReserveChannelOnboarding(ctx context.Context, req StartRequest) (Operation, error) {
	return s.operations.ReserveChannelOnboarding(ctx, req)
}
func (s deploymentStore) GetChannelOnboarding(ctx context.Context, id string) (Operation, error) {
	return s.operations.GetChannelOnboarding(ctx, id)
}
func (s deploymentStore) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	return s.operations.ListChannelOnboardingOperations(ctx)
}
func (s deploymentStore) AdvanceChannelOnboarding(ctx context.Context, req AdvanceRequest) (Operation, error) {
	return s.operations.AdvanceChannelOnboarding(ctx, req)
}
func (s deploymentStore) SetChannelClientLocale(ctx context.Context, req SetClientLocaleRequest) (Operation, error) {
	return s.operations.SetChannelClientLocale(ctx, req)
}
