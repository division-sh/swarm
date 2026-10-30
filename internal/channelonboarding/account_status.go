package channelonboarding

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

// AccountHolder is a current runtime observation, not persisted registration or
// permission to execute. The runtime's source/run owners produce these rows.
type AccountHolder struct {
	BundleHash   string `json:"bundle_hash"`
	Flow         string `json:"flow"`
	RunID        string `json:"run_id"`
	FlowInstance string `json:"flow_instance"`
}

type AccountStatus struct {
	Unclaimed *int64          `json:"account_unclaimed,omitempty"`
	Provider  string          `json:"provider"`
	Account   string          `json:"account"`
	State     string          `json:"state"`
	Reason    string          `json:"reason,omitempty"`
	Linked    bool            `json:"linked"`
	Health    string          `json:"health"`
	Holders   []AccountHolder `json:"holders"`
}

// DeploymentOperatorBinding observes the exact confirmed operator for one
// registered account, without granting runtime enrollment or card authority.
func (s *Service) DeploymentOperatorBinding(ctx context.Context, ref AccountReference) (Result, error) {
	if !s.deployment || ref != s.operatorAccount {
		return Result{}, ErrNotFound
	}
	op, err := RegisteredAccount(ctx, s.store, ref)
	if err != nil {
		return Result{}, err
	}
	if op.IdentityOperationID == "" {
		return Result{}, ErrNotFound
	}
	identity, err := s.identities.GetOperation(ctx, op.IdentityOperationID)
	if err != nil {
		return Result{}, err
	}
	if identity.State != operatorchannel.StateBound {
		return Result{}, ErrNotFound
	}
	candidate, err := s.currentCandidate(op)
	if err != nil {
		return Result{}, err
	}
	expected, err := providerIdentityAuthority(op, candidate)
	if err != nil {
		return Result{}, err
	}
	binding, proofCurrent, err := s.identities.CurrentBindingReadiness(ctx, op.Interface)
	if errors.Is(err, operatorchannel.ErrNotFound) || errors.Is(err, operatorchannel.ErrCredentialStale) || err == nil && !proofCurrent {
		return Result{}, ErrNotFound
	}
	if err != nil || binding.OperationID != identity.OperationID || binding.Revision != op.BindingRevision ||
		identity.OnboardingOperationID != op.OperationID || binding.ProviderAuthority != identity.ProviderAuthority || !binding.ProviderAuthority.SameProvenance(expected) {
		return Result{}, errors.Join(ErrRevisionConflict, err)
	}
	return Result{Operation: op, Candidate: &candidate, Binding: &binding}, nil
}

// RegisteredAccount selects only confirmed deployment evidence. A pending
// operator ceremony does not withdraw an already approved customer account.
func RegisteredAccount(ctx context.Context, operations OperationStore, ref AccountReference) (Operation, error) {
	if operations == nil || ref.Validate() != nil {
		return Operation{}, ErrInvalidRequest
	}
	rows, err := operations.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return Operation{}, err
	}
	var selected Operation
	for _, op := range rows {
		if !op.DeploymentRegistration || op.Provider != ref.Provider || op.AccountReference != ref.Account ||
			!op.AccountConfirmed || op.LogoutCompleted || op.Phase == PhaseRetired || op.Phase == PhaseFailed {
			continue
		}
		if op.ValidateDeploymentRegistration() != nil || op.ValidateSessionAccount() != nil ||
			(op.Phase != PhaseSucceeded && op.Phase != PhaseAwaitingExternalIdentity && op.Phase != PhaseAwaitingOperatorConfirmation) {
			return Operation{}, fmt.Errorf("%w: account registration is not usable", ErrConflict)
		}
		if selected.OperationID != "" {
			return Operation{}, fmt.Errorf("%w: account has competing registrations", ErrConflict)
		}
		selected = op
	}
	if selected.OperationID == "" {
		return Operation{}, ErrNotFound
	}
	return selected, nil
}

// RegisteredAccountStatus observes durable link state only. The retained
// transport owner, when present, separately supplies live connection health.
func RegisteredAccountStatus(ctx context.Context, operations OperationStore, ref AccountReference) (AccountStatus, Operation, error) {
	if ref.Validate() != nil {
		return AccountStatus{}, Operation{}, ErrInvalidRequest
	}
	status := AccountStatus{Provider: ref.Provider, Account: ref.Account, State: "stopped", Health: "disconnected", Holders: []AccountHolder{}}
	op, err := RegisteredAccount(ctx, operations, ref)
	if err == ErrNotFound {
		status.Reason = "not_linked"
		return status, Operation{}, nil
	}
	if err != nil {
		return AccountStatus{}, Operation{}, err
	}
	status.Linked = true
	return status, op, nil
}

func (s *Service) DeploymentAccountStatus(ctx context.Context, ref AccountReference) (AccountStatus, error) {
	if !s.deployment {
		return AccountStatus{}, ErrInvalidRequest
	}
	status, op, err := RegisteredAccountStatus(ctx, s.store, ref)
	if err != nil || op.OperationID == "" {
		return status, err
	}
	result, err := s.Get(ctx, op.OperationID)
	if err != nil {
		return AccountStatus{}, err
	}
	if result.Connection != nil {
		status.Unclaimed = result.Connection.Unclaimed
		status.Linked = result.Connection.Linked
		status.Health, status.Reason = result.Connection.State, result.Connection.Reason
		if result.Connection.State == "connected" {
			status.State = "live"
		} else if result.Connection.State == "disconnected" && result.Connection.Reason != "" {
			status.State = "refused"
		}
	}
	return status, nil
}
