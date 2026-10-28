package channelonboarding

import (
	"context"
	"fmt"
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

func (s *Service) DeploymentAccountStatus(ctx context.Context, ref AccountReference) (AccountStatus, error) {
	if !s.deployment || ref.Validate() != nil {
		return AccountStatus{}, ErrInvalidRequest
	}
	status := AccountStatus{Provider: ref.Provider, Account: ref.Account, State: "stopped", Health: "disconnected", Holders: []AccountHolder{}}
	op, err := RegisteredAccount(ctx, s.store, ref)
	if err == ErrNotFound {
		status.Reason = "not_linked"
		return status, nil
	}
	if err != nil {
		return AccountStatus{}, err
	}
	result, err := s.Get(ctx, op.OperationID)
	if err != nil {
		return AccountStatus{}, err
	}
	status.Linked = true
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
